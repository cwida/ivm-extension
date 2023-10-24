#define DUCKDB_EXTENSION_MAIN

#include "ivm_extension.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "ivm_parser.hpp"
#include "ivm_rewrite_rule.hpp"

#include <map>
#include <stdio.h>

namespace duckdb {

struct DoIVMData : public GlobalTableFunctionState {
	DoIVMData() : offset(0) {
	}
	idx_t offset;
	string view_name;
	// string view_string;
};

unique_ptr<GlobalTableFunctionState> DoIVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMData>();
	return std::move(result);
}

static unique_ptr<TableRef> DoIVM(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

static duckdb::unique_ptr<FunctionData> DoIVMBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// called when the pragma is executed
	// specifies the output format of the query (columns)
	// display the outputs (do not remove)
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
#ifdef DEBUG
	printf("View to be incrementally maintained: %s \n", view_name.c_str());
#endif

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	// obtain the bindings for view_name

	// obtain view definition from catalog
	// we need this to understand the column structure of the view
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	auto internal_view_name = "_duckdb_internal_" + view_name + "_ivm";
	auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, view_catalog_name, view_schema_name,
	                                           internal_view_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto view_entry = dynamic_cast<ViewCatalogEntry *>(view_catalog_entry.get());

	// generate column bindings for the view definition
	// we could try and avoid this but we need to know the column names
	// this is the plan of the view which will be fed to the optimizer rules
	Parser parser;
	parser.ParseQuery(view_entry->query->ToString());
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());

	// create result set using column bindings returned by the planner
	auto result = make_uniq<DoIVMFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
	}

	// add the multiplicity column
	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("_duckdb_ivm_multiplicity");

	return std::move(result);
}

static void DoIVMFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters) {
	// queries to run in order to materialize IVM upserts
	// these are executed whenever the pragma ivm_upsert is called
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();

	string view_catalog_name = StringValue::Get(parameters.values[0]);
	string view_schema_name = StringValue::Get(parameters.values[1]);
	string view_name = StringValue::Get(parameters.values[2]);

	auto delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name, "delta_" + view_name,
	                     OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto index_delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::INDEX_ENTRY, view_catalog_name, view_schema_name,
	                     "delta_" + view_name + "_ivm_index", OnEntryNotFound::RETURN_NULL, error_context);

	// extracting the query from the view definition
	Connection con(*context.db.get());
	auto view_query_entry = con.Query("select * from _duckdb_ivm_views where view_name = '" + view_name + "';");
	auto view_query_type_data = view_query_entry->GetValue(2, 0);
	IVMType view_query_type = static_cast<IVMType>(view_query_type_data.GetValue<int8_t>());

	// we cannot use column references in ART indexes since their implementation is really messy
	// we need to use column indexes; maybe there is a more efficient way (unique constraints?)
	// but I cannot be bothered to think about this now

	// note: joins are hash joins by default, with group hash (try forcing index joins?)

	// first of all we need to understand the keys
	auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
	// compiler is too stupid to figure out "auto" here
	const ColumnList &delta_view_columns = delta_view_entry->GetColumns();

	auto column_names = delta_view_columns.GetColumnNames();

	switch (view_query_type) {
	case IVMType::AGGREGATE_GROUP: {
		// let's construct the query step by step
		auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = dynamic_cast<ART *>(index_catalog_entry->index.get())->column_ids;

		vector<string> keys;
		vector<string> aggregates;
		for (auto i = 0; i < key_ids.size(); i++) {
			keys.emplace_back(column_names[key_ids[i]]);
		}

		// let's find all the columns that are not keys
		// create an unordered_set from the column names for efficient lookups
		unordered_set<std::string> keys_set(keys.begin(), keys.end());

		for (auto &column : column_names) {
			if (keys_set.find(column) == keys_set.end() && column != "_duckdb_ivm_multiplicity") {
				// we discard the multiplicity column
				aggregates.push_back(column);
			}
		}

		/*
		 example output query (using the example from the README):
		 insert or replace into product_sales
		 with ivm_cte AS (
		 select product_name, sum(case when _duckdb_ivm_multiplicity = false then -total_amount else total_amount end)
		 as total_amount, sum(case when _duckdb_ivm_multiplicity = false then -total_orders else total_orders end) as
		 total_orders from delta_product_sales group by product_name) select product_sales.product_name,
		 sum(product_sales.total_amount + delta_product_sales.total_amount),
		 sum(product_sales.total_orders + delta_product_sales.total_orders)
		 from ivm_cte as delta_product_sales
		 left join product_sales
		 on product_sales.product_name = delta_product_name
		 group by product_sales.product_name;
		 */

		// this should be the end of the painful part (famous last words)

		// we start building the CTE (assuming it's always named ivm_cte)
		string cte_string = "with ivm_cte AS (\n";
		string cte_select_string = "select ";
		for (auto &key : keys) {
			cte_select_string = cte_select_string + key + ", ";
		}
		// now we sum the columns
		for (auto &column : aggregates) {
			cte_select_string = cte_select_string + "sum(case when _duckdb_ivm_multiplicity = false then -" + column +
			                    " else " + column + " end) as " + column + ", ";
		}
		// remove the last comma
		cte_select_string.erase(cte_select_string.size() - 2, 2);
		cte_select_string += "\n";
		// from
		string cte_from_string = "from delta_" + view_name + "\n";
		// group by
		string cte_group_by_string = "group by ";
		for (auto &key : keys) {
			cte_group_by_string = cte_group_by_string + key + ", ";
		}
		// remove the last comma
		cte_group_by_string.erase(cte_group_by_string.size() - 2, 2);
		cte_group_by_string += "\n";

		cte_string = cte_string + cte_select_string + cte_from_string + cte_group_by_string + ")\n";

		// now build the external query
		// select is easy; both tables have the same columns
		// we assume that the delta view has the same columns as the view + the multiplicity column
		string select_string = "select ";
		// we only add the keys once
		for (auto &key : keys) {
			select_string = select_string + view_name + "." + key + ", ";
		}
		// now we sum the columns
		for (auto &column : aggregates) {
			select_string =
			    select_string + "sum(" + view_name + "." + column + " + delta_" + view_name + "." + column + "), ";
		}
		// remove the last comma
		select_string.erase(select_string.size() - 2, 2);
		select_string += "\n";

		// from is also easy, there's two tables
		// string from_string = "from " + view_name + ", delta_" + view_name + "\n";
		// string from_string = "from " + view_name + ", ivm_cte as delta_" + view_name + "\n";
		string from_string = "from ivm_cte as delta_" + view_name + "\n";
		// we need to left join the tables
		string join_string = "left join " + view_name + " on ";
		for (auto &key : keys) {
			join_string = join_string + view_name + "." + key + " = delta_" + view_name + "." + key + " and ";
		}
		// remove the last "and"
		join_string.erase(join_string.size() - 5, 5);
		join_string += "\n";

		// group by is also easy, we just group by the keys
		string group_by_string = "group by ";
		for (auto &key : keys) {
			group_by_string = group_by_string + view_name + "." + key + ", ";
		}
		// remove the last comma
		group_by_string.erase(group_by_string.size() - 2, 2);
		group_by_string += "\n";

		string external_query_string = select_string + from_string + join_string + group_by_string + ";\n";
		string query_string = cte_string + external_query_string;
		string upsert_query = "insert or replace into " + view_name + " " + query_string + "\n";
		// std::cout << "\n" << upsert_query << "\n";

		// DoIVM is a table function (root of the tree)
		string ivm_query = "INSERT INTO delta_" + view_name + " SELECT * from DoIVM('" + view_catalog_name + "','" +
		                   view_schema_name + "','" + view_name + "');";
		// string select_query = "SELECT * FROM delta_" + view_name + ";";
		// now we delete everything from the delta view
		string delete_view_query = "DELETE FROM delta_" + view_name + ";";
		string test = "SELECT * FROM " + view_name + ";";

		// todo - delete also from delta table and insert into original table

		// string query = ivm_query + select_query;
		// string query = ivm_query + select_query + upsert_query + delete_view_query + test;
		string query = ivm_query + upsert_query + delete_view_query + test;
		return query;
	}

	case IVMType::SIMPLE_FILTER:
	case IVMType::SIMPLE_PROJECTION: {
		// todo test with multiple insertions and deletions of the same row (updates)
		// we handle filters by performing a union
		// rewrite the query as union of the true multiplicity and difference of the false ones
		// we start with the difference
		string delete_query =
		    "delete from " + view_name + " where exists (select 1 from delta_" + view_name + " where ";
		// we can't just do SELECT * since the multiplicity column does not exist
		string select_columns;
		// we need to add the keys
		for (auto &column : column_names) {
			if (column != "_duckdb_ivm_multiplicity") { // we don't need the multiplicity column
				delete_query += view_name + "." + column + " = delta_" + view_name + "." + column + " and ";
				select_columns += column + ", ";
			}
		}
		// set multiplicity to false
		delete_query += "_duckdb_ivm_multiplicity = false);\n";
		// erase the last comma and space from the column list
		select_columns.erase(select_columns.size() - 2, 2);

		// now we insert as well
		string insert_query = "insert into " + view_name + " select " + select_columns + " from delta_" + view_name +
		                      " where _duckdb_ivm_multiplicity = true;\n";

		string ivm_query = "INSERT INTO delta_" + view_name + " SELECT * from DoIVM('" + view_catalog_name + "','" +
		                   view_schema_name + "','" + view_name + "');";
		string delete_view_query = "DELETE FROM delta_" + view_name + ";";
		// string select_query = "SELECT * FROM delta_" + view_name + ";";
		// return ivm_query + select_query;
		string test = "SELECT * FROM " + view_name + ";";
		return ivm_query + delete_query + insert_query + delete_view_query + test;
		// return delete_query + insert_query + delete_view_query + test;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		// this is the case of SELECT COUNT(*) / SUM(column) without aggregation columns
		// we need to rewrite the query as a sum/difference of the multiplicity column
		/*
		UPDATE product_sales
		SET total_amount = total_amount - (
		SELECT amount
		FROM delta_product_sales
		WHERE _duckdb_ivm_multiplicity = false
		) + (
		SELECT amount
		FROM delta_product_sales
		WHERE _duckdb_ivm_multiplicity = true);
		*/
		string ivm_query = "INSERT INTO delta_" + view_name + " SELECT * from DoIVM('" + view_catalog_name + "','" +
		                   view_schema_name + "','" + view_name + "');";
		string update_query = "update " + view_name + " set ";
		// there should be only one column here
		for (auto &column : column_names) {
			if (column != "_duckdb_ivm_multiplicity") { // we don't need the multiplicity column
				update_query += column + " = " + column + " - (select " + column + " from delta_" + view_name +
				                " where _duckdb_ivm_multiplicity = false) + (select " + column + " from delta_" +
				                view_name + " where _duckdb_ivm_multiplicity = true);";
			}
		}
		string select_query = "SELECT * FROM delta_" + view_name + ";";
		// return ivm_query + select_query;
		string delete_view_query = "DELETE FROM delta_" + view_name + ";";
		// string select_query = "SELECT * FROM delta_" + view_name + ";";
		// return ivm_query + select_query;
		string test = "SELECT * FROM " + view_name + ";";
		return ivm_query + update_query + delete_view_query + test;
	}
	}
}

static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension();

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();

	db_config.parser_extensions.push_back(ivm_parser);
	db_config.optimizer_extensions.push_back(ivm_rewrite_rule);

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DoIVMFunction,
	                       DoIVMBind, DoIVMInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVM);
	ivm_func.name = "DoIVM";
	ivm_func.named_parameters["view_catalog_name"];
	ivm_func.named_parameters["view_schema_name"];
	ivm_func.named_parameters["view_name"];
	CreateTableFunctionInfo ivm_func_info(ivm_func);
	catalog.CreateTableFunction(*con.context, &ivm_func_info);
	con.Commit();

	// this is called at the database startup and every time a query fails
	auto upsert_delta_func = PragmaFunction::PragmaCall(
	    "ivm_upsert", UpsertDeltaQueries, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	ExtensionUtil::RegisterFunction(instance, upsert_delta_func);
}

void IvmExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IvmExtension::Name() {
	return "ivm";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void ivm_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::IvmExtension>();
	// LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *ivm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
