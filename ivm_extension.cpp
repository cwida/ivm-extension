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
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	auto internal_view_name = "_duckdb_internal_" + view_name + "_ivm";
	auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, view_catalog_name, view_schema_name,
	                                           internal_view_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto view_entry = dynamic_cast<ViewCatalogEntry *>(view_catalog_entry.get());

	// generate column bindings for the view definition
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

	auto view_catalog_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name,
	                                           view_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name, "delta_" + view_name,
	                     OnEntryNotFound::THROW_EXCEPTION, error_context);
	// todo this will break if there is no aggregation
	auto index_delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::INDEX_ENTRY, view_catalog_name, view_schema_name,
	                     "delta_" + view_name + "_ivm_index", OnEntryNotFound::THROW_EXCEPTION, error_context);

	// we cannot use column references in ART indexes since their implementation is really messy
	// we need to use column indexes; maybe there is a more efficient way (unique constraints?)
	// but I cannot be bothered to think about this now

	// note: joins are hash joins by default, with group hash (try forcing index joins?)

	// let's construct the query step by step
	// first of all we need to understand the keys
	auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
	// compiler is too stupid to figure out "auto" here
	const ColumnList &delta_view_columns = delta_view_entry->GetColumns();

	auto column_names = delta_view_columns.GetColumnNames();
	auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
	auto key_ids = dynamic_cast<ART *>(index_catalog_entry->index.get())->column_ids;

	vector<string> keys;
	vector<string> aggregates;
	for (auto i = 0; i < key_ids.size(); i++) {
		keys.emplace_back(column_names[key_ids[i]]);
	}
	// todo - what the hell to do with the multiplicity column?
	// implementing the easiest way, will probably have to come back to this later
	// let's just pretend it does not exist and only support insertions

	// let's find all the columns that are not keys
	// create an unordered_set from the column names for efficient lookups
	unordered_set<std::string> keys_set(keys.begin(), keys.end());

	for (auto &column : column_names) {
		if (keys_set.find(column) == keys_set.end() && column != "_duckdb_ivm_multiplicity") {
			// we discard the multiplicity column
			aggregates.push_back(column);
		}
	}

	// this should be the end of the painful part (famous last words)

	// select is easy; both tables have the same columns
	// we assume that the delta view has the same columns as the view + the multiplicity column
	string select_string = "select ";
	// we only add the keys once
	for (auto &key : keys) {
		select_string = select_string + view_name + "." + key + ", ";
	}
	// now we sum the columns (todo - is it always sum here?)
	for (auto &column : aggregates) {
		select_string =
		    select_string + "sum(" + view_name + "." + column + " + delta_" + view_name + "." + column + "), ";
	}
	// remove the last comma
	select_string.erase(select_string.size() - 2, 2);
	select_string += "\n";

	// from is also easy, there's two tables
	string from_string = "from " + view_name + ", delta_" + view_name + "\n";
	// where clause (join), we need to join on keys
	string where_string = "where ";
	for (auto &key : keys) {
		where_string = where_string + view_name + "." + key + " = delta_" + view_name + "." + key + " and ";
	}
	// remove the last "and"
	where_string.erase(where_string.size() - 5, 5);
	where_string += "\n";

	// group by is also easy, we just group by the keys
	string group_by_string = "group by ";
	for (auto &key : keys) {
		group_by_string = group_by_string + view_name + "." + key + ", ";
	}
	// remove the last comma
	group_by_string.erase(group_by_string.size() - 2, 2);
	group_by_string += "\n";

	string query_string = select_string + from_string + where_string + group_by_string + ";";
	string upsert_query = "insert or replace into " + view_name + " " + query_string + "\n";

	/*
	select product_sales.product_name,
	    sum(product_sales.total_amount + delta_product_sales.total_amount),
	    sum(product_sales.total_orders + delta_product_sales.total_orders),
	    _duckdb_ivm_multiplicity
	from delta_product_sales, product_sales
	where delta_product_sales.product_name = product_sales.product_name
	group by product_sales.product_name, _duckdb_ivm_multiplicity;
	 */

	string ivm_query = "INSERT INTO delta_" + view_name + " SELECT * from DoIVM('" + view_catalog_name + "','" +
	                   view_schema_name + "','" + view_name + "');";
	string select_query = "SELECT * FROM delta_" + view_name + ";";
	// now we delete everything from the delta view
	string delete_view_query = "DELETE FROM delta_" + view_name + ";";
	string test = "SELECT * FROM " + view_name + ";";

	// todo - delete also from delta table and insert into original table

	// string query = ivm_query + select_query + upsert_query + delete_view_query + test;
	string query = ivm_query + upsert_query + delete_view_query + test;
	return query;
}

static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension(&con);

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
