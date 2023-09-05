#define DUCKDB_EXTENSION_MAIN

#include "ivm-extension.hpp"
#include "ivm_rewrite_rule.hpp"
#include "ivm_parser.hpp"

#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

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
	printf("View to be incrementally maintained: %s \n", view_name.c_str());

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	// obtain the bindings for view_name

	// obtain view defintion from catalog
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, view_catalog_name,
	                                           view_schema_name, view_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto view_entry = dynamic_cast<ViewCatalogEntry*>(view_catalog_entry.get());

	// generate column bindings for the view definition
	Parser parser;
	parser.ParseQuery(view_entry->query->ToString());
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());

	// create result set using column bindings returned by the planner
	auto result = make_uniq<DoIVMFunctionData>();
	for (int i=0;i<planner.names.size(); i++) {
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
	string view_catalog_name = StringValue::Get(parameters.values[0]);
	string view_schema_name = StringValue::Get(parameters.values[1]);
	string view_name = StringValue::Get(parameters.values[2]);

	string query_create_view_delta_table = "CREATE TABLE delta_"+view_name+" AS (SELECT * FROM "+view_name+" LIMIT 0);";
	string query_add_multiplicity_col = "ALTER TABLE delta_"+view_name+" ADD COLUMN _duckdb_ivm_multiplicity BOOL;";
	string ivm_query = "INSERT INTO delta_"+view_name+" SELECT * from DoIVM('"+view_catalog_name+"','"+view_schema_name+"','"+view_name+"');";
	string select_query = "SELECT * FROM delta_"+view_name+";";
	string query = query_create_view_delta_table + query_add_multiplicity_col + ivm_query + select_query;
	return query;
	return "create table delta_test as (select * from test limit 0); alter table delta_test add column _duckdb_ivm_multiplicity bool; insert into delta_test select * from DoIVM('memory', 's', 'test'); SELECT * FROM delta_test; ";
}

static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension(&con);

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();

	db_config.parser_extensions.push_back(ivm_parser);
	db_config.optimizer_extensions.push_back(ivm_rewrite_rule);

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       DoIVMFunction, DoIVMBind, DoIVMInit);
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

	auto upsert_delta_func = PragmaFunction::PragmaCall("ivm_upsert", UpsertDeltaQueries,
	                                                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	ExtensionUtil::RegisterFunction(instance, upsert_delta_func);
}

void IVMExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IVMExtension::Name() {
	return "ivm";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void ivm_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::IVMExtension>();
	// LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *ivm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
