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
	printf("In bind\n");

	string view_name = StringValue::Get(input.inputs[0]);
	printf("View to be incrementally maintained: %s \n", view_name.c_str());

	// obtain the bindings for view_name

	// obtain view defintion from catalog
	auto &catalog = Catalog::GetSystemCatalog(context);
	OnEntryNotFound if_not_found;
	QueryErrorContext error_context = QueryErrorContext();
	auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, "memory",
	                                           "main", view_name, if_not_found, error_context);
	// TODO: error if view itself does not exist
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
	printf("In table function\n");
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension(&con);

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();

	db_config.parser_extensions.push_back(ivm_parser);
	db_config.optimizer_extensions.push_back(ivm_rewrite_rule);

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR}, DoIVMFunction, DoIVMBind, DoIVMInit);
	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVM);
	ivm_func.name = "DoIVM";
	CreateTableFunctionInfo ivm_func_info(ivm_func);
	catalog.CreateTableFunction(*con.context, &ivm_func_info);
	con.Commit();
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
