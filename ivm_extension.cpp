#define DUCKDB_EXTENSION_MAIN

#include "ivm-extension.hpp"

#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
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
#include <unistd.h>

namespace duckdb {

struct DoIVMData : public GlobalTableFunctionState {
	DoIVMData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> DoIVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMData>();
	return std::move(result);
}

static unique_ptr<TableRef> DoIVM(ClientContext &context, TableFunctionBindInput &input) {
	printf("In bind_replace!\n");
	return nullptr;
}

struct DoIVMFunctionData : public TableFunctionData {
	DoIVMFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string catalog = INVALID_CATALOG;
	string schema = DEFAULT_SCHEMA;
	string suffix;
	bool overwrite = false;
	uint32_t children = 1;
	int step = -1;
};

static duckdb::unique_ptr<FunctionData> DoIVMBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	printf("In bind\n");
	auto result = make_uniq<DoIVMFunctionData>();
	return_types.emplace_back(LogicalType::BOOLEAN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("Success");
	names.emplace_back("Nums");
	return std::move(result);
}

static void DoIVMFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	printf("In table function\n");
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	output.SetValue(0, 0, true);
	output.SetValue(1, 0, 10000);
	output.SetCardinality(1);
	data.offset = data.offset+1;
	return;
}

static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension(&con);

	db_config.parser_extensions.push_back(ivm_parser);

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
