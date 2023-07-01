#define DUCKDB_EXTENSION_MAIN

#include "ivm-extension.hpp"

#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/query_error_context.hpp"

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// TODO cleanup unused libraries
#include <arpa/inet.h>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace duckdb {

struct IVMData : public GlobalTableFunctionState {
	IVMData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> IVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<IVMData>();
	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> IVMQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static string PragmaIVMFunction(ClientContext &context, const FunctionParameters &parameters) {
	printf("Pragma demo success!");
	return "select * from hello";
}

static void IVMQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<IVMData &>(*data_p.global_state);
	idx_t tpch_queries = 1;
	if (data.offset >= tpch_queries) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < tpch_queries && chunk_count < STANDARD_VECTOR_SIZE) {
		auto query = "SELECT * from hello";
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
		// "query", PhysicalType::VARCHAR
		output.SetValue(1, chunk_count, Value(query));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static unique_ptr<TableRef> Hello(ClientContext &context, TableFunctionBindInput &input) {
	printf("Hello!");

	auto &catalog = Catalog::GetSystemCatalog(context);
	OnEntryNotFound if_not_found;
	QueryErrorContext error_context = QueryErrorContext();
	auto t = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, "memory",
	                          "main", "hello", if_not_found, error_context);
	printf("\nTable entry: %s %hhu %s\n", t.get()->name.c_str(), t.get()->type, t.get()->ToSQL().c_str());
	auto v = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, "memory",
	                                            "main", "test", if_not_found, error_context);
	auto view = dynamic_cast<ViewCatalogEntry*>(v.get());
	printf("View entry: %s %hhu %s\n", view->name.c_str(), view->type, view->ToSQL().c_str());
	printf("View base query: %s\n", view->query->ToString().c_str());

	string s = "SELECT * FROM hello";
	Parser parser;
	parser.ParseQuery(s);

	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());

	printf("\nPlan: %s\n", planner.plan->ToString().c_str());

	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->table_name = "bellow";

	unique_ptr<TableRef> from_clause = std::move(table_ref);

	auto b = planner.binder;

	Optimizer o((Binder&)b, context);
	auto p = o.Optimize(std::move(planner.plan));

	printf("\nOptimized plan: %s\n", p->ToString().c_str());

	auto table_ref2 = make_uniq<BaseTableRef>();
	table_ref2->table_name = "hello";
	unique_ptr<TableRef> from_clause2 = std::move(table_ref2);
//
//	select_node->from_table = std::move(from_clause);
//
//	auto subquery = make_uniq<SelectStatement>();
//	subquery->node = std::move(select_node);
//
//	auto result = make_uniq<SubqueryRef>(std::move(subquery), ref->alias);
//	return result;

	return from_clause2;
}

struct DBGenFunctionData : public TableFunctionData {
	DBGenFunctionData() {
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

static duckdb::unique_ptr<FunctionData> DbgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	printf("In DBgenBind");
	auto result = make_uniq<DBGenFunctionData>();
	return_types.emplace_back(LogicalType::BOOLEAN);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("Success");
	names.emplace_back("Nums");
	return std::move(result);
}

static void DbgenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	printf("In DBgenFunction");
	auto &data = dynamic_cast<IVMData &>(*data_p.global_state);
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

	// create the IVM pragma that allows us to run the ivm functions
	auto ivm_func = PragmaFunction::PragmaCall("DoIVM", PragmaIVMFunction, {LogicalType::VARCHAR});
	ExtensionUtil::RegisterFunction(instance, ivm_func);

	TableFunction ivm_func2("PerformIVM", {LogicalType::VARCHAR}, DbgenFunction, DbgenBind, IVMInit);
	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func2.bind_replace = reinterpret_cast<table_function_bind_replace_t>(Hello);
	ivm_func2.name = "PerformIVM";
	CreateTableFunctionInfo ivm_func2_info(ivm_func2);
	catalog.CreateTableFunction(*con.context, &ivm_func2_info);
	con.Commit();

//	TableFunction ivm_test_func("ivm_test_func", {LogicalType::VARCHAR}, IVMQueryFunction,
//	                            IVMQueryBind, IVMInit);
	// ExtensionUtil::RegisterFunction(instance, ivm_test_func);

	con.BeginTransaction();
	// auto &catalog = Catalog::GetSystemCatalog(*con.context);
	TableFunction ivm_test_func("ivm_test_func", {LogicalType::VARCHAR}, IVMQueryFunction,
	                            IVMQueryBind, IVMInit);
	ivm_test_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(Hello);
	ivm_test_func.name = "ivm_test_func";
	CreateTableFunctionInfo ivm_test_func_info(ivm_test_func);
	catalog.CreateTableFunction(*con.context, &ivm_test_func_info);
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
