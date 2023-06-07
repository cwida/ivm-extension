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


static void LoadInternal(DatabaseInstance &instance) {

	// add a parser extension
	// will probably break, let's see
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	auto ivm_parser = duckdb::IVMParserExtension();
	// rdda_parser.path = config["db_path"] + config["db_name"];

	db_config.parser_extensions.push_back(ivm_parser);

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
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *ivm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
