#pragma once

#include "duckdb.hpp"

#ifndef DUCKDB_IVM_PARSER_HELPERS_H
#define DUCKDB_IVM_PARSER_HELPERS_H

namespace duckdb {
std::string ExtractViewName(const string &sql);
std::string ExtractViewQuery(string &query);
std::string ParseViewTables(string &query);

} // namespace duckdb

#endif // DUCKDB_RDDA_VIEW_H
