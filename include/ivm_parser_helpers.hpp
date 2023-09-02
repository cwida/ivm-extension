#pragma once

#include "duckdb.hpp"

#ifndef DUCKDB_IVM_PARSER_HELPERS_H
#define DUCKDB_IVM_PARSER_HELPERS_H

namespace duckdb {

void PlanToSQL(duckdb::unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

#endif // DUCKDB_RDDA_VIEW_H
