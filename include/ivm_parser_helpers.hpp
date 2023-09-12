#pragma once

#include "duckdb.hpp"

#include <stdio.h>

#ifndef DUCKDB_IVM_PARSER_HELPERS_H
#define DUCKDB_IVM_PARSER_HELPERS_H

namespace duckdb {

void OptimizedQueryPlan(ClientContext &context, string &catalog, string &schema, string &view, duckdb::unique_ptr<LogicalOperator> &plan);
string DeltaMaterializedViewQuery(ClientContext &context, string &catalog, string &schema, string &view);

} // namespace duckdb

#endif // DUCKDB_RDDA_VIEW_H
