#include "ivm_parser_helpers.hpp"

#include <regex>
#include <string>

namespace duckdb {

void PlanToSQL(duckdb::unique_ptr<LogicalOperator> &plan) {
	// reverse the actions in src/planner/binder/query_node/plan_select_node.cpp
	return;
}

} // namespace duckdb
