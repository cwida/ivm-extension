#include "ivm_logical_plan_to_string.hpp"

namespace duckdb {

void LogicalPlanToString(unique_ptr<LogicalOperator> &plan, string &plan_string) {
	// "table index . column index" -> column name
	std::unordered_map<string, string> column_names;
	// new name -> old name
	// we need a vector here to preserve the original ordering of columns
	// example: select "a, b, c" should not become select "b, a, c"
	// using trees or hash tables would not preserve the order
	std::vector<std::pair<string, string>> column_aliases;
	LogicalPlanToString(plan, plan_string, column_names, column_aliases);
}

void LogicalPlanToString(unique_ptr<LogicalOperator> &plan, string &plan_string,
                         std::unordered_map<string, string> &column_names,
                         std::vector<std::pair<string, string>> &column_aliases) {

	// we reached a root node
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto node = dynamic_cast<LogicalGet *>(plan.get());
		auto table_name = node->GetTable().get()->name;
		auto table_index = node->GetTableIndex();
		auto scan_column_names = node->names;
		auto scan_column_indexes = node->column_ids; // maybe this should be projection_ids;
		// we don't need the table function here; we assume it is a simple scan
		string from_string = "from " + table_name + "\n";
		// now let's see if the scan has any filters
		std::vector<string> filters;
		for (auto &filter : node->table_filters.filters) {
			// extract the column id of this filter
			auto id = filter.first;
			auto column_name = scan_column_names[id];
			filters.emplace_back(filter.second->ToString(column_name));
		}

		// column bindings: 0.0, 0.1, 0.2
		// we are (probably) at the bottom
		if (plan->children.empty()) {
			// add the select statement
			string select_string = "select ";
			// now we sort out the column aliases
			for (auto &pair : column_aliases) {
				if (pair.first == pair.second) {
					select_string = select_string + pair.first + ", ";
				} else {
					select_string = select_string + pair.second + " as " + pair.first + ", ";
				}
			}
			// erase the last comma and space
			select_string.erase(select_string.size() - 2, 2);
			select_string += "\n";
			// now construct the WHERE clause
			string where_string;
			if (!filters.empty()) {
				where_string = "where ";
				for (auto &filter : filters) {
					// todo handle the "a > 1 and a is not null" case (do we need both?)
					where_string += filter + " and ";
				}
				// trim the last " and "
				where_string.erase(where_string.size() - 5, 5);
				where_string += "\n";
			}
			// plan string is the group by
			plan_string = select_string + from_string + where_string + plan_string;
			plan_string += ";";
			return;
		} else {
			// uh oh
		}
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto node = dynamic_cast<LogicalAggregate *>(plan.get());
		// we only support SUM and COUNT, so we only search for these
		auto bindings = node->GetColumnBindings(); // 2.0, 3.0, 3.1
		// now we have to extract the old table indexes, contained in groups and expressions
		std::vector<idx_t> table_indexes;
		std::vector<idx_t> column_indexes;
		std::vector<std::pair<string, string>> aggregate_aliases;
		// this is probably unnecessary but helps code readability
		// todo - do we need column and table indexes vectors (spoiler: probably not)?
		// we want all the old bindings to be in the same place
		// we iterate groups first, then expressions
		auto first = true;
		for (auto &group : node->groups) {
			auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
			table_indexes.push_back(column->binding.table_index);
			column_indexes.push_back(column->binding.column_index);
			aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
			                                   std::to_string(column->binding.column_index),
			                               column->alias);
			if (first) {
				plan_string += "group by " + column->alias;
				first = false;
			} else {
				plan_string += ", " + column->alias;
			}
		}
		for (size_t i = 0; i < node->expressions.size(); i++) {
			if (node->expressions[i]->type == ExpressionType::BOUND_AGGREGATE) { // should always be true
				auto bound_aggregate = dynamic_cast<BoundAggregateExpression *>(node->expressions[i].get());
				if (!bound_aggregate->children.empty()) {
					auto name = bound_aggregate->function.name;
					auto column = dynamic_cast<BoundColumnRefExpression *>(bound_aggregate->children[0].get());
					if (name == "sum") {
						table_indexes.push_back(column->binding.table_index);
						column_indexes.push_back(column->binding.column_index);
						aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
						                                   std::to_string(column->binding.column_index),
						                               "sum(" + column->alias + ")");
					} else if (name == "count") {
						table_indexes.push_back(column->binding.table_index);
						column_indexes.push_back(column->binding.column_index);
						aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
						                                   std::to_string(column->binding.column_index),
						                               "count(" + column->alias + ")");
					} else {
						// todo error handling
					}
				} else {
					// we are in the count_star() case
					// this does not get bindings - the columns in the scan might be less
					// put a temporary placeholder
					table_indexes.push_back(-1);
					column_indexes.push_back(-1);
					aggregate_aliases.emplace_back("-1.-1", "count(*)");
				}
			}
		}

		// now we should replace old bindings with new ones in the aggregate aliases
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto key = std::to_string(bindings[i].table_index) + "." + std::to_string(bindings[i].column_index);
			aggregate_aliases[i] = std::make_pair(key, aggregate_aliases[i].second);
		}

		// now we iterate bindings to see if any alias has been replaced
		for (auto &pair : aggregate_aliases) {
			auto it = column_names.find(pair.first);
			if (it != column_names.end()) {
				for (auto &alias_pair : column_aliases) {
					if (alias_pair.first == it->second) {
						alias_pair.second = pair.second;
						break;
					}
				}
			} else {
				// error
			}
		}

		plan_string += "\n";
		return LogicalPlanToString(plan->children[0], plan_string, column_names, column_aliases);
	}

	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());
		auto bindings = node->GetColumnBindings(); // not really needed now
		for (auto &expression : node->expressions) {
			// todo handle the cases of other expression types
			// note: expression_rewriter can be turned off if it makes it simple to implement the rest
			if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
				auto column = dynamic_cast<BoundColumnRefExpression *>(expression.get());
				auto column_index = column->binding.column_index;
				auto table_index = column->binding.table_index;
				auto column_name = column->alias;
				column_names[std::to_string(table_index) + "." + std::to_string(column_index)] = column_name;
				// 3.0, 3.1, 2.0
				column_aliases.emplace_back(column_name, "ivm_placeholder_internal");
			}
		}
		return LogicalPlanToString(plan->children[0], plan_string, column_names, column_aliases);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// basically the same logic as the logical get
		auto node = dynamic_cast<LogicalFilter *>(plan.get());
		plan_string = "where " + node->ParamsToString() + "\n" + plan_string;
		return LogicalPlanToString(plan->children[0], plan_string, column_names, column_aliases);
	}
	}
}

} // namespace duckdb
