#include "include/ivm_parser.hpp"

#include "../server/include/rdda_table.hpp"
#include "../server/include/rdda_view.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include <iostream>
#include <stack>

namespace duckdb {

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find IVM statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	// the SQL-compliant query to be fed to the parser
	if (!StringUtil::Contains(query_lower, "create materialized view")) {
		return ParserExtensionParseResult();
	}

	// we see a materialized view - but we create a table under the hood
	// first we turn this instruction into a table ddl
	ReplaceMaterializedView(query_lower);

	Parser p;
	p.ParseQuery(query_lower);

	// if we have a view definition statement, we create the delta tables
	// auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	// auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());
	auto statement = p.statements[0].get();

	if (statement->type == StatementType::CREATE_STATEMENT) {

		// we extract the table name
		auto view_name = ExtractTableName(statement->query);
		// then we extract the query
		auto view_query = ExtractViewQuery(statement->query);

		// now we create the delta table based on the view definition
		// parsing the logical plan
		// todo db path??

		DuckDB db("../test_sales.db");
		Connection con(db);

		con.BeginTransaction();
		auto table_names = con.GetTableNames(statement->query);

		Planner planner(*con.context);

		planner.CreatePlan(statement->Copy());
		auto plan = move(planner.plan);

#ifdef DEBUG
		std::cout << plan->ToString() << std::endl;
#endif

		std::stack<LogicalOperator *> node_stack;
		node_stack.push(plan.get());

		bool contains_aggregation;
		vector<string> aggregate_columns;

		// todo rewrite this in a decent way
		while (!node_stack.empty()) {
			auto current = node_stack.top();
			node_stack.pop();

			if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				contains_aggregation = true;
				// find the aggregation column(s) in order to create an index
				auto node = dynamic_cast<LogicalAggregate *>(current);
				for (auto &group : node->groups) {
					auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
					aggregate_columns.emplace_back(column->alias);
				}
				break; // we assume one aggregate node (for now)
			}

			if (!current->children.empty()) {
				// push children onto the stack in reverse order to process them in a depth-first manner
				for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
					node_stack.push(it->get());
				}
			}
		}

		con.Rollback();

		// now we create the table (the view, internally stored as a table)
		con.BeginTransaction();
		auto table = "create table " + view_name + " as " + view_query;
		auto res = con.Query(table);
		con.Commit();

		con.BeginTransaction();
		// we have the table names; let's create the delta tables (to store insertions, deletions, updates)
		for (const auto &table_name : table_names) {
			// todo schema?
			// todo also add the view name here (there can be multiple views?)
			// todo exception handling
			auto delta_table = "create table delta_" + table_name + " as select * from " + table_name + " limit 0";
#ifdef DEBUG
			std::cout << delta_table << std::endl;
#endif
			con.Query(delta_table);
			if (contains_aggregation) {
				con.Query("alter table delta_" + table_name + " add column _duckdb_ivm_multiplicity bool");
			}
		}
		con.Commit();

		// todo handle the case of replacing column names

		// now we also create a view (for internal use, just to store the SQL query)
		con.BeginTransaction();
		auto view = "create view _duckdb_internal_" + view_name + "_ivm as " + view_query;
		con.Query(view);
		con.Commit();

		// now we create the delta table for the result (to store the IVM algorithm output)
		con.BeginTransaction();
		string delta_table = "create table delta_" + view_name + " as select * FROM " + view_name + " limit 0";
		string multiplicity_col = "alter table delta_" + view_name + " add column _duckdb_ivm_multiplicity bool";
		con.Query(delta_table);
		con.Query(multiplicity_col);

		con.Commit();

		// lastly, we need to create an index on the aggregation keys on the view and delta result table
		// todo - do we need PRAGMA force_index_join?
		// todo - apparently we can create non-unique indexes, will we need this?
		// todo handle the case of no aggregation
		if (contains_aggregation) {
			string index_query_delta_table =
			    "create unique index delta_" + view_name + "_ivm_index on delta_" + view_name + "(";
			string index_query_view = "create unique index " + view_name + "_ivm_index on " + view_name + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_delta_table += aggregate_columns[i];
				index_query_view += aggregate_columns[i];
				if (i != aggregate_columns.size() - 1) {
					index_query_delta_table += ", ";
					index_query_view += ", ";
				}
			}
			index_query_delta_table += ");";
			index_query_view += ");";
			con.BeginTransaction();
			auto r1 = con.Query(index_query_delta_table);
			auto r2 = con.Query(index_query_view);
			con.Commit();
		}
	}

	// return ParserExtensionParseResult(make_uniq_base<ParserExtensionParseData, IVMParseData>(move(p.statements[0])));
	// fixme
	return ParserExtensionParseResult("Success!");
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In IVM bind function\n");
	return BoundStatement();
}
}; // namespace duckdb
