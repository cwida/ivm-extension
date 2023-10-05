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

	return ParserExtensionParseResult(make_uniq_base<ParserExtensionParseData, IVMParseData>(move(p.statements[0])));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {

	// if we have a view definition statement, we create the delta tables
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	if (statement->type == StatementType::CREATE_STATEMENT) {

		// we extract the table name
		auto view_name = ExtractTableName(statement->query);
		// then we extract the query
		auto view_query = ExtractViewQuery(statement->query);

		// now we create the delta table based on the view definition
		// parsing the logical plan
		// todo db path??

		DuckDB db("../test_ivm.db");
		Connection con(db);

		con.BeginTransaction();
		auto table_names = con.GetTableNames(statement->query);

		Planner planner(context);

		planner.CreatePlan(statement->Copy());
		auto plan = move(planner.plan);

		std::cout << plan->ToString() << std::endl;

		std::stack<LogicalOperator *> node_stack;
		node_stack.push(plan.get());

		bool contains_aggregation;

		while (!node_stack.empty()) {
			auto current = node_stack.top();
			node_stack.pop();

			if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				contains_aggregation = true;
				break;
			}

			if (!current->children.empty()) {
				// push children onto the stack in reverse order to process them in a depth-first manner
				for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
					node_stack.push(it->get());
				}
			}
		}

		con.Rollback();

		con.BeginTransaction();
		// we have the table names; let's create the delta tables
		for (auto &table_name : table_names) {
			// todo schema?
			// todo also add the view name here (there can be multiple views?)
			// todo exception handling
			auto delta_table = "create table delta_" + table_name + " as select * from " + table_name + " limit 0";
			con.Query(delta_table);
			if (contains_aggregation) {
				con.Query("alter table delta_" + table_name + " add column _duckdb_ivm_multiplicity bool");
			}
		}
		con.Commit();

		// todo handle the case of replacing column names

		// now we also create the table
		con.BeginTransaction();
		auto table = "create table " + view_name + " as " + view_query;
		auto res = con.Query(table);
		con.Commit();

		// now we create the delta table for the result
		con.BeginTransaction();
		string delta_table = "create table delta_" + view_name + " as select * FROM " + view_name + " limit 0";
		string multiplicity_col = "alter table delta_" + view_name + " add column _duckdb_ivm_multiplicity bool";
		con.Query(delta_table);
		con.Query(multiplicity_col);

		con.Commit();

	} else if (statement->type == StatementType::SELECT_STATEMENT) {
		// genuinely don't know what to do here
	}

	/*
	printf("Plan function working: \n");
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);

	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());
	printf("fetching statement from parse data: %s \n", statement->ToString().c_str());

	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	printf("Trying to create plan by using plan cpp api: \n%s\n", planner.plan->ToString().c_str());

	Optimizer optimizer((Binder &)planner.binder, context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
	printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());

	printf("Level 2\n");
	auto l2 = optimized_plan->children[0].get();
	auto l2_aggop = dynamic_cast<LogicalAggregate *>(l2); */

	//	ParserExtensionPlanResult result;
	//	result.function = IVMFunction();
	//	result.parameters.push_back(Value::BIGINT(2));
	//	// TODO: what is this? how to obtain this?
	//	result.modified_databases = {};
	//	result.requires_valid_transaction = false;
	//	result.return_type = StatementReturnType::QUERY_RESULT;
	//	return result;
	return ParserExtensionPlanResult();
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In IVM bind function\n");
	return BoundStatement();
}
}; // namespace duckdb
