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

#include <fstream>
#include <iostream>
#include <stack>

namespace duckdb {

void IVMParserExtension::IVMWrite(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	file << compiled_query << '\n';
	file.close();
}

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

	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	// we extract the table name
	auto view_name = ExtractTableName(statement->query);
	// then we extract the query
	auto view_query = ExtractViewQuery(statement->query);

	// now we create the delta table based on the view definition
	// parsing the logical plan
	// todo db path??

	string compiled_file_path = "../ivm_compiled_queries_" + view_name + ".sql";

	DuckDB db("../test_sales.db");
	Connection con(db);

	con.BeginTransaction();
	auto table_names = con.GetTableNames(statement->query);

	Planner planner(*con.context);

	planner.CreatePlan(statement->Copy());
	auto plan = move(planner.plan);

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
	auto table = "create table if not exists " + view_name + " as " + view_query + ";\n";
	auto res = con.Query(table);
	con.Commit();
	IVMWrite(compiled_file_path, false, table);

	con.BeginTransaction();
	// we have the table names; let's create the delta tables (to store insertions, deletions, updates)
	for (const auto &table_name : table_names) {
		// todo schema?
		// todo also add the view name here (there can be multiple views?)
		// todo exception handling
		auto delta_table =
		    "create table if not exists delta_" + table_name + " as select * from " + table_name + " limit 0;\n";
#ifdef DEBUG
		std::cout << delta_table << std::endl;
#endif
		con.Query(delta_table);
		IVMWrite(compiled_file_path, true, delta_table);
		auto mul_col_table = "alter table delta_" + table_name + " add column _duckdb_ivm_multiplicity bool;\n";
		IVMWrite(compiled_file_path, true, mul_col_table);
		con.Query(mul_col_table);
	}
	con.Commit();

	// todo handle the case of replacing column names

	// now we also create a view (for internal use, just to store the SQL query)
	con.BeginTransaction();
	auto view = "create or replace view _duckdb_internal_" + view_name + "_ivm as " + view_query + ";\n";
	con.Query(view);
	con.Commit();
	IVMWrite(compiled_file_path, true, view);

	// now we create the delta table for the result (to store the IVM algorithm output)
	con.BeginTransaction();
	string delta_view =
	    "create table if not exists delta_" + view_name + " as select * FROM " + view_name + " limit 0;\n";
	string mul_col_view = "alter table delta_" + view_name + " add column _duckdb_ivm_multiplicity bool;\n";
	con.Query(delta_view);
	con.Query(mul_col_view);

	con.Commit();
	IVMWrite(compiled_file_path, true, delta_view);
	IVMWrite(compiled_file_path, true, mul_col_view);

	// lastly, we need to create an index on the aggregation keys on the view and delta result table
	// todo - do we need PRAGMA force_index_join?
	// todo handle the case of no aggregation
	if (contains_aggregation) {
		string index_query_view = "create unique index " + view_name + "_ivm_index on " + view_name + "(";
		for (size_t i = 0; i < aggregate_columns.size(); i++) {
			index_query_view += aggregate_columns[i];
			if (i != aggregate_columns.size() - 1) {
				index_query_view += ", ";
			}
		}
		index_query_view += ");\n";
		con.BeginTransaction();
		auto r2 = con.Query(index_query_view);
		con.Commit();
		// writing to file
		IVMWrite(compiled_file_path, true, index_query_view);
	}

	string comment = "-- code to propagate operations to the base table goes here\n";
	comment += "-- assuming the changes to be in the delta tables\n";
	IVMWrite(compiled_file_path, true, comment);

	// todo: serialize this plan and store it somewhere?

	ParserExtensionPlanResult result;
	result.function = IVMFunction();
	result.parameters.push_back(true); // this could be true or false if we add exception handling
	// todo make this boolean
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In IVM BIND function\n");
	return BoundStatement();
}
}; // namespace duckdb
