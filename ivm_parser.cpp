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
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"

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

	string db_path = context.db->GetFileSystem().GetWorkingDirectory();
	string compiled_file_path = db_path + "/ivm_compiled_queries_" + view_name + ".sql";

	Connection con(*context.db.get());

	con.BeginTransaction();
	auto table_names = con.GetTableNames(statement->query);

	Planner planner(context);

	planner.CreatePlan(statement->Copy());
	auto plan = move(planner.plan);

	std::stack<LogicalOperator *> node_stack;
	node_stack.push(plan.get());

	bool found_filter = false;
	bool found_aggregation = false;
	bool found_projection = false;
	vector<string> aggregate_columns;

	while (!node_stack.empty()) {
		auto current = node_stack.top();
		node_stack.pop();

		if (current->type == LogicalOperatorType::LOGICAL_FILTER) {
			found_filter = true;
		}

		if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			found_aggregation = true;
			// find the aggregation column(s) in order to create an index
			auto node = dynamic_cast<LogicalAggregate *>(current);
			for (auto &group : node->groups) {
				auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
				aggregate_columns.emplace_back(column->alias);
			}
		}

		if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			found_projection = true;
		}

		if (!current->children.empty()) {
			// push children onto the stack in reverse order to process them in a depth-first manner
			for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
				node_stack.push(it->get());
			}
		}
	}

	IVMType ivm_type;

	if (found_aggregation && !aggregate_columns.empty()) {
		// select stuff FROM table [WHERE condition] GROUP BY stuff;
		ivm_type = IVMType::AGGREGATE_GROUP;
	} else if (found_aggregation && aggregate_columns.empty()) {
		// we are in cases such as SELECT COUNT(*) (without GROUP BY)
		ivm_type = IVMType::SIMPLE_AGGREGATE;
	} else if (found_filter && !found_aggregation) {
		// SELECT stuff FROM table WHERE condition;
		ivm_type = IVMType::SIMPLE_FILTER;
	} else if (found_projection && !found_aggregation && !found_filter) {
		// SELECT stuff FROM table;
		ivm_type = IVMType::SIMPLE_PROJECTION;
	} else {
		throw NotImplementedException("IVM does not support this query type yet");
	}

	con.Rollback();

	// we create the lookup tables for views -> materialized_view_name | sql_string | type | plan
	auto system_table = "create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string "
	                    "varchar, type tinyint, plan varchar);\n";
	con.BeginTransaction();
	con.Query(system_table);
	con.Commit();
	IVMWrite(compiled_file_path, true, system_table);

	// now we insert the details in the ivm view lookup table
	// firstly we need to serialize the plan to a string
	MemoryStream target;
	BinarySerializer serializer(target);
	serializer.Begin();
	plan->Serialize(serializer);
	serializer.End();
	auto data = target.GetData();
	idx_t len = target.GetPosition();
	string serialized_plan(data, data + len);

	// do we need insert or replace here? insert or ignore? am I just overthinking?
	// todo this does not work because of special characters
	// todo also write this (and system table ddl) to file
	// else we just store the query and re-plan it each time or store the json
	// auto x = escapeSingleQuotes(serialized_plan);
	// auto test = "create table if not exists test (plan varchar);\n";
	// con.Query(test);
	// auto res = con.Query("insert into test values('" + x + "');\n");

	auto test = "abc";
	auto ivm_table_insert = "insert or replace into _duckdb_ivm_views values ('" + view_name + "', '" +
	                        escapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) + ", '" + test + "');\n";
	con.BeginTransaction();
	auto res = con.Query(ivm_table_insert);
	con.Commit();
	IVMWrite(compiled_file_path, false, ivm_table_insert);

	// now we create the table (the view, internally stored as a table)
	auto table = "create table if not exists " + view_name + " as " + view_query + ";\n";
	con.BeginTransaction();
	con.Query(table);
	con.Commit();
	IVMWrite(compiled_file_path, false, table);

	// we have the table names; let's create the delta tables (to store insertions, deletions, updates)
	// the API does not support consecutive CREATE + ALTER instructions, so we rewrite it as one query
	// CREATE TABLE IF NOT EXISTS delta_table AS SELECT *, TRUE AS _duckdb_ivm_multiplicity FROM my_table LIMIT 0;
	for (const auto &table_name : table_names) {
		// todo schema?
		// todo also add the view name here (there can be multiple views?)
		// todo exception handling
		auto delta_table = "create table if not exists delta_" + table_name +
		                   " as select *, true as _duckdb_ivm_multiplicity from " + table_name + " limit 0;\n";
		con.BeginTransaction();
		con.Query(delta_table);
		con.Commit();
		IVMWrite(compiled_file_path, true, delta_table);
	}

	// todo handle the case of replacing column names

	// now we also create a view (for internal use, just to store the SQL query)
	// todo - remove this if I manage to make the lookup table work
	auto view = "create or replace view _duckdb_internal_" + view_name + "_ivm as " + view_query + ";\n";
	con.BeginTransaction();
	con.Query(view);
	con.Commit();
	IVMWrite(compiled_file_path, true, view);

	// now we create the delta table for the result (to store the IVM algorithm output)
	string delta_view = "create table if not exists delta_" + view_name +
	                    " as select *, true as _duckdb_ivm_multiplicity from " + view_name + " limit 0;\n";
	con.BeginTransaction();
	con.Query(delta_view);
	con.Commit();
	IVMWrite(compiled_file_path, true, delta_view);

	// lastly, we need to create an index on the aggregation keys on the view and delta result table
	// the index is not unique here since the same row can be inserted and deleted (multiple times)
	if (ivm_type == IVMType::AGGREGATE_GROUP) {
		string index_query_view = "create index " + view_name + "_ivm_index on " + view_name + "(";
		for (size_t i = 0; i < aggregate_columns.size(); i++) {
			index_query_view += aggregate_columns[i];
			if (i != aggregate_columns.size() - 1) {
				index_query_view += ", ";
			}
		}
		index_query_view += ");\n";
		con.BeginTransaction();
		con.Query(index_query_view);
		con.Commit();
		// writing to file
		IVMWrite(compiled_file_path, true, index_query_view);
	}

	string comment = "-- code to propagate operations to the base table goes here\n";
	comment += "-- assuming the changes to be in the delta tables\n";
	IVMWrite(compiled_file_path, true, comment);

	ParserExtensionPlanResult result;
	result.function = IVMFunction();
	result.parameters.push_back(true); // this could be true or false if we add exception handling
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
