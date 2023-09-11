#include "ivm_parser.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find IVM statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	// string parser_query;  // the SQL-compliant query to be fed to the parser

	if (!StringUtil::Contains(query_lower, "create immv as")) {
		return ParserExtensionParseResult();
	}

	//TODO: Add support for proper table name using regex
	auto parser_query = StringUtil::Replace(query_lower, "create immv as", "");
	printf("parser query: %s \n", parser_query.c_str());

	Parser parser;
	parser.ParseQuery(parser_query);
	printf("Parsed statement\n");

	int l = parser.statements.size();
	for (int i=0;i<l;i++) {
		printf("S%d: %s\n", i+1, parser.statements[i]->ToString().c_str());
	}

	vector<unique_ptr<SQLStatement>> statements = std::move(parser.statements);

	auto s = statements[0].get();
	if (s->type == StatementType::SELECT_STATEMENT) {
		printf("SQL statement type select \n");
	}

	// TODO: what if there are more than one SQL statements?
	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(
	        std::move(statements[0])));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	printf("Plan function working: \n");
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);

	auto statement = dynamic_cast<SQLStatement*>(ivm_parse_data.statement.get());
	printf("fetching statement from parse data: %s \n", statement->ToString().c_str());

	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	printf("Trying to create plan by using plan cpp api: \n%s\n", planner.plan->ToString().c_str());

	Optimizer optimizer((Binder&)planner.binder, context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
	printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());

	printf("Level 2\n");
	auto l2 = optimized_plan->children[0].get();
	auto l2_aggop = dynamic_cast<LogicalAggregate*>(l2);


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

BoundStatement IVMBind(ClientContext &context, Binder &binder,
                         OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In ivm bind function\n");
	return BoundStatement();
}
}; // namespace duckdb
