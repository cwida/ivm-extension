#include "ivm_parser.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/tableref.hpp"

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
	if (statement->type == StatementType::SELECT_STATEMENT) {
		auto select_statement = dynamic_cast<SelectStatement*>(statement);
		auto select_node = dynamic_cast<SelectNode*>(select_statement->node.get());
		printf("Select node: ");
		for (int i = 0; i<select_node->select_list.size();i++) {
			printf(", %s %d %d %d %s", select_node->select_list[i]->ToString().c_str(),
			       	select_node->select_list[i]->IsAggregate(),
			       select_node->select_list[i]->HasParameter(),
			       select_node->select_list[i]->HasSubquery(),
			       ExpressionClassToString(select_node->select_list[i]->GetExpressionClass()).c_str());
		}


		auto from_table_function = dynamic_cast<TableRef*>(select_node->from_table.get());
		printf("\nFrom table func: %s \n", from_table_function->ToString().c_str());

		auto &catalog = Catalog::GetSystemCatalog(context);

	}

	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	printf("Trying to create plan by using plan cpp api: \n%s\n", planner.plan->ToString().c_str());

	printf("Node 0: %s", LogicalOperatorToString(planner.plan->type).c_str());
	auto t = planner.plan.get();

	printf("children: ");
	for (int i = 0; i<planner.plan->children.size();i++) {
		printf("%d: %s, ", i, planner.plan->children[i]->ToString().c_str());
		printf("\n types: %s %s", LogicalOperatorToString(planner.plan->children[i]->type).c_str(),
		       planner.plan->children[i]->ParamsToString().c_str());
	}
	printf("\n params to string: %s \n",planner.plan->ParamsToString().c_str());

	auto c = planner.plan->children[0]->children[0].get();
	printf("Node 2: %s %s %s\n", c->ToString().c_str(), LogicalOperatorToString(c->type).c_str(),
	       c->ParamsToString().c_str());

	planner.plan->children[0]->children[0]->AddChild(planner.plan->Copy(context));
	auto x = planner.plan->children[0]->children[0]->children[0].get();
	printf("Node added: %s %s %s\n", x->ToString().c_str(), LogicalOperatorToString(x->type).c_str(),
	       x->ParamsToString().c_str());

	ParserExtensionPlanResult result;
	result.function = IVMFunction();
	result.parameters.push_back(Value::BIGINT(2));
	// TODO: what is this? how to obtain this?
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
	return ParserExtensionPlanResult();
}

BoundStatement IVMBind(ClientContext &context, Binder &binder,
                         OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In ivm bind function\n");
	return BoundStatement();
}
}; // namespace duckdb
