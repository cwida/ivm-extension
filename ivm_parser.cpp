#include "ivm_parser.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find IVM statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	string parser_query;  // the SQL-compliant query to be fed to the parser

	DBConfig config;

	// reading config from settings table
	// not sure you need this
	// I use the cpp api because it's easier to generate a plan
	DuckDB db(IVMParserExtension::path, &config);
	Connection con(db);

	if (query_lower.substr(0, 6) == "create") {
		// this is a CREATE statement

		// check if this is a CREATE table or view
		if (query_lower.substr(0, 20) == "create materialized view") {

			auto result = con.Query(query_lower);
			if (!result->HasError()) {// adding the view to the system tables
				auto view_name = ExtractViewName(query_lower);
				auto view_query = ExtractViewQuery(query_lower);
				// extracting the FROM and JOIN clause tables (not sure if you need this?)
				auto view_tables = ParseViewTables(query_lower);

				con.BeginTransaction();  // wrapping in a transaction block so we can rollback
				auto result = con.Query(query_lower); // check for mistakes in the query
				con.Rollback(); // nothing happens

				// now you got everything, we can parse the query
				if (!result->HasError()) {

					Parser p;
					p.ParseQuery(view_query);
					printf("Parsed statement\n");

					Planner planner(*con.context);

					planner.CreatePlan(std::move(p.statements[0]));
					printf("Created plan\n");
					auto plan = std::move(planner.plan);

					Optimizer optimizer(*planner.binder, *con.context);
					plan = optimizer.Optimize(std::move(plan));

					// take it from here
				}

			}
		} else if (query_lower.substr(0, 20) == "create view") {
			// todo throw an exception here
		}
	} else {
		// whatever
	}
}
}; // namespace duckdb
