
#include <regex>
#include <string>

namespace duckdb {

std::string ExtractViewName(const std::string &sql) {
	std::regex view_name_regex(R"(CREATE\s+MATERIALIZED\s+VIEW\s+(\w+)\s+\()");
	std::smatch match;
	if (std::regex_search(sql, match, view_name_regex)) {
		return match[1].str();
	}
	// Return an empty string if there's no match
	return "";
}

std::string ExtractViewQuery(std::string &query) {
	std::regex rgx_create_view(R"(create\s+materialized\s+view\s+\S+\s+as\s+(\S+\s+)+)");

	std::smatch match;
	std::string query_string;

	if (std::regex_search(query, match, rgx_create_view)) {
		return match[1].str();
	}

	return "";
}

std::string ParseViewTables(std::string &query) {
	// IN also works with trailing comma

	// Define a regex pattern to match the FROM clause
	std::regex from_clause("FROM\\s+([^\\s,]+)(?:\\s+(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN)\\s+([^\\s,]+))?");
	std::vector<std::string> tables;

	// Create a regex iterator to iterate over matches
	std::sregex_iterator it(query.begin(), query.end(), from_clause);
	std::sregex_iterator end;

	// Iterate over matches and extract the table names
	for (; it != end; ++it) {
		const std::smatch& match = *it;
		tables.push_back(match[1]);
		if (match[2].str().length() > 0) {
			tables.push_back(match[2]);
		}
	}

	// Concatenate the matches in a string
	std::string in;
	for (auto t = tables.begin(); t != tables.end(); ++t) {
		if (t != tables.begin()) {
			in += ", ";
		}
		in += *t;
	}

	return in;
}

} // namespace duckdb
