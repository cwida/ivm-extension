#include "duckdb.hpp"
#include "ivm_parser_helpers.hpp"

#ifndef DUCKDB_IVM_PARSER_HPP
#define DUCKDB_IVM_PARSER_HPP

namespace duckdb {

class IVMParserExtension : public ParserExtension {
public:
	IVMParserExtension() {
		parse_function = IVMParseFunction;
		// plan_function = IVMPlanFunction;
		// uncomment this, you need this (ila)
	}

	static const string path; // added in my own implementation (ila) - not sure you need this; might break
	static ParserExtensionParseResult IVMParseFunction(ParserExtensionInfo *info, const string &query);
};
} // namespace duckdb

#endif // DUCKDB_IVM_PARSER_HPP
