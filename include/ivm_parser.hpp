#include "duckdb.hpp"
#include "ivm_parser_helpers.hpp"

#ifndef DUCKDB_IVM_PARSER_HPP
#define DUCKDB_IVM_PARSER_HPP

namespace duckdb {


struct IVMInfo : ParserExtensionInfo {
	unique_ptr<Connection> db_conn;
	explicit IVMInfo(unique_ptr<Connection> db_conn) : db_conn(std::move(db_conn)) {}
};

class IVMParserExtension : public ParserExtension {
public:
	explicit IVMParserExtension(Connection* con) {
		// unique_ptr<Connection> db_conn (con);
		parse_function = IVMParseFunction;
		plan_function = IVMPlanFunction;
		// parser_info = std::make_shared<IVMInfo>(std::move(db_conn));
	}

	static ParserExtensionParseResult IVMParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                 unique_ptr<ParserExtensionParseData> parse_data);

};


BoundStatement IVMBind(ClientContext &context, Binder &binder,
                       OperatorExtensionInfo *info, SQLStatement &statement);

struct IVMOperatorExtension : public OperatorExtension {
	IVMOperatorExtension() : OperatorExtension() { Bind = IVMBind; }

	std::string GetName() override { return "ivm"; }

	unique_ptr<LogicalExtensionOperator>
	Deserialize(LogicalDeserializationState &state,
	            FieldReader &reader) override {
		throw InternalException("prql operator should not be serialized");
	}
};

struct IVMParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, IVMParseData>(
		    statement->Copy());
	}

	explicit IVMParseData(unique_ptr<SQLStatement> statement)
	    : statement(std::move(statement)) {}
};

class IVMState : public ClientContextState {
public:
	explicit IVMState(unique_ptr<ParserExtensionParseData> parse_data)
	    : parse_data(std::move(parse_data)) {}

	void QueryEnd() override { parse_data.reset(); }

	unique_ptr<ParserExtensionParseData> parse_data;
};

} // namespace duckdb

#endif // DUCKDB_IVM_PARSER_HPP
