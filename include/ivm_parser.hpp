#include <utility>

#include "duckdb.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/pragma_function.hpp"

#ifndef DUCKDB_IVM_PARSER_HPP
#define DUCKDB_IVM_PARSER_HPP

namespace duckdb {

enum class IVMType : uint8_t { AGGREGATE_GROUP, SIMPLE_FILTER, SIMPLE_AGGREGATE, SIMPLE_PROJECTION };

struct DoIVMFunctionData : public TableFunctionData {
	DoIVMFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string catalog;
	string schema;
	string view;
	string suffix;
	bool overwrite = false;
	uint32_t children = 1;
	int step = -1;
};

struct IVMInfo : ParserExtensionInfo {
	unique_ptr<Connection> db_conn;
	explicit IVMInfo(unique_ptr<Connection> db_conn) : db_conn(std::move(db_conn)) {
	}
};

class IVMParserExtension : public ParserExtension {
public:
	explicit IVMParserExtension() {
		// unique_ptr<Connection> db_conn (con);
		parse_function = IVMParseFunction;
		plan_function = IVMPlanFunction;
		// parser_info = std::make_shared<IVMInfo>(std::move(db_conn));
	}

	static ParserExtensionParseResult IVMParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	unique_ptr<ParserExtensionParseData> parse_data);

	static void IVMWrite(const string& filename, bool append, const string& compiled_query);
};

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct IVMOperatorExtension : public OperatorExtension {
	IVMOperatorExtension() : OperatorExtension() {
		Bind = IVMBind;
	}

	std::string GetName() override {
		return "IVM";
	}
};

struct IVMParseData : ParserExtensionParseData {
	IVMParseData() {
	}

	unique_ptr<SQLStatement> statement;
	// string query;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, IVMParseData>(statement->Copy());
	}

	explicit IVMParseData(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
	}
};

class IVMState : public ClientContextState {
public:
	explicit IVMState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};


class IVMFunction : public TableFunction {
public:
	IVMFunction() {
		name = "IVM function";
		arguments.push_back(LogicalType::BOOLEAN); // parsing successful
		bind = IVMBind;
		init_global = IVMInit;
		function = IVMFunc;
	}

	struct IVMBindData : public TableFunctionData {

		explicit IVMBindData(bool result) : result(result) {
		}

		// string sql_query;
		bool result;
	};

	struct IVMGlobalData : public GlobalTableFunctionState {
		IVMGlobalData() : offset(0) {
		}

		idx_t offset;
	};

	static duckdb::unique_ptr<FunctionData> IVMBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {
		printf("Inside IVMBind of Table function class\n");
		names.emplace_back("MATERIALIZED VIEW CREATION");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = false;
		if (IntegerValue::Get(input.inputs[0]) == 1) {
			result = true; // explict creation of the result since the input is an integer value for some reason
		}
		return make_uniq<IVMBindData>(result);
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> IVMInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IVMGlobalData>();
	}

	static void IVMFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		// placeholder (this needs to return something)
		printf("Inside IVMFunc of Table function class\n");
		auto &bind_data = data_p.bind_data->Cast<IVMBindData>();
		auto &data = dynamic_cast<IVMGlobalData &>(*data_p.global_state);
		if (data.offset >= 1) {
			// finished returning values
			return;
		}
		auto result = Value::BOOLEAN(bind_data.result);
		data.offset++;
		output.SetValue(0, 0, result);
		output.SetCardinality(1);
	}
};

} // namespace duckdb

#endif // DUCKDB_IVM_PARSER_HPP
