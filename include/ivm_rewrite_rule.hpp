
#ifndef DUCKDB_IVM_REWRITE_RULE_HPP
#define DUCKDB_IVM_REWRITE_RULE_HPP

#include "duckdb.hpp"

namespace duckdb {

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		printf("Initializing optimizer rule");
		optimize_function = IVMRewriteRuleFunction;
	}

	static void IVMRewriteRuleFunction(ClientContext &context, OptimizerExtensionInfo *info,
	                                   duckdb::unique_ptr<LogicalOperator> &plan) {
		printf("In the optimize function\n");

		if (plan->children.size() == 0) {
			return;
		}

		// check if plan contains table function `DoIVM`
		// User query will be of the form `SELECT * from DoIVM('view_name');`
		// so, the plan's single child should be the DOIVM table function
		auto child = plan->children[0].get();
		if (child->GetName() == "DOIVM") {
			printf("Activating the rewrite rule\n");
		}

		return;
	}
};

}; // namespace duckdb

#endif // DUCKDB_IVM_REWRITE_RULE_HPP
