
#ifndef DUCKDB_IVM_REWRITE_RULE_HPP
#define DUCKDB_IVM_REWRITE_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

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
		// The query to trigger IVM will be of the form `SELECT * from DoIVM('view_name');`
		// so, the plan's single child should be the DOIVM table function
		auto child = plan->children[0].get();
		if (child->GetName() != "DOIVM") {
			return;
		}

		printf("Activating the rewrite rule\n");

		// obtain view defintion from catalog
		auto &catalog = Catalog::GetSystemCatalog(context);
		OnEntryNotFound if_not_found;
		QueryErrorContext error_context = QueryErrorContext();
		// TODO: how to get view name here?
		auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, "memory",
		                                           "main", "test", if_not_found, error_context);
		// TODO: error if view itself does not exist
		auto view_entry = dynamic_cast<ViewCatalogEntry*>(view_catalog_entry.get());
		printf("View base query: %s \n", view_entry->query->ToString().c_str());

		// generate the optimized logical plan
		Parser parser;
		parser.ParseQuery(view_entry->query->ToString());
		auto statement = parser.statements[0].get();
		Planner planner(context);
		planner.CreatePlan(statement->Copy());
		printf("Plan: %s\n", planner.plan->ToString().c_str());

		Parser newparser;
		newparser.ParseQuery("SELECT * from delta_bellow");
		statement = newparser.statements[0].get();
		Planner newplanner(context);
		newplanner.CreatePlan(statement->Copy());
		printf("Replace plan: %s\n", newplanner.plan->ToString().c_str());
		auto new_node = dynamic_cast<LogicalGet*>(newplanner.plan->children[0].get());

		Optimizer optimizer((Binder&)planner.binder, context);
		auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
		printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());

		auto modified_plan = std::move(optimized_plan);
		auto xchild = dynamic_cast<LogicalGet*>(modified_plan->children[0].get());
		auto col_bindings = xchild->GetColumnBindings();
		for (int i=0;i<col_bindings.size(); i++) {
			printf("CB %d %s\n", i, col_bindings[i].ToString().c_str());
		}
		modified_plan->children.clear();
		printf("Create replacement node \n");
		auto replacement_node = make_uniq<LogicalGet>(new_node->table_index, new_node->function, std::move(new_node->bind_data), new_node->returned_types, std::move(new_node->names));
		printf("Emplace back replacement node in modified plan \n");
		modified_plan->children.emplace_back(std::move(replacement_node));
		printf("Modified plan: %s\n", modified_plan->ToString().c_str());

		return;
	}
};

}; // namespace duckdb

#endif // DUCKDB_IVM_REWRITE_RULE_HPP
