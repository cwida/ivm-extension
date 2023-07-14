
#ifndef DUCKDB_IVM_REWRITE_RULE_HPP
#define DUCKDB_IVM_REWRITE_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

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
		idx_t table_index = 2000;

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
		Optimizer optimizer((Binder&)planner.binder, context);
		auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
		printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());

		auto modified_plan = std::move(optimized_plan->children[0]);
		auto xchild = dynamic_cast<LogicalGet*>(modified_plan->children[0].get());
		printf("Xchild node: %s\n", xchild->ToString().c_str());
		auto col_bindings = xchild->GetColumnBindings();
		for (int i=0;i<col_bindings.size(); i++) {
			printf("og get CB %d %s\n", i, col_bindings[i].ToString().c_str());
		}
		idx_t x = xchild->table_index;


		printf("Create replacement node \n");
		string delta_table = "delta_hello";
		auto table_catalog_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, "memory",
		                                             "main",delta_table, OnEntryNotFound::THROW_EXCEPTION, error_context);
		auto &table = table_catalog_entry->Cast<TableCatalogEntry>();
		unique_ptr<FunctionData> bind_data;
		auto scan_function = table.GetScanFunction(context, bind_data);
		vector<LogicalType> return_types = {};
		vector<string> return_names = {};
		vector<column_t> column_ids = {};
		column_t seed_column_id = 0;
		for (auto &col : table.GetColumns().Logical()) {
			printf("creating bind data: %s\n", col.GetName().c_str());
			return_types.push_back(col.Type());
			return_names.push_back(col.Name());
			column_ids.push_back(seed_column_id);
			seed_column_id += 1;
		}

		auto replacement_get_node = make_uniq<LogicalGet>(table_index += 1, scan_function,
		                                              std::move(bind_data), std::move(return_types),
		                                              std::move(return_names));
		replacement_get_node->column_ids = std::move(column_ids);
//		replacement_get_node->projection_ids = std::move(column_ids);

		for (int i=0;i<replacement_get_node.get()->GetColumnBindings().size(); i++) {
			printf("Replacement node CB %d %s\n", i, replacement_get_node.get()->GetColumnBindings()[i].ToString().c_str());
		}

		printf("Create projection node to project away columns \n");
		// create expressions
//		table_index += 1;
//		idx_t projection_table_idx = table_index;
		auto e1 = make_uniq<BoundColumnRefExpression>("a", LogicalType::INTEGER, ColumnBinding(table_index, 0));
		auto e2 = make_uniq<BoundColumnRefExpression>("c", LogicalType::VARCHAR, ColumnBinding(table_index, 2));
		auto e3 = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, ColumnBinding(table_index, 3));
		vector<unique_ptr<Expression>> select_list;
		select_list.emplace_back(std::move(e1));
		select_list.emplace_back(std::move(e2));
		select_list.emplace_back(std::move(e3));
		auto projection_node = make_uniq<LogicalProjection>(xchild->table_index, std::move(select_list));
		projection_node->AddChild(std::move(replacement_get_node));
		for (int i=0;i<projection_node.get()->GetColumnBindings().size(); i++) {
			printf("Projection node CB %d %s %s\n", i, projection_node.get()->GetColumnBindings()[i].ToString().c_str(),
			       projection_node->ParamsToString().c_str());
		}

		printf("Replacement plan: %s \n", projection_node->ToString().c_str());

		printf("Emplace back replacement node in modified plan \n");
		modified_plan->children.clear();
		modified_plan->children.emplace_back(std::move(projection_node));

		// modify the middle node
		auto modified_node_logical_agg = dynamic_cast<LogicalAggregate*>(modified_plan.operator->());
		auto mult_group_by = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, ColumnBinding(x, 2));
		modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));
		printf("Modified plan: %s %s\n", modified_plan->ToString().c_str(), modified_plan->ParamsToString().c_str());
		for (int i=0;i<modified_plan->GetColumnBindings().size(); i++) {
			printf("Middle node CB %d %s\n", i, modified_plan->GetColumnBindings()[i].ToString().c_str());
		}

		// modifying the top node
		vector<idx_t> middlenode_tableindex = modified_plan->GetTableIndex();
		printf("Middle node table index %lu: ", middlenode_tableindex.size());
		for (int i=0;i<middlenode_tableindex.size();i++) {
			printf("index [%llu]", middlenode_tableindex[i]);
		}
		printf("\nAdd the multiplicity column to the top node\n");
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, ColumnBinding(2, 0));
		printf("Add mult column to exp\n");
		optimized_plan->expressions.emplace_back(std::move(e));
		printf("Clear children\n");
		optimized_plan->children.clear();
		printf("Add child %lu\n", optimized_plan->children.size());
		optimized_plan->children.emplace_back(std::move(modified_plan));

		printf("Modified plan: %s %s\n", optimized_plan->ToString().c_str(), optimized_plan->ParamsToString().c_str());
		for (int i=0;i<optimized_plan.get()->GetColumnBindings().size(); i++) {
			printf("Top node CB %d %s\n", i, optimized_plan.get()->GetColumnBindings()[i].ToString().c_str());
		}

//		for (int i=0;i<modified_plan->children[0].get()->GetColumnBindings().size(); i++) {
//			printf("Updated CB %d %s\n", i, modified_plan->children[0].get()->GetColumnBindings()[i].ToString().c_str());
//		}

		plan = std::move(optimized_plan);

		printf("Updated plan: %s\n", plan->ToString().c_str());

		return;
	}
};
}; // namespace duckdb

#endif // DUCKDB_IVM_REWRITE_RULE_HPP
