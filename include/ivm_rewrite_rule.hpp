
#ifndef DUCKDB_IVM_REWRITE_RULE_HPP
#define DUCKDB_IVM_REWRITE_RULE_HPP

#include "ivm_parser.hpp"

#include <utility>

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
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	static void ModifyTopNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, idx_t &multiplicity_col_idx, idx_t &multiplicity_table_idx) {
		printf("\nAdd the multiplicity column to the top node...\n");
		printf("Plan: %s %s\n", plan->ToString().c_str(), plan->ParamsToString().c_str());
		for (int i=0;i<plan->GetColumnBindings().size(); i++) {
			printf("Top node CB before %d %s\n", i, plan->GetColumnBindings()[i].ToString().c_str());
		}

		// the table_idx used to create ColumnBinding will be that of the top node's child
		// the column_idx used to create ColumnBinding for multiplicity column will be stored context from the child node
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN,
		                                             ColumnBinding(multiplicity_table_idx, multiplicity_col_idx));
		printf("Add mult column to exp\n");
		plan->expressions.emplace_back(std::move(e));

		printf("Modified plan: %s %s\n", plan->ToString().c_str(), plan->ParamsToString().c_str());
		for (int i=0;i<plan.get()->GetColumnBindings().size(); i++) {
			printf("Top node CB %d %s\n", i, plan.get()->GetColumnBindings()[i].ToString().c_str());
		}
	}

	static void ModifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &plan, idx_t &table_index, idx_t &multiplicity_col_idx, idx_t &multiplicity_table_idx) {
		if (!plan->children[0]->children.empty()) {
			// Assume only one child per node
			// TODO: Add support for modification of plan with multiple children
			ModifyPlan(context, plan->children[0], table_index, multiplicity_col_idx, multiplicity_table_idx);
		}

		auto &catalog = Catalog::GetSystemCatalog(context);
		OnEntryNotFound if_not_found;
		QueryErrorContext error_context = QueryErrorContext();

		switch (plan->children[0].get()->type) {
			case LogicalOperatorType::LOGICAL_GET: {
			    auto child = std::move(plan->children[0]);
			    auto child_get = dynamic_cast<LogicalGet*>(child.get());

			    printf("Create replacement get node \n");
			    string delta_table = "delta_" + child_get->GetTable().get()->name;
			    string delta_table_schema = child_get->GetTable().get()->schema.name;
			    string delta_table_catalog = child_get->GetTable().get()->catalog.GetName();
			    auto table_catalog_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, delta_table_catalog,
			                                                 delta_table_schema,delta_table, OnEntryNotFound::THROW_EXCEPTION, error_context);
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

			    // the new get node that reads the delta table gets a new table index
			    auto replacement_get_node = make_uniq<LogicalGet>(table_index += 1, scan_function,
			                                                      std::move(bind_data), std::move(return_types),
			                                                      std::move(return_names));
			    replacement_get_node->column_ids = std::move(column_ids);

			    for (int i=0;i<replacement_get_node.get()->GetColumnBindings().size(); i++) {
				    printf("Replacement node CB %d %s\n", i, replacement_get_node.get()->GetColumnBindings()[i].ToString().c_str());
			    }

			    printf("Create projection node to project away unneeded columns \n");

			    /* The new get node which will read the delta table will read all columns in the delta table
			     * The original get node will read only the columns that the query uses
			     * So, we create a projection node to project away the extra columns.
			     * Thus, original get node will be replaced by a replacement get + projection node
			     * the column_ids field in the original get node contains the mapping of the logical ids that the columns have
			     * to ids that the get is using. The names field in the orignal get contains the column names. Using a
			     * combination of these two, we create the column mapping of the projection node.
			     * original_get->column_ids
					(duckdb::vector<unsigned long long, true>) $5 = {
					  std::__1::vector<unsigned long long, std::__1::allocator<unsigned long long> > = size=2 {
						[0] = 0
						[1] = 2
					  }
					}
			     * orignal_get->names
			     * (duckdb::vector<std::basic_string<char, std::char_traits<char>, std::allocator<char> >, true>) $3 = {
					std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > = size=3 {
					  [0] = "a"
					  [1] = "b"
					  [2] = "c"
					}
					}
			     * original_get->projection_ids
					(duckdb::vector<unsigned long long, true>) $4 = {
					  std::__1::vector<unsigned long long, std::__1::allocator<unsigned long long> > = size=2 {
						[0] = 0
						[1] = 1
					  }
					}
				*/

			    // the column bindings are created using the table index of the replacement get node. The column_idx are
			    // from the original get node, but can be assumed to have the same idx as the replacement get.
			    vector<unique_ptr<Expression>> select_list;
			    for (int i=0;i<child_get->column_ids.size();i++) {
				    printf("Create column mapping: %s, %llu", child_get->names[child_get->column_ids[i]].c_str(), child_get->column_ids[i]);
				    auto col = make_uniq<BoundColumnRefExpression>(child_get->names[child_get->column_ids[i]],
				                                                   child_get->returned_types[child_get->column_ids[i]],
				                                                   ColumnBinding(table_index, child_get->column_ids[i]));
				    select_list.emplace_back(std::move(col));
			    }

			    // for the creation projection node, multiplicity table idx and column idx will be fetched from the replacement get node
			    multiplicity_col_idx = std::find(replacement_get_node->names.begin(), replacement_get_node->names.end(), "_duckdb_ivm_multiplicity") - replacement_get_node->names.begin();
			    auto multiplicity_col = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, ColumnBinding(table_index, multiplicity_col_idx));
			    select_list.emplace_back(std::move(multiplicity_col));
			    // multiplicity column idx of the projection node (after binding) will be select_list.size() - 1
			    // because the multiplicity column was added to the projection node at the very end.
			    multiplicity_table_idx = child->GetTableIndex()[0];
			    multiplicity_col_idx = select_list.size() - 1;

			    // the projection node's table_idx is the table index of the original get node that is being replaced
			    // because that idx is already being used in the logical plan as reference
			    auto projection_node = make_uniq<LogicalProjection>(child->GetTableIndex()[0], std::move(select_list));
			    projection_node->AddChild(std::move(replacement_get_node));
			    for (int i=0;i<projection_node.get()->GetColumnBindings().size(); i++) {
				    printf("Projection node CB %d %s %s\n", i, projection_node.get()->GetColumnBindings()[i].ToString().c_str(),
				           projection_node->ParamsToString().c_str());
			    }

			    printf("Replacement plan: %s \n", projection_node->ToString().c_str());
			    printf("Emplace back replacement node in parent node \n");
			    plan->children.clear();
			    plan->children.emplace_back(std::move(projection_node));
			    break;
			}
		    case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

			    auto modified_node_logical_agg = dynamic_cast<LogicalAggregate*>(plan->children[0].get()); // dynamic_cast<LogicalAggregate*>(modified_plan.operator->());
			    printf("Aggregate index: %llu Group index: %llu\n", modified_node_logical_agg->aggregate_index, modified_node_logical_agg->group_index);
			    auto mult_group_by = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN,
			                                                             ColumnBinding(multiplicity_table_idx, multiplicity_col_idx));
			    modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));
			    multiplicity_col_idx = modified_node_logical_agg->groups.size() - 1;
			    multiplicity_table_idx = modified_node_logical_agg->group_index;
			    for (int i=0;i<modified_node_logical_agg->GetColumnBindings().size(); i++) {
				    printf("Middle node CB %d %s\n", i, modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
			    }

			    printf("Modified plan: %s %s\n", plan->ToString().c_str(), plan->ParamsToString().c_str());
			    break;
		    }
		    case LogicalOperatorType::LOGICAL_PROJECTION: {
			    // TODO: Implement modification of projection node
			    break;
		    }
		    default:
			    throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(plan->type));
		}
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

		auto child_get = dynamic_cast<LogicalGet*>(child);
		auto view = child_get->named_parameters["view_name"].ToString();
		auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
		auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

		idx_t table_index = 2000;

		// obtain view defintion from catalog
		auto &catalog = Catalog::GetSystemCatalog(context);
		OnEntryNotFound if_not_found;
		QueryErrorContext error_context = QueryErrorContext();
		auto view_catalog_entry = catalog.GetEntry(context, CatalogType::VIEW_ENTRY, view_catalog,
		                                           view_schema, view, if_not_found, error_context);
		// TODO: error if view itself does not exist
		auto view_entry = dynamic_cast<ViewCatalogEntry*>(view_catalog_entry.get());
		printf("View base query: %s \n", view_entry->query->ToString().c_str());

		// generate the optimized logical plan
		Parser parser;
		parser.ParseQuery(view_entry->query->ToString());
		auto statement = parser.statements[0].get();
		if (statement->type != StatementType::SELECT_STATEMENT) {
			throw NotImplementedException("Only select queries in view definition supported");
		}
		Planner planner(context);
		planner.CreatePlan(statement->Copy());
		Optimizer optimizer((Binder&)planner.binder, context);
		auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
		printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());

		// variable to store the column_idx for multiplicity column at each node
		// we do this while creation / modification of the node
		// because this information will not be available while modifying the parent node
		// for ex. parent.children[0] will not contain column names to find the index of the multiplicity column
		idx_t multiplicity_col_idx;
		idx_t multiplicity_table_idx;

		// Recursively modify the optimized logical plan
		ModifyPlan(context, optimized_plan, table_index, multiplicity_col_idx, multiplicity_table_idx);
		ModifyTopNode(context, optimized_plan, multiplicity_col_idx, multiplicity_table_idx);
		plan = std::move(optimized_plan);
		return;
	}
};
}; // namespace duckdb

#endif // DUCKDB_IVM_REWRITE_RULE_HPP
