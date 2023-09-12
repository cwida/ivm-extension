#include "ivm_parser_helpers.hpp"

#include <string>

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"


namespace duckdb {

void OptimizedQueryPlan(ClientContext &context, string &catalog, string &schema, string &view, duckdb::unique_ptr<LogicalOperator> &plan) {
	auto view_catalog_entry = Catalog::GetEntry(context, CatalogType::VIEW_ENTRY, catalog,
	                                           schema, view, OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());
	auto view_entry = dynamic_cast<ViewCatalogEntry*>(view_catalog_entry.get());
	printf("View base query: %s \n", view_entry->query->ToString().c_str());
	if (view_entry->query->type != StatementType::SELECT_STATEMENT) {
		throw NotImplementedException("Only select queries in view definition supported");
	}

	Parser parser;
	parser.ParseQuery(view_entry->query->ToString());
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	Optimizer optimizer((Binder&)planner.binder, context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
	printf("Optimized plan: %s\n", optimized_plan->ToString().c_str());
	plan = std::move(optimized_plan);
}

string DeltaMaterializedViewQuery(ClientContext &context, string &catalog, string &schema, string &view) {

	duckdb::unique_ptr<LogicalOperator> plan;
	OptimizedQueryPlan(context, catalog, schema, view, plan);

	/* We make the assumption that the top node of any plan will be a projection node.
	 * We take the expression alias from this node.
 	*/

	auto child = plan.get();
	if (child->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		throw NotImplementedException("Top node of the query plan should be projection");
	}
	// fetch column names for the delta_materialized_view
	vector<string> col_names = {};
	vector<LogicalType> col_types = {};
	for (int i=0;i<child->expressions.size();i++){
		col_names.emplace_back(child->expressions[i].get()->alias);
		col_types.emplace_back(child->expressions[i].get()->return_type);
	}
	col_names.emplace_back("_duckdb_ivm_multiplicity");
	col_types.emplace_back(LogicalType::BOOLEAN);

	/* fetch constraints for the delta_materialized_view.
	 * if group_by node exists and grouping is not null
	 * then, grouping columns of the topmost group_by node become the UNIQUE constraint
	 * else, if grouping is null or not group_by node exists
	 *	then, an autoincrement ID column is introduced. */
	child = plan->children[0].get();
	vector<string> grouping_cols = {};
	while (child->children.size() != 0) {
		if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto child_converted = dynamic_cast<LogicalAggregate*>(child);
			for (int i=0;i<child_converted->groups.size();i++) {
				grouping_cols.emplace_back(child_converted->groups[i].get()->alias);
			}
			break;
		}
		child = child->children[0].get();
	}

	// if no grouping found, then introduce ID column
	string unique_constraint;
	if (grouping_cols.empty()) {
		unique_constraint = "UNIQUE (ID)";
	} else {
		unique_constraint = "UNIQUE (";
		for (int i=0;i<grouping_cols.size();i++) {
			unique_constraint+=grouping_cols[i]+", ";
		}
		unique_constraint+=")";
	}

	string delta_materialized_view_query = "CREATE TABLE delta_"+view+" (";
	for (int i=0;i<col_names.size();i++) {
		delta_materialized_view_query+="\""+col_names[i]+"\""+" "+col_types[i].ToString()+", ";
	}
	delta_materialized_view_query += unique_constraint+");";
	printf("%s\n", delta_materialized_view_query.c_str());
	return delta_materialized_view_query;
}

} // namespace duckdb
