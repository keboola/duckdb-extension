#include "keboola_catalog.hpp"
#include "keboola_insert.hpp"
#include "keboola_update.hpp"
#include "keboola_delete.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

KeboolaCatalog::KeboolaCatalog(AttachedDatabase &db,
                                std::shared_ptr<KeboolaConnection> connection,
                                std::vector<KeboolaBucketInfo> buckets)
    : Catalog(db),
      connection_(std::move(connection)),
      catalog_loaded_at_(std::chrono::steady_clock::now()) {
    BuildSchemas(buckets);
}

// ---------------------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------------------

KeboolaCatalog::~KeboolaCatalog() {
    // Workspace cleanup happens via OnDetach (which has a ClientContext).
    // If destroyed without DETACH (e.g. crash), FindOrCreateWorkspace() will
    // reuse the orphaned workspace on the next ATTACH.
}

// ---------------------------------------------------------------------------
// BuildSchemas
// ---------------------------------------------------------------------------

void KeboolaCatalog::BuildSchemas(std::vector<KeboolaBucketInfo> &buckets) {
    for (auto &bucket : buckets) {
        CreateSchemaInfo schema_info;
        schema_info.schema   = bucket.id;
        schema_info.internal = false;

        auto schema_entry = make_uniq<KeboolaSchemaEntry>(*this, schema_info,
                                                           bucket, connection_);
        schemas_[bucket.id] = std::move(schema_entry);
    }
}

// ---------------------------------------------------------------------------
// LookupSchema
// ---------------------------------------------------------------------------

optional_ptr<SchemaCatalogEntry> KeboolaCatalog::LookupSchema(
    CatalogTransaction /*transaction*/,
    const EntryLookupInfo &schema_lookup,
    OnEntryNotFound if_not_found) {

    const auto &schema_name = schema_lookup.GetEntryName();

    auto it = schemas_.find(schema_name);
    if (it != schemas_.end()) {
        return it->second.get();
    }

    // If catalog is stale, try refreshing once before giving up
    if (IsCatalogStale()) {
        try {
            RefreshCatalog();
        } catch (...) {
            // Refresh failure is non-fatal; fall through to not-found handling
        }
        auto it2 = schemas_.find(schema_name);
        if (it2 != schemas_.end()) {
            return it2->second.get();
        }
    }

    if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
        throw BinderException("Schema with name \"%s\" not found in Keboola catalog", schema_name);
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// ScanSchemas
// ---------------------------------------------------------------------------

void KeboolaCatalog::ScanSchemas(ClientContext & /*context*/,
                                  std::function<void(SchemaCatalogEntry &)> callback) {
    for (auto &kv : schemas_) {
        callback(*kv.second);
    }
}

// ---------------------------------------------------------------------------
// CreateSchema / DropSchema
// ---------------------------------------------------------------------------

optional_ptr<CatalogEntry> KeboolaCatalog::CreateSchema(CatalogTransaction /*transaction*/,
                                                          CreateSchemaInfo &info) {
    const std::string &schema_name = info.schema;

    // Handle IF NOT EXISTS
    auto existing = schemas_.find(schema_name);
    if (existing != schemas_.end()) {
        if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ||
            info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
            return existing->second.get();
        }
        throw CatalogException("Schema with name \"%s\" already exists in Keboola catalog",
                               schema_name);
    }

    // Parse bucket stage and name from "stage.name" (e.g. "in.c-myapp")
    std::string stage = "in";
    std::string bucket_name = schema_name;

    auto dot = schema_name.find('.');
    if (dot != std::string::npos) {
        stage       = schema_name.substr(0, dot);
        bucket_name = schema_name.substr(dot + 1);
    }

    // Create bucket via Storage API
    KeboolaBucketInfo bucket = connection_->storage_client->CreateBucket(stage, bucket_name);

    // Build schema entry and store
    CreateSchemaInfo schema_info;
    schema_info.schema   = bucket.id;
    schema_info.internal = false;

    auto schema_entry = make_uniq<KeboolaSchemaEntry>(*this, schema_info, bucket, connection_);
    auto *ptr = schema_entry.get();
    schemas_[bucket.id] = std::move(schema_entry);

    return ptr;
}

void KeboolaCatalog::DropSchema(ClientContext & /*context*/, DropInfo &info) {
    const auto &schema_name = info.name;

    auto it = schemas_.find(schema_name);
    if (it == schemas_.end()) {
        if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
            return; // IF EXISTS — silent
        }
        throw CatalogException("Schema with name \"%s\" not found in Keboola catalog",
                               schema_name);
    }

    // Drop the bucket via Storage API
    connection_->storage_client->DropBucket(schema_name);

    // Remove from local cache
    schemas_.erase(it);
}

// ---------------------------------------------------------------------------
// Physical plan stubs — write operations Phase 4/5
// ---------------------------------------------------------------------------

PhysicalOperator &KeboolaCatalog::PlanCreateTableAs(ClientContext & /*context*/,
                                                      PhysicalPlanGenerator & /*planner*/,
                                                      LogicalCreateTable & /*op*/,
                                                      PhysicalOperator & /*plan*/) {
    throw NotImplementedException("CREATE TABLE AS is not yet implemented in the Keboola extension (Phase 4)");
}

PhysicalOperator &KeboolaCatalog::PlanInsert(ClientContext & /*context*/,
                                              PhysicalPlanGenerator &planner,
                                              LogicalInsert &op,
                                              optional_ptr<PhysicalOperator> plan) {
    D_ASSERT(plan);

    auto &insert = planner.Make<KeboolaInsert>(op, op.table, PhysicalIndex(DConstants::INVALID_INDEX));
    insert.children.push_back(*plan);
    return insert;
}

// ---------------------------------------------------------------------------
// ExtractDeleteParams — walks the child plan to find WHERE filters
// ---------------------------------------------------------------------------

//! Try to extract a string constant from an expression.
//! Returns true and sets out_value on success.
static bool TryExtractStringConstant(const Expression &expr, std::string &out_value) {
    if (expr.expression_class == ExpressionClass::BOUND_CONSTANT) {
        const auto &bce = expr.Cast<BoundConstantExpression>();
        if (!bce.value.IsNull()) {
            out_value = bce.value.ToString();
            return true;
        }
        return false;
    }
    // Unwrap CAST nodes (e.g. CAST('val' AS VARCHAR))
    if (expr.expression_class == ExpressionClass::BOUND_CAST) {
        const auto &cast = expr.Cast<BoundCastExpression>();
        return TryExtractStringConstant(*cast.child, out_value);
    }
    return false;
}

//! Try to resolve the column name from a ColumnRef expression within a LogicalGet.
static bool TryExtractColumnName(const Expression &expr,
                                  const LogicalGet &get,
                                  std::string &out_col_name) {
    if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
        const auto &cref = expr.Cast<BoundColumnRefExpression>();
        // column_index is the index within the output of the LogicalGet
        // In DuckDB v1.5.0, column_index is idx_t; in DuckDB main it is ProjectionIndex.
        // Use if constexpr to handle both without version macros.
        idx_t col_idx;
        if constexpr (std::is_integral_v<decltype(cref.binding.column_index)>) {
            col_idx = static_cast<idx_t>(cref.binding.column_index);
        } else {
            col_idx = static_cast<idx_t>(cref.binding.column_index.index);
        }
        // LogicalGet::names holds the projected column names
        if (col_idx < get.names.size()) {
            out_col_name = get.names[col_idx];
            return true;
        }
    }
    // Unwrap CAST
    if (expr.expression_class == ExpressionClass::BOUND_CAST) {
        const auto &cast = expr.Cast<BoundCastExpression>();
        return TryExtractColumnName(*cast.child, get, out_col_name);
    }
    return false;
}

//! Walk down the logical plan tree to find the first LogicalGet node.
static LogicalGet *FindLogicalGet(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        return &op.Cast<LogicalGet>();
    }
    for (auto &child : op.children) {
        auto *found = FindLogicalGet(*child);
        if (found) return found;
    }
    return nullptr;
}

//! Walk down the logical plan tree to find the first LogicalFilter node.
static LogicalFilter *FindLogicalFilter(LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
        return &op.Cast<LogicalFilter>();
    }
    for (auto &child : op.children) {
        auto *found = FindLogicalFilter(*child);
        if (found) return found;
    }
    return nullptr;
}

//! Parse a single filter expression into KeboolaDeleteParams.
//! Throws NotImplementedException for unsupported patterns.
static KeboolaDeleteParams ParseFilterExpression(const Expression &expr,
                                                   const LogicalGet &get) {
    KeboolaDeleteParams params;

    if (expr.expression_class == ExpressionClass::BOUND_COMPARISON) {
        const auto &cmp = expr.Cast<BoundComparisonExpression>();

        std::string col_name;
        std::string val;

        // Try col op const, then const op col
        if (!( (TryExtractColumnName(*cmp.left, get, col_name) &&
                TryExtractStringConstant(*cmp.right, val))
             || (TryExtractStringConstant(*cmp.left, val) &&
                 TryExtractColumnName(*cmp.right, get, col_name)) )) {
            throw NotImplementedException(
                "DELETE WHERE must compare a single column to a constant value "
                "(Storage API limitation). Got unsupported expression: %s",
                expr.ToString());
        }

        params.where_column = col_name;
        params.where_values  = {val};

        switch (cmp.type) {
            case ExpressionType::COMPARE_EQUAL:
            case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
                params.where_operator = "eq";
                break;
            case ExpressionType::COMPARE_NOTEQUAL:
            case ExpressionType::COMPARE_DISTINCT_FROM:
                params.where_operator = "ne";
                break;
            default:
                throw NotImplementedException(
                    "DELETE WHERE only supports = and != comparisons "
                    "(Storage API limitation). Got: %s",
                    expr.ToString());
        }

        return params;
    }

    // IN (col IN ('a', 'b', ...))
    if (expr.expression_class == ExpressionClass::BOUND_OPERATOR) {
        const auto &op_expr = expr.Cast<BoundOperatorExpression>();
        if (op_expr.type == ExpressionType::COMPARE_IN) {
            // children[0] is the column, children[1..] are the IN values
            if (op_expr.children.empty()) {
                throw NotImplementedException(
                    "DELETE WHERE IN expression has no children: %s", expr.ToString());
            }
            std::string col_name;
            if (!TryExtractColumnName(*op_expr.children[0], get, col_name)) {
                throw NotImplementedException(
                    "DELETE WHERE IN: left side must be a column reference "
                    "(Storage API limitation). Got: %s",
                    expr.ToString());
            }
            params.where_column   = col_name;
            params.where_operator = "eq";

            for (idx_t i = 1; i < op_expr.children.size(); i++) {
                std::string val;
                if (!TryExtractStringConstant(*op_expr.children[i], val)) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: all values must be constants "
                        "(Storage API limitation). Got: %s",
                        op_expr.children[i]->ToString());
                }
                params.where_values.push_back(std::move(val));
            }

            return params;
        }
    }

    throw NotImplementedException(
        "DELETE WHERE must use a simple column comparison (=, !=) or IN expression "
        "(Storage API limitation). Got: %s",
        expr.ToString());
}

static KeboolaDeleteParams ExtractDeleteParams(LogicalOperator &child_logical_op) {
    // Walk the logical plan tree (not the physical plan, which isn't built yet)
    // to find a LogicalFilter with the WHERE predicates.

    LogicalFilter *filter = FindLogicalFilter(child_logical_op);
    LogicalGet    *get    = FindLogicalGet(child_logical_op);

    if (!filter || filter->expressions.empty()) {
        // No WHERE clause — allow table truncation
        KeboolaDeleteParams params;
        params.allow_truncate = true;
        return params;
    }

    if (!get) {
        throw NotImplementedException(
            "DELETE: could not locate scan node in the query plan. "
            "Only simple DELETE FROM <table> WHERE <col> = <val> is supported.");
    }

    if (filter->expressions.size() > 1) {
        throw NotImplementedException(
            "DELETE WHERE with multiple predicates is not supported by the Storage API. "
            "Combine values using IN, or run multiple DELETE statements.");
    }

    return ParseFilterExpression(*filter->expressions[0], *get);
}

PhysicalOperator &KeboolaCatalog::PlanDelete(ClientContext & /*context*/,
                                              PhysicalPlanGenerator &planner,
                                              LogicalDelete &op,
                                              PhysicalOperator &plan) {
    // Extract WHERE parameters from the logical child plan
    KeboolaDeleteParams params;
    if (!op.children.empty()) {
        params = ExtractDeleteParams(*op.children[0]);
    } else {
        // No child — plain DELETE (truncate)
        params.allow_truncate = true;
    }

    auto &del = planner.Make<KeboolaDelete>(op, op.table, std::move(params));
    del.children.push_back(plan);
    return del;
}

PhysicalOperator &KeboolaCatalog::PlanUpdate(ClientContext & /*context*/,
                                              PhysicalPlanGenerator &planner,
                                              LogicalUpdate &op,
                                              PhysicalOperator &plan) {
    auto &update = planner.Make<KeboolaUpdate>(op, op.table,
                                               op.columns,
                                               std::move(op.bound_defaults));
    update.children.push_back(plan);
    return update;
}

// ---------------------------------------------------------------------------
// Phase 6: RefreshCatalog
// ---------------------------------------------------------------------------

void KeboolaCatalog::RefreshCatalog() {
    auto buckets = connection_->storage_client->ListBuckets();
    schemas_.clear();
    BuildSchemas(buckets);
    catalog_loaded_at_ = std::chrono::steady_clock::now();
}

// ---------------------------------------------------------------------------
// Phase 6: PullAllTables / PullTable
// ---------------------------------------------------------------------------

void KeboolaCatalog::PullAllTables(ClientContext &context) {
    for (auto &kv : schemas_) {
        kv.second->PullAllTables(context);
    }
}

void KeboolaCatalog::PullTable(ClientContext &context,
                                const std::string &schema_name,
                                const std::string &table_name) {
    auto it = schemas_.find(schema_name);
    if (it == schemas_.end()) {
        throw CatalogException("Schema \"%s\" not found in Keboola catalog", schema_name);
    }
    it->second->PullTable(context, table_name);
}

// ---------------------------------------------------------------------------
// GetDatabaseSize
// ---------------------------------------------------------------------------

DatabaseSize KeboolaCatalog::GetDatabaseSize(ClientContext & /*context*/) {
    DatabaseSize size;
    size.free_blocks  = 0;
    size.total_blocks = 0;
    size.used_blocks  = 0;
    size.wal_size     = 0;
    size.block_size   = 0;
    size.bytes        = 0;
    return size;
}

// ---------------------------------------------------------------------------
// OnDetach
// ---------------------------------------------------------------------------

void KeboolaCatalog::OnDetach(ClientContext & /*context*/) {
    if (connection_ && connection_->storage_client && !connection_->workspace_id.empty()) {
        connection_->storage_client->DeleteWorkspace(connection_->workspace_id);
        connection_->workspace_id.clear();
    }
}

} // namespace duckdb
