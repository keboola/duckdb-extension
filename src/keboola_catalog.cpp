#include "keboola_catalog.hpp"
#include "keboola_insert.hpp"
#include "keboola_update.hpp"
#include "keboola_delete.hpp"
#include "include/keboola_table.hpp"

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
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "util/sql_generator.hpp"

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

    // Schema not found: always try a refresh once before giving up.
    // Handles buckets created after ATTACH (e.g. concurrent sessions or test fixtures).
    try {
        RefreshCatalog();
    } catch (...) {
        // Refresh failure is non-fatal; fall through to not-found handling
    }
    auto it2 = schemas_.find(schema_name);
    if (it2 != schemas_.end()) {
        return it2->second.get();
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

    // Parse bucket stage and name from "stage.c-name" (e.g. "in.c-myapp")
    std::string stage = "in";
    std::string bucket_name = schema_name;

    auto dot = schema_name.find('.');
    if (dot != std::string::npos) {
        stage       = schema_name.substr(0, dot);
        bucket_name = schema_name.substr(dot + 1);
    }

    // The Storage API adds the "c-" prefix automatically.
    // Strip it here so the bucket is not created with a double prefix (e.g. "c-c-name").
    if (bucket_name.size() > 2 && bucket_name[0] == 'c' && bucket_name[1] == '-') {
        bucket_name = bucket_name.substr(2);
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

    // When only a subset of columns is specified (e.g. INSERT INTO t (a, b) VALUES ...)
    // the child plan produces only those columns.  We must expand the chunk to the full
    // table width — filling defaults/NULLs for omitted columns — before KeboolaInsert
    // sees it, so that the CSV always has exactly as many columns as the target table.
    if (!op.column_index_map.empty()) {
        plan = planner.ResolveDefaultsProjection(op, *plan);
    }

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

// Helper: extract idx_t from column_index, compatible with both DuckDB v1.5.0 (idx_t)
// and DuckDB main (ProjectionIndex struct with .index member).
template<typename T>
static typename std::enable_if<std::is_integral<T>::value, idx_t>::type
GetColumnIdx(T v) { return static_cast<idx_t>(v); }

template<typename T>
static typename std::enable_if<!std::is_integral<T>::value, idx_t>::type
GetColumnIdx(T v) { return static_cast<idx_t>(v.index); }

//! Try to resolve the column name from a ColumnRef expression within a LogicalGet.
static bool TryExtractColumnName(const Expression &expr,
                                  const LogicalGet &get,
                                  std::string &out_col_name) {
    if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
        const auto &cref = expr.Cast<BoundColumnRefExpression>();
        // column_index is idx_t in DuckDB v1.5.0; ProjectionIndex (struct) in DuckDB main.
        // Use SFINAE helpers (C++11 compatible) to dispatch without version macros.
        idx_t col_idx = GetColumnIdx(cref.binding.column_index);
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

//! Try to resolve the column name from an expression using a scan's column_ids + names.
//!
//! Handles two expression forms:
//!  - BoundColumnRefExpression: binding.column_index is the schema-absolute column index.
//!    Looks it up directly in names[].
//!  - BoundReferenceExpression: index is the DataChunk projected column position.
//!    Maps dc_index → column_ids[dc_index].GetPrimaryIndex() → names[schema_index].
//!
//! Used for PhysicalFilter expressions, which DuckDB converts from BoundColumnRef to
//! BoundReference during physical plan generation (column binding resolution).
static bool TryExtractColumnNameFromScan(const Expression &expr,
                                          const vector<string> &names,
                                          const vector<ColumnIndex> &column_ids,
                                          std::string &out_col_name) {
    if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
        const auto &cref = expr.Cast<BoundColumnRefExpression>();
        idx_t col_idx = GetColumnIdx(cref.binding.column_index);
        if (col_idx < names.size()) {
            out_col_name = names[col_idx];
            return true;
        }
    }
    if (expr.expression_class == ExpressionClass::BOUND_REF) {
        // PhysicalFilter expressions use BoundReferenceExpression (DataChunk index).
        // Map through column_ids to get the schema-absolute index, then names[].
        const auto &bref = expr.Cast<BoundReferenceExpression>();
        idx_t dc_idx = static_cast<idx_t>(bref.index);
        if (dc_idx < column_ids.size()) {
            idx_t schema_idx = column_ids[dc_idx].GetPrimaryIndex();
            if (schema_idx < names.size()) {
                out_col_name = names[schema_idx];
                return true;
            }
        }
    }
    if (expr.expression_class == ExpressionClass::BOUND_CAST) {
        const auto &cast = expr.Cast<BoundCastExpression>();
        return TryExtractColumnNameFromScan(*cast.child, names, column_ids, out_col_name);
    }
    return false;
}

//! Parse a filter expression from a PhysicalFilter into KeboolaDeleteParams.
//! Uses scan.names and scan.column_ids to resolve column references, since
//! PhysicalFilter expressions use BoundReferenceExpression (DataChunk index).
static KeboolaDeleteParams ParsePhysicalFilterExpression(const Expression &expr,
                                                          const vector<string> &names,
                                                          const vector<ColumnIndex> &column_ids) {
    KeboolaDeleteParams params;

    // Simple equality / inequality comparison: col = val or col != val
    if (expr.expression_class == ExpressionClass::BOUND_COMPARISON) {
        const auto &cmp = expr.Cast<BoundComparisonExpression>();
        std::string col_name, val;
        bool ok = (TryExtractColumnNameFromScan(*cmp.left, names, column_ids, col_name) &&
                   TryExtractStringConstant(*cmp.right, val))
               || (TryExtractStringConstant(*cmp.left, val) &&
                   TryExtractColumnNameFromScan(*cmp.right, names, column_ids, col_name));
        if (!ok) {
            throw NotImplementedException(
                "DELETE WHERE must compare a single column to a constant value "
                "(Storage API limitation). Got: %s", expr.ToString());
        }
        params.where_column = col_name;
        params.where_values  = {val};
        switch (cmp.type) {
            case ExpressionType::COMPARE_EQUAL:
            case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
                params.where_operator = "eq"; break;
            case ExpressionType::COMPARE_NOTEQUAL:
            case ExpressionType::COMPARE_DISTINCT_FROM:
                params.where_operator = "ne"; break;
            default:
                throw NotImplementedException(
                    "DELETE WHERE only supports = and != comparisons "
                    "(Storage API limitation). Got: %s", expr.ToString());
        }
        return params;
    }

    // IN expression: col IN (val1, val2, ...)
    if (expr.expression_class == ExpressionClass::BOUND_OPERATOR) {
        const auto &op_expr = expr.Cast<BoundOperatorExpression>();
        if (op_expr.type == ExpressionType::COMPARE_IN && !op_expr.children.empty()) {
            std::string col_name;
            if (!TryExtractColumnNameFromScan(*op_expr.children[0], names, column_ids, col_name)) {
                throw NotImplementedException(
                    "DELETE WHERE IN: left side must be a column reference. Got: %s",
                    expr.ToString());
            }
            params.where_column   = col_name;
            params.where_operator = "eq";
            for (idx_t i = 1; i < op_expr.children.size(); i++) {
                std::string v;
                if (!TryExtractStringConstant(*op_expr.children[i], v)) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: all values must be constants. Got: %s",
                        op_expr.children[i]->ToString());
                }
                params.where_values.push_back(std::move(v));
            }
            return params;
        }
    }

    // OR conjunction: DuckDB rewrites `col IN ('a','b')` as `col = 'a' OR col = 'b'`
    if (expr.expression_class == ExpressionClass::BOUND_CONJUNCTION) {
        const auto &conj = expr.Cast<BoundConjunctionExpression>();
        if (conj.type == ExpressionType::CONJUNCTION_OR) {
            std::string col_name;
            std::vector<std::string> vals;
            for (const auto &child : conj.children) {
                if (child->expression_class != ExpressionClass::BOUND_COMPARISON) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: OR children must be simple comparisons. Got: %s",
                        child->ToString());
                }
                const auto &cmp = child->Cast<BoundComparisonExpression>();
                if (cmp.type != ExpressionType::COMPARE_EQUAL &&
                    cmp.type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: OR children must be equality comparisons. Got: %s",
                        child->ToString());
                }
                std::string c, v;
                bool ok = (TryExtractColumnNameFromScan(*cmp.left, names, column_ids, c) &&
                           TryExtractStringConstant(*cmp.right, v))
                       || (TryExtractStringConstant(*cmp.left, v) &&
                           TryExtractColumnNameFromScan(*cmp.right, names, column_ids, c));
                if (!ok) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: each OR branch must compare a column to a constant. Got: %s",
                        child->ToString());
                }
                if (!col_name.empty() && col_name != c) {
                    throw NotImplementedException(
                        "DELETE WHERE: multi-column OR is not supported by the Storage API.");
                }
                col_name = c;
                vals.push_back(std::move(v));
            }
            params.where_column   = std::move(col_name);
            params.where_values   = std::move(vals);
            params.where_operator = "eq";
            return params;
        }
    }

    throw NotImplementedException(
        "DELETE WHERE must use a simple column comparison (=, !=) or IN expression "
        "(Storage API limitation). Got: %s", expr.ToString());
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

    // OR conjunction: DuckDB rewrites `col IN ('a','b')` as `col = 'a' OR col = 'b'`
    // which is represented as BoundConjunctionExpression(CONJUNCTION_OR).
    if (expr.expression_class == ExpressionClass::BOUND_CONJUNCTION) {
        const auto &conj = expr.Cast<BoundConjunctionExpression>();
        if (conj.type == ExpressionType::CONJUNCTION_OR) {
            std::string col_name;
            std::vector<std::string> vals;

            for (const auto &child : conj.children) {
                if (child->expression_class != ExpressionClass::BOUND_COMPARISON) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: OR conjunction children must be simple comparisons "
                        "(Storage API limitation). Got: %s", child->ToString());
                }
                const auto &cmp = child->Cast<BoundComparisonExpression>();
                if (cmp.type != ExpressionType::COMPARE_EQUAL &&
                    cmp.type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: OR conjunction children must be equality comparisons "
                        "(Storage API limitation). Got: %s", child->ToString());
                }
                std::string c, v;
                bool ok = (TryExtractColumnName(*cmp.left, get, c) &&
                           TryExtractStringConstant(*cmp.right, v))
                       || (TryExtractStringConstant(*cmp.left, v) &&
                           TryExtractColumnName(*cmp.right, get, c));
                if (!ok) {
                    throw NotImplementedException(
                        "DELETE WHERE IN: each OR branch must compare a column to a constant "
                        "(Storage API limitation). Got: %s", child->ToString());
                }
                if (!col_name.empty() && col_name != c) {
                    throw NotImplementedException(
                        "DELETE WHERE: multi-column OR is not supported by the Storage API. "
                        "All OR branches must reference the same column.");
                }
                col_name = c;
                vals.push_back(std::move(v));
            }

            KeboolaDeleteParams or_params;
            or_params.where_column   = std::move(col_name);
            or_params.where_values   = std::move(vals);
            or_params.where_operator = "eq";
            return or_params;
        }
    }

    throw NotImplementedException(
        "DELETE WHERE must use a simple column comparison (=, !=) or IN expression "
        "(Storage API limitation). Got: %s",
        expr.ToString());
}

//! Convert a single TableFilter to KeboolaDeleteParams for the given column name.
static KeboolaDeleteParams TableFilterToDeleteParams(const std::string &col_name,
                                                     const TableFilter &tf) {
    KeboolaDeleteParams params;
    params.where_column = col_name;

    if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
        const auto &cf = tf.Cast<ConstantFilter>();
        params.where_values = {cf.constant.ToString()};
        if (cf.comparison_type == ExpressionType::COMPARE_EQUAL ||
            cf.comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
            params.where_operator = "eq";
        } else if (cf.comparison_type == ExpressionType::COMPARE_NOTEQUAL ||
                   cf.comparison_type == ExpressionType::COMPARE_DISTINCT_FROM) {
            params.where_operator = "ne";
        } else {
            throw NotImplementedException(
                "DELETE WHERE only supports = and != comparisons "
                "(Storage API limitation). Got comparison type: %s",
                ExpressionTypeToString(cf.comparison_type));
        }
        return params;
    }

    if (tf.filter_type == TableFilterType::IN_FILTER) {
        const auto &inf = tf.Cast<InFilter>();
        params.where_operator = "eq";
        for (const auto &v : inf.values) {
            params.where_values.push_back(v.ToString());
        }
        return params;
    }

    if (tf.filter_type == TableFilterType::CONJUNCTION_OR) {
        // IN ('a','b') is often pushed as ConjunctionOrFilter of ConstantFilter(EQUAL)
        const auto &orf = tf.Cast<ConjunctionOrFilter>();
        params.where_operator = "eq";
        for (const auto &child : orf.child_filters) {
            if (child->filter_type != TableFilterType::CONSTANT_COMPARISON) {
                throw NotImplementedException(
                    "DELETE WHERE IN: unsupported filter type in OR conjunction.");
            }
            const auto &cf = child->Cast<ConstantFilter>();
            if (cf.comparison_type != ExpressionType::COMPARE_EQUAL &&
                cf.comparison_type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
                throw NotImplementedException(
                    "DELETE WHERE IN: OR children must be equality comparisons.");
            }
            params.where_values.push_back(cf.constant.ToString());
        }
        return params;
    }

    throw NotImplementedException(
        "DELETE WHERE: unsupported table filter type for column '%s'. "
        "Only simple = and != comparisons and IN lists are supported.",
        col_name);
}

//! Extract KeboolaDeleteParams from a PhysicalTableScan's table_filters.
//!
//! Background: DuckDB's physical planner (plan_get.cpp::CreateTableFilterSet) MOVES the
//! filters from LogicalGet::table_filters into the PhysicalTableScan using std::move,
//! leaving the LogicalGet's filters as null pointers. We must read from the physical
//! scan (which has the valid filter pointers) rather than the logical plan.
//!
//! In the PhysicalTableScan the filter key is the RELATIVE index (position in column_ids),
//! and column_ids[k].GetPrimaryIndex() gives the absolute schema column index into names[].
static KeboolaDeleteParams ExtractDeleteParamsFromPhysicalScan(const PhysicalTableScan &scan) {
#if KEBOOLA_DUCKDB_NEW_FILTER_API
    if (!scan.table_filters || !scan.table_filters->HasFilters()) {
#else
    if (!scan.table_filters || scan.table_filters->filters.empty()) {
#endif
        // No filters in the scan → plain DELETE (no WHERE clause)
        KeboolaDeleteParams params;
        params.allow_truncate = true;
        return params;
    }

#if KEBOOLA_DUCKDB_NEW_FILTER_API
    // Count filters via iterator to check if there is more than one
    {
        idx_t filter_count = 0;
        for (const auto &e : *scan.table_filters) {
            (void)e;
            ++filter_count;
        }
        if (filter_count > 1) {
            throw NotImplementedException(
                "DELETE WHERE with multiple predicates is not supported by the Storage API. "
                "Combine values using IN, or run multiple DELETE statements.");
        }
    }
    const auto &first_entry = *scan.table_filters->begin();
    idx_t        rel_idx    = first_entry.ColumnIndex();
    const TableFilter &tf   = first_entry.Filter();
#else
    const auto &tf_map = scan.table_filters->filters;

    if (tf_map.size() > 1) {
        throw NotImplementedException(
            "DELETE WHERE with multiple predicates is not supported by the Storage API. "
            "Combine values using IN, or run multiple DELETE statements.");
    }

    const auto &entry     = *tf_map.begin();
    idx_t        rel_idx  = entry.first;   // relative index into scan.column_ids
    const TableFilter &tf = *entry.second; // valid non-null pointer in the physical scan
#endif

    // Map relative index → absolute column index → column name
    if (rel_idx >= scan.column_ids.size()) {
        throw NotImplementedException(
            "DELETE: relative filter column index %llu out of range (column_ids.size=%llu)",
            (unsigned long long)rel_idx, (unsigned long long)scan.column_ids.size());
    }
    idx_t abs_idx = scan.column_ids[rel_idx].GetPrimaryIndex();
    if (abs_idx >= scan.names.size()) {
        throw NotImplementedException(
            "DELETE: absolute filter column index %llu out of range (names.size=%llu)",
            (unsigned long long)abs_idx, (unsigned long long)scan.names.size());
    }
    const std::string &col_name = scan.names[abs_idx];

    // OptionalFilter is for zone-map pruning only — it is NOT a correctness filter.
    // The actual WHERE is in the PhysicalFilter expression on top of the scan.
    // Signal the caller to fall through and use PhysicalFilter instead.
    if (tf.filter_type == TableFilterType::OPTIONAL_FILTER) {
        KeboolaDeleteParams params;
        params.allow_truncate = true;
        return params;
    }

    return TableFilterToDeleteParams(col_name, tf);
}

static KeboolaDeleteParams ExtractDeleteParams(LogicalOperator &child_logical_op) {
    // Walk the logical plan to find a LogicalFilter node.
    // NOTE: When filter_pushdown=true the optimizer may have pushed the LogicalFilter
    // INTO LogicalGet::table_filters. In that case FindLogicalFilter returns nullptr.
    // However, those filters are MOVED to the PhysicalTableScan during physical planning
    // (plan_get.cpp::CreateTableFilterSet), so we cannot read them from the logical plan here.
    // We fall back to the physical scan in PlanDelete instead (see caller).

    LogicalFilter *filter = FindLogicalFilter(child_logical_op);
    LogicalGet    *get    = FindLogicalGet(child_logical_op);

    if (!filter || filter->expressions.empty()) {
        // Filter was either absent (no WHERE) or pushed into table_filters.
        // Signal the caller to extract from the physical scan instead.
        KeboolaDeleteParams params;
        params.allow_truncate = true; // temporary; overridden by PlanDelete if scan has filters
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
    // Extract WHERE parameters from the logical child plan.
    KeboolaDeleteParams params;
    if (!op.children.empty()) {
        params = ExtractDeleteParams(*op.children[0]);
    } else {
        // No child — plain DELETE (truncate)
        params.allow_truncate = true;
    }

    // IMPORTANT: DuckDB's physical planner MOVES data from the logical plan before
    // calling PlanDelete, so we cannot rely on the logical plan for filter data.
    // There are two cases:
    //
    // Case A: Simple equality filter (col = val):
    //   The optimizer pushes it to LogicalGet::table_filters. plan_get.cpp then
    //   MOVES it into PhysicalTableScan::table_filters. plan = PhysicalTableScan.
    //
    // Case B: IN / OR filter (col IN ('a','b') → col = 'a' OR col = 'b'):
    //   The optimizer keeps it in LogicalFilter. plan_filter.cpp STD-MOVES the
    //   expressions into a new PhysicalFilter, leaving LogicalFilter empty.
    //   plan = PhysicalFilter wrapping PhysicalTableScan.
    //
    // In both cases, allow_truncate=true is returned by ExtractDeleteParams because
    // the logical plan no longer holds valid filter data after physical planning.
    if (params.allow_truncate) {
        if (plan.type == PhysicalOperatorType::TABLE_SCAN) {
            // Case A: check PhysicalTableScan's table_filters (moved from LogicalGet)
            auto &scan = plan.Cast<PhysicalTableScan>();
#if KEBOOLA_DUCKDB_NEW_FILTER_API
            if (scan.table_filters && scan.table_filters->HasFilters()) {
#else
            if (scan.table_filters && !scan.table_filters->filters.empty()) {
#endif
                params = ExtractDeleteParamsFromPhysicalScan(scan);
            }
        } else if (plan.type == PhysicalOperatorType::FILTER &&
                   !plan.children.empty() &&
                   plan.children[0].get().type == PhysicalOperatorType::TABLE_SCAN) {
            // Case B: PhysicalFilter holds the moved LogicalFilter expressions.
            // DuckDB converts BoundColumnRefExpression → BoundReferenceExpression during
            // physical planning, so expressions use DataChunk position indices.
            // Use scan.column_ids to map DataChunk index → schema index → column name.
            auto &phys_filter = plan.Cast<PhysicalFilter>();
            auto &scan        = plan.children[0].get().Cast<PhysicalTableScan>();
            if (phys_filter.expression) {
                params = ParsePhysicalFilterExpression(*phys_filter.expression,
                                                       scan.names, scan.column_ids);
            }
        }
    }

    // Guard against accidental full-table deletion: require an explicit WHERE clause.
    // The Storage API's allowTruncate endpoint deletes ALL rows without recovery,
    // so we disallow it to prevent data loss from unintentional plain DELETE statements.
    if (params.allow_truncate) {
        throw NotImplementedException(
            "DELETE without a WHERE clause is not supported (would delete all rows). "
            "Use DELETE FROM <table> WHERE <column> = <value> to delete specific rows.");
    }

    auto &del = planner.Make<KeboolaDelete>(op, op.table, std::move(params));
    del.children.push_back(plan);
    return del;
}

PhysicalOperator &KeboolaCatalog::PlanUpdate(ClientContext & /*context*/,
                                              PhysicalPlanGenerator &planner,
                                              LogicalUpdate &op,
                                              PhysicalOperator &plan) {
    // -----------------------------------------------------------------------
    // Step 1: Walk down `plan` to find the PhysicalTableScan.
    // `plan` is the child physical plan created from LogicalProjection,
    // so it may be PROJECTION → (FILTER →) TABLE_SCAN.
    // -----------------------------------------------------------------------
    PhysicalTableScan *scan_ptr = nullptr;
    PhysicalFilter    *filter_ptr = nullptr;

    PhysicalOperator *cur = &plan;

    // Skip ALL PhysicalProjection layers.
    // There can be more than one when LogicalFilter.HasProjectionMap() is true:
    // DuckDB's CreatePlan(LogicalFilter) adds an extra PhysicalProjection for the
    // projection_map on top of the PhysicalFilter, producing:
    //   PROJECTION(SET exprs) → PROJECTION(filter_map) → FILTER → TABLE_SCAN
    while (cur->type == PhysicalOperatorType::PROJECTION && !cur->children.empty()) {
        cur = &cur->children[0].get();
    }
    // Optional PhysicalFilter layer
    if (cur->type == PhysicalOperatorType::FILTER && !cur->children.empty()) {
        filter_ptr = &cur->Cast<PhysicalFilter>();
        cur = &cur->children[0].get();
    }
    if (cur->type == PhysicalOperatorType::TABLE_SCAN) {
        scan_ptr = &cur->Cast<PhysicalTableScan>();
    }

    // -----------------------------------------------------------------------
    // Step 2: Extract WHERE params (same logic as PlanDelete)
    // -----------------------------------------------------------------------
    KeboolaDeleteParams where_params;
    where_params.allow_truncate = true; // default: no WHERE

    if (scan_ptr) {
#if KEBOOLA_DUCKDB_NEW_FILTER_API
        if (scan_ptr->table_filters && scan_ptr->table_filters->HasFilters()) {
#else
        if (scan_ptr->table_filters && !scan_ptr->table_filters->filters.empty()) {
#endif
            where_params = ExtractDeleteParamsFromPhysicalScan(*scan_ptr);
        }
    }
    if (where_params.allow_truncate && filter_ptr && filter_ptr->expression) {
        // Filter was not pushed into table_filters; use PhysicalFilter expression
        if (scan_ptr) {
            where_params = ParsePhysicalFilterExpression(*filter_ptr->expression,
                                                         scan_ptr->names,
                                                         scan_ptr->column_ids);
        }
    }

    // -----------------------------------------------------------------------
    // Step 3: Validate WHERE column is a primary key column
    // -----------------------------------------------------------------------
    auto &keboola_table = op.table.Cast<KeboolaTableEntry>();
    const auto &pk = keboola_table.GetKeboolaTableInfo().primary_key;

    if (!pk.empty() && !where_params.where_column.empty()) {
        bool is_pk = false;
        for (const auto &pk_col : pk) {
            if (pk_col == where_params.where_column) { is_pk = true; break; }
        }
        if (!is_pk) {
            throw NotImplementedException(
                "UPDATE WHERE on non-primary key column '%s' is not supported. "
                "Only PRIMARY KEY columns may be used in the WHERE clause "
                "(Keboola Storage deduplication requires a primary key match).",
                where_params.where_column);
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Extract SET params from the PhysicalProjection's select_list.
    // The projection's select_list[0..op.columns.size()-1] are the SET expressions.
    // For simple constant SET values (e.g. SET name = 'Alice'), these are
    // BoundConstantExpression objects.
    // -----------------------------------------------------------------------
    vector<KeboolaUpdateSetColumn> set_columns;

    const vector<unique_ptr<Expression>> *proj_exprs = nullptr;
    if (plan.type == PhysicalOperatorType::PROJECTION) {
        proj_exprs = &plan.Cast<PhysicalProjection>().select_list;
    }

    // Build ordered list of all logical column names for index mapping
    vector<std::string> all_col_names;
    for (const auto &col : op.table.GetColumns().Logical()) {
        all_col_names.push_back(col.GetName());
    }

    for (idx_t i = 0; i < op.columns.size(); i++) {
        KeboolaUpdateSetColumn sc;
        sc.col_index = op.columns[i].index;

        // Map physical index → column name
        if (sc.col_index < all_col_names.size()) {
            sc.col_name = all_col_names[sc.col_index];
        } else {
            sc.col_name = "col_" + std::to_string(sc.col_index);
        }

        // Extract the new value from the projection's select_list
        if (proj_exprs && i < proj_exprs->size() && (*proj_exprs)[i]) {
            const auto &expr = *(*proj_exprs)[i];
            if (expr.expression_class == ExpressionClass::BOUND_CONSTANT) {
                const auto &bce = expr.Cast<BoundConstantExpression>();
                if (bce.value.IsNull()) {
                    sc.new_value = "";
                    sc.is_null   = true;
                } else {
                    sc.new_value = bce.value.ToString();
                    sc.is_null   = false;
                }
            } else {
                // Non-constant expression (subquery, column reference, etc.)
                // Not supported in this implementation.
                throw NotImplementedException(
                    "UPDATE SET '%s' = <expression>: only constant literal values are "
                    "supported in Keboola UPDATE (e.g. SET col = 'value'). "
                    "Got expression: %s",
                    sc.col_name, expr.ToString());
            }
        }

        set_columns.push_back(std::move(sc));
    }

    // -----------------------------------------------------------------------
    // Step 5: Build KeboolaUpdate and attach child plan (needed for pipeline)
    // -----------------------------------------------------------------------
    auto &update = planner.Make<KeboolaUpdate>(op, op.table,
                                               std::move(set_columns),
                                               std::move(where_params));
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
