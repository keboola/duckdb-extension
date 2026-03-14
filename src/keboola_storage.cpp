#include "keboola_storage.hpp"
#include "keboola_catalog.hpp"
#include "keboola_transaction.hpp"
#include "keboola_connection.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/access_mode.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// Helper: look up a named Keboola secret and extract TOKEN/URL/BRANCH
// ---------------------------------------------------------------------------

static void ReadFromSecret(ClientContext &context,
                            const std::string &secret_name,
                            std::string &token,
                            std::string &url,
                            std::string &branch) {
    auto &secret_manager = SecretManager::Get(context);
    auto txn = CatalogTransaction::GetSystemCatalogTransaction(context);

    unique_ptr<SecretEntry> secret_entry;
    secret_entry = secret_manager.GetSecretByName(txn, secret_name, "memory");
    if (!secret_entry) {
        secret_entry = secret_manager.GetSecretByName(txn, secret_name, "local_file");
    }
    if (!secret_entry) {
        throw BinderException("Keboola secret with name \"%s\" not found", secret_name);
    }

    const auto &kv = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
    Value v;

    v = kv.TryGetValue("token");
    if (!v.IsNull()) token = v.ToString();

    v = kv.TryGetValue("url");
    if (!v.IsNull()) url = v.ToString();

    v = kv.TryGetValue("branch");
    if (!v.IsNull()) branch = v.ToString();
}

// ---------------------------------------------------------------------------
// KeboolaAttach — called by DuckDB when ATTACH TYPE keboola is executed
// ---------------------------------------------------------------------------

static unique_ptr<Catalog> KeboolaAttach(optional_ptr<StorageExtensionInfo> /*storage_info*/,
                                          ClientContext &context,
                                          AttachedDatabase &db,
                                          const string &name,
                                          AttachInfo &info,
                                          AttachOptions & /*options*/) {
    std::string token;
    std::string url;
    std::string branch;
    std::string secret_name;
    bool snapshot_mode = false;

    // Parse ATTACH options
    for (auto &entry : info.options) {
        const auto lower = StringUtil::Lower(entry.first);
        if (lower == "type" || lower == "read_only" || lower == "read_write") {
            // handled by DuckDB core
        } else if (lower == "token") {
            token = entry.second.ToString();
        } else if (lower == "url") {
            url = entry.second.ToString();
        } else if (lower == "branch") {
            branch = entry.second.ToString();
        } else if (lower == "secret") {
            secret_name = entry.second.ToString();
        } else if (lower == "snapshot") {
            // SNAPSHOT option: pull all tables into local storage at attach time
            // Accept any truthy value or bare keyword (value="true"/"1"/etc.)
            const auto val = StringUtil::Lower(entry.second.ToString());
            snapshot_mode = (val.empty() || val == "true" || val == "1" || val == "yes");
        } else {
            throw BinderException("Unrecognized option for Keboola ATTACH: %s", entry.first);
        }
    }

    // If a named secret was provided (or token/url are empty), read from secret
    if (!secret_name.empty()) {
        ReadFromSecret(context, secret_name, token, url, branch);
    } else if (token.empty() || url.empty()) {
        // Try default keboola secret
        try {
            ReadFromSecret(context, "__default_keboola", token, url, branch);
        } catch (...) {
            // No default secret — proceed with what we have
        }
    }

    // The ATTACH path argument (first positional string) is the URL.
    // Example: ATTACH 'https://connection.keboola.com' AS kbc (TYPE keboola, TOKEN '...')
    // The URL option in the options map takes precedence if both are provided.
    if (url.empty() && !info.path.empty()) {
        url = info.path;
    }

    if (token.empty()) {
        throw BinderException(
            "Keboola ATTACH requires TOKEN. "
            "Provide TOKEN 'xxx' directly or use SECRET 'my_secret'.");
    }
    if (url.empty()) {
        throw BinderException(
            "Keboola ATTACH requires the Keboola URL as the path argument. "
            "Example: ATTACH 'https://connection.keboola.com' AS kbc "
            "(TYPE keboola, TOKEN 'my-token') "
            "or use SECRET 'my_secret'.");
    }

    // Build the shared connection state
    auto conn = std::make_shared<KeboolaConnection>();
    conn->token  = token;
    conn->url    = url;
    conn->branch = branch;
    conn->storage_client = std::make_shared<StorageApiClient>(url, token);

    // Verify token and discover service URLs
    try {
        conn->service_urls = conn->storage_client->VerifyAndDiscover();
    } catch (const IOException &e) {
        throw;
    } catch (const std::exception &e) {
        throw IOException("Keboola authentication failed: %s", std::string(e.what()));
    }

    // Resolve branch
    try {
        auto branch_info = conn->storage_client->ResolveBranch(branch);
        conn->branch_id = branch_info.id;
        if (conn->branch.empty()) {
            conn->branch = branch_info.name;
        }
    } catch (const IOException &e) {
        throw;
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (branch resolution): %s",
                          std::string(e.what()));
    }

    // Find or create workspace
    try {
        auto ws = conn->storage_client->FindOrCreateWorkspace();
        conn->workspace_id = ws.id;
    } catch (const IOException &e) {
        throw;
    } catch (const std::exception &e) {
        throw IOException("Failed to create Keboola workspace: %s", std::string(e.what()));
    }

    // Load catalog metadata
    std::vector<KeboolaBucketInfo> buckets;
    try {
        buckets = conn->storage_client->ListBuckets();
    } catch (const IOException &e) {
        throw;
    } catch (const std::exception &e) {
        throw IOException("Keboola connection failed (list buckets): %s", std::string(e.what()));
    }

    auto catalog = make_uniq<KeboolaCatalog>(db, std::move(conn), std::move(buckets));

    // If SNAPSHOT mode was requested, mark the connection so each table is lazily
    // pulled into local storage on first scan (instead of eagerly pulling all tables).
    if (snapshot_mode) {
        catalog->GetConnection()->snapshot_mode = true;
    }

    return catalog;
}

// ---------------------------------------------------------------------------
// KeboolaCreateTransactionManager
// ---------------------------------------------------------------------------

static unique_ptr<TransactionManager> KeboolaCreateTransactionManager(
    optional_ptr<StorageExtensionInfo> /*storage_info*/,
    AttachedDatabase &db,
    Catalog &catalog) {

    // We don't actually need the catalog reference for our no-op manager,
    // but we cast it to confirm it's the right type.
    (void)catalog.Cast<KeboolaCatalog>();
    return make_uniq<KeboolaTransactionManager>(db);
}

// ---------------------------------------------------------------------------
// Constructor — wire up callbacks
// ---------------------------------------------------------------------------

KeboolaStorageExtension::KeboolaStorageExtension() {
    attach = KeboolaAttach;
    create_transaction_manager = KeboolaCreateTransactionManager;
}

} // namespace duckdb
