#include "keboola_secret.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// CREATE SECRET handler for provider "config"
//
//   CREATE SECRET my_kbc (
//       TYPE keboola,
//       TOKEN 'sapi-token-xxx',
//       URL   'https://connection.keboola.com',
//       BRANCH 'main'          -- optional
//   );
// ---------------------------------------------------------------------------

static unique_ptr<BaseSecret> CreateKeboolaSecret(ClientContext & /*context*/,
                                                   CreateSecretInput &input) {
    auto scope = input.scope;
    auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

    // TOKEN (required, sensitive)
    auto token_it = input.options.find("token");
    if (token_it != input.options.end()) {
        secret->secret_map["token"] = token_it->second;
    }

    // URL (required)
    auto url_it = input.options.find("url");
    if (url_it != input.options.end()) {
        secret->secret_map["url"] = url_it->second;
    }

    // BRANCH (optional)
    auto branch_it = input.options.find("branch");
    if (branch_it != input.options.end()) {
        secret->secret_map["branch"] = branch_it->second;
    } else {
        secret->secret_map["branch"] = Value("");
    }

    // Redact TOKEN in duckdb_secrets() output
    secret->redact_keys.insert("token");

    return unique_ptr<BaseSecret>(std::move(secret));
}

// ---------------------------------------------------------------------------
// RegisterKeboolaSecret
// ---------------------------------------------------------------------------

void RegisterKeboolaSecret(DatabaseInstance &db) {
    // 1. Register the secret type via the SecretManager
    SecretType secret_type;
    secret_type.name             = "keboola";
    secret_type.deserializer     = KeyValueSecret::Deserialize<KeyValueSecret>;
    secret_type.default_provider = "config";

    auto &config = DBConfig::GetConfig(db);
    config.secret_manager->RegisterSecretType(secret_type);

    // 2. Register the CREATE SECRET handler
    CreateSecretFunction secret_fun("keboola", "config", CreateKeboolaSecret);
    secret_fun.named_parameters["token"]  = LogicalType::VARCHAR;
    secret_fun.named_parameters["url"]    = LogicalType::VARCHAR;
    secret_fun.named_parameters["branch"] = LogicalType::VARCHAR;

    config.secret_manager->RegisterSecretFunction(std::move(secret_fun), OnCreateConflict::ERROR_ON_CONFLICT);
}

} // namespace duckdb
