#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/reference_map.hpp"

#include <mutex>

namespace duckdb {

//! A no-op transaction — Keboola Storage is non-transactional.
class KeboolaTransaction : public Transaction {
public:
    KeboolaTransaction(TransactionManager &manager, ClientContext &context)
        : Transaction(manager, context) {}

    //! Retrieve the KeboolaTransaction active for the given context+database.
    static KeboolaTransaction &Get(ClientContext &context, AttachedDatabase &db);
};

//! No-op transaction manager for the Keboola catalog.
class KeboolaTransactionManager : public TransactionManager {
public:
    explicit KeboolaTransactionManager(AttachedDatabase &db);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
    void Checkpoint(ClientContext &context, bool force = false) override {}

private:
    mutex transaction_lock_;
    //! Map of active transactions — same pattern as duckdb-postgres
    reference_map_t<Transaction, unique_ptr<KeboolaTransaction>> transactions_;
};

} // namespace duckdb
