#include "keboola_transaction.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// KeboolaTransaction
// ---------------------------------------------------------------------------

KeboolaTransaction &KeboolaTransaction::Get(ClientContext &context, AttachedDatabase &db) {
    return Transaction::Get(context, db).Cast<KeboolaTransaction>();
}

// ---------------------------------------------------------------------------
// KeboolaTransactionManager
// ---------------------------------------------------------------------------

KeboolaTransactionManager::KeboolaTransactionManager(AttachedDatabase &db)
    : TransactionManager(db) {}

Transaction &KeboolaTransactionManager::StartTransaction(ClientContext &context) {
    auto txn = make_uniq<KeboolaTransaction>(*this, context);
    auto &result = *txn;
    lock_guard<mutex> lk(transaction_lock_);
    transactions_[result] = std::move(txn);
    return result;
}

ErrorData KeboolaTransactionManager::CommitTransaction(ClientContext &context,
                                                        Transaction &transaction) {
    lock_guard<mutex> lk(transaction_lock_);
    transactions_.erase(transaction);
    return ErrorData();
}

void KeboolaTransactionManager::RollbackTransaction(Transaction &transaction) {
    lock_guard<mutex> lk(transaction_lock_);
    transactions_.erase(transaction);
}

} // namespace duckdb
