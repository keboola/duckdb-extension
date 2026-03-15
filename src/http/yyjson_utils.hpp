#pragma once

// yyjson is bundled with DuckDB.
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

#include "duckdb/common/exception.hpp"

#include <string>

namespace duckdb {

//! RAII wrapper so we never forget to free a yyjson document.
struct YyjsonDoc {
    yyjson_doc *doc = nullptr;
    explicit YyjsonDoc(yyjson_doc *d) : doc(d) {}
    ~YyjsonDoc() {
        if (doc) {
            yyjson_doc_free(doc);
        }
    }
    // movable, non-copyable
    YyjsonDoc(YyjsonDoc &&other) noexcept : doc(other.doc) { other.doc = nullptr; }
    YyjsonDoc &operator=(YyjsonDoc &&other) noexcept {
        if (this != &other) {
            if (doc) yyjson_doc_free(doc);
            doc = other.doc;
            other.doc = nullptr;
        }
        return *this;
    }
    YyjsonDoc(const YyjsonDoc &) = delete;
    YyjsonDoc &operator=(const YyjsonDoc &) = delete;

    yyjson_doc *operator->() const { return doc; }
    bool ok() const { return doc != nullptr; }
};

//! Parse a JSON string, throwing IOException on failure.
inline YyjsonDoc ParseJson(const std::string &json_str, const std::string &context_hint) {
    yyjson_read_err err;
    yyjson_doc *doc = yyjson_read_opts(const_cast<char *>(json_str.c_str()),
                                       json_str.size(),
                                       YYJSON_READ_NOFLAG,
                                       nullptr,
                                       &err);
    if (!doc) {
        throw IOException("Keboola: failed to parse JSON response (%s): %s",
                          context_hint, err.msg ? err.msg : "unknown error");
    }
    return YyjsonDoc(doc);
}

} // namespace duckdb
