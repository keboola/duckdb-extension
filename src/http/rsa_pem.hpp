#pragma once

// Self-contained helpers to serialize an RSA public key (raw big-endian
// modulus + exponent) into a SubjectPublicKeyInfo PEM string — the format the
// Keboola Workspace API expects in the `publicKey` field.
//
// This exists because the platforms generate the key pair with different
// crypto backends (OpenSSL on POSIX, CNG/BCrypt on Windows) but both can
// export the raw modulus/exponent, and hand-rolling ~60 lines of DER beats
// dragging in a PEM writer per backend. No dependencies beyond <string>.

#include <algorithm>
#include <cstddef>
#include <string>

namespace duckdb {
namespace keboola_rsa {

//! Standard Base64 (RFC 4648, with padding, no line breaks).
inline std::string Base64Encode(const std::string &in) {
    static const char *ALPHABET =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    size_t i = 0;
    while (i + 3 <= in.size()) {
        unsigned v = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8) |
                     static_cast<unsigned char>(in[i + 2]);
        out += ALPHABET[(v >> 18) & 0x3F];
        out += ALPHABET[(v >> 12) & 0x3F];
        out += ALPHABET[(v >> 6) & 0x3F];
        out += ALPHABET[v & 0x3F];
        i += 3;
    }
    size_t rest = in.size() - i;
    if (rest == 1) {
        unsigned v = static_cast<unsigned char>(in[i]) << 16;
        out += ALPHABET[(v >> 18) & 0x3F];
        out += ALPHABET[(v >> 12) & 0x3F];
        out += "==";
    } else if (rest == 2) {
        unsigned v = (static_cast<unsigned char>(in[i]) << 16) |
                     (static_cast<unsigned char>(in[i + 1]) << 8);
        out += ALPHABET[(v >> 18) & 0x3F];
        out += ALPHABET[(v >> 12) & 0x3F];
        out += ALPHABET[(v >> 6) & 0x3F];
        out += '=';
    }
    return out;
}

//! Append a DER definite-form length (short or long form).
inline void AppendDerLength(std::string &out, size_t len) {
    if (len < 0x80) {
        out += static_cast<char>(len);
        return;
    }
    // Long form: count the bytes needed for len, big-endian.
    char tmp[8];
    int n = 0;
    size_t v = len;
    while (v > 0) {
        tmp[n++] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
    out += static_cast<char>(0x80 | n);
    for (int j = n - 1; j >= 0; --j) {
        out += tmp[j];
    }
}

//! Append a DER INTEGER from an unsigned big-endian byte string: strips
//! redundant leading zeros, then prepends 0x00 when the MSB is set so the
//! value stays non-negative.
inline void AppendDerUInt(std::string &out, const std::string &be_bytes) {
    size_t start = 0;
    while (start + 1 < be_bytes.size() && be_bytes[start] == '\0') {
        start++;
    }
    bool pad = !be_bytes.empty() &&
               (static_cast<unsigned char>(be_bytes[start]) & 0x80) != 0;
    size_t body_len = (be_bytes.size() - start) + (pad ? 1 : 0);
    out += '\x02';
    AppendDerLength(out, body_len);
    if (pad) {
        out += '\0';
    }
    out.append(be_bytes, start, be_bytes.size() - start);
}

//! Build the PEM ("-----BEGIN PUBLIC KEY-----", SubjectPublicKeyInfo /
//! RFC 5280) representation of an RSA public key given its raw big-endian
//! modulus and public exponent.
inline std::string BuildRsaSubjectPublicKeyInfoPem(const std::string &modulus,
                                                   const std::string &exponent) {
    if (modulus.empty() || exponent.empty()) {
        return "";
    }
    // RSAPublicKey ::= SEQUENCE { modulus INTEGER, publicExponent INTEGER }
    std::string rsa_key_body;
    AppendDerUInt(rsa_key_body, modulus);
    AppendDerUInt(rsa_key_body, exponent);
    std::string rsa_key;
    rsa_key += '\x30';
    AppendDerLength(rsa_key, rsa_key_body.size());
    rsa_key += rsa_key_body;

    // AlgorithmIdentifier ::= SEQUENCE { OID rsaEncryption, NULL }
    static const char ALG_ID[] = "\x30\x0d\x06\x09\x2a\x86\x48\x86\xf7\x0d\x01\x01\x01\x05\x00";
    std::string alg_id(ALG_ID, sizeof(ALG_ID) - 1);

    // subjectPublicKey BIT STRING (leading 0x00 = no unused bits)
    std::string bit_string;
    bit_string += '\x03';
    AppendDerLength(bit_string, rsa_key.size() + 1);
    bit_string += '\0';
    bit_string += rsa_key;

    // SubjectPublicKeyInfo ::= SEQUENCE { AlgorithmIdentifier, BIT STRING }
    std::string spki;
    spki += '\x30';
    AppendDerLength(spki, alg_id.size() + bit_string.size());
    spki += alg_id;
    spki += bit_string;

    std::string b64 = Base64Encode(spki);
    std::string pem = "-----BEGIN PUBLIC KEY-----\n";
    for (size_t i = 0; i < b64.size(); i += 64) {
        pem.append(b64, i, std::min<size_t>(64, b64.size() - i));
        pem += '\n';
    }
    pem += "-----END PUBLIC KEY-----\n";
    return pem;
}

} // namespace keboola_rsa
} // namespace duckdb
