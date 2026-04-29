#pragma once

#include "common/typedefs.hpp"

#include <cmath>
#include <cstdint>
#include <vector>

namespace babydb {

/**
 * A fast, cache-friendly Bloom filter for RPT-style pushdown filtering.
 *
 * Design decisions:
 *
 * 1. Backed by std::vector<uint64_t> instead of std::vector<bool>.
 *    Bit access is done via explicit shifts and masks, avoiding the
 *    per-access overhead of the bit-packing iterator in std::vector<bool>.
 *
 * 2. Capacity is always a power of two.  Probing uses (hash & mask_)
 *    instead of (hash % size_).
 *
 * 3. Hashing uses splitmix64 for the primary hash (good avalanche,
 *    fast on 64-bit hardware) and a second independent mix for h2.
 *    The k-th probe position is (h1 + i*h2) & mask_ (Kirsch-Mitzenmacher
 *    double-hashing), which produces k probes from two hash evaluations.
 *
 * 4. k (number of hash functions) is clamped to [1, 6].  The optimal-k
 *    formula can recommend large k for low false-positive rates, but
 *    beyond 6 the cache-miss cost per probe exceeds the selectivity gain.
 *
 * 5. An explicit ready_ flag.  While false, MightContain always returns
 *    true (i.e., no filtering).  This keeps the contract correct in the
 *    window between BF construction and the end of BuildHashTable.
 *    MarkReady() is called unconditionally at the end of build, so even
 *    an empty build side correctly rejects every probe tuple.
 */
class BloomFilter {
public:
    /**
     * @param expected_insertions   Upper bound on distinct keys to insert.
     * @param false_positive_rate   Target false-positive rate (e.g. 0.01).
     */
    BloomFilter(size_t expected_insertions, double false_positive_rate = 0.01)
        : ready_(false) {
        // Compute optimal bit count: m = -n * ln(p) / (ln 2)^2
        size_t m = 64;
        if (expected_insertions > 0 && false_positive_rate > 0.0 && false_positive_rate < 1.0) {
            double raw = -static_cast<double>(expected_insertions) *
                         std::log(false_positive_rate) /
                         (std::log(2.0) * std::log(2.0));
            m = static_cast<size_t>(raw);
            // Cap to avoid excessive memory: at most 64 MB worth of bits
            const size_t kMaxBits = 64ULL * 1024 * 1024 * 8;
            if (m > kMaxBits) m = kMaxBits;
            if (m < 64) m = 64;
        }

        // Round up to the next power of two so we can use mask_ instead of modulo
        size_t pow2 = 64;
        while (pow2 < m) pow2 <<= 1;
        num_bits_ = pow2;
        mask_ = pow2 - 1;

        // Allocate words (each uint64_t holds 64 bits)
        words_.assign((num_bits_ + 63) / 64, uint64_t{0});

        // Compute optimal k: k = (m/n) * ln 2, clamped to [1, 6]
        if (expected_insertions > 0) {
            double k_real = static_cast<double>(num_bits_) *
                            std::log(2.0) /
                            static_cast<double>(expected_insertions);
            size_t k = static_cast<size_t>(k_real);
            if (k < 1) k = 1;
            if (k > 6) k = 6;
            num_hash_functions_ = k;
        } else {
            num_hash_functions_ = 1;
        }
    }

    /** Insert a key into the filter. */
    void Insert(const data_t &key) {
        auto [h1, h2] = GetHashes(key);
        for (size_t i = 0; i < num_hash_functions_; ++i) {
            size_t bit_idx = (h1 + i * h2) & mask_;
            words_[bit_idx >> 6] |= (uint64_t{1} << (bit_idx & 63u));
        }
    }

    /**
     * Returns false if the key is definitely not in the set.
     * Returns true if it might be (subject to false-positive rate).
     * While ready_ is false, always returns true.
     */
    bool MightContain(const data_t &key) const {
        if (!ready_) return true;
        auto [h1, h2] = GetHashes(key);
        for (size_t i = 0; i < num_hash_functions_; ++i) {
            size_t bit_idx = (h1 + i * h2) & mask_;
            if (!(words_[bit_idx >> 6] & (uint64_t{1} << (bit_idx & 63u)))) {
                return false;
            }
        }
        return true;
    }

    /** Call once after all inserts are done.  Enables actual filtering. */
    void MarkReady() { ready_ = true; }

    /** Reset to empty (not ready) state. */
    void Clear() {
        std::fill(words_.begin(), words_.end(), uint64_t{0});
        ready_ = false;
    }

private:
    /** splitmix64 hash — good avalanche, no division. */
    static uint64_t Splitmix64(uint64_t x) {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }

    /** Produce two independent hashes from the key for Kirsch-Mitzenmacher. */
    std::pair<size_t, size_t> GetHashes(const data_t &key) const {
        // data_t is int64_t; reinterpret as uint64_t for hashing
        uint64_t u;
        __builtin_memcpy(&u, &key, sizeof(u));
        uint64_t h1 = Splitmix64(u);
        uint64_t h2 = Splitmix64(h1 ^ 0xdeadbeefcafe1234ULL);
        return {static_cast<size_t>(h1), static_cast<size_t>(h2) | 1u};  // h2 odd -> full period
    }

    std::vector<uint64_t> words_;   // bit array backed by 64-bit words
    size_t num_bits_;               // total capacity in bits (power of two)
    size_t mask_;                   // num_bits_ - 1  (used in place of modulo)
    size_t num_hash_functions_;     // k, clamped to [1, 6]
    bool ready_;                    // if false, MightContain always returns true
};

}  // namespace babydb
