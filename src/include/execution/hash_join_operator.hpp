#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

namespace babydb {

/**
 * Simple Bloom Filter for fast negative filtering
 */
class BloomFilter {
public:
    BloomFilter(size_t expected_insertions = 1024, double false_positive_rate = 0.01);
    
    void insert(const data_t& key);
    bool might_contain(const data_t& key) const;
    void clear();
    
private:
    std::vector<bool> bits_;
    size_t num_hash_functions_;
    
    std::array<size_t, 2> get_hashes(const data_t& key) const;
    static size_t optimal_bit_count(size_t n, double p);
    static size_t optimal_hash_count(size_t m, size_t n);
};

/**
 * Hash Join Operator
 * We only support equavilant join on one column.
 * The output schema is just the union of the input's schema.
 * 
 * Optimized with Bloom filter for early rejection of non-matching tuples.
 */
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(const ExecutionContext &exec_ctx,
                     const std::shared_ptr<Operator> &probe_child_operator,
                     const std::shared_ptr<Operator> &build_child_operator,
                     const std::string &probe_column_name,
                     const std::string &build_column_name);

    ~HashJoinOperator() override = default;
    
    OperatorState Next(Chunk &output_chunk) override;

    void SelfInit() override;

    void SelfCheck() override;

private:
    void BuildHashTable();

private:
    std::string probe_column_name_;
    std::string build_column_name_;

    // Columnar storage for build-side tuples
    std::vector<data_t> tuples_;
    idx_t tuple_count_;
    idx_t width_;

    // Hash table mapping key -> offset in tuples_
    std::unordered_multimap<data_t, idx_t> pointer_table_;
    
    // Bloom filter for fast negative filtering
    std::unique_ptr<BloomFilter> bloom_filter_;

    // Probe-side buffering
    Chunk buffer_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
};

}