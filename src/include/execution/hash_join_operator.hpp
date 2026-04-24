#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

namespace babydb {

/**
 * Hash Join Operator with Bloom Filter Optimization
 * Uses Bloom filter to quickly reject non-matching probe tuples
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
    void BuildBloomFilter();
    
    // Hash functions for Bloom filter
    size_t Hash1(const data_t& key) const;
    size_t Hash2(const data_t& key) const;
    size_t Hash3(const data_t& key) const;
    size_t Hash4(const data_t& key) const;
    
    // Fast string hash (FNV-1a for better distribution)
    static size_t FNV1aHash(const data_t& key);

private:
    std::string probe_column_name_;
    std::string build_column_name_;

    std::vector<data_t> tuples_;
    idx_t tuple_count_;
    idx_t width_;

    std::unordered_multimap<data_t, idx_t> pointer_table_;

    Chunk buffer_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
    
    // Bloom filter
    std::vector<bool> bloom_filter_;
    size_t bloom_filter_size_;
    bool use_bloom_filter_;
    
    // // Statistics for debugging (optional)
    // size_t bloom_filter_hits_;
    // size_t bloom_filter_misses_;
    // size_t hash_table_probes_;
};

}