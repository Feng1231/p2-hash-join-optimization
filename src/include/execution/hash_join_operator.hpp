#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Hash Join Operator
 * Optimized version with custom hash table for better cache locality
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
    
    // Helper functions for custom hash table
    size_t HashKey(const data_t& key) const;
    void InsertIntoHashTable(const data_t& key, idx_t offset);

private:
    std::string probe_column_name_;
    std::string build_column_name_;

    std::vector<data_t> tuples_;
    idx_t tuple_count_;
    idx_t width_;

    // Custom hash table for better cache locality
    struct HashEntry {
        data_t key;
        idx_t tuple_offset;
        idx_t next;  // next entry index (-1 for end)
        HashEntry() : key(), tuple_offset(0), next(-1) {}
        HashEntry(const data_t& k, idx_t off, idx_t n) : key(k), tuple_offset(off), next(n) {}
    };
    
    std::vector<HashEntry> hash_entries_;
    std::vector<idx_t> hash_buckets_;
    idx_t hash_mask_;

    Chunk buffer_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
    
    // For iteration
    idx_t current_bucket_;
    idx_t current_entry_;
};

}