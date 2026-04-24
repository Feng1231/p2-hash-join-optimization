#pragma once

#include "execution/operator.hpp"
#include "execution/bloom_filter.hpp"
#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Hash Join Operator with Bloom Filter Optimization
 * Uses Bloom filter to early reject probe tuples that cannot match
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
    
    // Store build side tuples in columnar format for cache efficiency
    struct ColumnarStorage {
        std::vector<data_t> columns_;  // Flattened column storage
        std::vector<size_t> offsets_;   // Start offset for each tuple
        size_t tuple_width_;
        
        ColumnarStorage() : tuple_width_(0) {}
        
        void Reserve(size_t tuple_count, size_t width) {
            columns_.reserve(tuple_count * width);
            offsets_.reserve(tuple_count);
            tuple_width_ = width;
        }
        
        void AddTuple(const Tuple& tuple) {
            offsets_.push_back(columns_.size());
            columns_.insert(columns_.end(), tuple.begin(), tuple.end());
        }
        
        Tuple GetTuple(size_t index) const {
            size_t start = offsets_[index];
            return Tuple(columns_.begin() + start, columns_.begin() + start + tuple_width_);
        }
        
        size_t Size() const { return offsets_.size(); }
    };

private:
    std::string probe_column_name_;
    std::string build_column_name_;
    
    ColumnarStorage build_tuples_;
    
    // Hash table mapping key -> list of tuple indices
    std::unordered_multimap<data_t, size_t> hash_table_;
    
    // Bloom filter for early pruning
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    // Probe batching for better cache locality
    struct ProbeBatch {
        std::vector<size_t> tuple_indices;
        std::vector<data_t> keys;
    };
    
    Chunk probe_buffer_;
    size_t probe_buffer_pos_;
    bool probe_exhausted_;
    bool hash_table_built_;
    
    // Statistics for adaptive optimization
    size_t build_size_;
    size_t probe_tuples_processed_;
    size_t probe_tuples_filtered_;
    size_t probe_tuples_matched_;
};

}