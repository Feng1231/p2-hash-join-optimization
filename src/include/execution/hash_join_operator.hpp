#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Hash Join Operator with Columnar Output
 * Stores results in columnar format to reduce tuple copying
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
    
    // Convert columnar output to chunk format
    void FlushColumnarToChunk(Chunk &output_chunk);

private:
    std::string probe_column_name_;
    std::string build_column_name_;

    std::vector<data_t> tuples_;  // Flattened build tuples
    idx_t tuple_count_;
    idx_t width_;

    std::unordered_multimap<data_t, idx_t> pointer_table_;

    Chunk buffer_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
    
    // Columnar output buffers
    std::vector<std::vector<data_t>> columnar_output_;  // Each column is a vector
    std::vector<idx_t> columnar_row_count_;
    idx_t num_output_columns_;
    bool use_columnar_;
};

}