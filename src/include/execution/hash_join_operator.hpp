#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Hash Join Operator with Batch Output Optimization
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
    
    // Batch output helper
    void FlushBatchOutput(Chunk &output_chunk, idx_t &output_size);

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
    
    // Batch output buffers
    static constexpr idx_t BATCH_SIZE = 1024;
    std::vector<data_t> output_buffer_;  // Contiguous data for multiple tuples
    std::vector<idx_t> output_offsets_;  // Offsets into output_buffer_
    std::vector<idx_t> output_row_ids_;  // Row IDs for each output tuple
    idx_t current_batch_count_;
    
    // Reusable temporary buffer for building output
    std::vector<data_t> temp_tuple_buffer_;
};

}