#pragma once

#include "execution/operator.hpp"
#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

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

    // Store build side as shared tuples (zero-copy)
    std::vector<SharedTuple> build_tuples_;
    idx_t tuple_count_;
    idx_t width_;

    // Hash table: key -> index into build_tuples_
    std::unordered_multimap<data_t, idx_t> pointer_table_;

    // Reusable buffers
    Chunk probe_buffer_;
    size_t probe_pos_;
    bool probe_exhausted_;
    bool hash_table_built_;
};

} 