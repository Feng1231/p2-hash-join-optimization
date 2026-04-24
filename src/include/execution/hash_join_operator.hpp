#pragma once

#include "execution/operator.hpp"
#include "execution/bloom_filter.hpp"
#include <memory>
#include <unordered_map>
#include <vector>

namespace babydb {

class HashJoinOptimizer;

class HashJoinOperator : public Operator {
public:
    HashJoinOperator(const ExecutionContext &exec_ctx,
                     const std::shared_ptr<Operator> &probe_child_operator,
                     const std::shared_ptr<Operator> &build_child_operator,
                     const std::string &probe_column_name,
                     const std::string &build_column_name);
    
    ~HashJoinOperator();

    OperatorState Next(Chunk &output_chunk) override;

    const std::string& GetBuildColumnName() const { return build_column_name_; }
    
    // Helper to check if this is a hash join (for RPT traversal)
    bool IsHashJoin() const { return true; }
    
    const std::shared_ptr<Operator>& GetProbeChild() const { return child_operators_[0]; }
    const std::shared_ptr<Operator>& GetBuildChild() const { return child_operators_[1]; }

protected:
    void SelfInit() override;
    void SelfCheck() override;

private:
    void BuildHashTable();
    
    std::string probe_column_name_;
    
    std::string build_column_name_;

    std::unique_ptr<HashJoinOptimizer> optimizer_;
    
    // Legacy members for compatibility
    idx_t tuple_count_;

    idx_t width_;

    std::vector<data_t> tuples_;

    std::unordered_multimap<std::string, idx_t> pointer_table_;

    Chunk buffer_;

    idx_t buffer_ptr_;

    bool probe_child_exhausted_;

    bool hash_table_build_;

    bool use_bloom_;

    BloomFilter bloom_filter_;
};

} // namespace babydb