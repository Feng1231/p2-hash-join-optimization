#pragma once

#include "execution/operator.hpp"

#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Optimized Hash Join Operator
 * Improvements:
 * 1. Columnar storage for build side to reduce copying
 * 2. Replace unordered_multimap with map of vectors for better cache locality
 */
class HashJoinOperator : public Operator {
public:
    // Columnar storage: store each column separately for cache efficiency
    struct ColumnarStorage {
        std::vector<std::vector<data_t>> columns;
        idx_t tuple_count{0};
        idx_t width{0};
        
        void add_tuple(const Tuple& tuple) {
            if (columns.empty()) {
                width = tuple.size();
                columns.resize(width);
            }
            for (idx_t i = 0; i < width; i++) {
                columns[i].push_back(tuple[i]);
            }
            tuple_count++;
        }
        
        void clear() {
            columns.clear();
            tuple_count = 0;
            width = 0;
        }
    };

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
    
    // Columnar storage for build side (cache-friendly)
    ColumnarStorage build_storage_;
    
    // Hash table: key -> list of tuple indices (replaces unordered_multimap)
    std::unordered_map<data_t, std::vector<idx_t>> hash_table_;
    
    // Probe state
    Chunk probe_buffer_;
    idx_t probe_buffer_ptr_{0};
    bool probe_child_exhausted_{false};
    bool hash_table_built_{false};
};

}