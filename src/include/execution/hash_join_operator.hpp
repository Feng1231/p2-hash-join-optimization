// hash_join_operator.hpp
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
 * 2. Cache-friendly hash table with separate chaining
 * 3. Pre-allocated memory pools
 */
class HashJoinOperator : public Operator {
public:
    // Columnar storage: store each column separately for cache efficiency
    struct ColumnarStorage {
        std::vector<std::vector<data_t>> columns;
        idx_t tuple_count{0};
        idx_t width{0};
        
        void reserve(idx_t capacity, idx_t col_width) {
            width = col_width;
            columns.resize(col_width);
            for (auto& col : columns) {
                col.reserve(capacity);
            }
        }
        
        void add_tuple(const Tuple& tuple) {
            for (idx_t i = 0; i < width; i++) {
                columns[i].push_back(tuple[i]);
            }
            tuple_count++;
        }
        
        Tuple get_tuple(idx_t index) const {
            Tuple result;
            result.reserve(width);
            for (idx_t i = 0; i < width; i++) {
                result.push_back(columns[i][index]);
            }
            return result;
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
    
    // Optimized storage: columnar format
    ColumnarStorage build_storage_;
    
    // Cache-friendly hash table: key -> list of tuple indices
    std::unordered_map<data_t, std::vector<idx_t>> hash_table_;
    
    // Probe state
    Chunk probe_buffer_;
    idx_t probe_buffer_ptr_{0};
    bool probe_child_exhausted_{false};
    bool hash_table_built_{false};
};

}