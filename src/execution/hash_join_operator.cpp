#include "execution/hash_join_operator.hpp"

#include "common/config.hpp"

namespace babydb {

HashJoinOperator::HashJoinOperator(const ExecutionContext &exec_ctx,
                                   const std::shared_ptr<Operator> &probe_child_operator,
                                   const std::shared_ptr<Operator> &build_child_operator,
                                   const std::string &probe_column_name,
                                   const std::string &build_column_name)
    : Operator(exec_ctx, {probe_child_operator, build_child_operator}),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name),
      tuple_count_(0),
      width_(0),
      probe_chunk_pos_(0),
      probe_child_exhausted_(false),
      hash_table_build_(false) {
    // Pre-reserve reasonable capacity for temp buffer
    temp_buffer_.reserve(256);
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
    }

    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    // Reserve capacity in output chunk to avoid repeated resizing
    output_chunk.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);
    
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill probe chunk if needed
        if (probe_chunk_pos_ >= probe_chunk_.size() && !probe_child_exhausted_) {
            probe_chunk_.clear();
            if (probe_child_operator->Next(probe_chunk_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            probe_chunk_pos_ = 0;
        }
        
        // Check if we're done
        if (probe_chunk_pos_ >= probe_chunk_.size()) {
            return EXHAUSETED;
        }
        
        // Get current probe tuple (by reference, no copy)
        auto &probe_entry = probe_chunk_[probe_chunk_pos_];
        probe_chunk_pos_++;
        
        auto &probe_tuple = probe_entry.first;
        auto probe_key = probe_tuple.KeyFromTuple(probe_key_attr);
        
        // Find matching build tuples
        auto match_range = pointer_table_.equal_range(probe_key);
        
        for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
            // Build output tuple directly in output_chunk without temporary
            // This eliminates the UnionTuple temporary creation
            
            if (output_chunk.size() == output_chunk.capacity()) {
                // If we're at capacity, break to return current batch
                break;
            }
            
            // Add new entry to output chunk
            output_chunk.emplace_back();
            auto &output_entry = output_chunk.back();
            auto &output_tuple = output_entry.first;
            output_entry.second = INVALID_ID;
            
            // Construct output tuple efficiently:
            // 1. First copy probe tuple (must copy since we need to own the data)
            output_tuple = probe_tuple;
            
            // 2. Reserve space for build tuple to avoid multiple reallocations
            output_tuple.reserve(output_tuple.size() + width_);
            
            // 3. Append build tuple data directly
            auto build_start = tuples_.begin() + match_ite->second;
            output_tuple.insert(output_tuple.end(), build_start, build_start + width_);
        }
    }
    
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    tuples_.clear();
    pointer_table_.clear();
    probe_chunk_.clear();
    probe_chunk_pos_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
    temp_buffer_.clear();
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

void HashJoinOperator::BuildHashTable() {
    auto &build_child_operator = child_operators_[1];
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    const idx_t build_key_attr = build_child_operator->GetOutputSchema().GetKeyAttrs({build_column_name_})[0];
    
    // First pass: collect all build side tuples
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            // Flatten tuple into contiguous storage
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }
    
    // Reserve space in hash table to reduce rehashing
    pointer_table_.reserve(tuple_count_);
    
    // Second pass: build hash table
    for (idx_t i = 0; i < tuple_count_; i++) {
        idx_t offset = i * width_;
        data_t key = tuples_[offset + build_key_attr];
        pointer_table_.emplace(key, offset);  // Use emplace instead of make_pair
    }
}

}