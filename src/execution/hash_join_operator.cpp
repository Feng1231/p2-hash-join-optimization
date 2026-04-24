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
      buffer_ptr_(0),
      probe_child_exhausted_(false),
      hash_table_build_(false),
      num_output_columns_(0),
      use_columnar_(true) {}

static Tuple UnionTuple(const Tuple &a, const std::vector<data_t>::iterator &start, idx_t width) {
    Tuple result = a;
    result.insert(result.end(), start, start + width);
    return result;
}

void HashJoinOperator::FlushColumnarToChunk(Chunk &output_chunk) {
    if (columnar_output_.empty() || columnar_output_[0].empty()) {
        return;
    }
    
    idx_t num_rows = columnar_output_[0].size();
    
    // Pre-allocate exact size needed
    output_chunk.reserve(output_chunk.size() + num_rows);
    
    // Convert columnar to row format
    for (idx_t row = 0; row < num_rows; row++) {
        Tuple result;
        result.reserve(num_output_columns_);
        
        for (idx_t col = 0; col < num_output_columns_; col++) {
            result.push_back(columnar_output_[col][row]);
        }
        
        output_chunk.emplace_back(std::move(result), INVALID_ID);
        
        // Early exit if output chunk is full
        if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
            // Keep remaining rows in columnar buffers for next call
            // Remove the rows we've already output
            for (idx_t col = 0; col < num_output_columns_; col++) {
                columnar_output_[col].erase(columnar_output_[col].begin(), 
                                           columnar_output_[col].begin() + row + 1);
            }
            return;
        }
    }
    
    // Clear columnar buffers after flushing all rows
    for (auto& col : columnar_output_) {
        col.clear();
    }
    columnar_output_.clear();
    columnar_output_.shrink_to_fit();
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
        
        // Setup columnar output
        if (use_columnar_) {
            auto& probe_schema = child_operators_[0]->GetOutputSchema();
            auto& build_schema = child_operators_[1]->GetOutputSchema();
            num_output_columns_ = probe_schema.size() + build_schema.size();
            columnar_output_.resize(num_output_columns_);
            // Reserve reasonable capacity
            for (auto& col : columnar_output_) {
                col.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);
            }
        }
    }

    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill buffer if needed
        if (buffer_ptr_ == buffer_.size() && !probe_child_exhausted_) {
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        
        // No more input - flush remaining columnar data
        if (buffer_ptr_ == buffer_.size()) {
            if (use_columnar_ && !columnar_output_.empty() && !columnar_output_[0].empty()) {
                FlushColumnarToChunk(output_chunk);
            }
            if (output_chunk.empty()) {
                return EXHAUSETED;
            }
            return HAVE_MORE_OUTPUT;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        
        auto match_range = pointer_table_.equal_range(probe_tuple.KeyFromTuple(probe_key_attr));
        
        if (match_range.first == match_range.second) {
            continue;  // No matches
        }
        
        if (use_columnar_) {
            // Columnar output path
            idx_t probe_col_offset = 0;
            idx_t build_col_offset = child_operators_[0]->GetOutputSchema().size();
            
            for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
                // Check if columnar buffer is getting full
                if (columnar_output_[0].size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                    // Flush current batch
                    FlushColumnarToChunk(output_chunk);
                    if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                        // Output chunk is full, reprocess current tuple next time
                        buffer_ptr_--;
                        return HAVE_MORE_OUTPUT;
                    }
                }
                
                // Add probe tuple columns
                for (idx_t i = 0; i < probe_tuple.size(); i++) {
                    columnar_output_[probe_col_offset + i].push_back(probe_tuple[i]);
                }
                
                // Add build tuple columns
                auto build_start = tuples_.begin() + match_ite->second;
                for (idx_t i = 0; i < width_; i++) {
                    columnar_output_[build_col_offset + i].push_back(*(build_start + i));
                }
            }
        } else {
            // Row-based output path (fallback)
            for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
                if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                    buffer_ptr_--;  // Reprocess current tuple next time
                    return HAVE_MORE_OUTPUT;
                }
                output_chunk.emplace_back(UnionTuple(probe_tuple, tuples_.begin() + match_ite->second, width_), INVALID_ID);
            }
        }
    }
    
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    tuples_.clear();
    tuples_.shrink_to_fit();
    pointer_table_.clear();
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
    
    // Clear columnar buffers and free memory
    for (auto& col : columnar_output_) {
        col.clear();
        std::vector<data_t>().swap(col);  // Force deallocation
    }
    columnar_output_.clear();
    columnar_row_count_.clear();
    num_output_columns_ = 0;
    use_columnar_ = true;
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
    
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }
    
    pointer_table_.reserve(tuple_count_);
    
    for (idx_t i = 0; i < tuple_count_; i++) {
        idx_t offset = i * width_;
        pointer_table_.insert(std::make_pair(tuples_[offset + build_key_attr], offset));
    }
}

}