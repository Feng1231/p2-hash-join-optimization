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
    if (columnar_row_count_.empty()) {
        return;
    }
    
    // Reserve space in output chunk
    output_chunk.reserve(output_chunk.size() + columnar_row_count_[0]);
    
    // Convert columnar to row-by-row format
    idx_t num_rows = columnar_row_count_[0];
    for (idx_t row = 0; row < num_rows; row++) {
        Tuple result;
        result.reserve(num_output_columns_);
        
        // Build tuple by collecting from each column
        for (idx_t col = 0; col < num_output_columns_; col++) {
            result.push_back(columnar_output_[col][row]);
        }
        
        output_chunk.emplace_back(std::move(result), INVALID_ID);
    }
    
    // Clear columnar buffers for next batch
    for (auto& col : columnar_output_) {
        col.clear();
    }
    columnar_row_count_.assign(num_output_columns_, 0);
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
            columnar_row_count_.assign(num_output_columns_, 0);
        }
    }

    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    while (true) {
        // Refill buffer if needed
        if (buffer_ptr_ == buffer_.size() && !probe_child_exhausted_) {
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        
        // No more input
        if (buffer_ptr_ == buffer_.size()) {
            // Flush remaining columnar data
            if (use_columnar_ && columnar_row_count_[0] > 0) {
                FlushColumnarToChunk(output_chunk);
                if (!output_chunk.empty()) {
                    return HAVE_MORE_OUTPUT;
                }
            }
            return EXHAUSETED;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        
        auto match_range = pointer_table_.equal_range(probe_tuple.KeyFromTuple(probe_key_attr));
        
        if (use_columnar_ && match_range.first != match_range.second) {
            // Columnar output path - store values directly in column vectors
            idx_t probe_col_offset = 0;
            idx_t build_col_offset = child_operators_[0]->GetOutputSchema().size();
            
            for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
                // Check if we need to flush (buffer full)
                if (columnar_row_count_[0] >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                    FlushColumnarToChunk(output_chunk);
                    if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                        // Output chunk is full, return
                        buffer_ptr_--;  // Reprocess current tuple next time
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
                
                // Update row counts
                for (idx_t i = 0; i < num_output_columns_; i++) {
                    columnar_row_count_[i]++;
                }
            }
        } else {
            // Row-based output path (fallback for small result sets or when columnar disabled)
            for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
                if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                    buffer_ptr_--;  // Reprocess current tuple next time
                    return HAVE_MORE_OUTPUT;
                }
                output_chunk.emplace_back(UnionTuple(probe_tuple, tuples_.begin() + match_ite->second, width_), INVALID_ID);
            }
        }
    }
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    tuples_.clear();
    pointer_table_.clear();
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
    
    // Clear columnar buffers
    for (auto& col : columnar_output_) {
        col.clear();
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
    
    pointer_table_.reserve(tuple_count_ * 2);
    
    for (idx_t i = 0; i < tuple_count_; i++) {
        pointer_table_.insert(std::make_pair(tuples_[i * width_ + build_key_attr], i * width_));
    }
}

}