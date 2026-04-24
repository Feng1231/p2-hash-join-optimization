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
      current_batch_count_(0) {}

static Tuple UnionTuple(const Tuple &a, const std::vector<data_t>::iterator &start, idx_t width) {
    Tuple result = a;
    result.insert(result.end(), start, start + width);
    return result;
}

void HashJoinOperator::FlushBatchOutput(Chunk &output_chunk, idx_t &output_size) {
    if (current_batch_count_ == 0) return;
    
    // Convert batch buffer to output chunks
    for (idx_t i = 0; i < current_batch_count_; i++) {
        idx_t start_offset = output_offsets_[i];
        idx_t end_offset = (i + 1 < current_batch_count_) ? output_offsets_[i + 1] : output_buffer_.size();
        
        Tuple tuple;
        tuple.reserve(end_offset - start_offset);
        for (idx_t j = start_offset; j < end_offset; j++) {
            tuple.push_back(output_buffer_[j]);
        }
        
        if (output_size == output_chunk.size()) {
            output_chunk.emplace_back(std::move(tuple), output_row_ids_[i]);
        } else {
            output_chunk[output_size].first = std::move(tuple);
            output_chunk[output_size].second = output_row_ids_[i];
        }
        output_size++;
    }
    
    // Reset batch buffers
    output_buffer_.clear();
    output_offsets_.clear();
    output_row_ids_.clear();
    current_batch_count_ = 0;
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    idx_t output_size = 0;

    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
    }

    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    while (output_size < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        if (buffer_ptr_ == buffer_.size() && !probe_child_exhausted_) {
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        if (buffer_ptr_ == buffer_.size()) {
            // Flush any remaining batch output
            FlushBatchOutput(output_chunk, output_size);
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        
        auto match_range = pointer_table_.equal_range(probe_tuple.KeyFromTuple(probe_key_attr));
        
        for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
            // Build output tuple in batch buffer
            temp_tuple_buffer_.clear();
            temp_tuple_buffer_.reserve(probe_tuple.size() + width_);
            temp_tuple_buffer_.insert(temp_tuple_buffer_.end(), probe_tuple.begin(), probe_tuple.end());
            temp_tuple_buffer_.insert(temp_tuple_buffer_.end(), 
                                    tuples_.begin() + match_ite->second,
                                    tuples_.begin() + match_ite->second + width_);
            
            // Add to batch
            output_offsets_.push_back(output_buffer_.size());
            output_buffer_.insert(output_buffer_.end(), temp_tuple_buffer_.begin(), temp_tuple_buffer_.end());
            output_row_ids_.push_back(INVALID_ID);
            current_batch_count_++;
            
            // Flush batch if full
            if (current_batch_count_ >= BATCH_SIZE) {
                FlushBatchOutput(output_chunk, output_size);
                if (output_size >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                    output_chunk.resize(output_size);
                    return HAVE_MORE_OUTPUT;
                }
            }
        }
    }
    
    FlushBatchOutput(output_chunk, output_size);
    output_chunk.resize(output_size);
    return HAVE_MORE_OUTPUT;
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
    
    // Initialize batch buffers
    output_buffer_.clear();
    output_buffer_.reserve(BATCH_SIZE * 16);  // Reserve reasonable capacity
    output_offsets_.clear();
    output_offsets_.reserve(BATCH_SIZE);
    output_row_ids_.clear();
    output_row_ids_.reserve(BATCH_SIZE);
    current_batch_count_ = 0;
    temp_tuple_buffer_.reserve(256);  // Reserve for typical tuple size
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
        pointer_table_.insert(std::make_pair(tuples_[i * width_ + build_key_attr], i * width_));
    }
}

}