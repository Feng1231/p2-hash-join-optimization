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
      probe_pos_(0),
      probe_exhausted_(false),
      hash_table_built_(false) {}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    output_chunk.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);

    if (!hash_table_built_) {
        hash_table_built_ = true;
        BuildHashTable();
    }

    auto &probe_child = child_operators_[0];
    auto probe_key_attr = probe_child->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill probe buffer
        if (probe_pos_ >= probe_buffer_.size()) {
            if (probe_exhausted_) {
                break;
            }
            probe_buffer_.clear();
            if (probe_child->Next(probe_buffer_) == EXHAUSETED) {
                probe_exhausted_ = true;
                break;
            }
            probe_pos_ = 0;
        }
        
        auto& probe_entry = probe_buffer_[probe_pos_];
        probe_pos_++;
        
        data_t probe_key = (*probe_entry.tuple)[probe_key_attr];
        auto match_range = pointer_table_.equal_range(probe_key);
        
        for (auto it = match_range.first; it != match_range.second; ++it) {
            if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                break;
            }
            
            idx_t build_idx = it->second;
            const SharedTuple& build_tuple = build_tuples_[build_idx];
            
            // Create combined tuple - this is the only copy in the join
            auto combined = std::make_shared<Tuple>();
            combined->reserve(probe_entry.tuple->size() + build_tuple->size());
            combined->insert(combined->end(), probe_entry.tuple->begin(), probe_entry.tuple->end());
            combined->insert(combined->end(), build_tuple->begin(), build_tuple->end());
            
            output_chunk.emplace_back(std::move(combined), INVALID_ID);
        }
    }
    
    return output_chunk.empty() ? EXHAUSETED : HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    build_tuples_.clear();
    pointer_table_.clear();
    probe_buffer_.clear();
    probe_pos_ = 0;
    probe_exhausted_ = false;
    hash_table_built_ = false;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

void HashJoinOperator::BuildHashTable() {
    auto &build_child = child_operators_[1];
    const idx_t build_key_attr = build_child->GetOutputSchema().GetKeyAttrs({build_column_name_})[0];
    
    Chunk build_chunk;
    OperatorState state = HAVE_MORE_OUTPUT;
    
    // Collect build tuples - store shared_ptr (no copy of tuple data)
    while (state != EXHAUSETED) {
        state = build_child->Next(build_chunk);
        for (auto& entry : build_chunk) {
            build_tuples_.push_back(std::move(entry.tuple));
            tuple_count_++;
        }
    }
    
    if (tuple_count_ > 0) {
        width_ = (*build_tuples_[0]).size();
    }
    
    // Build hash table
    pointer_table_.reserve(tuple_count_);
    for (idx_t i = 0; i < tuple_count_; i++) {
        data_t key = (*build_tuples_[i])[build_key_attr];
        pointer_table_.emplace(key, i);
    }
}

}