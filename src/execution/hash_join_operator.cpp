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
      build_column_name_(build_column_name) {}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    if (!hash_table_built_) {
        hash_table_built_ = true;
        BuildHashTable();
    }

    auto& probe_child = child_operators_[0];
    auto probe_key_attr = probe_child->GetOutputSchema()
                          .GetKeyAttrs({probe_column_name_})[0];
    
    output_chunk.clear();
    
    // Process probe side
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill probe buffer if needed
        if (probe_buffer_ptr_ >= probe_buffer_.size() && !probe_child_exhausted_) {
            probe_buffer_.clear();
            if (probe_child->Next(probe_buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            probe_buffer_ptr_ = 0;
        }
        
        // No more data
        if (probe_buffer_ptr_ >= probe_buffer_.size()) {
            return EXHAUSETED;
        }
        
        // Get next probe tuple
        auto& probe_entry = probe_buffer_[probe_buffer_ptr_];
        auto& probe_tuple = probe_entry.first;
        data_t probe_key = probe_tuple[probe_key_attr];
        probe_buffer_ptr_++;
        
        // Probe hash table
        auto it = hash_table_.find(probe_key);
        if (it == hash_table_.end()) {
            continue;  // No match, skip
        }
        
        // Emit all matches
        for (idx_t build_idx : it->second) {
            // Build output tuple without copying build side's columnar data
            Tuple output_tuple;
            output_tuple.reserve(probe_tuple.size() + build_storage_.width);
            
            // Add probe tuple
            output_tuple.insert(output_tuple.end(), 
                               probe_tuple.begin(), 
                               probe_tuple.end());
            
            // Add build tuple from columnar storage (zero-copy for each column)
            for (idx_t i = 0; i < build_storage_.width; i++) {
                output_tuple.push_back(build_storage_.columns[i][build_idx]);
            }
            
            output_chunk.emplace_back(std::move(output_tuple), INVALID_ID);
            
            if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                return HAVE_MORE_OUTPUT;
            }
        }
    }
    
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::BuildHashTable() {
    auto& build_child = child_operators_[1];
    auto build_schema = build_child->GetOutputSchema();
    auto build_key_attr = build_schema.GetKeyAttrs({build_column_name_})[0];
    
    // Build hash table
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    
    while (state != EXHAUSETED) {
        state = build_child->Next(build_chunk);
        
        for (auto& [tuple, row_id] : build_chunk) {
            // Store tuple in columnar format
            build_storage_.add_tuple(tuple);
            idx_t tuple_idx = build_storage_.tuple_count - 1;
            
            // Get join key and add to hash table
            data_t join_key = tuple[build_key_attr];
            hash_table_[join_key].push_back(tuple_idx);
        }
    }
}

void HashJoinOperator::SelfInit() {
    build_storage_.clear();
    hash_table_.clear();
    probe_buffer_.clear();
    probe_buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_built_ = false;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

}