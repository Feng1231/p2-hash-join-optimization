#include "execution/hash_join_operator.hpp"
#include "common/config.hpp"
#include <algorithm>

namespace babydb {

HashJoinOperator::HashJoinOperator(const ExecutionContext &exec_ctx,
                                   const std::shared_ptr<Operator> &probe_child_operator,
                                   const std::shared_ptr<Operator> &build_child_operator,
                                   const std::string &probe_column_name,
                                   const std::string &build_column_name)
    : Operator(exec_ctx, {probe_child_operator, build_child_operator}),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name),
      probe_buffer_pos_(0),
      probe_exhausted_(false),
      hash_table_built_(false),
      build_size_(0),
      probe_tuples_processed_(0),
      probe_tuples_filtered_(0),
      probe_tuples_matched_(0) {}

static Tuple UnionTupleNoCopy(const Tuple& a, const Tuple& b) {
    Tuple result;
    result.reserve(a.size() + b.size());
    result.insert(result.end(), a.begin(), a.end());
    result.insert(result.end(), b.begin(), b.end());
    return result;
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    
    if (!hash_table_built_) {
        hash_table_built_ = true;
        BuildHashTable();
        
        // Only use Bloom filter if build side is significantly smaller
        // (at least 10x smaller than expected probe side)
        build_size_ = build_tuples_.Size();
        if (build_size_ > 0 && build_size_ < 10000) {  // Small build side threshold
            bloom_filter_ = std::make_unique<BloomFilter>(build_size_);
            // Insert all build keys into Bloom filter
            for (const auto& pair : hash_table_) {
                bloom_filter_->Insert(pair.first);
            }
        }
    }
    
    auto& probe_child = child_operators_[0];
    auto probe_key_attr = probe_child->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    size_t output_size = 0;
    const size_t chunk_size = exec_ctx_.config_.CHUNK_SUGGEST_SIZE;
    
    while (output_size < chunk_size) {
        // Refill probe buffer if needed
        if (probe_buffer_pos_ >= probe_buffer_.size() && !probe_exhausted_) {
            probe_buffer_.clear();
            if (probe_child->Next(probe_buffer_) == EXHAUSETED) {
                probe_exhausted_ = true;
            }
            probe_buffer_pos_ = 0;
        }
        
        if (probe_buffer_pos_ >= probe_buffer_.size()) {
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }
        
        // Process one probe tuple
        auto& probe_tuple = probe_buffer_[probe_buffer_pos_].first;
        auto probe_key = probe_tuple.KeyFromTuple(probe_key_attr);
        probe_buffer_pos_++;
        probe_tuples_processed_++;
        
        // Bloom filter check - fast rejection
        if (bloom_filter_ && !bloom_filter_->MightContain(probe_key)) {
            probe_tuples_filtered_++;
            continue;  // Skip this tuple entirely
        }
        
        // Actual hash table lookup
        auto match_range = hash_table_.equal_range(probe_key);
        bool has_match = false;
        
        for (auto it = match_range.first; it != match_range.second; ++it) {
            has_match = true;
            auto build_tuple = build_tuples_.GetTuple(it->second);
            
            if (output_size >= output_chunk.size()) {
                output_chunk.emplace_back(
                    UnionTupleNoCopy(probe_tuple, build_tuple), 
                    INVALID_ID
                );
            } else {
                output_chunk[output_size].first = UnionTupleNoCopy(probe_tuple, build_tuple);
                output_chunk[output_size].second = INVALID_ID;
            }
            output_size++;
        }
        
        if (has_match) {
            probe_tuples_matched_++;
        }
    }
    
    output_chunk.resize(output_size);
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    build_tuples_ = ColumnarStorage();
    hash_table_.clear();
    bloom_filter_.reset();
    probe_buffer_.clear();
    probe_buffer_pos_ = 0;
    probe_exhausted_ = false;
    hash_table_built_ = false;
    build_size_ = 0;
    probe_tuples_processed_ = 0;
    probe_tuples_filtered_ = 0;
    probe_tuples_matched_ = 0;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

void HashJoinOperator::BuildHashTable() {
    auto& build_child = child_operators_[1];
    auto build_schema = build_child->GetOutputSchema();
    auto build_key_attr = build_schema.GetKeyAttrs({build_column_name_})[0];
    size_t tuple_width = build_schema.size();
    
    // First pass: count tuples for pre-allocation
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    size_t tuple_count = 0;
    
    while (state != EXHAUSETED) {
        state = build_child->Next(build_chunk);
        tuple_count += build_chunk.size();
    }
    
    // Pre-allocate storage
    build_tuples_.Reserve(tuple_count, tuple_width);
    hash_table_.reserve(tuple_count);
    
    // Re-scan and actually store tuples
    build_child->Init();  // Reset child operator
    state = HAVE_MORE_OUTPUT;
    
    while (state != EXHAUSETED) {
        build_chunk.clear();
        state = build_child->Next(build_chunk);
        
        for (auto& chunk_row : build_chunk) {
            auto& tuple = chunk_row.first;
            build_tuples_.AddTuple(tuple);
            
            data_t key = tuple.KeyFromTuple(build_key_attr);
            hash_table_.insert({key, build_tuples_.Size() - 1});
        }
    }
}

}