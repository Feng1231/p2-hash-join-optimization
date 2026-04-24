#include "execution/hash_join_operator.hpp"

#include "common/config.hpp"
#include <functional>

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
      bloom_filter_size_(0),
      use_bloom_filter_(false)
    //   bloom_filter_hits_(0),
    //   bloom_filter_misses_(0),
    //   hash_table_probes_(0) 
      {}

// Simple integer hash functions - much faster than string hashing
size_t HashJoinOperator::Hash1(const data_t& key) const {
    // Thomas Wang's integer hash - good for 64-bit integers
    size_t hash = static_cast<size_t>(key);
    hash = (hash ^ 61) ^ (hash >> 16);
    hash = hash + (hash << 3);
    hash = hash ^ (hash >> 4);
    hash = hash * 0x27d4eb2d;
    hash = hash ^ (hash >> 15);
    return hash;
}

size_t HashJoinOperator::Hash2(const data_t& key) const {
    // Murmur-inspired mixing
    size_t hash = static_cast<size_t>(key);
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

size_t HashJoinOperator::Hash3(const data_t& key) const {
    // Simple but effective - multiply by large prime and xor
    return static_cast<size_t>(key) * 0x9e3779b97f4a7c15ULL;
}

void HashJoinOperator::BuildBloomFilter() {
    if (!use_bloom_filter_ || tuple_count_ == 0) {
        return;
    }
    
    // For integer keys, bloom filter helps even with smaller tables
    // Size: next power of 2 for fast modulo using bitwise AND
    bloom_filter_size_ = 16;
    while (bloom_filter_size_ < tuple_count_ * 4) {  // 4 bits per key
        bloom_filter_size_ <<= 1;
    }
    
    bloom_filter_.assign(bloom_filter_size_, false);
    
    const idx_t build_key_attr = child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_})[0];
    
    // Use 2 hash functions for integers (sufficient due to good distribution)
    for (idx_t i = 0; i < tuple_count_; i++) {
        data_t key = tuples_[i * width_ + build_key_attr];
        
        size_t h1 = Hash1(key) & (bloom_filter_size_ - 1);
        size_t h2 = Hash2(key) & (bloom_filter_size_ - 1);
        
        bloom_filter_[h1] = true;
        bloom_filter_[h2] = true;
    }
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    idx_t output_size = 0;

    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
        BuildBloomFilter();
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
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        
        data_t probe_key = probe_tuple.KeyFromTuple(probe_key_attr);
        
        // Bloom filter check - fast integer operations
        if (use_bloom_filter_ && bloom_filter_size_ > 0) {
            size_t h1 = Hash1(probe_key) & (bloom_filter_size_ - 1);
            size_t h2 = Hash2(probe_key) & (bloom_filter_size_ - 1);
            
            // Quick reject if any bit is 0
            if (!bloom_filter_[h1] || !bloom_filter_[h2]) {
                // bloom_filter_misses_++;
                continue;  // Definitely not in build side
            }
            // bloom_filter_hits_++;
        }
        
        // Only probe hash table if bloom filter says "maybe present"
        // hash_table_probes_++;
        auto match_range = pointer_table_.equal_range(probe_key);
        
        for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
            if (output_size == output_chunk.size()) {
                Tuple result = probe_tuple;
                result.insert(result.end(), 
                            tuples_.begin() + match_ite->second, 
                            tuples_.begin() + match_ite->second + width_);
                output_chunk.emplace_back(std::move(result), INVALID_ID);
            } else {
                output_chunk[output_size].first = probe_tuple;
                output_chunk[output_size].first.insert(
                    output_chunk[output_size].first.end(),
                    tuples_.begin() + match_ite->second,
                    tuples_.begin() + match_ite->second + width_);
                output_chunk[output_size].second = INVALID_ID;
            }
            output_size++;
        }
    }
    
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
    bloom_filter_.clear();
    bloom_filter_size_ = 0;
    use_bloom_filter_ = true;
    // bloom_filter_hits_ = 0;
    // bloom_filter_misses_ = 0;
    // hash_table_probes_ = 0;
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
        data_t key = tuples_[offset + build_key_attr];
        pointer_table_.insert(std::make_pair(key, offset));
    }
}

}