#include "execution/hash_join_operator.hpp"
#include  <array>
#include "common/config.hpp"
#include <cmath>

namespace babydb {

// ============== BloomFilter Implementation ==============

BloomFilter::BloomFilter(size_t expected_insertions, double false_positive_rate)
    : bits_(optimal_bit_count(expected_insertions, false_positive_rate), false),
      num_hash_functions_(optimal_hash_count(bits_.size(), expected_insertions)) {}

size_t BloomFilter::optimal_bit_count(size_t n, double p) {
    // m = -n * ln(p) / (ln(2)^2)
    return std::max<size_t>(64, static_cast<size_t>(-n * std::log(p) / (std::log(2) * std::log(2))));
}

size_t BloomFilter::optimal_hash_count(size_t m, size_t n) {
    // k = (m/n) * ln(2)
    return std::max<size_t>(1, std::min<size_t>(10, static_cast<size_t>(m * std::log(2) / n)));
}

std::array<size_t, 2> BloomFilter::get_hashes(const data_t& key) const {
    std::hash<data_t> hasher;
    size_t h1 = hasher(key);
    // Double hashing: h2 = h1 + (h1 >> 33) + 1
    // This gives good distribution without needing a second hash function
    size_t h2 = h1 + (h1 >> 33) + 1;
    return {h1, h2};
}

void BloomFilter::insert(const data_t& key) {
    auto [h1, h2] = get_hashes(key);
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t index = (h1 + i * h2) % bits_.size();
        bits_[index] = true;
    }
}

bool BloomFilter::might_contain(const data_t& key) const {
    auto [h1, h2] = get_hashes(key);
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t index = (h1 + i * h2) % bits_.size();
        if (!bits_[index]) {
            return false;  // Definitely not in set
        }
    }
    return true;  // Might be in set
}

void BloomFilter::clear() {
    std::fill(bits_.begin(), bits_.end(), false);
}

// ============== HashJoinOperator Implementation ==============

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
      hash_table_build_(false) {}

static Tuple UnionTuple(const Tuple &a, const std::vector<data_t>::iterator &start, idx_t width) {
    Tuple result = a;
    result.insert(result.end(), start, start + width);
    return result;
}

void HashJoinOperator::BuildHashTable() {
    auto &build_child_operator = child_operators_[1];
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    
    // Get the attribute indices
    const idx_t build_key_attr = build_child_operator->GetOutputSchema()
                                    .GetKeyAttrs({build_column_name_})[0];
    width_ = build_child_operator->GetOutputSchema().size();
    
    // First pass: collect all build-side tuples
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }
    
    // Build the hash table and Bloom filter
    // Bloom filter size: about 2x the number of distinct keys for good cache performance
    // But we don't know distinct count, so use tuple_count as estimate
    bloom_filter_ = std::make_unique<BloomFilter>(tuple_count_ * 2, 0.01);
    pointer_table_.reserve(tuple_count_ * 2);  // Reserve extra space for collisions
    
    for (idx_t i = 0; i < tuple_count_; i++) {
        data_t key = tuples_[i * width_ + build_key_attr];
        pointer_table_.insert(std::make_pair(key, i * width_));
        bloom_filter_->insert(key);
    }
}

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();
    
    // Build hash table on first call
    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
    }
    
    auto &probe_child_operator = child_operators_[0];
    const idx_t probe_key_attr = probe_child_operator->GetOutputSchema()
                                    .GetKeyAttrs({probe_column_name_})[0];
    
    output_chunk.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);
    
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill buffer if needed
        if (buffer_ptr_ >= buffer_.size() && !probe_child_exhausted_) {
            buffer_.clear();
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        
        // Check if we're done
        if (buffer_ptr_ >= buffer_.size()) {
            return EXHAUSETED;
        }
        
        // Get next probe tuple
        auto &probe_data = buffer_[buffer_ptr_];
        buffer_ptr_++;
        
        auto &probe_tuple = probe_data.first;
        auto probe_key = probe_tuple.KeyFromTuple(probe_key_attr);
        
        // BLOOM FILTER OPTIMIZATION:
        // Fast pre-filter - if Bloom filter says the key is definitely not in the build set,
        // skip this tuple entirely without expensive hash table lookup.
        // This is the key optimization that improves cache locality because:
        // 1. Bloom filter is much smaller (~2x distinct keys) and fits in L2/L3 cache
        // 2. Hash table lookup is expensive due to random memory access and pointer chasing
        // 3. Many probe tuples in JOB queries have no matches, especially early in the join chain
        if (!bloom_filter_->might_contain(probe_key)) {
            continue;  // Early reject - this tuple has no match
        }
        
        // Only reach here if the key might exist - now do expensive hash table lookup
        auto match_range = pointer_table_.equal_range(probe_key);
        
        // Generate all matching output tuples
        for (auto match_ite = match_range.first; match_ite != match_range.second; ++match_ite) {
            if (output_chunk.size() == output_chunk.capacity()) {
                // Need to grow - but we reserved, so this shouldn't happen often
                output_chunk.reserve(output_chunk.size() * 2);
            }
            
            output_chunk.emplace_back();
            auto &new_tuple = output_chunk.back().first;
            
            // Build output tuple: probe tuple + build tuple
            new_tuple = probe_tuple;
            new_tuple.reserve(new_tuple.size() + width_);
            new_tuple.insert(new_tuple.end(), 
                           tuples_.begin() + match_ite->second,
                           tuples_.begin() + match_ite->second + width_);
            output_chunk.back().second = INVALID_ID;
        }
    }
    
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = 0;
    tuples_.clear();
    pointer_table_.clear();
    bloom_filter_.reset();
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

}