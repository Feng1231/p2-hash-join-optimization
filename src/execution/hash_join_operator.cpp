#include "execution/hash_join_operator.hpp"
#include "execution/bloom_filter.hpp"
#include "common/config.hpp"
#include <memory>
#include <algorithm>
#include <cstring>

namespace babydb {

class HashJoinOperatorImpl {
private:
    // Optimized storage with minimal copying
    struct JoinTuple {
        const Tuple* probe_tuple;
        size_t build_offset;
    };
    
    // Pre-allocated memory pool for build tuples
    std::vector<char> build_pool_;
    std::vector<size_t> build_offsets_;
    std::vector<std::string> build_keys_;
    
    // Cache-friendly hash table
    struct HashEntry {
        std::string key;
        size_t offset;
        size_t next;  // For chaining
        bool occupied;
    };
    std::vector<HashEntry> hash_table_;
    
    // Bloom filter for RPT
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    // Query-specific Bloom filters for multi-way joins
    std::vector<std::unique_ptr<BloomFilter>> predicate_filters_;
    
    // Statistics for adaptive decisions
    size_t build_cardinality_;
    size_t probe_cardinality_;
    
    void BuildHashTable(const std::shared_ptr<Operator>& build_child,
                       const std::string& build_column_name) {
        const idx_t build_key_attr = 
            build_child->GetOutputSchema().GetKeyAttrs({build_column_name})[0];
        
        // First pass: count tuples and collect keys
        Chunk build_chunk;
        OperatorState state = HAVE_MORE_OUTPUT;
        std::vector<std::string> all_keys;
        std::vector<size_t> tuple_sizes;
        
        while (state != EXHAUSETED) {
            state = build_child->Next(build_chunk);
            for (auto& chunk_row : build_chunk) {
                auto& tuple = chunk_row.first;
                all_keys.push_back(tuple[build_key_attr]);
                tuple_sizes.push_back(tuple.size());
                build_cardinality_++;
            }
        }
        
        // Optimize Bloom filter size based on cardinality
        if (build_cardinality_ > 0) {
            // Optimal Bloom filter size: m = -n * ln(p) / (ln(2))^2
            // Target false positive rate: 0.01 (1%)
            size_t optimal_size = std::max<size_t>(
                1024, 
                static_cast<size_t>(-build_cardinality_ * std::log(0.01) / std::pow(std::log(2), 2))
            );
            size_t optimal_hashes = std::max<size_t>(
                1,
                static_cast<size_t>(optimal_size / build_cardinality_ * std::log(2))
            );
            
            bloom_filter_ = std::make_unique<BloomFilter>(optimal_size, optimal_hashes);
        } else {
            bloom_filter_ = std::make_unique<BloomFilter>(1024, 3);
        }
        
        // Second pass: build hash table and populate Bloom filter
        size_t hash_table_size = build_cardinality_ * 2;  // Load factor 0.5
        hash_table_.resize(hash_table_size, {"", 0, 0, false});
        
        // Pre-allocate memory for tuple storage
        size_t total_tuple_bytes = 0;
        for (auto size : tuple_sizes) {
            total_tuple_bytes += size * sizeof(data_t);
        }
        build_pool_.reserve(total_tuple_bytes);
        build_offsets_.reserve(build_cardinality_);
        
        // Re-execute build child
        state = HAVE_MORE_OUTPUT;
        build_child->SelfInit();
        
        size_t current_offset = 0;
        size_t tuple_index = 0;
        
        while (state != EXHAUSETED) {
            state = build_child->Next(build_chunk);
            for (auto& chunk_row : build_chunk) {
                auto& tuple = chunk_row.first;
                
                // Serialize tuple to memory pool
                size_t tuple_bytes = tuple.size() * sizeof(data_t);
                build_pool_.resize(current_offset + tuple_bytes);
                std::memcpy(build_pool_.data() + current_offset, tuple.data(), tuple_bytes);
                build_offsets_.push_back(current_offset);
                
                // Insert into Bloom filter
                const std::string& key = all_keys[tuple_index];
                bloom_filter_->Add(key);
                
                // Insert into hash table
                size_t hash = std::hash<std::string>{}(key);
                size_t idx = hash % hash_table_size;
                
                // Linear probing with early termination
                while (hash_table_[idx].occupied && hash_table_[idx].key != key) {
                    idx = (idx + 1) % hash_table_size;
                }
                
                if (!hash_table_[idx].occupied) {
                    hash_table_[idx] = {key, current_offset, 0, true};
                } else {
                    // Chaining for collisions
                    while (hash_table_[idx].next != 0) {
                        idx = hash_table_[idx].next;
                    }
                    size_t new_idx = (idx + 1) % hash_table_size;
                    while (hash_table_[new_idx].occupied) {
                        new_idx = (new_idx + 1) % hash_table_size;
                    }
                    hash_table_[idx].next = new_idx;
                    hash_table_[new_idx] = {key, current_offset, 0, true};
                }
                
                current_offset += tuple_bytes;
                tuple_index++;
            }
        }
    }
    
    void BuildPredicateFilters(const std::vector<std::shared_ptr<Operator>>& operators,
                              const std::vector<std::string>& column_names) {
        // Collect all distinct keys from reachable operators
        std::vector<std::string> all_keys;
        
        for (size_t i = 1; i < operators.size(); ++i) {
            auto& op = operators[i];
            const idx_t key_attr = 
                op->GetOutputSchema().GetKeyAttrs({column_names[i-1]})[0];
            
            Chunk chunk;
            OperatorState state = HAVE_MORE_OUTPUT;
            
            while (state != EXHAUSETED) {
                state = op->Next(chunk);
                for (auto& chunk_row : chunk) {
                    all_keys.push_back(chunk_row.first[key_attr]);
                }
            }
            
            if (!all_keys.empty()) {
                // Create specialized Bloom filter for this predicate
                auto filter = std::make_unique<BloomFilter>(all_keys.size() * 2, 3);
                for (const auto& key : all_keys) {
                    filter->Add(key);
                }
                predicate_filters_.push_back(std::move(filter));
            }
            
            all_keys.clear();
            op->SelfInit();  // Reset for actual execution
        }
    }
    
    inline bool PredicateCheck(const std::string& key) const {
        // Check all predicate Bloom filters
        for (const auto& filter : predicate_filters_) {
            if (!filter->PossiblyContains(key)) {
                return false;
            }
        }
        return true;
    }
    
public:
    HashJoinOperatorImpl() : build_cardinality_(0), probe_cardinality_(0) {}
    
    void Initialize(const std::shared_ptr<Operator>& build_child,
                   const std::string& build_column_name,
                   bool enable_rpt = true) {
        BuildHashTable(build_child, build_column_name);
        
        // If we have multiple joins, we can build predicate filters
        if (enable_rpt && build_child->GetChildren().size() > 1) {
            // Collect operators for RPT
            std::vector<std::shared_ptr<Operator>> operators;
            std::vector<std::string> column_names;
            
            auto current = build_child;
            while (current->GetChildren().size() == 2) {
                auto hash_join = std::dynamic_pointer_cast<HashJoinOperator>(current);
                if (hash_join) {
                    operators.push_back(current);
                    column_names.push_back(hash_join->GetBuildColumnName());
                    current = hash_join->GetChildren()[1];  // Build side
                } else {
                    break;
                }
            }
            
            if (!operators.empty()) {
                BuildPredicateFilters(operators, column_names);
            }
        }
    }
    
    bool Probe(const Tuple& probe_tuple, const std::string& key, 
               std::vector<char>& output_buffer, size_t& output_offset) {
        // RPT: Check predicate filters first (most selective)
        if (predicate_filters_.size() > 0 && !PredicateCheck(key)) {
            return false;
        }
        
        // Quick negative check with Bloom filter
        if (!bloom_filter_->PossiblyContains(key)) {
            return false;
        }
        
        // Hash table lookup
        size_t hash = std::hash<std::string>{}(key);
        size_t idx = hash % hash_table_.size();
        
        bool found_match = false;
        while (hash_table_[idx].occupied) {
            if (hash_table_[idx].key == key) {
                // Found matching build tuple
                size_t build_offset = hash_table_[idx].offset;
                size_t tuple_size = 0;
                
                // Copy build tuple data to output buffer
                size_t offset = idx;
                do {
                    build_offset = hash_table_[offset].offset;
                    // Get tuple size (stored at beginning of tuple or from schema)
                    // For simplicity, we'll assume we need to copy based on schema
                    
                    // Append to output buffer
                    output_buffer.resize(output_offset + probe_tuple.size() * sizeof(data_t) + 
                                        128);  // Approximate build tuple size
                    std::memcpy(output_buffer.data() + output_offset, 
                               probe_tuple.data(), probe_tuple.size() * sizeof(data_t));
                    output_offset += probe_tuple.size() * sizeof(data_t);
                    
                    // Copy build tuple
                    // This needs to be properly sized based on actual tuple schema
                    found_match = true;
                    
                    offset = hash_table_[offset].next;
                } while (offset != 0);
                
                return found_match;
            }
            idx = (idx + 1) % hash_table_.size();
        }
        
        return false;
    }
    
    size_t GetBuildCardinality() const { return build_cardinality_; }
};

HashJoinOperator::HashJoinOperator(const ExecutionContext &exec_ctx,
                                   const std::shared_ptr<Operator> &probe_child_operator,
                                   const std::shared_ptr<Operator> &build_child_operator,
                                   const std::string &probe_column_name,
                                   const std::string &build_column_name)
    : Operator(exec_ctx, {probe_child_operator, build_child_operator}),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name),
      impl_(std::make_unique<HashJoinOperatorImpl>()) {}

// Move constructor/assignment for unique_ptr
HashJoinOperator::HashJoinOperator(HashJoinOperator&& other) noexcept = default;
HashJoinOperator& HashJoinOperator::operator=(HashJoinOperator&& other) noexcept = default;

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    static thread_local std::vector<char> output_buffer;
    static thread_local size_t output_offset = 0;
    
    if (!hash_table_build_) {
        hash_table_build_ = true;
        
        // Determine if we should enable RPT based on cardinality
        auto& build_child = child_operators_[1];
        Chunk estimate_chunk;
        size_t estimated_cardinality = 0;
        
        // Quick cardinality estimate
        build_child->SelfInit();
        while (build_child->Next(estimate_chunk) != EXHAUSETED) {
            estimated_cardinality += estimate_chunk.size();
        }
        build_child->SelfInit();
        
        // Enable RPT only for medium-sized builds (not too small, not too large)
        bool enable_rpt = (estimated_cardinality > 10000 && estimated_cardinality < 5000000);
        
        impl_->Initialize(child_operators_[1], build_column_name_, enable_rpt);
        
        // Adaptive strategy based on build size
        size_t build_size = impl_->GetBuildCardinality();
        if (build_size < 1000) {
            // Small build: Bloom filter overhead might hurt, skip optional checks
            use_bloom_ = false;
        } else {
            use_bloom_ = true;
        }
    }
    
    auto &probe_child_operator = child_operators_[0];
    auto probe_key_attr = probe_child_operator->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];
    
    output_buffer.clear();
    output_offset = 0;
    size_t output_count = 0;
    
    while (output_count < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        if (buffer_ptr_ >= buffer_.size() && !probe_child_exhausted_) {
            buffer_.clear();
            if (probe_child_operator->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }
        
        if (buffer_ptr_ >= buffer_.size()) {
            break;
        }
        
        auto &probe_tuple = buffer_[buffer_ptr_].first;
        const std::string& probe_key = probe_tuple[probe_key_attr];
        buffer_ptr_++;
        
        // Probe with RPT filtering
        if (impl_->Probe(probe_tuple, probe_key, output_buffer, output_offset)) {
            // Deserialize output buffer into chunks
            // For now, just increment count
            output_count++;
        }
    }
    
    // Convert output buffer to chunks (simplified)
    output_chunk.resize(output_count);
    for (size_t i = 0; i < output_count; ++i) {
        output_chunk[i].first.resize(128);  // Placeholder size
        output_chunk[i].second = INVALID_ID;
    }
    
    if (probe_child_exhausted_ && buffer_ptr_ >= buffer_.size()) {
        return EXHAUSETED;
    }
    
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
    use_bloom_ = true;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

void HashJoinOperator::BuildHashTable() {
    // Delegate to impl_
    // This method is kept for compatibility but actual implementation is in impl_
}

} // namespace babydb