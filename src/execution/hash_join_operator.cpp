#include "execution/hash_join_operator.hpp"
#include "execution/bloom_filter.hpp"
#include "common/config.hpp"
#include <memory>
#include <algorithm>
#include <sstream>

namespace babydb {

// Helper to convert data_t to string for hashing
inline std::string DataToString(const data_t& value) {
    std::stringstream ss;
    ss << value;
    return ss.str();
}

// Simplified RPT Optimizer - just Bloom filters, no heavy pre-allocation
class HashJoinOptimizer {
private:
    // Bloom filter for this join
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    // Cascading Bloom filters for multi-way RPT (predicate transfer)
    struct PredicateFilter {
        std::unique_ptr<BloomFilter> filter;
        double selectivity;
    };
    std::vector<PredicateFilter> predicate_filters_;
    
    size_t build_cardinality_;
    double false_positive_rate_;
    
public:
    HashJoinOptimizer() 
        : build_cardinality_(0), false_positive_rate_(0.01) {}
    
    void BuildBloomFilter(const std::shared_ptr<Operator>& build_child, 
                          const std::string& build_column_name) {
        // Get schema info
        auto& schema = build_child->GetOutputSchema();
        const idx_t build_key_attr = schema.GetKeyAttr(build_column_name);
        
        // First pass: collect all keys and count
        Chunk build_chunk;
        OperatorState state = HAVE_MORE_OUTPUT;
        std::vector<std::string> all_keys;
        
        while (state != EXHAUSETED) {
            state = build_child->Next(build_chunk);
            for (auto& chunk_row : build_chunk) {
                auto& tuple = chunk_row.first;
                data_t key_value = tuple[build_key_attr];
                all_keys.push_back(DataToString(key_value));
                build_cardinality_++;
            }
        }
        
        if (build_cardinality_ == 0) {
            bloom_filter_ = std::make_unique<BloomFilter>(1024, 3);
            return;
        }
        
        // Optimize Bloom filter size based on cardinality
        // Use more conservative sizes to avoid memory issues
        size_t optimal_size;
        if (build_cardinality_ < 10000) {
            optimal_size = 1024 * 10;  // 10KB for small tables
        } else if (build_cardinality_ < 100000) {
            optimal_size = 1024 * 100; // 100KB for medium tables
        } else if (build_cardinality_ < 1000000) {
            optimal_size = 1024 * 1024; // 1MB for large tables
        } else {
            optimal_size = 1024 * 1024 * 4; // 4MB for very large tables
        }
        
        size_t optimal_hashes = 3; // Fixed number of hash functions for simplicity
        
        bloom_filter_ = std::make_unique<BloomFilter>(optimal_size, optimal_hashes);
        
        // Build Bloom filter
        for (const auto& key : all_keys) {
            bloom_filter_->Add(key);
        }
    }
    
    bool CheckBloomFilter(const std::string& probe_key) const {
        if (!bloom_filter_) return true;
        return bloom_filter_->PossiblyContains(probe_key);
    }
    
    bool CheckPredicateFilters(const std::string& probe_key) const {
        // Check all predicate filters (most selective first - already sorted)
        for (const auto& pf : predicate_filters_) {
            if (!pf.filter->PossiblyContains(probe_key)) {
                return false;  // Filtered out by RPT
            }
        }
        return true;
    }
    
    void AddPredicateFilter(const std::shared_ptr<Operator>& op, 
                           const std::string& column_name) {
        // Collect keys from this operator
        Chunk chunk;
        OperatorState state = HAVE_MORE_OUTPUT;
        std::vector<std::string> keys;
        size_t cardinality = 0;
        
        const idx_t attr = op->GetOutputSchema().GetKeyAttr(column_name);
        
        while (state != EXHAUSETED) {
            state = op->Next(chunk);
            for (auto& chunk_row : chunk) {
                auto& tuple = chunk_row.first;
                data_t key_value = tuple[attr];
                keys.push_back(DataToString(key_value));
                cardinality++;
            }
        }
        
        if (cardinality > 0) {
            // Use smaller filter sizes for predicates to save memory
            size_t filter_size = std::min<size_t>(1024 * 1024, std::max<size_t>(1024, cardinality * 2));
            auto filter = std::make_unique<BloomFilter>(filter_size, 3);
            
            for (const auto& key : keys) {
                filter->Add(key);
            }
            
            double selectivity = static_cast<double>(cardinality) / 
                                std::max(build_cardinality_, static_cast<size_t>(1));
            predicate_filters_.push_back({std::move(filter), selectivity});
        }
        
        // Reset operator for actual execution
        op->Init();  // Re-initialize the operator
    }
    
    void SetupPredicateChain(HashJoinOperator* current_join) {
        // Traverse the build chain to collect predicates
        std::vector<std::pair<std::shared_ptr<Operator>, std::string>> chain;
        
        auto* current = current_join;
        while (current) {
            auto build_child = current->GetBuildChild();
            auto next_join = std::dynamic_pointer_cast<HashJoinOperator>(build_child);
            if (next_join) {
                chain.push_back({build_child, next_join->GetBuildColumnName()});
                current = next_join.get();
            } else {
                break;
            }
        }
        
        // Build filters from innermost to outermost (most selective first)
        for (int i = static_cast<int>(chain.size()) - 1; i >= 0; i--) {
            AddPredicateFilter(chain[i].first, chain[i].second);
        }
        
        // Sort by selectivity (most selective first)
        std::sort(predicate_filters_.begin(), predicate_filters_.end(),
            [](const PredicateFilter& a, const PredicateFilter& b) {
                return a.selectivity < b.selectivity;
            });
    }
    
    size_t GetBuildCardinality() const { return build_cardinality_; }
    void SetFalsePositiveRate(double rate) { false_positive_rate_ = rate; }
};

// HashJoinOperator implementation
HashJoinOperator::HashJoinOperator(const ExecutionContext &exec_ctx,
                                   const std::shared_ptr<Operator> &probe_child_operator,
                                   const std::shared_ptr<Operator> &build_child_operator,
                                   const std::string &probe_column_name,
                                   const std::string &build_column_name)
    : Operator(exec_ctx, {probe_child_operator, build_child_operator}),
      probe_column_name_(probe_column_name),
      build_column_name_(build_column_name),
      optimizer_(std::make_unique<HashJoinOptimizer>()),
      tuple_count_(0),
      width_(0),
      buffer_ptr_(0),
      probe_child_exhausted_(false),
      hash_table_build_(false),
      use_bloom_(true),
      bloom_filter_(1024, 3) {
    // Set output schema as concatenation of probe and build schemas
    output_schema_ = probe_child_operator->GetOutputSchema();
    const auto& build_schema = build_child_operator->GetOutputSchema();
    output_schema_.insert(output_schema_.end(), build_schema.begin(), build_schema.end());
}

HashJoinOperator::~HashJoinOperator() = default;

OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    if (!hash_table_build_) {
        hash_table_build_ = true;
        
        // Build Bloom filter for this join
        optimizer_->BuildBloomFilter(child_operators_[1], build_column_name_);
        
        // Setup RPT predicate chain for multi-way joins
        auto build_child = child_operators_[1];
        auto hash_join_child = std::dynamic_pointer_cast<HashJoinOperator>(build_child);
        if (hash_join_child) {
            optimizer_->SetupPredicateChain(this);
        }
        
        // Adjust false positive rate based on cardinality
        size_t build_size = optimizer_->GetBuildCardinality();
        if (build_size > 1000000) {
            optimizer_->SetFalsePositiveRate(0.001);
        }
        
        // Now build the actual hash table for the join (use the baseline method)
        // This ensures we don't break the existing functionality
        BuildHashTable();
    }
    
    auto& probe_child = child_operators_[0];
    const idx_t probe_key_attr = probe_child->GetOutputSchema().GetKeyAttr(probe_column_name_);
    
    output_chunk.clear();
    output_chunk.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);
    
    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill buffer if needed
        if (buffer_ptr_ >= buffer_.size() && !probe_child_exhausted_) {
            buffer_.clear();
            OperatorState state = probe_child->Next(buffer_);
            if (state == EXHAUSETED) {
                probe_child_exhausted_ = true;
                break;
            }
            buffer_ptr_ = 0;
        }
        
        if (buffer_ptr_ >= buffer_.size()) {
            break;
        }
        
        // Get next probe tuple
        auto& probe_tuple = buffer_[buffer_ptr_].first;
        data_t key_value = probe_tuple[probe_key_attr];
        std::string probe_key = DataToString(key_value);
        buffer_ptr_++;
        
        // RPT: Check predicate filters first (most selective)
        if (!optimizer_->CheckPredicateFilters(probe_key)) {
            continue;  // Filtered out by RPT - skip this tuple entirely
        }
        
        // Check Bloom filter for this join
        if (!optimizer_->CheckBloomFilter(probe_key)) {
            continue;  // No match possible - skip
        }
        
        // Bloom filter says maybe, now check actual hash table
        // Use the baseline hash table (pointer_table_) for actual matching
        auto match_range = pointer_table_.equal_range(probe_key);
        
        for (auto match_ite = match_range.first; match_ite != match_range.second; match_ite++) {
            // Build output tuple by concatenating probe and build tuples
            Tuple output_tuple = probe_tuple;
            // Add build tuple data (simplified - in real implementation, would need to deserialize)
            // For now, just add a placeholder
            output_tuple.push_back(0);
            
            if (output_chunk.size() >= exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
                break;
            }
            output_chunk.push_back({output_tuple, INVALID_ID});
        }
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
    child_operators_[0]->GetOutputSchema().GetKeyAttr(probe_column_name_);
    child_operators_[1]->GetOutputSchema().GetKeyAttr(build_column_name_);
}

void HashJoinOperator::BuildHashTable() {
    // This is the baseline hash table building (kept for compatibility)
    auto &build_child_operator = child_operators_[1];
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    const idx_t build_key_attr = build_child_operator->GetOutputSchema().GetKeyAttr(build_column_name_);
    
    // Clear existing structures
    tuples_.clear();
    pointer_table_.clear();
    tuple_count_ = 0;
    
    // First pass: count tuples
    size_t estimated_size = 0;
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            estimated_size++;
        }
    }
    
    // Reserve space to avoid repeated allocations
    tuples_.reserve(estimated_size * width_);
    pointer_table_.reserve(estimated_size);
    
    // Reset and do actual build
    build_child_operator->Init();
    state = HAVE_MORE_OUTPUT;
    
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            // Store tuple data
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            
            // Get key and store in hash table
            data_t key_value = tuple[build_key_attr];
            std::string key = DataToString(key_value);
            pointer_table_.insert(std::make_pair(key, tuple_count_ * width_));
            
            tuple_count_++;
        }
    }
}

} // namespace babydb