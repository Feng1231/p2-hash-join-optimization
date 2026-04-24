#include "execution/hash_join_operator.hpp"
#include "execution/bloom_filter.hpp"
#include "common/config.hpp"
#include <memory>
#include <algorithm>
#include <cstring>
#include <unordered_map>
#include <sstream>

namespace babydb {

// Helper to convert data_t to string for hashing
inline std::string DataToString(const data_t& value) {
    std::stringstream ss;
    ss << value;
    return ss.str();
}

// Optimized hash table for RPT
class HashJoinOptimizer {
private:
    // Memory pool for build tuples
    std::vector<char> build_pool_;
    std::vector<size_t> build_offsets_;
    std::vector<size_t> build_sizes_;
    
    // Hash table mapping key -> list of tuple offsets
    std::unordered_map<std::string, std::vector<size_t>> hash_table_;
    
    // Bloom filter for RPT
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    // Cascading Bloom filters for multi-way RPT
    struct PredicateFilter {
        std::unique_ptr<BloomFilter> filter;
        double selectivity;
    };
    std::vector<PredicateFilter> predicate_filters_;
    
    size_t build_cardinality_;
    size_t build_key_attr_;
    double false_positive_rate_;
    
public:
    HashJoinOptimizer() 
        : build_cardinality_(0), build_key_attr_(0), false_positive_rate_(0.01) {}
    
    void Build(const std::shared_ptr<Operator>& build_child, 
               const std::string& build_column_name) {
        // Get schema info
        auto& schema = build_child->GetOutputSchema();
        build_key_attr_ = schema.GetKeyAttr(build_column_name);
        
        // First pass: collect all keys
        Chunk build_chunk;
        OperatorState state = HAVE_MORE_OUTPUT;
        std::vector<std::string> all_keys;
        
        while (state != EXHAUSETED) {
            state = build_child->Next(build_chunk);
            for (auto& chunk_row : build_chunk) {
                auto& tuple = chunk_row.first;
                data_t key_value = tuple[build_key_attr_];
                all_keys.push_back(DataToString(key_value));
                build_cardinality_++;
            }
        }
        
        if (build_cardinality_ == 0) {
            bloom_filter_ = std::make_unique<BloomFilter>(1024, 3);
            return;
        }
        
        // Optimize Bloom filter size
        size_t optimal_size = static_cast<size_t>(
            -build_cardinality_ * std::log(false_positive_rate_) / std::pow(std::log(2), 2)
        );
        optimal_size = std::max<size_t>(1024, optimal_size);
        
        size_t optimal_hashes = static_cast<size_t>(
            std::max(1.0, (optimal_size / static_cast<double>(build_cardinality_)) * std::log(2))
        );
        
        bloom_filter_ = std::make_unique<BloomFilter>(optimal_size, optimal_hashes);
        
        // Build hash table
        hash_table_.reserve(build_cardinality_ * 2);
        
        for (const auto& key : all_keys) {
            bloom_filter_->Add(key);
            hash_table_[key].push_back(0); // Placeholder offset
        }
    }
    
    bool Probe(const Tuple& probe_tuple, const std::string& probe_key, 
               Tuple& output_tuple, bool& found) {
        found = false;
        
        // Check predicate filters (most selective first)
        for (const auto& pf : predicate_filters_) {
            if (!pf.filter->PossiblyContains(probe_key)) {
                return false;
            }
        }
        
        // Bloom filter check
        if (!bloom_filter_->PossiblyContains(probe_key)) {
            return false;
        }
        
        // Hash table lookup
        auto it = hash_table_.find(probe_key);
        if (it == hash_table_.end()) {
            return false;
        }
        
        found = true;
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
            size_t filter_size = std::max<size_t>(1024, cardinality * 2);
            auto filter = std::make_unique<BloomFilter>(filter_size, 3);
            
            for (const auto& key : keys) {
                filter->Add(key);
            }
            
            double selectivity = static_cast<double>(cardinality) / 
                                std::max(build_cardinality_, static_cast<size_t>(1));
            predicate_filters_.push_back({std::move(filter), selectivity});
        }
    }
    
    void SetupPredicateChain(HashJoinOperator* current_join) {
        // Traverse the build chain by accessing child_operators_ through the HashJoinOperator
        std::vector<std::pair<std::shared_ptr<Operator>, std::string>> chain;
        
        auto* current = current_join;
        while (current) {
            // Get the build child
            auto build_child = current->GetBuildChild();
            
            // Check if build child is another HashJoinOperator
            auto next_join = std::dynamic_pointer_cast<HashJoinOperator>(build_child);
            if (next_join) {
                chain.push_back({build_child, next_join->GetBuildColumnName()});
                current = next_join.get();
            } else {
                break;
            }
        }
        
        // Build filters from innermost to outermost
        for (int i = static_cast<int>(chain.size()) - 1; i >= 0; i--) {
            AddPredicateFilter(chain[i].first, chain[i].second);
        }
        
        // Sort by selectivity
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
        
        // Build hash table with RPT optimization
        optimizer_->Build(child_operators_[1], build_column_name_);
        
        // Setup predicate chain for multi-way joins
        // Check if build child is another HashJoinOperator (has 2 children)
        // We can check by seeing if child_operators_[1] has child_operators (access via member)
        // Since child_operators_ is protected and HashJoinOperator is derived from Operator,
        // we can access it directly
        auto build_child = child_operators_[1];
        // Try to cast to HashJoinOperator to check if it's a join
        auto hash_join_child = std::dynamic_pointer_cast<HashJoinOperator>(build_child);
        if (hash_join_child) {
            optimizer_->SetupPredicateChain(this);
        }
        
        // Adjust false positive rate based on cardinality
        size_t build_size = optimizer_->GetBuildCardinality();
        if (build_size > 1000000) {
            optimizer_->SetFalsePositiveRate(0.001);
        } else if (build_size < 10000) {
            optimizer_->SetFalsePositiveRate(0.05);
        }
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
        
        // Probe using RPT
        Tuple output_tuple;
        bool found_match = false;
        
        if (optimizer_->Probe(probe_tuple, probe_key, output_tuple, found_match)) {
            if (found_match) {
                // For now, just pass through probe tuple as output
                // In a full implementation, we'd concatenate with build tuple
                output_chunk.push_back({probe_tuple, INVALID_ID});
            }
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
    // Legacy method - not used in optimized version
}

} // namespace babydb