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
      current_bucket_(0),
      current_entry_(-1) {}

static Tuple UnionTuple(const Tuple &a, const std::vector<data_t>::iterator &start, idx_t width) {
    Tuple result = a;
    result.insert(result.end(), start, start + width);
    return result;
}

size_t HashJoinOperator::HashKey(const data_t& key) const {
    // Use std::hash for the string
    return std::hash<data_t>()(key) & hash_mask_;
}

void HashJoinOperator::InsertIntoHashTable(const data_t& key, idx_t offset) {
    size_t bucket = HashKey(key);
    idx_t entry_idx = hash_entries_.size();
    hash_entries_.emplace_back(key, offset, hash_buckets_[bucket]);
    hash_buckets_[bucket] = entry_idx;
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
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }

        auto &probe_tuple = buffer_[buffer_ptr_].first;
        buffer_ptr_++;
        
        // Probe using custom hash table
        data_t probe_key = probe_tuple.KeyFromTuple(probe_key_attr);
        size_t bucket = HashKey(probe_key);
        idx_t entry_idx = hash_buckets_[bucket];
        
        while (entry_idx != static_cast<idx_t>(-1)) {
            const auto& entry = hash_entries_[entry_idx];
            if (entry.key == probe_key) {
                if (output_size == output_chunk.size()) {
                    output_chunk.emplace_back(
                        std::make_pair(UnionTuple(probe_tuple, tuples_.begin() + entry.tuple_offset, width_),
                                      INVALID_ID));
                } else {
                    output_chunk[output_size].first = UnionTuple(probe_tuple, tuples_.begin() + entry.tuple_offset, width_);
                    output_chunk[output_size].second = INVALID_ID;
                }
                output_size++;
            }
            entry_idx = entry.next;
        }
    }
    
    output_chunk.resize(output_size);
    return HAVE_MORE_OUTPUT;
}

void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = child_operators_[1]->GetOutputSchema().size();
    tuples_.clear();
    hash_entries_.clear();
    hash_buckets_.clear();
    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
    current_bucket_ = 0;
    current_entry_ = -1;
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
    
    // First pass: collect all tuples
    while (state != EXHAUSETED) {
        state = build_child_operator->Next(build_chunk);
        for (auto &chunk_row : build_chunk) {
            auto &tuple = chunk_row.first;
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }
    
    // Initialize hash table with size = next power of 2 >= tuple_count_ * 2
    size_t bucket_count = 16;
    while (bucket_count < tuple_count_ * 2) {
        bucket_count <<= 1;
    }
    hash_mask_ = bucket_count - 1;
    hash_buckets_.assign(bucket_count, static_cast<idx_t>(-1));
    hash_entries_.reserve(tuple_count_);
    
    // Second pass: build hash table
    for (idx_t i = 0; i < tuple_count_; i++) {
        idx_t offset = i * width_;
        data_t key = tuples_[offset + build_key_attr];
        InsertIntoHashTable(key, offset);
    }
}

}