#include "execution/hash_join_operator.hpp"

#include "common/config.hpp"

#include <algorithm>

namespace babydb {

// ============================================================
// HashJoinOperator
// ============================================================

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
      hash_table_build_(false) {

    // ---- RPT Bloom-filter pushdown ----
    //
    // Allocate the BF now (with a conservative initial size; it will be
    // re-created with the actual tuple count in BuildHashTable if necessary).
    // Push it down the probe pipeline so the leaf SeqScanOperator can hold it
    // and apply it before any per-tuple extraction work.
    //
    // The BF is NOT ready_ yet — MightContain returns true for everything until
    // MarkReady() is called at the end of BuildHashTable().  This means the
    // scan produces all tuples until the build phase has finished, which is
    // exactly what Volcano semantics require.
    bloom_filter_ = std::make_shared<BloomFilter>(1024, 0.01);
    probe_child_operator->RegisterBloomFilter(bloom_filter_, probe_column_name_);
}

// ------------------------------------------------------------
// RegisterBloomFilter — forward to whichever child owns the column
// ------------------------------------------------------------
void HashJoinOperator::RegisterBloomFilter(std::shared_ptr<BloomFilter> bf,
                                           const std::string &column_name) {
    // Exactly one of our two children contains column_name in its output schema
    // (CheckSchema() has already verified no duplicates across the whole plan).
    // Forward to the right one.
    auto &probe_schema = child_operators_[0]->GetOutputSchema();
    for (const auto &col : probe_schema) {
        if (col == column_name) {
            child_operators_[0]->RegisterBloomFilter(bf, column_name);
            return;
        }
    }
    // Not in probe child → must be in build child
    child_operators_[1]->RegisterBloomFilter(bf, column_name);
}

// ------------------------------------------------------------
// BuildHashTable
// ------------------------------------------------------------
void HashJoinOperator::BuildHashTable() {
    auto &build_child = child_operators_[1];
    const idx_t build_key_attr =
        build_child->GetOutputSchema().GetKeyAttrs({build_column_name_})[0];
    width_ = build_child->GetOutputSchema().size();

    // --- Pass 1: materialise all build-side tuples ---
    OperatorState state = HAVE_MORE_OUTPUT;
    Chunk build_chunk;
    while (state != EXHAUSETED) {
        state = build_child->Next(build_chunk);
        for (auto &[tuple, rid] : build_chunk) {
            tuples_.insert(tuples_.end(), tuple.begin(), tuple.end());
            tuple_count_++;
        }
    }

    // --- Re-create the BF with the exact tuple count now that we know it ---
    // This replaces the placeholder allocated in the constructor.  The scan
    // holds a shared_ptr to the *same object* (bloom_filter_), so we must
    // modify it in place (Clear + re-insert) rather than allocate a new one.
    //
    // We reconstruct by resetting and re-inserting; the shared_ptr the scan
    // holds continues to point to the same BloomFilter instance.
    //
    // If tuple_count_ is small the original 1024-entry filter is fine; we
    // only reconstruct when we have significantly more insertions than the
    // placeholder was sized for.
    if (tuple_count_ > 1024) {
        // Move-assign a fresh, properly-sized BF into the object *behind* the
        // shared_ptr so both this operator and the scan see the updated state.
        *bloom_filter_ = BloomFilter(tuple_count_, 0.01);
    } else {
        bloom_filter_->Clear();
    }

    // --- Pass 2: build the hash table and populate the BF ---
    pointer_table_.reserve(tuple_count_ * 2);

    for (idx_t i = 0; i < tuple_count_; i++) {
        const data_t key = tuples_[i * width_ + build_key_attr];
        pointer_table_.insert({key, i * width_});
        bloom_filter_->Insert(key);
    }

    // Signal to the scan that it may now apply the filter
    bloom_filter_->MarkReady();
}

// ------------------------------------------------------------
// Next
// ------------------------------------------------------------
OperatorState HashJoinOperator::Next(Chunk &output_chunk) {
    output_chunk.clear();

    // Build the hash table on the very first call
    if (!hash_table_build_) {
        hash_table_build_ = true;
        BuildHashTable();
    }

    auto &probe_child = child_operators_[0];
    const idx_t probe_key_attr =
        probe_child->GetOutputSchema().GetKeyAttrs({probe_column_name_})[0];

    output_chunk.reserve(exec_ctx_.config_.CHUNK_SUGGEST_SIZE);

    while (output_chunk.size() < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        // Refill the probe buffer when exhausted
        if (buffer_ptr_ >= buffer_.size() && !probe_child_exhausted_) {
            buffer_.clear();
            if (probe_child->Next(buffer_) == EXHAUSETED) {
                probe_child_exhausted_ = true;
            }
            buffer_ptr_ = 0;
        }

        // Done when buffer is empty (and child is exhausted)
        if (buffer_ptr_ >= buffer_.size()) {
            return EXHAUSETED;
        }

        // Fetch next probe tuple (already passed the BF at the scan)
        auto &[probe_tuple, probe_rid] = buffer_[buffer_ptr_++];
        const data_t probe_key = probe_tuple.KeyFromTuple(probe_key_attr);

        // Hash table probe — no in-operator BF check needed: tuples that
        // reach here have already been filtered by the pushed-down BF at the
        // SeqScanOperator level.
        auto [match_begin, match_end] = pointer_table_.equal_range(probe_key);

        for (auto it = match_begin; it != match_end; ++it) {
            if (output_chunk.size() == output_chunk.capacity()) {
                output_chunk.reserve(output_chunk.size() * 2);
            }

            output_chunk.emplace_back();
            auto &new_tuple = output_chunk.back().first;

            // Output: probe columns followed by build columns
            new_tuple = probe_tuple;
            new_tuple.reserve(new_tuple.size() + width_);
            new_tuple.insert(new_tuple.end(),
                             tuples_.begin() + it->second,
                             tuples_.begin() + it->second + width_);
            output_chunk.back().second = INVALID_ID;
        }
    }

    return HAVE_MORE_OUTPUT;
}

// ------------------------------------------------------------
// SelfInit / SelfCheck
// ------------------------------------------------------------
void HashJoinOperator::SelfInit() {
    tuple_count_ = 0;
    width_ = 0;
    tuples_.clear();
    pointer_table_.clear();

    // Reset the BF and re-push it down (Init is called before every execution)
    bloom_filter_->Clear();

    buffer_.clear();
    buffer_ptr_ = 0;
    probe_child_exhausted_ = false;
    hash_table_build_ = false;
}

void HashJoinOperator::SelfCheck() {
    child_operators_[0]->GetOutputSchema().GetKeyAttrs({probe_column_name_});
    child_operators_[1]->GetOutputSchema().GetKeyAttrs({build_column_name_});
}

}  // namespace babydb
