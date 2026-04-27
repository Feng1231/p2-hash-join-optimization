#pragma once

#include "execution/bloom_filter.hpp"
#include "execution/operator.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace babydb {

/**
 * Hash Join Operator
 *
 * Supports equality joins on a single column.
 * Output schema is the concatenation of probe schema followed by build schema.
 *
 * ### RPT Bloom-filter pushdown
 *
 * On construction this operator:
 *   1. Allocates a BloomFilter sized for the build side (estimated at
 *      construction time; resized at build time if needed).
 *   2. Calls probe_child->RegisterBloomFilter(bf, probe_column_name_) so the
 *      BF travels down the probe pipeline to the originating SeqScanOperator.
 *      The scan will apply the BF before any per-tuple work, rejecting
 *      non-matching rows before they enter the Volcano pipeline at all.
 *
 * At BuildHashTable() time the BF is populated from build-side keys and
 * MarkReady() is called.  Because the build phase completes before the probe
 * phase starts (Volcano semantics), the BF is always ready by the time the
 * scan that holds it produces its first tuple.
 *
 * The old in-operator BF check (between the probe loop and equal_range) has
 * been removed: by the time a tuple reaches Next(), it has already passed the
 * BF at the scan level, so the check here would be redundant.
 */
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(const ExecutionContext &exec_ctx,
                     const std::shared_ptr<Operator> &probe_child_operator,
                     const std::shared_ptr<Operator> &build_child_operator,
                     const std::string &probe_column_name,
                     const std::string &build_column_name);

    ~HashJoinOperator() override = default;

    OperatorState Next(Chunk &output_chunk) override;

    void SelfInit() override;

    void SelfCheck() override;

    /**
     * Forward a BF from an ancestor join down through this operator's probe
     * or build child — whichever one contains column_name in its output schema.
     *
     * This allows multi-level pushdown: a BF built at the outermost join can
     * travel all the way down to the leaf scan.
     */
    void RegisterBloomFilter(std::shared_ptr<BloomFilter> bf,
                             const std::string &column_name) override;

private:
    void BuildHashTable();

private:
    std::string probe_column_name_;
    std::string build_column_name_;

    // ---- Build-side storage ----
    // Columnar layout: all build tuples packed into a flat vector.
    // tuple i starts at offset i * width_.
    std::vector<data_t> tuples_;
    idx_t tuple_count_;
    idx_t width_;

    // Hash table: key -> byte offset into tuples_
    std::unordered_multimap<data_t, idx_t> pointer_table_;

    // ---- RPT Bloom filter ----
    // Shared with the SeqScanOperator(s) on the probe side.
    // Populated at BuildHashTable() time; MarkReady() enables filtering.
    std::shared_ptr<BloomFilter> bloom_filter_;

    // ---- Probe-side buffering ----
    Chunk buffer_;
    idx_t buffer_ptr_;
    bool probe_child_exhausted_;
    bool hash_table_build_;
};

}  // namespace babydb
