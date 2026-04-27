#pragma once

#include "execution/bloom_filter.hpp"
#include "execution/operator.hpp"

#include <memory>
#include <string>
#include <vector>

namespace babydb {

/**
 * Sequential Scan Operator
 *
 * By default uses "<table_name>.<column_name>" as the output schema.
 * The table name used in the output schema can be overridden, or the full
 * output schema can be supplied directly.
 *
 * ### RPT Bloom-filter support
 *
 * Ancestor HashJoinOperators register Bloom filters here via
 * RegisterBloomFilter().  Each registration supplies:
 *   - a shared BloomFilter (shared_ptr so the join operator can populate it
 *     during its build phase while this scan holds it safely), and
 *   - the fully-qualified *output* column name the BF was built for.
 *
 * At SelfInit() time (which runs after Check() has verified the schema) we
 * resolve each column name to the corresponding raw table column index so
 * that the hot path in Next() only ever deals with integers.
 *
 * In Next(), before KeysFromTuple() or any other per-tuple work, we test
 * every resolved BF.  A tuple that fails any BF is skipped entirely — it
 * never gets extracted, never enters a Chunk, and never reaches any join.
 */
class SeqScanOperator : public Operator {
public:
    SeqScanOperator(const ExecutionContext &exec_ctx, const std::string &table_name);

    SeqScanOperator(const ExecutionContext &exec_ctx, const std::string &table_name,
                    const Schema &fetch_columns);

    SeqScanOperator(const ExecutionContext &exec_ctx, const std::string &table_name,
                    const Schema &fetch_columns, const std::string &table_output_name);

    SeqScanOperator(const ExecutionContext &exec_ctx, const std::string &table_name,
                    const Schema &fetch_columns, const Schema &output_schema);

    ~SeqScanOperator() override = default;

    OperatorState Next(Chunk &output_chunk) override;

    void SelfInit() override;

    void SelfCheck() override;

    std::string BindTableName() override { return table_name_; }

    /**
     * Register a Bloom filter to be applied during sequential scan.
     *
     * @param bf           The BloomFilter shared with the join that built it.
     * @param column_name  Fully-qualified output column name (e.g. "t.id").
     */
    void RegisterBloomFilter(std::shared_ptr<BloomFilter> bf,
                             const std::string &column_name) override;

private:
    std::string table_name_;
    Schema fetch_columns_;
    idx_t next_row_id{0};

    // ---- RPT filter state ----

    /**
     * Pending registrations: pairs of (BF, output_column_name).
     * Accumulated via RegisterBloomFilter(); resolved to raw column indices
     * at SelfInit() time.
     */
    struct PendingFilter {
        std::shared_ptr<BloomFilter> bf;
        std::string output_column_name;
    };
    std::vector<PendingFilter> pending_filters_;

    /**
     * Resolved filters: ready for the hot path in Next().
     * Each entry holds the BF and the index into the *raw table row* (not the
     * fetch-column subset) so we can test before any extraction.
     */
    struct ResolvedFilter {
        std::shared_ptr<BloomFilter> bf;
        idx_t raw_column_idx;  // index into the full table tuple
    };
    std::vector<ResolvedFilter> resolved_filters_;
};

}  // namespace babydb
