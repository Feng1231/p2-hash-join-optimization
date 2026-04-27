#pragma once

#include "common/config.hpp"
#include "common/macro.hpp"
#include "common/typedefs.hpp"
#include "execution/execution_context.hpp"

#include <algorithm>
#include <memory>
#include <string>

namespace babydb {

// Forward declaration so Operator can reference BloomFilter without pulling in
// the full header everywhere.  Operators that actually use BFs (HashJoin,
// SeqScan) will include bloom_filter.hpp directly.
class BloomFilter;

typedef std::vector<std::pair<Tuple, idx_t>> Chunk;

enum OperatorState {
    HAVE_MORE_OUTPUT,
    EXHAUSETED
};

class Operator {
public:
    virtual ~Operator() = default;

    DISALLOW_COPY_AND_MOVE(Operator);

    Operator(const ExecutionContext &exec_ctx, std::vector<std::shared_ptr<Operator>> &&child_operators,
             const Schema &output_schema)
        : exec_ctx_(exec_ctx), child_operators_(std::move(child_operators)), 
          output_schema_(output_schema) {}

    Operator(const ExecutionContext &exec_ctx, std::vector<std::shared_ptr<Operator>> &&child_operators)
        : exec_ctx_(exec_ctx), child_operators_(std::move(child_operators)), output_schema_{} {
        for (auto &child_operator : child_operators_) {
            auto child_schema = child_operator->GetOutputSchema();
            output_schema_.insert(output_schema_.end(), child_schema.begin(), child_schema.end());
        }
    }

    virtual OperatorState Next(Chunk &output_chunk) = 0;

    void Init() {
        for (auto &child_operator : child_operators_) {
            child_operator->Init();
        }
        SelfInit();
    }

    const Schema& GetOutputSchema() {
        return output_schema_;
    }

    void Check() {
        for (auto &child_operator : child_operators_) {
            child_operator->Check();
        }
        CheckSchema();
        SelfCheck();
    }

    virtual std::string BindTableName() { return INVALID_NAME; }

    /**
     * RPT-style Bloom filter pushdown.
     *
     * A HashJoinOperator calls this on its probe child (and the probe child
     * forwards it further down the tree) to register a BF that should be
     * applied as early as possible — ideally inside a SeqScanOperator before
     * any per-tuple work is done.
     *
     * The default implementation is a no-op: operators that sit in the middle
     * of a probe pipeline but cannot reason about column provenance (e.g.,
     * future aggregates or projections) simply ignore the registration, which
     * is safe — the BF will just not be applied at that level.
     *
     * Operators that *can* forward the BF (HashJoinOperator) or consume it
     * (SeqScanOperator) override this method.
     *
     * @param bf           Shared Bloom filter built over the build-side keys.
     * @param column_name  The fully-qualified output column name that the BF
     *                     was built for (e.g. "movie_info.movie_id").
     */
    virtual void RegisterBloomFilter(std::shared_ptr<BloomFilter> /*bf*/,
                                     const std::string & /*column_name*/) {
        // Default: no-op (safe to ignore)
    }

protected:
    virtual void SelfInit() = 0;

    virtual void SelfCheck() = 0;

    void CheckSchema() {
        auto schema_copy = output_schema_;
        std::sort(schema_copy.begin(), schema_copy.end());
        for (idx_t column_id = 1; column_id < schema_copy.size(); column_id++) {
            if (schema_copy[column_id] == schema_copy[column_id - 1]) {
                throw std::logic_error("Duplicated column name in operator's output");
            }
        }
    }

protected:
    ExecutionContext exec_ctx_;

    std::vector<std::shared_ptr<Operator>> child_operators_;

    Schema output_schema_;
};

}  // namespace babydb
