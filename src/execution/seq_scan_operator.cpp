#include "execution/seq_scan_operator.hpp"

#include "storage/catalog.hpp"
#include "storage/table.hpp"

namespace babydb {

// ---- Local helpers --------------------------------

static const Schema &FetchTableSchema(const ExecutionContext &exec_ctx,
                                      const std::string &table_name) {
    return exec_ctx.catalog_.FetchTable(table_name).schema_;
}

static Schema CombineSchema(const std::string &table_name, const Schema &schema) {
    auto schema_copy = schema;
    for (auto &column : schema_copy) {
        column = table_name + "." + column;
    }
    return schema_copy;
}

// ---- Constructors ---------------------------------

SeqScanOperator::SeqScanOperator(const ExecutionContext &exec_ctx,
                                 const std::string &table_name)
    : SeqScanOperator(exec_ctx, table_name, FetchTableSchema(exec_ctx, table_name)) {}

SeqScanOperator::SeqScanOperator(const ExecutionContext &exec_ctx,
                                 const std::string &table_name,
                                 const Schema &fetch_columns)
    : SeqScanOperator(exec_ctx, table_name, fetch_columns, table_name) {}

SeqScanOperator::SeqScanOperator(const ExecutionContext &exec_ctx,
                                 const std::string &table_name,
                                 const Schema &fetch_columns,
                                 const std::string &table_output_name)
    : SeqScanOperator(exec_ctx, table_name, fetch_columns,
                      CombineSchema(table_output_name, fetch_columns)) {}

SeqScanOperator::SeqScanOperator(const ExecutionContext &exec_ctx,
                                 const std::string &table_name,
                                 const Schema &fetch_columns,
                                 const Schema &output_schema)
    : Operator(exec_ctx, {}, output_schema),
      table_name_(table_name),
      fetch_columns_(fetch_columns) {}

// ---- RegisterBloomFilter ----------------------------------------------------

void SeqScanOperator::RegisterBloomFilter(std::shared_ptr<BloomFilter> bf,
                                          const std::string &column_name) {
    // Store as pending; we resolve to a raw column index in SelfInit()
    // so that the hot path in Next() uses only integer arithmetic.
    pending_filters_.push_back({std::move(bf), column_name});
}

// ---- SelfInit ---------------------------------------------------------------

void SeqScanOperator::SelfInit() {
    next_row_id = 0;

    // Resolve pending BF registrations into raw table column indices.
    //
    // output_schema_ uses fully-qualified names like "t.col".
    // fetch_columns_ uses the bare column name as it appears in the table.
    // We find the position of the registered column name in output_schema_,
    // then map that to the corresponding entry in fetch_columns_, and then
    // ask the table schema for the raw column index.

    resolved_filters_.clear();

    const Schema &table_schema = FetchTableSchema(exec_ctx_, table_name_);

    for (const auto &pf : pending_filters_) {
        // Find position of column_name in the output schema
        idx_t output_pos = INVALID_ID;
        for (idx_t i = 0; i < output_schema_.size(); ++i) {
            if (output_schema_[i] == pf.output_column_name) {
                output_pos = i;
                break;
            }
        }
        if (output_pos == INVALID_ID) {
            // Column not in this scan's output — ignore (shouldn't happen
            // after correct pushdown routing, but safe to skip)
            continue;
        }

        // output_pos is also the index into fetch_columns_
        const std::string &bare_col = fetch_columns_[output_pos];

        // Ask the table schema for the raw column index
        idx_t raw_idx = INVALID_ID;
        for (idx_t i = 0; i < table_schema.size(); ++i) {
            if (table_schema[i] == bare_col) {
                raw_idx = i;
                break;
            }
        }
        if (raw_idx == INVALID_ID) {
            continue;  // shouldn't happen after SelfCheck passes
        }

        resolved_filters_.push_back({pf.bf, raw_idx});
    }
}

// ---- Next -------------------------------------------------------------------

OperatorState SeqScanOperator::Next(Chunk &output_chunk) {
    idx_t output_size = 0;

    auto &table = exec_ctx_.catalog_.FetchTable(table_name_);
    const auto key_attrs = table.schema_.GetKeyAttrs(fetch_columns_);
    const bool has_filters = !resolved_filters_.empty();

    auto read_guard = table.GetReadTableGuard();
    const auto &rows = read_guard.Rows();
    const idx_t total_rows = rows.size();

    while (output_size < exec_ctx_.config_.CHUNK_SUGGEST_SIZE) {
        if (next_row_id >= total_rows) {
            output_chunk.resize(output_size);
            return EXHAUSETED;
        }

        const auto &[tuple, meta] = rows[next_row_id];
        next_row_id++;

        if (meta.is_deleted_) {
            continue;
        }

        // ---- RPT Bloom-filter early rejection ----
        // Test every registered BF before paying for KeysFromTuple.
        // A tuple that fails any BF is provably not going to join with
        // anything on the build side that registered that BF, so we skip it
        // entirely — it never enters the pipeline above this scan.
        if (has_filters) {
            bool rejected = false;
            for (const auto &rf : resolved_filters_) {
                if (!rf.bf->MightContain(tuple[rf.raw_column_idx])) {
                    rejected = true;
                    break;
                }
            }
            if (rejected) {
                continue;
            }
        }

        // ---- Tuple extraction (only for tuples that passed the BFs) ----
        if (output_size == output_chunk.size()) {
            output_chunk.emplace_back(tuple.KeysFromTuple(key_attrs), next_row_id - 1);
        } else {
            output_chunk[output_size].first = tuple.KeysFromTuple(key_attrs);
            output_chunk[output_size].second = next_row_id - 1;
        }
        output_size++;
    }

    output_chunk.resize(output_size);
    return HAVE_MORE_OUTPUT;
}

// ---- SelfCheck --------------------------------------------------------------

void SeqScanOperator::SelfCheck() {
    FetchTableSchema(exec_ctx_, table_name_).GetKeyAttrs(fetch_columns_);

    if (fetch_columns_.size() != output_schema_.size()) {
        throw std::logic_error("SeqScanOperator: Fetch columns and output schema do not match");
    }
}

}  // namespace babydb
