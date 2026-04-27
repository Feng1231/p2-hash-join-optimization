# RPT-Style Bloom Filter Pushdown for BabyDB Hash Join

## 1. Background

BabyDB executes multi-way joins with a Volcano-style pull pipeline. Each
`HashJoinOperator` materialises its build side into a hash table on first
call, then probes one tuple at a time pulled up from its probe child. The
benchmark suite (10 queries derived from the Join Order Benchmark) runs over
this pipeline and the harness records end-to-end wall time.

A student attempted to speed up the baseline by adding a textbook Bloom
filter to `HashJoinOperator`. Their reasoning was the standard one: build a
BF over the build-side keys; for every probe tuple, consult the BF before
the hash table; reject early on negative.

In their measurements, however, the BF version was **slower** than the
baseline. They asked whether they had implemented it wrong.

This report documents (a) why their version slowed things down, (b) the
RPT-style pushdown that fixes the structural problem, and (c) the resulting
performance.

---

## 2. The Original Problem

We re-ran the student's submission against the baseline, three runs each
on `reduced_data`:

| Version                   | Total time (3 runs)                |
|---------------------------|-------------------------------------|
| Baseline (no BF)          | 11875 / 11930 / 11896 ms            |
| Student version (BF, no pushdown) | 13110 / 13569 / 12965 ms |

The student's BF version is consistently ~10% **slower**. Reading the code
shows why.

### 2.1 The BF was not pushed down

The student's BF was used inside the same operator that built it:

```cpp
// student version, inside HashJoinOperator::Next
if (bloom_filter_ && !bloom_filter_->might_contain(probe_key)) {
    continue;  // early reject
}
auto match_range = pointer_table_.equal_range(probe_key);
```

In other words: by the time a probe tuple reaches this BF check, it has
already

1. been read at the bottom scan,
2. paid for `KeysFromTuple` to be materialised into the output chunk,
3. flown up through every operator between that scan and this join,
4. survived all intermediate hash-join probes (`UnionTuple` on each).

The "savings" from the BF amount to one `unordered_multimap::equal_range`
call per rejected tuple. That is the cheapest part of the pipeline. The
expensive work — copying, hashing through every level, polluting cache —
has already happened.

### 2.2 The BF itself was expensive to probe

The student's BF was also slow per probe:

* Backed by `std::vector<bool>` (bit-packed but per-access bounds checks
  and bit shifts in the standard library implementation).
* Indexed with `(h1 + i*h2) % bits_.size()` — a real 64-bit IDIV every
  probe because `bits_.size()` was not a power of two.
* `optimal_hash_count` capped `k` at 10, so the hot loop did up to 10
  scattered memory reads per probe.

For the JOB-derived workload, where the join produces many output rows
(probe keys mostly do match), the BF rejection rate is low. Each probe
pays the BF cost without recovering it.

### 2.3 Net effect

The student saved a cheap thing (one hash table lookup) at the cost of a
more expensive thing (a multi-hash bit probe), on a workload where the BF
rarely rejects, with no reduction in upstream pipeline work. The ~10%
slowdown is exactly what the README warned about (Reference 1, paper
section on in-memory BFs).

---

## 3. The Fix: RPT-Style Pushdown

The Robust Predicate Transfer paper (Zhao et al. 2024, Reference 1) makes
the structural argument: the BF must filter at the **source**, so that a
rejected tuple never enters the pipeline at all. The savings then are not
"one hash lookup" but "all the work that tuple would have caused upstream:
extraction, copying, every intermediate join probe, every cache line
written."

We implement that for BabyDB by giving every `HashJoinOperator` a BF that
it pushes down through its probe pipeline to the originating
`SeqScanOperator`.

### 3.1 Files touched

| File                                       | Change                                |
|--------------------------------------------|---------------------------------------|
| `src/include/execution/bloom_filter.hpp`   | New header-only `BloomFilter` class    |
| `src/include/execution/operator.hpp`       | Added virtual `RegisterBloomFilter`   |
| `src/include/execution/hash_join_operator.hpp` | Owns a `shared_ptr<BloomFilter>`  |
| `src/execution/hash_join_operator.cpp`     | Pushdown at construction; populate in build; drop in-operator BF check |
| `src/include/execution/seq_scan_operator.hpp`  | Holds pending/resolved filter lists |
| `src/execution/seq_scan_operator.cpp`      | Resolves columns at SelfInit; filters raw tuples in Next |

No CMake changes (the new BF is header-only).

### 3.2 A faster Bloom filter

We replaced the BF with a small, cache-aware implementation:

* Backed by `std::vector<uint64_t>`, accessed via direct bit ops.
* Capacity rounded up to a power of two; probing uses `idx & mask` (no
  modulo, no IDIV in the hot loop).
* Hash is `splitmix64` over the int64 key, then a re-mix for `h2`;
  the `k`-th probe is `(h1 + i*h2) & mask` (Kirsch–Mitzenmacher).
* `k` is computed from the optimal-`k` formula and clamped to `[1, 6]`.
* An explicit `ready_` flag. While `false`, `MightContain` returns
  `true` (i.e., no filtering). This keeps the contract correct in the
  small window between BF construction and the end of `BuildHashTable`.
  `MarkReady()` is called unconditionally at the end of build, so even
  an empty build side correctly rejects every probe.

### 3.3 The pushdown protocol

We added one virtual method to `Operator`:

```cpp
virtual void RegisterBloomFilter(std::shared_ptr<BloomFilter> bf,
                                 const std::string &column_name) {}
```

Default implementation is a no-op (safe for any operator that cannot
reason about column provenance, e.g., a hypothetical aggregate or
projection in the middle of a probe pipeline).

* **`HashJoinOperator::RegisterBloomFilter`** forwards. The plan's
  `CheckSchema` already guarantees disjoint column names across schemas,
  so at most one of `probe_child` or `build_child` contains
  `column_name`; we forward the BF to that one.
* **`SeqScanOperator::RegisterBloomFilter`** is the terminal: it stores
  `(bf, column_name)` in a pending list. At `SelfInit`, we resolve each
  column name (an entry of this scan's *output* schema) into the
  corresponding *raw* table column index. The hot loop in `Next` only
  ever touches integer indices.

### 3.4 Where pushdown happens

In `HashJoinOperator`'s constructor:

```cpp
bloom_filter_ = std::make_shared<BloomFilter>(...);
probe_child_operator->RegisterBloomFilter(bloom_filter_, probe_column_name_);
```

Construction order in BabyDB is bottom-up, so by the time the outermost
join is constructed every BF has already settled into the right scan.

### 3.5 Where filtering happens

In `SeqScanOperator::Next`, before `KeysFromTuple`:

```cpp
for (const auto &rf : resolved_filters_) {
    if (!rf.bf->MightContain(tuple[rf.raw_column_idx])) {
        rejected = true;
        break;
    }
}
if (rejected) continue;
// ... only now do KeysFromTuple ...
```

A rejected row never gets extracted, never enters a chunk, never reaches
any join above it.

### 3.6 Timing argument

Pushdown is only safe if every BF a scan is asked to apply is `ready_`
by the time that scan starts emitting tuples. In a Volcano pipeline this
is automatic:

```
outer.Next()
  └─ outer.BuildHashTable()           // outer.bf is filled here
      └─ drains outer's build side
  └─ probe phase
      └─ inner.Next()
          └─ inner.BuildHashTable()   // inner.bf is filled here
              └─ drains inner's build side
                  ↑ outer.bf was already ready before this point,
                    so a BF outer pushed onto inner's build-side scan
                    still applies in time
          └─ probe phase
              └─ scanA.Next()         // both inner.bf and outer.bf ready
```

The build side of any join is fully drained before the probe side runs.
Therefore, for *any* leaf scan in the plan, every ancestor join whose BF
was registered on it has already finished its build phase by the time
that scan starts producing tuples. The invariant holds for arbitrary
plan depth and for BFs that target either branch of an intermediate
join.

### 3.7 What we removed

The student's in-operator BF check (between `equal_range` and the loop)
is now strictly redundant: every tuple that reaches `HashJoinOperator::Next`
has already passed our BF at the scan. We deleted it. The probe loop is
back to its baseline shape, except that the input stream is sparser.

---

## 4. Results

All 10 benchmark queries pass with `correct` output cardinality. The
project's existing unit tests (`make check-tests`) still pass 13/13.

### 4.1 Total time

| Version                  | Run 1 | Run 2 | Run 3 | Mean      |
|--------------------------|-------|-------|-------|-----------|
| Baseline                 | 11875 | 11930 | 11896 | ~11900 ms |
| Student (BF, no pushdown)| 13110 | 13569 | 12965 | ~13200 ms |
| **RPT pushdown (this work)** | **2570** | **2381** | **2341** | **~2430 ms** |

Mean speedup vs. baseline: **~4.9×**.

### 4.2 Per-query breakdown

| Query | Baseline (ms) | RPT (ms) | Speedup |
|-------|---------------|----------|---------|
| 10a   |  377          |  38      |  9.9×   |
| 11a   |  457          |  41      | 11.1×   |
| 12a   |  293          |  17      | 17.2×   |
| 13a   | 1654          | 238      |  6.9×   |
| 14a   |  505          |  47      | 10.7×   |
| 15a   |  339          |  18      | 18.8×   |
| 16a   | 3797          | 936      |  4.1×   |
| 17a   | 3402          | 1161     |  2.9×   |
| 18a   | 1222          |  63      | 19.4×   |
| 19a   |  319          |  25      | 12.8×   |

### 4.3 Reading the per-query numbers

Most queries get a 10× or larger speedup. The two outliers, 16a and 17a,
are the queries with the largest absolute baselines and the highest
intermediate cardinalities — most tuples in their probe pipelines do
match somewhere, so the BF rejection rate at the scan is lower than for
the high-selectivity queries. Even there the speedup is 3–4×, because
the rejected fraction still avoids the expensive cascade of upstream
joins.

The speedup is not from BF probing being faster than hash-table probing;
it is from removing tuples from the pipeline at the bottom rather than
the top. This matches the qualitative prediction in the RPT paper: in an
in-memory database the BF only pays for itself when it is pushed down
the pipeline, never when it lives next to the hash probe it was meant
to short-circuit.

---

## 5. Summary

| | |
|---|---|
| Diagnosis | Student's BF lived next to the hash probe it was supposed to short-circuit, after every upstream operator had already paid the per-tuple cost. The BF itself was also slow per probe (vector\<bool\>, modulo, k up to 10). Net: ~10% slowdown. |
| Fix | (1) Replaced the BF with a power-of-two-sized, mask-indexed, splitmix64-hashed version with k ≤ 6 and an explicit ready flag. (2) Added a `RegisterBloomFilter` virtual on `Operator`; `HashJoinOperator` pushes its BF down to the originating `SeqScanOperator` at construction time. (3) Scans filter raw tuples before `KeysFromTuple`. (4) Removed the redundant in-operator BF check. |
| Outcome | All 10 queries produce correct results; 13/13 unit tests pass. Total time: 11900 ms → 2430 ms, ~4.9× speedup. Per-query speedup ranges from 2.9× (17a) to 19.4× (18a). |

## References

1. Zhao, Su, Yang, Yu, Koutris, Zhang. *Debunking the Myth of Join
   Ordering: Toward Robust SQL Analytics.* 2024.
2. Qiao, Zhang. *Data Chunk Compaction in Vectorized Execution.* 2024.
3. Lang, Neumann, Kemper, Boncz. *Performance-Optimal Filtering: Bloom
   Overtakes Cuckoo at High Throughput.* 2019.
