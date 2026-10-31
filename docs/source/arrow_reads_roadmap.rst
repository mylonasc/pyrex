Arrow Buffer Reads Roadmap
==========================

The parent request is `#31 <https://github.com/mylonasc/pyrex/issues/31>`_.
ABR identifiers are stable feature names, not release-version promises.
Priority is a delivery order: **P0** first, then **P1**, then **P2**. Dependencies
are explicit delivery/validation gates and are also recorded with GitHub's
native blocked-by and sub-issue relationships.

Delivered baseline
------------------

Before the ABR follow-ups, native Arrow C Streams supported lazy bounded batches,
full/prefix/range scans, owned binary output, named column families, shared
pinned snapshots and committed-state TransactionDB reads. Existing iterator
APIs remain supported. ``multi_get`` / ``multi_get_cf`` return Python lists;
0.7.1 additionally provides ``multi_get_arrow`` / ``multi_get_arrow_cf``
for Arrow point materialization. DB-level TransactionDB scans observe committed
state; explicit transaction-local scans added in 0.7.1 provide read-your-writes.

The original design text and unchecked boxes in #31 are historical, not a
current feature-availability matrix. All indexed follow-ups below are merged
into ``main`` and included in 0.7.1 alongside the ordinary point-read fix.

Feature index
-------------

.. list-table:: Merged features and dependencies
   :header-rows: 1
   :widths: 12 42 10 20 16

   * - Index
     - Feature
     - Priority
     - Depends on
     - Status
   * - `ABR-01 <https://github.com/mylonasc/pyrex/issues/38>`_
     - Opt-in native metrics and Arrow buffer accounting
     - P0
     - None
     - Included in 0.7.1
   * - `ABR-02 <https://github.com/mylonasc/pyrex/issues/39>`_
     - Reproducible scale, memory and interoperability benchmarks
     - P1
     - ABR-01
     - Included in 0.7.1
   * - `ABR-03 <https://github.com/mylonasc/pyrex/issues/40>`_
     - Cooperative in-flight cancellation and C Stream errors
     - P1
     - ABR-01
     - Included in 0.7.1
   * - `ABR-04 <https://github.com/mylonasc/pyrex/issues/41>`_
     - Snapshot-consistent Arrow batched point lookups
     - P1
     - ABR-01, ABR-03
     - Included in 0.7.1
   * - `ABR-05 <https://github.com/mylonasc/pyrex/issues/42>`_
     - Multi-range/multi-prefix scans at one horizon
     - P2
     - ABR-01, ABR-03
     - Included in 0.7.1
   * - `ABR-06 <https://github.com/mylonasc/pyrex/issues/43>`_
     - Validated scan controls and capability contract
     - P2
     - ABR-01
     - Included in 0.7.1
   * - `ABR-07 <https://github.com/mylonasc/pyrex/issues/44>`_
     - Measured native builder/buffer-copy optimization
     - P2
     - ABR-01, ABR-02
     - Included in 0.7.1
   * - `ABR-08 <https://github.com/mylonasc/pyrex/issues/45>`_
     - Transaction-local Arrow scans with read-your-writes
     - P2
     - ABR-01, ABR-03
     - Included in 0.7.1

Why ABR-01 first
----------------

Merge checkpoint (2026-10-30): all indexed features are merged into ``main``.
PRs #46 (ABR-01), #47 (ABR-02), #48 (ABR-03), #49 (ABR-04), #50 (ABR-05),
#51 (ABR-06), #52 (ABR-07) and #53 (ABR-08) were merged in dependency order,
preserving commit ancestry. That checkpoint preceded release preparation;
the combined changes are included in the 2026-10-31 0.7.1 release.
``benchmarks/arrow_integration.md`` records cumulative native/sanitizer
validation and the independent ABR-02 runner's 216-worker matrix plus
18-worker 1M profile against the final native implementation.

Before tuning buffer builders or introducing new streaming sources, measure
what the current producer emits, what it allocates, how long native phases
take, and what consumers retain. The highest-priority feature adds that
observability without changing the default path's timing collection or
requiring PyArrow/Polars in core PyRex. See :doc:`arrow_scans` for its public
``collect_metrics=True`` / ``ArrowScan.stats`` contract.

ABR-01 does not replace the existing benchmark. ABR-02 adds an isolated mode
and uses ABR-01 counters for fresh-process resource experiments; see
:doc:`arrow_benchmarks`. ABR-03 adds an independent, atomic cancellation
request; existing ``close()`` remains the synchronous resource-release API.

Scope boundaries
----------------

Every feature retains binary schema/ordering, shared-horizon validation,
owned-output lifetimes and explicit error handling. Graph predicates,
payload decoding, temporal publication and reasoning remain application
responsibilities; raw KV Arrow output is not a decoded graph relation.

Optional optimizations are adopted only with correctness tests and measured
benefits. No batch-cap or buffer-counter result is a whole-process RSS bound.
