Arrow Buffer Reads Roadmap
==========================

The parent request is `#31 <https://github.com/mylonasc/pyrex/issues/31>`_.
ABR identifiers are stable feature names, not release-version promises.
Priority is a delivery order: **P0** first, then **P1**, then **P2**. Dependencies
are explicit delivery/validation gates and are also recorded with GitHub's
native blocked-by and sub-issue relationships.

Delivered baseline
------------------

Through 0.7.1, native Arrow C Streams already support lazy bounded batches,
full/prefix/range scans, owned binary output, named column families, shared
pinned snapshots and committed-state TransactionDB reads. Existing iterator
APIs remain supported. ``multi_get`` / ``multi_get_cf`` return Python lists;
they are **not** an Arrow point-materialization API. TransactionDB scans
observe committed state, **not** transaction-local read-your-writes.

The original design text and unchecked boxes in #31 are historical, not a
current feature-availability matrix. These follow-ups cover remaining work.

Feature index
-------------

.. list-table:: Remaining features and dependencies
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
     - Implemented on feature branch; review pending
   * - `ABR-02 <https://github.com/mylonasc/pyrex/issues/39>`_
     - Reproducible scale, memory and interoperability benchmarks
     - P1
     - ABR-01
     - Implemented on stacked feature branch; review pending
   * - `ABR-03 <https://github.com/mylonasc/pyrex/issues/40>`_
     - Cooperative in-flight cancellation and C Stream errors
     - P1
     - ABR-01
     - Implemented on feature branch; review pending
   * - `ABR-04 <https://github.com/mylonasc/pyrex/issues/41>`_
     - Snapshot-consistent Arrow batched point lookups
     - P1
     - ABR-01, ABR-03
     - Planned
   * - `ABR-05 <https://github.com/mylonasc/pyrex/issues/42>`_
     - Multi-range/multi-prefix scans at one horizon
     - P2
     - ABR-01, ABR-03
     - Planned
   * - `ABR-06 <https://github.com/mylonasc/pyrex/issues/43>`_
     - Validated scan controls and capability contract
     - P2
     - ABR-01
     - Planned
   * - `ABR-07 <https://github.com/mylonasc/pyrex/issues/44>`_
     - Measured native builder/buffer-copy optimization
     - P2
     - ABR-01, ABR-02
     - Planned
   * - `ABR-08 <https://github.com/mylonasc/pyrex/issues/45>`_
     - Transaction-local Arrow scans with read-your-writes
     - P2
     - ABR-01, ABR-03
     - Planned

Why ABR-01 first
----------------

Before tuning buffer builders or introducing new streaming sources, measure
what the current producer emits, what it allocates, how long native phases
take, and what consumers retain. The highest-priority feature adds that
observability without changing the default path's timing collection or
requiring PyArrow/Polars in core PyRex. See :doc:`arrow_scans` for its public
``collect_metrics=True`` / ``ArrowScan.stats`` contract.

ABR-01 does not replace the existing benchmark or implement ABR-02's
large-scale, fresh-process memory experiments. Its integration simply makes
native counters available to those measurements. ABR-02 is tracked separately
in PR #47. ABR-03 adds an independent, atomic cancellation request; existing
``close()`` remains the synchronous resource-release API.

Scope boundaries
----------------

Every feature retains binary schema/ordering, shared-horizon validation,
owned-output lifetimes and explicit error handling. Graph predicates,
payload decoding, temporal publication and reasoning remain application
responsibilities; raw KV Arrow output is not a decoded graph relation.

Optional optimizations are adopted only with correctness tests and measured
benefits. No batch-cap or buffer-counter result is a whole-process RSS bound.
