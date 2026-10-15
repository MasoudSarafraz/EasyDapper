# High-load validation — 2026-10-12

The October 15 reviews corrected additional query-composition defects. Current
hashes, 758-test suites and fresh load checks are in
[DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md). Measurements below describe the
recorded October 12 binaries; the 10-million-operation matrix was not rerun on
the newer DLLs.

The measured optimizations improve query construction and the SQL workload that
constructs a fresh query for each call. Existing consumer APIs, parameter values,
callback boundaries, connection ownership and transaction coordination are
preserved. This is evidence from bounded workloads on one machine, not a capacity
guarantee for all applications using the ORM.

Across the main SQL matrix, diagnostics, longer runs and basic-read follow-up,
the final optimized DLL passed **84/84 SQL cases** with **10,184,796 measured
operations**, zero errors and correct fixture data. A portable summary of these
observed checks is saved in
[validation-summary.json](EasyDapper.LoadTests/Results/2026-10-12/validation-summary.json).

## Changes and correctness

- Parameter checkpoints now record their own writes instead of copying every
  existing parameter before every clause. Rollback preserves committed nested
  and concurrent writes, nulls, original references and surviving name ownership.
  Successful translations discard their undo history.
- Ordinary captured instance fields use a guarded reflection read instead of
  compiling a lambda for every predicate. Values are still read per translation,
  within the same consumer-code boundary. Properties, methods, static/volatile
  fields, value-type and special constant receivers retain the compiled route.

No expression/value cache is shared between independent queries. The command
gate, service connection and transaction lifetime are unchanged. These are
internal changes; consumers do not need new options, calls or migrations.

Both full suites passed **689/689** tests, with **139 actual SQL Server facts per
target**, zero failures and zero skips. The 31 new cases comprise 17 checkpoint
ownership/rollback cases and 14 expression-evaluation cases. Framework execution
used the net462 test target on installed Framework 4.8.1; this does not establish
execution on a separately installed 4.5/4.6.2 runtime. Partial-trust/CAS behavior
was source-reviewed, not exercised in a partial-trust host.

Binary-only revalidation passed 24 legacy checks on each runtime with identical
baseline output. All 85 baseline public API entries remain; the two previously
added optional sorting extensions account for the current 87 entries. Independent
Dapper 2.1.66/2.1.89 consumers passed 64 captured-parameter/CRUD cases, eight
Identity modes, 24 callback/no-column comparisons and four APPLY/direct-SQL result
comparisons. Consumers were copied and their library DLL replaced without
rebuilding them. The legacy, Identity/callback and APPLY consumers retain their
previously recorded hashes. The captured-CRUD consumers' hashes were first
recorded on October 12; they prove unchanged executables during this revalidation,
not a historical hash comparison that the older evidence did not record.

## Repeated measurements

Host: Intel Core i7-6700HQ, four physical/eight logical processors, 16 GB memory,
Windows 11 Enterprise 10.0.22631. Performance runs used .NET 8.0.19 x64, workstation
GC, Dapper 1.50.2, System.Data.SqlClient 4.8.6 and local SQL Server 2019 Enterprise
15.0.2000.5. SQL whole-process CPU was inaccessible; own-SPID DMV counters were
recorded. Missing counters are reported as unavailable.

One consumer binary was used in every performance case. Only EasyDapper.dll was
swapped between immutable copies: `before` is the corrected October 11 version;
`optimized` adds the two changes above; `original` is an independent build of
commit `1957c8a39c66860ba16cb38b35632d581cb0c238`. Provider/runtime dependencies were
held fixed. Each repetition used a fresh process; version order alternated.
No other benchmark, build or test ran during the timed load phases.

The CPU matrix has 54 runs: three versions, 1/64/512 clauses, 1/8 workers and three
repetitions with three measured seconds. All passed with identical SQL per shape.
Median construction throughput improved **3.86–29.64× versus before**. Allocation
fell **17.2–81.3% versus before**. Against original, large-query allocation also
fell, but one-clause builders still allocate **43.6–51.2% more**. Eight workers
yielded only 1.37–1.58× the optimized single-worker rate; this is not linear CPU
scaling. Full ranges and sample-count limitations are in
[CPU_QUERY_BUILD_RESULTS.md](CPU_QUERY_BUILD_RESULTS.md).

The main SQL matrix has 234 runs: 72 each for before, optimized and direct Dapper,
plus 18 descriptive original-version probes. Each comparative shape has three
repetitions, five measured seconds and one warmup second. Workers were
1/16/64/128/256 for point read/mixed workloads and 1/64/256 for fresh queries with
1/64/128 clauses. Warmup, initial connection opening, fixture creation and final
validation were outside the measured interval; throughput includes drain time.

The optimized DLL passed **72/72**, with **3,187,791 measured operations**
(2,626,052 reads and 561,739 updates), zero warmup/measured errors, incorrect
results, drain timeouts or cleanup failures. Before and direct Dapper also passed
72/72 each. Original failed the shared non-MARS connection probes at 64/256
workers, producing 320 errors in warmup and another 320 in measurement. Those
failed runs are explicitly excluded from successful capacity comparisons.
All main-matrix rates, ranges, latency, allocation and GC data are retained in
[SQL_LOAD_RESULTS.md](SQL_LOAD_RESULTS.md).

For fresh-query SQL execution, median throughput improved **1.31–8.21× versus
before** across the nine shapes; allocation fell **10.4–63.2%**. Parallel
64/256-worker shapes improved **1.31–4.03×**. These gains do not extend to every
CRUD operation. Basic reads use neither changed class: their short-window
results include both increases and decreases. Direct Dapper remained faster in
these SQL scenarios; it does not perform expression translation or the ORM's
parameter preparation and command/transaction coordination.

All 90 local-query SQL reports produced one identical SQL hash and the expected
parameter count within each 1/64/128-clause shape. Direct Dapper used the same
captured SQL and parameter values. Its shared-connection control included an
explicit harness semaphore, so both compared connection scopes were valid.

## Longer 256-worker checks

Three separate optimized runs each used 120 measured seconds and three warmup
seconds. All **3,535,279 measured operations** completed correctly, with zero
warmup/measured errors, missing rows, drain timeouts or fixture cleanup failures.
Every fixture's full 4096-row contents were checked after each phase.

| Scenario | Operations | Operations/s | p95 ms | p99 ms | Sampled peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared point reads | 501,602 | 4,177.6 | 72.192 | 79.360 | 108.9 |
| Independent reads/full updates | 2,816,668 | 23,469.6 | 21.632 | 26.112 | 119.2 |
| Independent fresh queries, 64 clauses | 217,009 | 1,808.1 | 168.960 | 211.968 | 200.9 |

These are single longer correctness/drain observations, not three-repeat speed
comparisons. Fresh 64-clause SQL calls still allocate about 274,134 bytes each
(including provider/harness work), with 581 generation-2 collections over the
measured interval. Reduced allocation does not mean negligible GC cost, and
two-minute peaks/end values do not prove multi-hour memory stability.

## Basic-read follow-up

The main five-second matrix showed lower optimized point-read medians at 16/64
workers (11.8%/7.0%). Basic GetById execution does not use either changed class,
so the cause was not assigned from those short observations. Twelve additional
fresh-process cases repeated before/optimized at both worker counts, with three
repetitions, 20 measured seconds and three warmup seconds. All passed with zero
errors and correct rows/cleanup.

| Workers | Before median [min,max] operations/s | Optimized median [min,max] operations/s | Before/optimized p99 ms |
| ---: | ---: | ---: | ---: |
| 16 | 27,296.2 [26,583.0,28,119.5] | 27,850.1 [24,758.0,27,981.3] | 3.088 / 2.992 |
| 64 | 28,939.8 [26,827.1,29,857.6] | 29,697.3 [27,200.2,29,804.5] | 6.560 / 6.432 |

The median regression did not reproduce in these longer checks. Individual
runs still vary, including a lower optimized 16-worker repeat; these overlapping
ranges do not prove a universal basic-read speedup or explain the variation.

## Interpretation and limits

A shared service still owns one connection. Concurrent callers queue for command
admission; raising their number alone does not increase that connection's
capacity. Independent service/connection results describe a separate workload
scope, not a required change to existing consumer usage. Benchmark pool size 512,
MARS=false and 30-second timeouts are harness settings only.

SQL cases use a warmed 4096-row tempdb table, small buffered results and independent
worker rows for updates. They do not test cross-worker write contention, realistic
joins/business transactions, bulk throughput, cold network connections, pool
exhaustion, an externally imposed arrival rate or multi-hour memory stability.
CPU cases construct independent builders; they do not load-test concurrent
mutation of one builder. Correctness regression tests cover additional lifecycle,
transaction, cancellation, alias and callback behavior separately.

Percentiles are descriptive observations, not production p99 guarantees. SQL
histograms report bucket upper bounds with at most 0.8% relative width above
128 microseconds; three repeats provide ranges, not confidence intervals. The
slow 512-clause CPU controls have only 30–44 samples at one worker, so their p99
is the maximum observed sample. Process allocation includes in-window harness
and provider work; working-set peaks include live harness storage.

Provider diagnostics are separate from the throughput matrix. Pre-command time
includes preparation, scheduling and admission; provider ExecuteReader time may
end before materialization. Neither is pure gate wait or server-only SQL time.
Definitions and primary provider-source references are in
[PERFORMANCE_MEASUREMENT_NOTES.md](PERFORMANCE_MEASUREMENT_NOTES.md).

All nine diagnostic runs passed. Their measured native Before/End events matched
the public-call counts without missing IDs, ends or unmatched events. Some async
end events arrived after public return and were retained rather than discarded.
For the optimized shared service at 128 workers, observed p95 pre-command/provider
intervals were 50.176/0.342 ms, consistent with most time preceding the native
command. These separate quantiles cannot be added or treated as pure gate wait.

## Evidence and reproduction

Local evidence root:
`C:\Users\Masoud\AppData\Local\Temp\EasyDapperHighLoad-20261012`.

- `results/manifest-query.json`, `manifest-sql.json` and per-case JSON/logs retain
  options, actual DLL/dependency hashes, timestamps, counts, timings and cleanup.
- `results/manifest-diagnostics.json`, `manifest-sustained.json`,
  `DIAGNOSTIC_SUMMARY.json` and `SUSTAINED_SUMMARY.json` retain the separate
  explanatory and longer checks.
- `results/manifest-read-check.json` and `READ_CHECK_ANALYSIS.json` retain the
  longer basic-read repetitions without replacing the original matrix.
- `SQL_ANALYSIS.json`, `SQL_ANALYSIS_AGGREGATES.csv`, `SQL_ANALYSIS_COMPARISONS.csv`
  and `SQL_LABEL_TOTALS.json` retain all median/min/max values and failed original
  probes; `CPU_BENCHMARK_ANALYSIS.json` retains the CPU comparisons.
- `TEST_VALIDATION.json` identifies both full TRX files and source-to-test SQL
  identity matches. TRX does not contain DLL hashes; current output hashes were
  checked separately against the immutable optimized copies.
- `final-validation/legacy`, `final-validation/captured-runtime` and
  `final-validation-complete/runtime` contain final binary-only consumer output,
  dependency graphs/hashes and API/result comparisons.

Final library SHA-256:

| Target | SHA-256 |
| --- | --- |
| net45 | `A10FCC656792D8E950F04A2B0CCEE69BC5C3611026BCB7B85414E035947FA064` |
| netstandard2.0 | `B64AEFA0B695369DF07494E8DF699C366812A2DC0BB8D72CE6011D5324807A08` |

The unchanged load consumer SHA-256 is
`3E5A179B60052A6DB88E464730A27DD2E75757E0AB575A288698AEBEC6CC5566`.
Use the [standalone harness instructions](EasyDapper.LoadTests/README.md) and
`RunComparison.ps1` to reproduce on an isolated test SQL instance. No package was
published or deployed and no application tables were changed. Every SQL case
checks all fixture rows and drops only its own GUID-owned table.

The final library and harness NuGet scans, including transitive packages, reported
no vulnerable packages from the configured sources on October 12. Logs are retained
as `library-vulnerability-scan.txt` and `harness-vulnerability-scan.txt` in the
evidence root. `git diff --check` passed. These scans are limited to their configured
advisory sources, not a claim of universal security verification.
