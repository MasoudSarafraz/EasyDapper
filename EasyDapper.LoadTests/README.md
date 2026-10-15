# Reproducible EasyDapper load measurements

This standalone .NET 8 consumer is outside the main solution. It references an
existing EasyDapper DLL, pins Dapper 1.50.2 and System.Data.SqlClient 4.8.6, and
does not build the library. It is a bounded synthetic **closed-loop** benchmark:
each worker waits for one operation to finish before issuing the next. It does
not establish production capacity, arrival-rate limits, or overload stability.

Build **once**, then use the same consumer executable for every library version:

```powershell
dotnet build .\EasyDapper.LoadTests\EasyDapper.LoadTests.csproj -c Release `
  -p:OverrideLibraryPath="C:\evidence\before\EasyDapper.dll"

$env:EASYDAPPER_SQL_TEST_CONNECTION = '<supplied SQL connection string>'
$consumer = '.\EasyDapper.LoadTests\bin\Release\net8.0'
Copy-Item -LiteralPath 'C:\evidence\before\EasyDapper.dll' -Destination "$consumer\EasyDapper.dll" -Force
dotnet "$consumer\EasyDapper.LoadTests.dll" sql --backend easy --scenario local-read `
  --workers 64 --seconds 5 --warmup 2 --label before --output 'C:\evidence\before-local-read-64.json'

Copy-Item -LiteralPath 'C:\evidence\after\EasyDapper.dll' -Destination "$consumer\EasyDapper.dll" -Force
dotnet "$consumer\EasyDapper.LoadTests.dll" sql --backend easy --scenario local-read `
  --workers 64 --seconds 5 --warmup 2 --label after --output 'C:\evidence\after-local-read-64.json'
```

Run cases sequentially, in fresh processes, without concurrent benchmark or
application workloads. Record multiple repetitions in alternating version order
before drawing conclusions. Library version labels are descriptive; every JSON
records the **actual loaded DLL SHA256**, along with the consumer, Dapper and
SqlClient hashes, runtime, OS, CPU count, GC settings and SQL Server version.
`query-build` dispatches to the separate CPU-only query-generation benchmark and
does not require the SQL environment variable.

`RunComparison.ps1` swaps only the library DLL, checks that the consumer hash
stays fixed, writes a per-case report/log and an incremental manifest, and stops
on any failure in before/optimized/direct-Dapper cases. It retains original
shared-connection failures separately. Re-running a phase requires a new evidence
directory; existing case files/manifests are protected from overwrite.
`-Phase query` and `-Phase sql` use three
repetitions by default with alternating version order. `-Phase diagnostics`
runs nine explanatory cases with provider observations. `-Phase sustained`
runs the optimized DLL once in each of shared-read, local-mixed and local-query
(64 clauses), with 256 workers; specify a longer `-Seconds` window explicitly.
Sustained results are separate correctness/drain observations, not repeated
comparative throughput statistics.
`-Phase read-check` separately repeats local reads at 16/64 workers against before
and optimized, allowing longer windows to examine short-window differences.

```powershell
.\EasyDapper.LoadTests\RunComparison.ps1 `
  -BeforeLibrary 'C:\evidence\before\EasyDapper.dll' `
  -OptimizedLibrary 'C:\evidence\optimized\EasyDapper.dll' `
  -OriginalLibrary 'C:\evidence\original\EasyDapper.dll' `
  -EvidenceDirectory 'C:\evidence\results' -Phase sql `
  -Repetitions 3 -Seconds 5 -WarmupSeconds 1
```

## SQL scenarios

Use `--backend easy` or `--backend dapper` for each of these scopes, with
`--workers 1`, `16`, `64`, `128` or `256`:

| Scenario | Connection/service scope | Work |
| --- | --- | --- |
| `shared-read` | One pre-opened connection and one EasyDapper service shared by workers | Point reads distributed deterministically over 4096 rows |
| `local-read` | One pre-opened connection/service per worker | The same point reads |
| `local-mixed` | One pre-opened connection/service per worker | Alternating point read and untracked full update of that worker's unique row |
| `local-query` | One pre-opened connection/service per worker | Fresh QueryBuilder per read, Id equality plus captured redundant cutoffs; `--clauses 1`, `64` or `128` |

`local-query` exercises expression translation and parameter checkpoint work in
real SQL calls. Every operation creates a fresh builder, calls `.Where(row =>
row.Id == id)`, adds `N-1` captured `.Where(row => row.Id > cutoff)` predicates
with `cutoff=id-1`, and executes the buffered result. It reads that worker's
unique row and checks the same Id/Value/Payload invariants. Its direct reference
uses exactly the SQL captured from that public builder before the timed window,
with the same N Int32 parameters, and rebuilds DynamicParameters per call. Direct
Dapper does not translate expressions; that difference is intentional and
reported. The SQL sample, hash, parameter inventory and binding rules are in
`benchmark.queryShape`; compare these between library versions before comparing
their rates. `--clauses` defaults to 1 and is restricted to 1/64/128 in SQL cases.

```powershell
dotnet "$consumer\EasyDapper.LoadTests.dll" sql --backend easy --scenario local-query `
  --clauses 64 --workers 16 --seconds 5 --warmup 2 --label before `
  --output 'C:\evidence\before-local-query-64clauses-16workers.json'
```

The direct-Dapper shared case uses a harness `SemaphoreSlim(1,1)` so that commands
on the single shared `SqlConnection` are valid. This gate is explicit in the
report. Direct worker-local cases have no shared gate. No scenario creates extra
connections per operation. Services wrap benchmark-owned external connections;
connections and initial SQL/metadata caches are warmed before measured work.

Every connection uses `tempdb`, connect/command timeout 30 seconds, default
`--mars false` and `--max-pool-size 512`. The JSON records these effective
settings without credentials. `--mars true` is a separate experiment, not a
change to the normal consumer API. Changing these settings changes the case.

## Fixture ownership and correctness

The model contains a GUID table name generated once when writing this consumer:
`tempdb.dbo.__EasyDapperLoad_d8cd825aaad34d4fa388efb25d761a11`. A fixed attributed
model lets exactly the same compiled consumer run against old and new DLLs.
Each process holds an exclusive session application lock, **refuses an existing
table**, creates 4096 rows, writes a run-specific GUID extended property, and
checks that GUID before dropping the table in `finally`. It never clears or drops
an existing application table. Simultaneous runs of this compiled consumer are
refused. If the process is killed or cleanup fails, the report identifies its
table/run GUID; inspect ownership before manual cleanup. The SQL login needs
permission to create/drop its table in tempdb and use the application lock.

Every read checks Id, Value and Payload. Every update must affect exactly one
row. Worker `n` only updates Id `n+1`; there is no cross-worker lost-update race.
Warmup and measured phases separately verify all 4096 rows, no missing IDs,
expected per-worker update totals and untouched payloads. Warmup values are
reset after workers drain, outside the measured window. A worker stops on its
first error so retries cannot conceal corruption or create a failure storm.
Errors and data validation failures produce exit code 3; configuration errors
produce exit code 2. A clean, correct run with successful cleanup exits 0.

## Timing and output

`--seconds 5` defines the monotonic **dispatch** window; operations already in
flight finish afterwards. Throughput uses total wall time **including drain**.
The JSON reports both durations. `--warmup 2` and `--drain-seconds 35` are defaults.
Public `GetByIdAsync`, `UpdateAsync` and QueryBuilder `ExecuteAsync` have no cancellation-token overload, so
the harness does not claim API cancellation. It stops dispatch at the deadline,
keeps 30-second native command timeouts, and closes benchmark-owned connections
if draining exceeds its bound. A drain timeout is a failed benchmark, never a
throughput success.

Schema version 1 reports:

- Configuration, timestamps, exact binaries, client/server identity and fixture cleanup.
- Warmup/measured successes, attempted operations, errors, worker stops and examples.
- Read/update/all p50, p95, p99, mean and max client-call latency, including failures.
- CPU time, wall time, allocations and GC generation deltas, working set and threadpool state.
- Optional local **whole SQL process** CPU delta and own-SPID session CPU/read/write DMV deltas.
- Full final correctness checks and a top-level `passed` result.

Latency histograms have fixed memory per worker, 1 microsecond buckets below
128 microseconds and at most 0.8% relative bucket width above that. Percentiles
are bucket **upper bounds**, not falsely exact samples. Process/threadpool/DMV
sampling is every 200ms on one observer connection; its overhead is included in
process metrics. SQL process CPU includes unrelated server activity if present.
Remote servers or insufficient process/DMV permissions produce explicit
unavailable reasons, not invented zero values. Session DMVs are counters, not a
complete attribution of server waits. Lifetime peak working set includes warmup;
sampled phase peak is reported separately. Histogram storage is allocated before
the phase snapshots and correctness validation runs after them, so those
allocations are excluded from the phase delta. In-window harness, latency
recording, observer and provider work are included. Working-set measurements
still include the histogram storage and other live harness objects.

`--provider-timings true` adds opt-in SqlClient DiagnosticListener intervals.
Use this for a few explanatory cases, e.g. shared/local reads at 128 workers,
**separate from the main throughput matrix**, because observing adds overhead.
It reports public call to first native Before (preparation, scheduling and lease
wait combined), matched native Before-to-After/Error (provider Execute only),
and the remainder before public return where event ordering permits. Reader
After can precede row materialization, and async After may arrive after the
public call returns. Missing, unmatched and late events are counted. None of
these measurements is described as pure gate wait or pure SQL/server time.
