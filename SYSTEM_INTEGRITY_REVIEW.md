# System integrity validation — 2026-10-11

For the latest October 15 corrections and 758-test revalidation on each target,
see [DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md). Dated results below remain
historical evidence for their recorded binaries.

This review checks the combined changes against the original library at commit
`1957c8a39c66860ba16cb38b35632d581cb0c238`. Consumer usage, existing public contracts,
target frameworks, return values and borrowed-connection ownership remain the
compatibility target. No package was published or deployed.

## Performance follow-up — 2026-10-12

The subsequent measured parameter-journal and captured-field optimizations passed
689 tests on each target, including 139 SQL facts, with zero failures/skips. The
same final DLLs passed binary-only legacy/API and Dapper-runtime revalidation;
consumer usage and connection/transaction coordination are preserved. Current
results, exact hashes and evidence are in [HIGH_LOAD_VALIDATION.md](HIGH_LOAD_VALIDATION.md).
The October 11 counts and hashes below remain the historical integrity-stage results.

## Confirmed findings and corrections

| Finding | Correction and verification |
| --- | --- |
| A whole-operation lease could block consumer getters, identity setters, attachment cloning and lazy list iterators waiting for another service call | Prepare values and native parameters outside the SQL lease; execute each command under its lease; run identity setters outside it. Sync/async, suppressed ExecutionContext and streaming list cases passed. |
| Snapshot acceptance reread live getters after the write and could accept a value that was never written | Capture values once and accept a prepared snapshot. A getter that changes its result after SQL execution now causes the next update to write that change. |
| Trigger row counts could accept a missing target row; NOCOUNT could prevent accepting a successful update | Use a collision-free native output parameter containing the target statement's `@@ROWCOUNT`. Preserve the original public affected-row return value, including `-1` with NOCOUNT. Both cases passed on SQL Server. |
| Preparation could overwrite an explicit detach/reattach or a newer attachment version | Validate immutable attachment versions before execution and acceptance; retain ownership tokens in rollback journals. Reattachment, partial updates, savepoints and root rollback passed. |
| Buffered materialization could block result constructors/setters making nested calls with MARS enabled | Acquire the reader under the lease, then release the lease before consumer materialization when SQL Server MARS is enabled. Drain remaining results and retain completion semantics. QueryBuilder, GetById, procedures and table functions passed sync/async tests. |
| Custom Dapper binders could see a different Identity or lose native command mutations | Bind once on the original provider command with the original parameter/result type, command-type hint and grid index. Restore the command state supplied by the binder at execution. Native command changes and generated binders passed. |
| Async multi-result output callbacks could run too late; buffered MARS mapping failures could spuriously deliver callbacks | Preserve DynamicParameters callback delivery inside the mapper and suppress completion after failed buffered hydration where direct Dapper also suppresses it. Differential callback/output tests passed. |
| Opening an async borrowed connection while holding the SQL lease could block its Open event's nested service query or Dispose | Open before acquiring the execution lease, preserve already admitted calls, and avoid waiting on the open gate when disposing a borrowed connection. Public procedure/function/CRUD/query paths passed. Bulk execution still holds its lease through all writes. |
| Failed SELECT/GROUP BY could commit a partial clause; failed WHERE/HAVING/APPLY could retain unused parameters | Stage complete clauses and restore failed parameter translations. Successful clauses added by a reentrant captured getter retain their own parameters. Retry, SET composition and the SQL Server parameter limit tests passed. |
| A pending SET transition could change the source scope while a captured predicate getter ran outside the state lock | Hold a separate translation scope through consumer evaluation. Pending scope writers wait for active translations, while nested predicate callbacks retain admission. Atomic SET/WHERE and suppressed-flow nested calls passed. |
| Constructor projections without an expression member list could fail after mutating the builder | Stage projection metadata with its SQL and preserve the original unaliased physical/projected output name for boxed constructor arguments. Direct execution and SET/filter/order composition passed. |
| APPLY type roles from a frozen SET operand could leak into a later JOIN/APPLY | Clear active subquery type bindings at the scope transition while keeping historical alias reservations and frozen SQL. Mixed JOIN/APPLY/SET values passed. |
| An independent APPLY builder or a parent SET performed inside its callback could reuse a parent source alias | Capture the returned query's SQL, source/output bindings and active sources together. Compile ON once with distinct reserved temporary symbols, then use a one-row parent-field projection when child sources could shadow the parent. Preserve child SQL, TOP/order, builder reuse and captured getter counts. |
| SQL comments containing WHERE/GROUP BY/OFFSET could receive the APPLY correlation instead of the real query scope | Ignore quoted text, line comments and nested block comments when locating top-level clauses. A correlated TOP query with misleading comment keywords returns the correct child for each parent. |
| The internal WithTableAlias caller could invalidate previously stored SQL fragments | Reject a rename before mutation when committed current-scope typed references depend on the alias. Early/idempotent renames, inactive SET scopes, independent APPLY scopes and alias-independent clauses remain usable. This method is absent from the public IQueryBuilder interface. |
| Newer Dapper releases clear prepared parameters or change internal binding signatures | Protect parameters during binding-only adapter cleanup, support the verified nullable/non-nullable DbType lookup and Identity constructor shapes, and temporarily restore the original parameter objects for deferred callbacks. Independent consumers checked Dapper 2.1.66 and 2.1.89; the package dependency remains 1.50.2. |
| Freezing full-update POCO parameters could discard an entity's own IDynamicParameters implementation | Preserve the original custom entity binder for full updates, including native stored-procedure overrides, while retaining generated parameters for tracked partial updates. Sync/async SQL controls and independent Dapper 2 consumers passed. |

## October 11 validation

| Check | Result |
| --- | --- |
| Full suite, net462 target on installed Framework 4.8.1 | 658 passed, 0 failed, 0 skipped |
| Full suite, .NET 8 | 658 passed, 0 failed, 0 skipped |
| SQL Server facts included in each full suite | 139 passed |
| System integrity and alias lifecycle tests | 63 passed on each runtime; 62 use actual SQL Server |
| AliasManager and alias lifecycle filter | 123 passed on each runtime |
| Existing public API | All 85 baseline entries preserved; 0 removed or changed |
| Existing interfaces | Exact member sets preserved, including third-party implementations |
| Old compiled consumer with only EasyDapper DLL replaced | 24 checks passed on each runtime; outputs identical to baseline |
| Old consumer binary hashes | Identical to the independently compiled baseline; no consumer rebuild |
| Library targets | .NET Framework 4.5 and .NET Standard 2.0 built successfully |
| NuGet vulnerability scan, including transitive packages | No vulnerable packages reported by the configured sources |
| `git diff --check` | Passed |

The SQL instance was local SQL Server 2019 Enterprise 15.0.2000.5. Tests used private
connection-scoped temporary tables or disposable objects with cleanup; application
tables were not changed. Tests cover bulk identity correlation, streaming CRUD,
tracking, partial entities, root/nested transaction commit and rollback, cancellation,
output parameters, callbacks, MARS, query composition and alias state together.

The previous eight comparative memory-optimized bulk runs remain documented in
COMPATIBILITY_VALIDATION.md. They are earlier evidence and were not rerun in this pass.

Independent consumers also exercised Dapper 2.1.66 and 2.1.89 on both test targets:
64 captured-parameter/CRUD differential cases, eight Identity/grid/native-command probes,
and 24 native-callback/no-column comparisons. These are separate consumer checks, not
additional tests included in the full-suite totals. The package dependency stays at
Dapper 1.50.2.

## Evidence

Final suite reports:

- `C:\Users\Masoud\AppData\Local\Temp\EasyDapperSystemIntegrity-20261011\Masoud_MASOUD-PC_2026-10-11_05_44_23.trx` — net462 target.
- `C:\Users\Masoud\AppData\Local\Temp\EasyDapperSystemIntegrity-20261011\Masoud_MASOUD-PC_2026-10-11_05_44_33.trx` — .NET 8.

The same directory contains API snapshots, API comparison summary, legacy-consumer
logs, unchanged consumer hashes and the hashes of the validated library binaries.
The independently built baseline and original consumer are retained under
`C:\Users\Masoud\AppData\Local\Temp\EasyDapperCompatibility-20261009`.

The differential async Open callback probe is retained under
`C:\Users\Masoud\AppData\Local\Temp\EasyDapperOpenCompatibilityProbe20261010`.

Independent Dapper runtime evidence:

- `C:\Users\Masoud\AppData\Local\Temp\EasyDapperCapturedRuntime-20261010\runtime-evidence.json`
  records the 64 captured-parameter/CRUD cases, each consumer's library hash and the
  actual Dapper product version.
- `C:\Users\Masoud\AppData\Local\Temp\EasyDapperRuntimeIntegrity-20261010\final-library-hashes.json`
  records the library hashes for the eight runtime consumers. Their successful logs are
  `cases\net8-dapper-2.1.66\result.txt`, `cases\net8-dapper-2.1.89\result.txt`,
  `cases\net462-dapper-2.1.66\result.txt`, `cases\net462-dapper-2.1.89\result.txt`,
  and the four `native-callback\cases\<target>-dapper-<version>\result.txt` files.
- The APPLY collision reproductions and final controls are retained under
  `C:\Users\Masoud\AppData\Local\Temp\EasyDapperApplyPivotScope-20261011` and
  `C:\Users\Masoud\AppData\Local\Temp\EasyDapperApplyRendererReview-20261011`.
  In each directory, `result.txt` preserves the failing reproduction and
  `final-result.txt` records matching direct SQL/current results with the final DLL.
  The consolidated final executions, exit codes and unchanged consumer hashes are in
  `C:\Users\Masoud\AppData\Local\Temp\EasyDapperSystemIntegrity-20261011\runtime-consumer-summary.json`.

Final validated library SHA-256 values:

- net45: `5FC0A7ADE280DD1C1E4609F54495B8E3AE0ABE1028474E558DD89235C48BBD96`.
- netstandard2.0: `665FD1FE50EF51D45CBF5C09E316D95D776BCA34C0DD9A033FA96DE991F180A9`.

To reproduce the full suite:

```powershell
$env:EASYDAPPER_SQL_TEST_CONNECTION = '<test SQL Server connection string>'
dotnet test EasyDapper.sln -c Release --logger trx -p:RunAnalyzers=false
dotnet list EasyDapper/EasyDapper.csproj package --vulnerable --include-transitive
git -c core.safecrlf=false diff --check
```

## Practical limits

The API comparison, binary-consumer checks and SQL tests establish compatibility for
the tested workflows. The 3,000 consumer applications and their production settings
were not available, so these results do not prove every application is unaffected.
Framework test assemblies target 4.6.2 and ran on the installed Framework 4.8.1
(registry version 4.8.09032, Release 533320); an isolated 4.6.2 runtime was not tested.
The 4.5 library target was built. SQL Server
2005 and every SQL collation/provider were not available. Runtime Dapper compatibility
was checked for the named versions, rather than assumed for all future releases.

Waiting for a suppressed-flow nested service operation from a synchronous connection
Open event was observed to fail in both the original and current library. The proven
async Open regression was corrected; this pass does not claim that synchronous
pattern has become supported.
