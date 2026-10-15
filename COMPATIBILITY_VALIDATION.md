# Compatibility validation — 2026-10-11

The final October 15 binaries were rechecked against the unchanged legacy and
independent Dapper consumers; all 85 baseline API entries remain. Current test
counts, hashes and findings are in [DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md).

The fixes preserve the existing public interfaces, method signatures, optional arguments,
generic constraints and target frameworks. Existing usage remains the compatibility target;
the new `OrderByColumns` extension is optional. No database migration or consumer API
migration is introduced.

## Performance follow-up — 2026-10-12

After two internal query-construction optimizations, both suites passed 689 tests
including 139 SQL facts per target. Binary-only revalidation again preserved all
85 baseline API entries and identical output for 24 legacy checks per runtime.
The existing independent Dapper consumers also passed on the final DLLs without
rebuilding. Current binary hashes, provenance and performance limits are recorded
in [HIGH_LOAD_VALIDATION.md](HIGH_LOAD_VALIDATION.md). The October 11 results below
are retained as earlier evidence.

## Baseline and environment

- Baseline: repository commit `1957c8a39c66860ba16cb38b35632d581cb0c238`, built independently
  from a clean `git archive` extraction.
- Library targets: .NET Framework 4.5 and .NET Standard 2.0.
- Test targets: net462 and net8.0; SDK 8.0.316 on Windows. Framework execution used the
  installed 4.8.1 runtime (registry version 4.8.09032, Release 533320).
- Real database: local SQL Server 2019 Enterprise, engine version 15.0.2000.5.
- Production application tables were not modified. Disposable database tests verified cleanup.

## October 11 results

| Check | Result |
| --- | --- |
| Full test suite, net462 target on installed Framework 4.8.1 | 658 passed, 0 failed, 0 skipped |
| Full test suite, .NET 8 | 658 passed, 0 failed, 0 skipped |
| SQL Server facts included in each suite | 139 passed on each runtime |
| Existing public API comparison | 85 baseline entries preserved on both targets; 0 removed or changed |
| Existing interface member sets | Unchanged, including support for third-party implementations |
| Consumer compiled against the old DLL, .NET Framework | 24 checks passed after replacing only the library DLL |
| Consumer compiled against the old DLL, .NET 8 | 24 checks passed after replacing only the library DLL |
| Consumer executable/assembly SHA-256 before and after | Identical; consumer was not recompiled |
| Legacy consumer output versus baseline | Identical on both runtimes |
| Memory-optimized bulk insert comparison | Baseline and current version succeeded in all 8 runs |
| NuGet vulnerability scan, including transitive packages | No vulnerable packages reported by the configured sources |
| `git diff --check` | Passed |

Memory-optimized checks covered snapshot elevation OFF/ON, administrator and restricted
users, SELECT/INSERT-only target permissions, and denied VIEW DEFINITION. Generated IDs
matched their input rows and transaction counts returned to zero. The dedicated generated
test database, data files and memory container were confirmed removed.

## Compatibility regressions addressed during review

- The complete system integrity pass includes 63 integrity/lifecycle tests and corrected cross-module
  regressions in CRUD callbacks, tracking acceptance, MARS hydration, native parameter
  binding, async opening, failed query translations and APPLY/SET scope changes.
  APPLY collisions with returned independent builders and SET scopes now retain child
  SQL, TOP/order, native projection names and builder reuse. Detailed findings and final
  evidence are recorded in SYSTEM_INTEGRITY_REVIEW.md, including independent Dapper
  2.1.66/2.1.89 consumers on both targets.
- Restored trusted raw `OrderBy` expressions and `CustomJoin` hints. Restrictive validation
  is available only through the optional extension.
- Preserved unnamed aggregate result metadata, duplicate projection names in plain set
  queries, legacy string overload behavior and nullable projection naming.
- Preserved multi-result mapper calls into the same service, with the existing SQL Server
  MARS requirements when a reader is still active.
- Prepared stored-procedure/function parameters outside the command lease, keeping native
  provider commands for custom binders and evaluating bound getters once. Covered nested
  commands with both flowing and suppressed ExecutionContext, output parameters, completion
  callbacks and synchronous/asynchronous execution.
- Preserved asynchronous validation as faulted tasks and externally owned connection lifetime.
- Restored tracking baselines on root/savepoint rollback without requiring another Attach,
  including partial entities and explicit detach/reattach during a transaction.
- Preserved bulk insertion into targets that reject MERGE without requiring metadata rights;
  validated fallback rollback, cancellation and protection against retrying trigger failures.

Other regression tests cover bulk identity ordering, nullable/BIGINT identities, mapped
primary keys, cross-type tracking keys, savepoint names, APPLY/set parameter isolation,
post-set filtering/paging, captured NULL, negated HasValue, empty Contains, literal LIKE
characters, asynchronous opening and shared-connection command coordination.

## Evidence and reproduction

The final TRX reports and binary-consumer comparison logs after the system integrity review
are saved locally in:

`C:\Users\Masoud\AppData\Local\Temp\EasyDapperSystemIntegrity-20261011`

Successful complete runs:

- `Masoud_MASOUD-PC_2026-10-11_05_44_23.trx` — net462 target.
- `Masoud_MASOUD-PC_2026-10-11_05_44_33.trx` — .NET 8.

The earlier focused alias findings are documented in ALIAS_MANAGER_REVIEW.md. The
internal caller limitation now has a guard and six SQL lifecycle tests; late renaming
of an actively referenced alias is rejected before changing the builder's state.
Earlier validation artifacts remain in
`C:\Users\Masoud\AppData\Local\Temp\EasyDapperFinalCompatibility-20261009`.

The consumer harness and independently built old library are in:

`C:\Users\Masoud\AppData\Local\Temp\EasyDapperCompatibility-20261009`

The public API baseline tests are part of the repository. Separate baseline resources retain
each runtime's Reflection formatting rather than treating formatting differences as API changes.

To rerun the suite, supply a SQL connection with the test permissions described in README:

```powershell
$env:EASYDAPPER_SQL_TEST_CONNECTION = '<test SQL Server connection string>'
dotnet test EasyDapper.sln -c Release --logger trx
dotnet list EasyDapper/EasyDapper.csproj package --vulnerable --include-transitive
```

These checks establish compatibility for the public API and tested workflows. They do not
prove every behavior in all 3,000 consumer applications: those applications and their
production configurations were not available for execution. SQL Server 2005 was not available;
its non-MERGE path was exercised on SQL Server 2019. The library's .NET 4.5 target was built,
but the net462 tests ran on installed Framework 4.8.1; an isolated 4.6.2 runtime was not
tested. No package was published or deployed.
