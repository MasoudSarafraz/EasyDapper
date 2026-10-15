# Deep composition review — 2026-10-15

Three additional defects were reproduced and corrected after the earlier
[same-day review](REVIEW_2026_10_15.md). The final library passes **758 tests on
each target**, including **199 actual SQL cases across 153 methods**. Existing
consumer signatures and required usage remain compatible. This pass changes
production code only in [QueryBuilderCore.cs](EasyDapper/QueryBuilding/QueryBuilderCore.cs).

## Confirmed findings

| Priority | Defect and observed behavior | Correction |
| --- | --- | --- |
| P1 | When two projected columns had the same name, SET/APPLY could bind different members to the first column. SET returned four rows instead of one; APPLY returned parent IDs `1,1,2,3` instead of amounts `7,9,7,5`, without a SQL exception. Aggregate and ROW_NUMBER name collisions could also redirect a member. | Keep each selected/aggregate member's output ordinal and translate that position to the unique names required by a derived source. Preserve the latest selected occurrence, aggregate-first SQL order, ROW_NUMBER offsets and nested SET reuse. |
| P2 | A JOIN added from a captured getter, on the same or another thread, could reuse an alias belonging to a JOIN whose translation was still in progress. SQL Server rejected the repeated correlation name. | Reserve each in-flight JOIN role until its translation commits or fails. The reservation is local to the builder and created only for builders using JOIN. Previously committed nested joins and preselected role bindings survive a failed outer translation. |
| P2 | Ordered CROSS/OUTER APPLY without TOP or paging generated a derived query that SQL Server rejected. | Apply the existing ordered-operand preparation to the embedded APPLY snapshot, adding OFFSET 0 only when neither TOP nor OFFSET is present. The original child SQL remains unchanged and reusable. |

New regressions and controls are in
[DeepCompositionRegressionTests.cs](EasyDapper.Tests/QueryBuilder/DeepCompositionRegressionTests.cs):
46 actual SQL cases across ten methods. Coverage includes duplicate names,
case-only collisions, repeated member selection, aggregate/selection call order,
ROW_NUMBER, root-member output supplied by a joined CLR role, nested SET reuse,
all four SET operators, TOP, paging, CROSS/OUTER APPLY and child-query reuse.
Concurrency controls include 32 nested parallel joins while another JOIN's
getter is active, producing 33 joins on the same builder, and failure/retry paths.

## Final verification and compatibility

| Check | Final result |
| --- | --- |
| Full suite, net462 target on installed Framework 4.8.1 | 758 passed, 0 failed, 0 skipped |
| Full suite, .NET 8.0.19 | 758 passed, 0 failed, 0 skipped |
| Actual SQL cases in each full suite | 199 across 153 methods |
| Identical compiled new-regression consumer, DLL replacement only | 16 failed / 30 passed on the previous library; 46 passed on the final library |
| Existing public API | All 85 baseline entries retained, zero removed/changed; current 87 includes two optional extensions from earlier work |
| Frozen legacy consumers, each runtime | 24 checks, identical ordered baseline output, no consumer rebuild |
| Independent Dapper 2.1.66 / 2.1.89 consumers | 64 captured-parameter/CRUD cases, eight Identity modes and 24 native-callback/no-column comparisons passed |
| Existing independent APPLY consumers | Four direct-SQL comparisons passed; child SQL unchanged |
| Library targets | net45 and netstandard2.0 built successfully |

The SQL opt-in convention is retained: with the connection environment variable
unset, all ten new SQL methods skip cleanly with zero failures.

Framework execution is on registry version 4.8.09032, Release 533320; this is not
a separate execution on a Framework 4.5/4.6.2 installation. The normal suite uses
pinned Dapper 1.50.2 and SqlClient 4.8.6. Consumer hashes and DLL hashes are checked
by the validation scripts. Captured-CRUD consumer hashes were first recorded on
October 12; earlier historical hashes were unavailable. Their provenance remains
explicit in the retained manifests.

The same new-regression executable was used before and after replacing only
EasyDapper.dll. Its SHA-256 is
`7C1A5F1CDD6D56657FF50D807572C69A7BC788EF52E95418E35CE75CEE6D1ED9`.
Full TRX files and the method-to-case audit retain all failing before-fix results.
Connection ownership, transaction admission, tracking, parameter checkpoint
journals and captured-field evaluation were not changed in this pass; their
integrated and independent-consumer checks pass.

## Fresh load checks

The frozen load consumer, SHA-256
`3E5A179B60052A6DB88E464730A27DD2E75757E0AB575A288698AEBEC6CC5566`, is used with
only the library replaced. Final-library workloads cover shared point reads,
independent read/update operations and fresh queries with 64 predicates, each
with 256 workers, three warmup seconds and 20 measured seconds. Exact operation
counts, binaries, errors, correctness, draining and cleanup are recorded in
[deep-review-summary.json](EasyDapper.LoadTests/Results/2026-10-15/deep-review-summary.json).

All three final runs passed: **83,742 shared reads**, **459,000 mixed operations**
and **33,438 fresh-query reads**, totaling **576,180 measured operations**. Warmup
and measured errors were zero; all 4,096 fixture rows validated, every worker
drained and all three fixtures were removed successfully. These are correctness
checks, with no new comparative throughput claim.

The October 12 comparative benchmark and earlier same-day load checks belong to
their recorded binaries. They were not replayed in full and are not measurements
of this final revision. These bounded local SQL2019 workloads establish tested
behavior; they do not certify production capacity or every application of the
approximately 3,000 consumers.

## Exact final binaries and evidence

| Target | SHA-256 |
| --- | --- |
| net45 | `13A406D4E9253021945F96C36FDD4DC019215F2AD5EA1BE20171FB6B463D3869` |
| netstandard2.0 | `736A6BBFE560114C6AAF63F8FE7D31651808610ABD77A9C832E68AC2A1871EDA` |

Evidence root: `G:\EasyDapperDeepReview-20261015`.

- `final-complete-tests/`, `final-test-audit.json`: final suites and actual SQL case identities.
- `frozen-current-before-results/`, `frozen-current-after-results/`, `frozen-current-hashes.csv`: identical consumer, 16 reproduced failures to zero, DLL replacement only.
- `final-current/`, `final-current-hashes.csv`: immutable final library and source copies.
- `compatibility-current/`: final legacy/API, captured-runtime, Identity, callback and APPLY results with frozen input manifests.
- `load-current/`, `load-current-consumer/`: final workloads and unchanged load executable.
- `audit-results.ps1`, `deep-review-summary.json`: checked extraction and portable summary.

Earlier `final/`, `final-tests/`, `load/` and intermediate directories remain
separate historical evidence. Their hashes and counts must not be mixed with the
final-current artifacts above. No application tables were modified. SQL fixtures
use connection-scoped temporary tables or the harness's GUID-owned disposable
tempdb table. No package was published, deployed or committed.
