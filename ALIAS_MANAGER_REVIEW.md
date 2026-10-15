# AliasManager review — 2026-10-09

The latest integrated re-review and final binary compatibility checks are in
[DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md). The dated findings below retain their
original evidence; AliasManager was not changed in the October 15 pass.

Reviewed the complete AliasManager class, its three existing test files, QueryBuilderCore,
ExpressionParser and the public query-builder interface. Added 27 regression/control tests.
Existing public signatures and consumer call patterns remain unchanged.

The October 12 performance follow-up retained AliasManager behavior and passed
the complete 689-test suites on both targets. Final binary-only compatibility
checks and load evidence are in [HIGH_LOAD_VALIDATION.md](HIGH_LOAD_VALIDATION.md).
The dated findings and counts below remain their respective earlier-stage evidence.

## Confirmed defects and corrections

| Defect | Observable effect | Correction |
| --- | --- | --- |
| SetTableAlias removed the previous registration before checking the replacement owner | A rejected rename left a usable lookup missing from the registry and allowed its old alias to be reused by another table | Check conflicts before mutation; update registration as one operation |
| Canonical table renames left type bindings stale | Table and type lookups disagreed; SELECT/WHERE could reference an alias absent from FROM | Refresh types bound to the old canonical alias, retaining distinct joined roles |
| SetTypeAlias could register an existing unique alias without any canonical table lookup | Type and table lookup generated different aliases for the initial binding | Initialize a missing canonical table binding; preserve an existing one when assigning a role |
| Type reassignment to a new canonical alias retained the obsolete registration | An alias no longer used as the canonical binding could not be reused | Use the common canonical registration path |
| Reservation/registration and unique-generation/Clear operations used different synchronization | Successful lookup could lack an inventory entry; generation could cross a cleared counter epoch | Coordinate all registration, reservation and generation writes with one manager lock; snapshot another manager before acquiring this manager's lock |
| Generation could return an already registered/reserved name | Caller had to encounter or repair a collision during registration | Skip occupied generated names, including case-only variants; keep explicit case-distinct aliases for case-sensitive usage |
| Distinct CLR roles mapped to the main table, or a main type rejoined through another left type, reused FROM aliases | SQL Server error 1011; an incomplete repair could select the main row instead of the joined row | Check active source aliases; bind the new distinct role and its previously selected columns after ON parsing succeeds |
| Column references did not quote numeric or delimited aliases consistently | Real SQL syntax errors near a dot or a keyword, despite quoted FROM aliases | Use consistent escaped references; retain the original formatting for ordinary generated aliases |

The new SQL tests verified joined child IDs 2, 3 and 10 rather than the main row's ID 1.
Case-only correlated APPLY was verified using actual SQL results 10 and 20. Numeric,
reserved-word, whitespace, closing-bracket and Persian aliases were executed against SQL
Server, using private connection-scoped temporary tables.

## Intentional behavior preserved

- Unique join aliases do not replace the existing primary type binding for same-CLR self
  joins or repeated same-CLR joins.
- A distinct CLR role may bind a unique occurrence of the same physical table while the
  original FROM/table binding remains intact.
- Earlier subquery aliases stay registered when another APPLY of the same type is added:
  earlier SQL fragments still reference those occurrences.
- Explicit aliases differing only in case remain separate registrations. Automatic
  generation avoids case-only collisions; no database open or collation query is added to
  BuildQuery.
- ClearAliases retains its existing counter reset and alias reuse behavior. Reserving a
  manager into itself is a no-op.
- Alternate CLR projections without JOIN keep using the main physical table. Projections
  declared before a successful JOIN bind the correct joined role.
- Unsupported JOIN expressions do not change a previously usable projection's binding.
- Trusted raw ORDER BY expressions retain the existing distinct-table alias numbering;
  occupied names alone advance generated counters.

Three sensitive compatibility controls were run against the independently built original
library: alternate-view same-CLR self join, reuse after a rejected JOIN, and qualified raw
ORDER BY inside APPLY. All three passed on the original library and on the final version.

## Final validation

| Check | Result |
| --- | --- |
| Alias-related tests targeting net462 | 115 passed |
| Alias-related tests on .NET 8 | 115 passed |
| Newly added alias tests | 10 state tests, 5 concurrency tests, 12 actual SQL tests |
| Complete suite on each runtime | 593 passed, 0 failed, 0 skipped |
| SQL integration tests within each complete suite | 73 passed |
| Previously compiled consumer after replacing only the library DLL | 24 checks passed on each runtime; output identical to baseline |
| Consumer executable/assembly hashes before and after | Identical |
| Public API baseline checks | Passed on both runtimes |
| git diff --check | Passed |

The concurrency tests include 1,000 coordinated reservation/registration trials and bounded
reverse-reservation waits. They check valid final lookup/inventory states and role bindings,
not just absence of exceptions. These finite stress tests supplement the locking review;
they do not constitute a proof of every possible thread schedule.

Final full-suite reports:

`C:\Users\Masoud\AppData\Local\Temp\EasyDapperAliasValidation-20261009\Masoud_MASOUD-PC_2026-10-09_16_10_21.trx`

`C:\Users\Masoud\AppData\Local\Temp\EasyDapperAliasValidation-20261009\Masoud_MASOUD-PC_2026-10-09_16_10_30.trx`

The same directory contains legacy-consumer logs, unchanged-binary hashes and
`legacy-join-controls-net8.trx` for the three original-library compatibility controls.

```powershell
$env:EASYDAPPER_SQL_TEST_CONNECTION = '<test SQL Server connection string>'
dotnet test EasyDapper.Tests/EasyDapper.Tests.csproj -c Release --filter 'FullyQualifiedName~AliasManager'
dotnet test EasyDapper.sln -c Release --logger trx
```

## Internal caller follow-up — 2026-10-11

The concrete internal QueryBuilder has WithTableAlias methods that are absent from the
public IQueryBuilder interface. The system integrity pass now rejects a rename atomically
if committed current-scope typed fragments depend on that alias. Six actual SQL tests
cover preserved execution after rejection, early/idempotent renames, frozen SET scopes,
independent APPLY scopes and alias-independent operations. Replacing a table alias used
by committed typed references is rejected before mutation. General replay of already rendered clauses under a new
alias is not supported. The October 11 full suites passed 658 tests on each runtime; the combined
AliasManager/lifecycle filter has 123 passing tests. Final evidence is recorded in
SYSTEM_INTEGRITY_REVIEW.md; the 593-test results above belong to the earlier focused pass.

The follow-up also corrected source shadowing when an independent APPLY builder or a
callback SET transition reused the parent's alias, including collisions with child JOIN/APPLY
outputs. A private one-row parent-field projection preserves correlation while leaving child
SQL untouched. Temporary ON symbols and correlation aliases use atomic reservations without
changing the ordinary table/subquery counters or type bindings. Two new reservation controls
check existing-owner preservation, case-only collision, Clear and concurrent single ownership.

SQL execution used the local SQL Server 2019 test instance. Case-insensitive comparisons
were exercised, and ordinal explicit registration was preserved. Exhaustive identifier
equivalence under every SQL collation (including accent, width and language-specific rules)
was not tested. The net462 tests ran on the installed Framework 4.8.1 runtime; an isolated
4.6.2 runtime was not tested. Consumer applications were not deployed or modified.
