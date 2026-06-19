# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.8.5] - 2026-06-19

### Added (60 new advanced AliasManager tests, total 262)

Comprehensive test suite for `AliasManager` covering complex real-world scenarios that the
basic tests did not exercise. Tests are organised into 6 new test classes:

#### `AliasManagerSelfJoinTests` (7 tests)
Simulates the Self-Join scenario (e.g. `Employee INNER JOIN Employee`) where
`GetUniqueAliasForType` must allocate a fresh alias without overwriting the type's primary
FROM alias. Verifies the fix for the critical Self-Join bug (scenario C1).
- Two aliases are distinct
- Left alias is the primary FROM alias
- Right alias is registered in `_allAliases`
- `GetAliasForType` still returns the primary alias after Self-Join
- Multiple Self-Joins produce fresh aliases each time
- Primary alias is reusable as table alias
- Right alias does not pollute `_typeToAlias` registry

#### `AliasManagerRepeatedJoinTests` (5 tests)
Simulates the Repeated-Join scenario (e.g. `Order INNER JOIN Customer INNER JOIN Customer`)
where the second join to the same type must allocate a fresh alias via `GetUniqueAliasForType`.
- First call returns the primary alias
- Second call returns a different alias
- Third call returns yet another alias
- Primary alias unchanged after multiple calls
- All aliases registered as Table (not SubQuery)

#### `AliasManagerApplySubQueryTests` (6 tests)
Simulates the CROSS/OUTER APPLY scenario where a sub-query alias must be tracked separately
from the table alias for the same type.
- Sub-query alias is distinct from table alias
- Sub-query alias is recognised as SubQuery by `IsSubqueryAlias`
- `TryGetSubQueryAlias` returns the registered alias
- Sub-query and table aliases for the same type both work
- Multiple sub-queries for the same type: last one wins
- Sub-query alias does not affect table alias lookup

#### `AliasManagerEdgeCaseTests` (20 tests)
Edge cases that exercise boundary conditions:
- Alias generation for exactly 10/11 character table names (truncation boundary)
- Alias generation for single-character table names
- Alias generation for tables with custom schema (only table name is used)
- Alias generation for names with and without brackets
- Sub-query alias truncation boundary (8/9 characters)
- 1000 aliases all unique (stress)
- 1000 sub-query aliases all unique (stress)
- Mixed table names produce unique aliases
- Idempotent `SetTableAlias`/`SetTypeAlias`/`SetSubQueryAlias` for same alias
- Duplicate alias detection across Table/SubQuery categories
- Different schemas treated as different tables
- Different types produce different aliases
- `ClearAliases` resets counters and allows reusing old aliases
- `GetAllTableAliases` returns empty after `ClearAliases`

#### `AliasManagerAliasLifecycleTests` (4 tests)
End-to-end simulations that mirror what `QueryBuilderCore` actually does:
- Full query simulation (FROM + 2 JOINs + 1 APPLY) produces consistent aliases
- Self-Join with APPLY produces consistent aliases
- Repeated JOIN with APPLY produces consistent aliases
- UNION scenario: two independent `AliasManager` instances produce same aliases for same type

#### `AliasManagerComplexScenarioTests` (6 tests)
Complex real-world scenarios:
- Deep nested APPLY chain (3 levels of sub-queries)
- Multiple Self-Joins for different types in same query
- Mixed Table/Type/SubQuery aliases with proper isolation
- Alias collision avoidance: 200 generated aliases all unique
- Table alias switch preserves other tables' aliases
- Switching to an alias used by another table throws

#### `AliasManagerConcurrencyAdvancedTests` (10 stress tests)
High-concurrency stress tests with up to 50 threads × 1000 iterations:
- Concurrent `GetUniqueAliasForType`: 20000 aliases all distinct
- Concurrent `GenerateAlias` + `GenerateSubQueryAlias`: 1000 aliases all distinct
- Concurrent `SetTableAlias`: no lost updates, all tables registered
- Concurrent read during write: no exceptions
- Concurrent `GetAliasForType` first-call-wins: 50 threads, single distinct result
- Concurrent `SetSubQueryAlias` last-write-wins
- Concurrent `ClearAliases` during use: no corruption
- Concurrent mixed operations (6 different methods): stable
- High-volume: 10000 aliases all unique
- Many table names: each gets distinct primary alias

## [4.8.4] - 2026-06-19

### Fixed (Critical thread-safety bugs for production use)

This release hardens the library for high-concurrency scenarios where thousands of users may
share a `DapperService` instance (e.g. ASP.NET singleton) or build queries concurrently from
multiple threads. Several pre-existing race conditions could produce wrong SQL, lost updates,
or `InvalidOperationException` under load.

- **Critical: Removed static expression template cache from `ExpressionParser`.** The cache was
  keyed by a structural hash code that could collide for different expressions. On collision,
  the second expression would receive the first expression's SQL template, producing silently
  wrong SQL with mismatched parameters. The cache also leaked memory unboundedly because
  expression hash codes are not stable across GC. The cache has been removed; each parse now
  builds the SQL directly. This is slightly slower (~5-10% on micro-benchmarks) but is correct
  under all concurrency scenarios.

- **Critical: `AliasManager` now uses a single `_registrationLock` for all alias registration
  operations.** Previously, `SetTableAlias`, `SetTypeAlias`, `SetSubQueryAlias` and the factory
  inside `GetAliasForTable`/`GetAliasForType` performed multi-step read-modify-write sequences
  on `ConcurrentDictionary` without atomicity guarantees. Two concurrent calls could:
  - Both succeed in `_allAliases.TryAdd` with different aliases for the same table, but only
    one would be stored in `_tableToAlias`, leaking the other in `_allAliases` forever.
  - Both observe the same "old alias" in `_tableToAlias` and both try to remove it from
    `_allAliases`, with one succeeding and the other silently no-op'ing.

  All registration operations now hold `_registrationLock` for the entire check-and-add
  sequence, eliminating the TOCTOU race. Read operations (`TryGetTypeAlias`,
  `TryGetSubQueryAlias`, `IsSubqueryAlias`) remain lock-free for read-heavy workloads.

- **Critical: `QueryBuilderCore` state mutations are now protected by a single `_stateLock`.**
  Previously, the builder used `ConcurrentQueue<T>` for filters, joins, etc., but the scalar
  fields (`_distinctClause`, `_topClause`, `_isCountQuery`, `_rowNumberClause`, `_havingClause`,
  `_limit`, `_offset`) were not protected at all. Two concurrent calls to `Distinct()` and
  `Top()` could race on the `ref` parameters of `SetClause`, losing one of the updates. The
  builder now uses a plain `List<T>` plus an explicit `_stateLock` for all state mutations,
  which is both simpler and provably correct.

- **Critical: `QueryBuilderCore.Execute` and `ExecuteAsync` are now serialized by
  `_executeLock`.** Previously, two concurrent `Execute` calls would both invoke
  `BuildQuery()` (which mutates `_orderByQueue` via the synthetic `(SELECT 1)` injection for
  paging) and both would consume the same `_parameterBuilder`, producing duplicate parameter
  names and wrong SQL. Now, `Execute` is serialized so that only one thread can build and run
  a query at a time. The lock is held only for the synchronous portion of `ExecuteAsync`
  (query building and parameter snapshotting); the actual async I/O happens outside the lock.

- **`ParameterBuilder.GetParameters()` now returns a snapshot copy.** Previously it returned
  the live `ConcurrentDictionary`, which could be modified by another thread while Dapper was
  iterating it, causing `InvalidOperationException`. Callers that need the live dictionary for
  internal merge operations can use the new `GetInternalParameters()` method.

- **`ParameterBuilder.MergeParameters` now accepts `IDictionary<string, object>` instead of
  `ConcurrentDictionary<string, object>`.** This decouples the merge logic from the storage
  type and makes it safe to pass snapshots.

- **`QueryBuilderCore.ExecuteAsync` no longer holds `_executeLock` across an `await`.**
  Previously, `async` methods that took a lock would compile to a synchronous method (with a
  CS1998 warning) because C# 5 / net45 does not support `await` inside `lock`. The lock is
  now released before the `await` to properly support true async I/O.

### Added (11 new stress tests, total 202)

- `ThreadSafetyStressTests` — 11 tests with up to 20 threads × 1000 iterations each:
  - `AliasManager_ConcurrentGetAliasForTable_NoLeaks` — 20 threads × 100 iters, verifies
    no alias leaks in `_allAliases`.
  - `AliasManager_ConcurrentGetAliasForDifferentTables_AllUnique` — 10 threads × 100 iters
    across 6 different tables, verifies each table gets exactly one alias.
  - `AliasManager_ConcurrentSetTableAlias_NoCorruption` — 20 threads × 50 iters with
    concurrent `SetTableAlias` for different tables.
  - `QueryBuilder_ConcurrentBuildQuery_ConsistentResults` — 10 threads × 50 iters, verifies
    identical SQL is produced every time.
  - `QueryBuilder_ConcurrentWhereAdd_AllFiltersPresent` — 5 threads × 20 iters × 10
    concurrent `Where` calls per builder.
  - `QueryBuilder_ConcurrentDistinctAndTop_NoCorruption` — 10 threads × 50 iters, verifies
    both `DISTINCT` and `TOP` appear in every generated SQL.
  - `ParameterBuilder_ConcurrentAddAndMerge_NoDuplicates` — 20 threads × 100 iters, verifies
    exactly 2000 distinct parameters are produced.
  - `QueryBuilder_DisposeDuringBuildQuery_DoesNotCorrupt` — 3 threads with 1000 iterations
    each, where one thread disposes mid-stream.
  - `QueryBuilderCache_ConcurrentGetTableName_AllSameInstance` — 20 threads × 100 iters,
    verifies the static cache is thread-safe.
  - `MultipleDapperServices_ConcurrentUse_NoCrossContamination` — 10 threads each with its
    own `DapperService` instance, verifies no cross-contamination.
  - `AliasManager_ConcurrentMixedOperations_Stable` — 4 threads running different operations
    (GetAliasForTable, GetAliasForType, SetTableAlias, IsSubqueryAlias) concurrently.

## [4.8.3] - 2026-06-19

### Fixed

- **Critical: ConnectionManager now checks `_disposed` before every public operation.**
  Previously, calling `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`,
  `GetOpenConnection` or `GetOpenConnectionAsync` on a disposed `ConnectionManager` could
  result in `ObjectDisposedException` from the underlying `IDbConnection`, `NullReferenceException`
  from accessing a disposed transaction, or silently corrupt internal state. All public methods
  now perform an explicit `ThrowIfDisposed()` check at entry, providing a clear
  `ObjectDisposedException(nameof(ConnectionManager))` instead of cryptic downstream errors.

- **Critical: Race condition in `GetOpenConnectionAsync`.** Two concurrent calls could both observe
  `_connection.State != Open`, both set `needOpen = true`, and both invoke `OpenAsync` on the
  same `SqlConnection`. The second call would throw `InvalidOperationException` ("The connection
  is already open"). Now the async path uses a separate `_connectionOpenLock` for the actual
  `Open` call, ensuring that only one thread opens the connection at a time. The state check is
  repeated inside the lock to avoid redundant opens.

- **`EnsureConnectionOpen` now also accepts `ConnectionState.Connecting`.** Previously, if the
  connection was in the middle of connecting (e.g. after a previous async open was in flight),
  `EnsureConnectionOpen` would call `Open()` again, throwing an exception. Now `Connecting`
  is treated as "in progress" and the connection is returned as-is.

- **`GetOpenConnectionAsync` now opens external connections via `DbConnection.OpenAsync`.**
  Previously, only the connection-string-owned path used `OpenAsync`; external connections
  fell back to the synchronous `Open()` call. This blocked the thread pool on slow-opening
  external connections.

- **`DapperService.Dispose` now uses `SafeDispose` for each collaborator.** Previously, if
  `_connectionManager.Dispose()` threw an exception, `_entityTracker.Dispose()` and
  `_queryCache.Dispose()` would never run, leaking tracked entities and cached SQL. Each
  collaborator is now disposed in its own try/catch, ensuring that one failure cannot prevent
  cleanup of the others.

- **`ConnectionManager.Dispose` now disposes the connection even if `Close()` fails.**
  Previously, if `_connection.Close()` threw an exception, `_connection.Dispose()` was skipped
  because both calls were in the same try block. The two calls are now in separate try/catch
  blocks so that a failed close does not prevent disposal.

- **`ConnectionManager.Dispose` now disposes the transaction even if `Rollback()` fails.**
  Same pattern as the connection fix above.

- **`DapperService` now performs `ThrowIfDisposed()` on every public method.** Previously, only
  `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction` checked disposal. All
  other methods (Insert, Update, Delete, GetById, Query, ExecuteStoredProcedure, etc.) would
  pass through to the underlying collaborators, which could fail with cryptic
  `NullReferenceException` if the connection manager had been disposed. All public methods now
  throw `ObjectDisposedException(nameof(DapperService))` upfront.

- **`DapperService.TransactionCount()` returns 0 after disposal** instead of attempting to
  access the disposed connection manager.

### Added

- **34 new unit tests** covering connection and transaction lifecycle, bringing the total to
  191 (up from 157). New test files:
  - `ConnectionLifecycleTests` (29 tests) — disposal semantics, lazy connection opening,
    broken-connection recovery, savepoint rollback, begin/commit/rollback cycles, external
    connection handling
  - `ConnectionConcurrencyTests` (5 tests) — concurrent Begin/Commit, Begin/Rollback, nested
    savepoints, concurrent reads, dispose-during-use with 10 threads × 100 iterations each

## [4.8.2] - 2026-06-19

### Fixed

- **Critical: RollbackTransaction destroyed the entire transaction when rolling back a
  savepoint.** Previously, when a savepoint was active, `RollbackTransaction` issued the
  `ROLLBACK TRANSACTION <savepoint>` command but then unconditionally called
  `CleanupTransaction()` in a `finally` block, which disposed the entire outer transaction and
  cleared the savepoint stack. This meant that nested rollback not only undid the work since
  the savepoint but also rolled back the entire transaction. Now, savepoint rollback returns
  early without disposing the outer transaction (mirroring the behaviour of `CommitTransaction`).

- **ConnectionManager now opens external connections when needed.** Previously, when an
  external `IDbConnection` was supplied (via `DapperServiceFactory.Create(IDbConnection)`), the
  connection was returned as-is even if it was in `Closed` state. This caused `BeginTransaction`
  and CRUD operations to throw `InvalidOperationException` because the connection was not open.
  Now `GetOpenConnection()` and `GetOpenConnectionAsync()` open external connections that are
  not yet open, without taking ownership of their lifetime (external connections are still not
  closed or disposed by the service).

### Added

- **84 new unit tests** covering `IDapperService` and its implementation. Total test count is
  now 157 (up from 73). New test files:
  - `DapperServiceConstructorTests` — constructor validation, disposal semantics
  - `DapperServiceTransactionTests` — TransactionCount, Begin/Commit/Rollback, savepoint
    nesting, error conditions
  - `DapperServiceCrudTests` — Insert/Update/Delete/GetById with SQL assertion via spy
    connection, composite key handling, attach/detach change tracking
  - `DapperSpyCompatibilityTests` — verifies that Dapper works with the spy IDbConnection
  - `QueryCacheTests` — SQL generation for Insert/Update/Delete/GetById, primary key and
    identity property resolution, column name mapping, identifier sanitisation
  - `StoredProcedureExecutorValidationTests` — input validation for stored procedure, scalar
    function and table function name arguments (theory-based with 25 cases)
- **SpyDbConnection** — a test-only `IDbConnection` implementation that records all executed
  commands without contacting a real database. Allows tests to assert on the exact SQL text and
  parameters that Dapper would have sent.
- **ListDataReader<T>** — a test-only `IDataReader` implementation that wraps a list of
  objects, useful for tests that need to simulate result sets returned by the database.

## [4.8.1] - 2026-06-19

### Changed

- **All XML doc comments and inline comments removed from source files.** The library now ships
  with clean source code; documentation lives in the README and CHANGES.md files instead.
  Applies to all `.cs` files in both `EasyDapper` and `EasyDapper.Tests` projects.

### Fixed

- **QueryBuilder now opens the connection lazily.** Previously, constructing a QueryBuilder via
  `DapperService.Query<T>()` immediately opened the underlying SQL connection, even if the caller
  never called `Execute()` or `ExecuteAsync()`. This could cause connection-pool exhaustion in
  scenarios that build many queries but only execute some of them. Now the connection is only
  opened when `Execute`, `ExecuteAsync`, or `BuildQuery`-with-execution is invoked.
- **QueryBuilderCore constructor refactored** into three overloads: the legacy
  `(IDbConnection, ...)` constructor, a new `(ConnectionManager, ...)` constructor that
  participates in the parent service's transaction, and a private canonical constructor that
  both delegate to.
- **AddApply (CROSS APPLY / OUTER APPLY) now constructs the sub-query's QueryBuilder via the
  `ConnectionManager` overload** when one is available, so that the sub-query participates in
  the same transaction as its parent without eagerly opening a connection.

### Added

- New test `Constructor_DirectConnection_DoesNotOpenConnection` verifying that constructing a
  `QueryBuilder<T>` with a direct `IDbConnection` does not open the connection.

## [4.8.0] - 2026-06-19

### Changed

- **Refactor: split monolithic files into per-class files.** The previous `DapperService.cs`
  (1,176 lines, 7 classes) and `QueryBuilder.cs` (1,469 lines, 6 classes) have been split into
  one class per file, organised into the following folders:
  - `Core/` — `DapperService` (facade)
  - `Transactions/` — `ConnectionManager`
  - `Caching/` — `QueryCache`, `SimpleConcurrentCache`
  - `Crud/` — `CrudOperations`
  - `Bulk/` — `BulkOperations`
  - `StoredProcedures/` — `StoredProcedureExecutor`
  - `Tracking/` — `EntityTracker`
  - `Sql/` — `SqlBuilder`
  - `QueryBuilding/` — `QueryBuilder`, `QueryBuilderCore`, `ExpressionParser`, `AliasManager`,
    `ParameterBuilder`, `QueryBuilderCache`, `SqlTemplate`

- **`IQueryBuilder<T>` now extends `IDisposable`.** This allows callers to dispose of query
  builders with a `using` statement. Existing implementations that already implement
  `IDisposable` (the default `QueryBuilder<T>` does) require no changes.

- **`IQueryBuilder<T>` now exposes `Distinct()`** (previously it was only on the concrete
  `QueryBuilder<T>` class, so the documented `dapperService.Query<T>().Distinct()` pattern did
  not compile when coded against the interface).

- **`IQueryBuilder<T>.BuildQuery()`** is now formally declared on the interface (it was
  previously declared as a separate `string BuildQuery()` method at the end of the interface
  in earlier versions, with a comment about being public for outer-apply scenarios).

### Fixed

- **Critical: Self-Join produced invalid SQL.** When a query self-joined an entity
  (`InnerJoin<Employee, Employee>`), the SELECT clause referenced the JOIN-side alias instead
  of the FROM-side alias, causing SQL Server error 4104 ("multi-part identifier could not be
  bound"). Root cause: `AliasManager.GetUniqueAliasForType` was overwriting the type's primary
  alias in `_typeToAlias`. Fix: `GetUniqueAliasForType` no longer updates `_typeToAlias`.

- **High: `TransactionCount()` returned only 0 or 1** even when nested savepoints were active.
  Now returns `1 + savepointCount` to reflect the actual nesting depth (matching the behaviour
  of the legacy `DapperServiceOld`).

- **High: `GetById<T>(object id)` for composite keys** previously sent `new { Id = id }` as
  parameters, which did not match the generated `WHERE [Key1] = @Key1 AND [Key2] = @Key2`
  clause. Now accepts either an entity instance with populated PK properties or an anonymous
  object whose property names match the PK property names.

- **High: `UpdateSingleWithCompositeKeysAsync` called the synchronous `GetOpenConnection()`**
  inside an async method, which can starve the thread pool under load. Now uses
  `GetOpenConnectionAsync()`.

- **High: `ConnectionManager.GetConnectionTimeout` opened a real SQL connection** in the
  constructor just to read the timeout. Now reads `Connect Timeout` from the connection string
  via `SqlConnectionStringBuilder` without opening a connection.

- **High: `WithTableAlias` leaked the previous alias** in `_allAliases` forever. Now releases
  the old alias from the registry when a new alias is registered for the same table.

- **High: `GetTableAliasForMember` used `member.DeclaringType`** which broke for entities that
  inherit from a generic base class (the base class was used to look up the alias instead of
  the actual parameter type). Now uses `paramExpr.Type`.

- **Medium: `AliasManager` registered `GetUniqueAliasForType` results as `AliasType.Type`**
  which caused `IsSubqueryAlias` to return incorrect results. Now registers as
  `AliasType.Table`.

- **Medium: `InvalidAliasChars` for custom aliases did not include space, parentheses, comma,
  or other characters that produce invalid SQL.** Aligned with `QueryBuilderCache.InvalidIdentifierChars`.

- **Medium: `QueryBuilder.Execute()` did not pass the active transaction** to Dapper, so
  queries silently ran outside the parent `DapperService` transaction. Now `QueryBuilder`
  accepts a `ConnectionManager` and uses its `CurrentTransaction` when executing.

- **Medium: `StoredProcedureExecutor` re-compiled the validation regex** on every call. Now
  uses a static compiled `Regex`.

- **Medium: `EntityTracker.CreateCompositeKey` for multi-column keys** used naive
  `string.Join("|", ...)` which produced collisions (e.g. `"A|B","C"` collided with
  `"A","B|C"`). Now includes property names and escapes pipe characters inside values.

- **Medium: `EntityTracker.GetChangedProperties` used reference equality for `byte[]`**,
  reporting unchanged arrays as changed. Now uses structural equality for arrays.

- **Medium: `EntityTracker.CloneEntity` did not clone arrays**, leaving the snapshot and the
  live entity sharing the same `byte[]` reference. Now clones arrays element-by-element.

- **Low: `AliasManager` counter started at 1, producing aliases like `Foo_A2`** for the first
  reference. Now starts at 0 so the first alias is `Foo_A1`.

- **Low: `ParameterBuilder` accessed the `_orderOfCreation` list without a lock** in
  `AddParameter`. Now uses the same lock as `GetUniqueParameterName`.

- **Low: `QueryBuilderCore` had a finalizer** (`~QueryBuilderCore`) even though it only holds
  managed resources. Removed.

### Removed

- **`DapperServiceOld.cs`** — 1,088 lines of commented-out legacy code. The file has been
  deleted; the Git history retains it for anyone who needs to consult the old implementation.

### Added

- **Test project `EasyDapper.Tests`** with 72 unit tests covering:
  - AliasManager (24 tests, including regression tests for the Self-Join and WithTableAlias
    leak bugs)
  - QueryBuilder SQL generation (40+ tests covering JOIN, APPLY, UNION, paging, aggregates,
    nullable value access, boolean members, LIKE translation, etc.)
  - EntityTracker composite-key builder (4 tests including the byte[] equality regression)
  - ConnectionManager lifecycle (2 tests)

  Tests target `net462` and `net8.0` so they exercise both the `net45` and `netstandard2.0`
  builds of the library.

- **`InternalsVisibleTo("EasyDapper.Tests")`** so tests can access internal types directly
  without reflection.

### Migration notes

This is a backwards-compatible release for the vast majority of users:

- All public method signatures on `IDapperService`, `IQueryBuilder<T>`, `DapperServiceFactory`
  and the attribute classes are unchanged.
- The only addition to `IQueryBuilder<T>` is the `Distinct()` method (which was already
  documented in the README but missing from the interface) and the `IDisposable` interface
  inheritance.
- If you have written your own implementation of `IQueryBuilder<T>`, you will need to add a
  `Distinct()` method and a `Dispose()` method. The default `QueryBuilder<T>` implementation
  provided by `DapperService.Query<T>()` already has both.

If you encounter any regressions, please open an issue on GitHub with the SQL that was
produced before and after the upgrade.
