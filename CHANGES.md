# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
