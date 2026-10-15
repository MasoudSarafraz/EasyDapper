using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public sealed class SqlServerFactAttribute : FactAttribute
    {
        public SqlServerFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
                Skip = "Set EASYDAPPER_SQL_TEST_CONNECTION to run isolated SQL Server temporary-table tests.";
        }
    }

    public class SqlServerRegressionTests
    {
        private static SqlConnection OpenConnection()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            return connection;
        }

        private static void CreateRows(SqlConnection connection)
        {
            connection.Execute(@"CREATE TABLE #EDRegressionRows
                (Id INT PRIMARY KEY, StoredName NVARCHAR(100) NULL, Age INT NOT NULL, Flag BIT NULL);
                INSERT #EDRegressionRows VALUES (1,NULL,10,NULL),(2,N'ordinary',25,0),(3,N'literal%sign',40,1);");
        }

        [SqlServerFact]
        public void BulkIdentities_CorrespondToTheirSourceRows_AcrossLargeBatches()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionBulk (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100) NOT NULL);");
                for (int round = 0; round < 3; round++)
                {
                    var entities = Enumerable.Range(0, 20000)
                        .Select(i => new BulkRow { Name = round + ":" + i }).ToList();
                    Assert.Equal(entities.Count, service.InsertList(entities, generateIdentities: true));
                    var actual = connection.Query<BulkRow>("SELECT Id, Name FROM #EDRegressionBulk")
                        .ToDictionary(x => x.Id, x => x.Name);
                    Assert.All(entities, entity => Assert.Equal(entity.Name, actual[entity.Id]));
                    Assert.Equal(entities.Count, entities.Select(x => x.Id).Distinct().Count());
                }
            }
        }

        [SqlServerFact]
        public async Task BulkIdentitiesAsync_CorrespondToTheirSourceRows()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionBulk (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100) NOT NULL);");
                var entities = Enumerable.Range(0, 20000).Select(i => new BulkRow { Name = i.ToString() }).ToList();
                Assert.Equal(entities.Count, await service.InsertListAsync(entities, generateIdentities: true));
                var actual = connection.Query<BulkRow>("SELECT Id, Name FROM #EDRegressionBulk")
                    .ToDictionary(x => x.Id, x => x.Name);
                Assert.All(entities, entity => Assert.Equal(entity.Name, actual[entity.Id]));
            }
        }

        [SqlServerFact]
        public async Task NullableMappedIdentities_WorkForSingleAndBulkInsert()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionNullable ([Audit Id] INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));");
                var first = new NullableRow { Name = "single" };
                var second = new NullableRow { Name = "single async" };
                Assert.Equal(1, service.Insert(first));
                Assert.Equal(1, await service.InsertAsync(second));
                var sync = new[] { new NullableRow { Name = "bulk 1" }, new NullableRow { Name = "bulk 2" } };
                var asyncRows = new[] { new NullableRow { Name = "async 1" }, new NullableRow { Name = "async 2" } };
                Assert.Equal(2, service.InsertList(sync, true));
                Assert.Equal(2, await service.InsertListAsync(asyncRows, true));
                foreach (var entity in new[] { first, second }.Concat(sync).Concat(asyncRows))
                {
                    Assert.True(entity.Id.HasValue);
                    Assert.Equal(entity.Name, service.GetById(entity).Name);
                }
                Assert.Equal(6, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM #EDRegressionNullable"));
            }
        }

        [SqlServerFact]
        public async Task BigintIdentity_DoesNotOverflowInt32()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionLong (Id BIGINT IDENTITY(2147483648,1) PRIMARY KEY, Name NVARCHAR(100));");
                var first = new LongRow { Name = "sync" };
                var second = new LongRow { Name = "async" };
                Assert.Equal(1, service.Insert(first));
                Assert.Equal(1, await service.InsertAsync(second));
                Assert.Equal(2147483648L, first.Id);
                Assert.Equal(2147483649L, second.Id);
                Assert.Equal(first.Name, service.GetById<LongRow>(first.Id).Name);
                Assert.Equal(second.Name, (await service.GetByIdAsync(second)).Name);
            }
        }

        [SqlServerFact]
        public async Task IdentityOnlyEntity_UsesDefaultValues()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionIdentityOnly (Id INT IDENTITY PRIMARY KEY);");
                var first = new IdentityOnlyRow();
                var second = new IdentityOnlyRow();
                Assert.Equal(1, service.Insert(first));
                Assert.Equal(1, await service.InsertAsync(second));
                Assert.Equal(1, first.Id);
                Assert.Equal(2, second.Id);
            }
        }

        [SqlServerFact]
        public async Task GetById_EntityAndScalarOverloadsSupportRenamedKeyAndReservedAliases()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionKey (PersonId INT PRIMARY KEY, [Order] INT); INSERT #EDRegressionKey VALUES (3,42);");
                var entity = new RenamedKeyRow { PersonId = 3 };
                Assert.Equal(42, service.GetById(entity).Order);
                Assert.Equal(42, service.GetById<RenamedKeyRow>(3).Order);
                Assert.Equal(42, (await service.GetByIdAsync(entity)).Order);
                Assert.Equal(42, (await service.GetByIdAsync<RenamedKeyRow>(3)).Order);
            }
        }

        [SqlServerFact]
        public async Task TrackedUpdates_CanRevertToTheirOriginalValue()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var entity = service.GetById<Row>(2);
                service.Attach(entity);
                entity.Name = "changed";
                Assert.Equal(1, service.Update(entity));
                entity.Name = "ordinary";
                Assert.Equal(1, await service.UpdateAsync(entity));
                Assert.Equal("ordinary", service.GetById<Row>(2).Name);
                Assert.Equal(0, service.Update(entity));
            }
        }

        [SqlServerFact]
        public void NestedTransactions_RollbackOnlyTheirOwnChanges()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var entity = service.GetById<Row>(2);
                service.BeginTransaction();
                entity.Name = "outer";
                service.Update(entity);
                service.BeginTransaction();
                Assert.Equal(2, service.TransactionCount());
                entity.Name = "inner";
                service.Update(entity);
                service.RollbackTransaction();
                Assert.Equal(1, service.TransactionCount());
                Assert.Equal("outer", service.GetById<Row>(2).Name);
                service.CommitTransaction();
                Assert.Equal(0, service.TransactionCount());
                Assert.Equal("outer", service.GetById<Row>(2).Name);
                service.BeginTransaction();
                service.BeginTransaction();
                entity.Name = "rolled back";
                service.Update(entity);
                service.CommitTransaction();
                service.RollbackTransaction();
                Assert.Equal("outer", service.GetById<Row>(2).Name);
            }
        }

        [SqlServerFact]
        public void Rollback_DoesNotLeaveAStaleTrackingSnapshot()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var entity = service.GetById<Row>(2);
                service.Attach(entity);
                service.BeginTransaction();
                entity.Name = "changed";
                Assert.Equal(1, service.Update(entity));
                service.RollbackTransaction();
                Assert.Equal("ordinary", service.GetById<Row>(2).Name);
                Assert.Equal(1, service.Update(entity));
                Assert.Equal("changed", service.GetById<Row>(2).Name);
            }
        }

        [SqlServerFact]
        public void NestedRollback_DoesNotLeaveAStaleTrackingSnapshot()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var entity = service.GetById<Row>(2);
                service.Attach(entity);
                service.BeginTransaction();
                entity.Name = "outer";
                service.Update(entity);
                service.BeginTransaction();
                entity.Name = "inner";
                service.Update(entity);
                service.RollbackTransaction();
                Assert.Equal("outer", service.GetById<Row>(2).Name);
                Assert.Equal(1, service.Update(entity));
                service.CommitTransaction();
                Assert.Equal("inner", service.GetById<Row>(2).Name);
            }
        }

        [SqlServerFact]
        public void Union_UsesEachBranchesOwnParameterValue()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var query = service.Query<Row>().Where(x => x.Age > 30)
                    .Union(service.Query<Row>().Where(x => x.Age < 20));
                Assert.Equal(new[] { 1, 3 }, query.Execute().Select(x => x.Id).OrderBy(x => x).ToArray());
            }
        }

        [SqlServerFact]
        public void Apply_ParametersRemainIndependentInBothFluentOrders()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                connection.Execute("CREATE TABLE #EDRegressionChild (Id INT PRIMARY KEY, ParentId INT, Amount INT); INSERT #EDRegressionChild VALUES (1,3,10);");
                var first = service.Query<Row>().Where(x => x.Age > 30)
                    .OuterApply<Child>((x, c) => x.Id == c.ParentId, sub => sub.Where(c => c.Amount > 5))
                    .Select(x => x.Id).Select<Child>(x => x.Amount);
                var second = service.Query<Row>()
                    .OuterApply<Child>((x, c) => x.Id == c.ParentId, sub => sub.Where(c => c.Amount > 5))
                    .Where(x => x.Age > 30).Select(x => x.Id).Select<Child>(x => x.Amount);
                Assert.Equal(10, first.Execute<AppliedRow>().Single().Amount);
                Assert.Equal(10, second.Execute<AppliedRow>().Single().Amount);
            }
        }

        [SqlServerFact]
        public void SelfApply_SubsequentPredicatesAndProjectionsStillReferenceTheRoot()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var query = service.Query<Row>()
                    .OuterApply<Row>((root, inner) => root.Id == inner.Id, sub => sub.Where(x => x.Age > 30))
                    .Where(x => x.Age > 20).Select(x => x.Id, x => x.Name).OrderByAscending(x => x.Id);
                var actual = query.Execute().ToArray();
                Assert.Equal(new[] { 2, 3 }, actual.Select(x => x.Id).ToArray());
                Assert.Equal("ordinary", actual[0].Name);
            }
        }

        [SqlServerFact]
        public void NestedApply_CorrelationsDoNotShadowAliasesInOuterScopes()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                connection.Execute("CREATE TABLE #EDRegressionChild (Id INT PRIMARY KEY, ParentId INT, Amount INT); INSERT #EDRegressionChild VALUES (1,3,10);");
                var query = service.Query<Row>()
                    .OuterApply<Child>((root, child) => root.Id == child.ParentId, sub => sub
                        .Where(x => x.Amount > 5)
                        .OuterApply<Row>((child, row) => child.ParentId == row.Id, rows => rows.Where(x => x.Age > 30)))
                    .Where(x => x.Id == 3).Select(x => x.Id).Select<Child>(x => x.Amount);
                Assert.Equal(10, query.Execute<AppliedRow>().Single().Amount);
            }
        }

        [SqlServerFact]
        public void SetOperations_ApplySubsequentFiltersOrderTopAndPaging()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                Assert.Empty(service.Query<Row>().Union(service.Query<Row>()).Where(x => x.Id == 12345).Execute());
                var top = service.Query<Row>().Union(service.Query<Row>()).Where(x => x.Id >= 2)
                    .OrderByDescending(x => x.Id).Top(1).Execute().Single();
                Assert.Equal(3, top.Id);
                var page = service.Query<Row>().Union(service.Query<Row>()).OrderByAscending(x => x.Id).Paging(1, 2)
                    .Execute().Single();
                Assert.Equal(2, page.Id);
            }
        }

        [SqlServerFact]
        public void SetOperations_SupportMappedColumnFiltersAfterSubsetProjection()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var query = service.Query<Row>().Select(x => x.Name)
                    .Union(service.Query<Row>().Select(x => x.Name))
                    .Where(x => x.Name == "ordinary");
                Assert.Equal(new[] { "ordinary" }, query.Execute<string>().ToArray());
            }
        }

        [SqlServerFact]
        public void ExplicitAndSetProjections_QuoteReservedPropertyNames()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Execute("CREATE TABLE #EDRegressionKey (PersonId INT PRIMARY KEY, [Order] INT); INSERT #EDRegressionKey VALUES (3,42);");
                Assert.Equal(42, service.Query<RenamedKeyRow>().Select(x => x.Order).Execute<int>().Single());
                var query = service.Query<RenamedKeyRow>().Select(x => x.Order)
                    .Union(service.Query<RenamedKeyRow>().Select(x => x.Order))
                    .Where(x => x.Order == 42).OrderByAscending(x => x.Order);
                Assert.Equal(42, query.Execute<int>().Single());
            }
        }

        [SqlServerFact]
        public void SetOperations_KeepScalarAggregateQueriesValid()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                Assert.Equal(75, service.Query<Row>().Sum(x => x.Age)
                    .Union(service.Query<Row>().Sum(x => x.Age)).Execute<int>().Single());
                Assert.Equal(3, service.Query<Row>().Count()
                    .Union(service.Query<Row>().Count()).Execute<int>().Single());
            }
        }

        [SqlServerFact]
        public void SetOperations_PreserveExplicitlyRenamedProjectionBindings()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var query = service.Query<Row>().Select(x => new { Renamed = x.Name })
                    .Union(service.Query<Row>().Select(x => new { Renamed = x.Name }))
                    .Where(x => x.Name == "ordinary").OrderByAscending(x => x.Name);
                Assert.Equal("ordinary", query.Execute<RenamedProjection>().Single().Renamed);
            }
        }

        [SqlServerFact]
        public void MixedSetOperations_PreserveTheirFluentOrder()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                var query = service.Query<Row>().Where(x => x.Id == 1)
                    .Union(service.Query<Row>().Where(x => x.Id == 2))
                    .Intersect(service.Query<Row>().Where(x => x.Id == 2))
                    .Union(service.Query<Row>().Where(x => x.Id == 3));
                Assert.Equal(new[] { 2, 3 }, query.Execute().Select(x => x.Id).OrderBy(x => x).ToArray());
            }
        }

        [SqlServerFact]
        public void NullAndNullablePredicates_UseSqlNullSemantics()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                string name = null;
                Assert.Equal(1, service.Query<Row>().Where(x => x.Name == name).Execute().Single().Id);
                Assert.Equal(new[] { 2, 3 }, service.Query<Row>().Where(x => x.Name != name)
                    .OrderByAscending(x => x.Id).Execute().Select(x => x.Id).ToArray());
                Assert.Equal(1, service.Query<Row>().Where(x => !x.Flag.HasValue).Execute().Single().Id);
                var ids = new List<int>();
                Assert.Empty(service.Query<Row>().Where(x => ids.Contains(x.Id)).Execute());
                Assert.Equal(3, service.Query<Row>().Where(x => !ids.Contains(x.Id)).Execute().Count());
            }
        }

        [SqlServerFact]
        public void StringSearch_TreatsLikeWildcardsAsLiteralText()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                connection.Execute("INSERT #EDRegressionRows VALUES (4,N'literal_under',50,NULL),(5,N'[bracket]',60,NULL),(6,N'back\\slash',70,NULL);");
                Assert.Equal(3, service.Query<Row>().Where(x => x.Name.Contains("%")).Execute().Single().Id);
                Assert.Equal(4, service.Query<Row>().Where(x => x.Name.Contains("_")).Execute().Single().Id);
                Assert.Equal(5, service.Query<Row>().Where(x => x.Name.StartsWith("[")).Execute().Single().Id);
                Assert.Equal(5, service.Query<Row>().Where(x => x.Name.EndsWith("]")).Execute().Single().Id);
                Assert.Equal(6, service.Query<Row>().Where(x => x.Name.Contains("\\")).Execute().Single().Id);
            }
        }

        [SqlServerFact]
        public async Task Functions_SupportDynamicParametersAndDictionaries()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parameters = new DynamicParameters();
                parameters.Add("Base", 2);
                parameters.Add("Exponent", 3);
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", parameters));
                Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", parameters));
                var templateParameters = new DynamicParameters(new { Base = 2, Exponent = 3 });
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", templateParameters));
                Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", templateParameters));
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new Dictionary<string, object>
                {
                    { "Base", 2 }, { "Exponent", 3 }
                }));
            }
        }

        [SqlServerFact]
        public async Task TableFunctions_SupportExplicitDynamicParameters()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parameters = new DynamicParameters();
                parameters.Add("Text", "a,b");
                parameters.Add("Separator", ",", DbType.StringFixedLength, size: 1);
                Assert.Equal(new[] { "a", "b" }, service.ExecuteTableFunction<SplitRow>("STRING_SPLIT", parameters)
                    .Select(x => x.Value).OrderBy(x => x).ToArray());
                Assert.Equal(new[] { "a", "b" }, (await service.ExecuteTableFunctionAsync<SplitRow>("STRING_SPLIT", parameters))
                    .Select(x => x.Value).OrderBy(x => x).ToArray());
            }
        }

        [SqlServerFact]
        public void RawOrderAndJoinFragments_AreRejectedBeforeExecution()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateRows(connection);
                Assert.Throws<ArgumentException>(() => service.Query<Row>().OrderByColumns("Id; SELECT 9001;--"));
                Assert.Equal(3, service.Query<Row>().OrderBy("Id DESC").Execute().First().Id);
                Assert.Equal(3, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM #EDRegressionRows"));
            }
        }

        [SqlServerFact]
        public async Task ConcurrentOperations_WorkOnClosedAndOpenConnectionsWithoutMars()
        {
            var builder = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"))
            { MultipleActiveResultSets = false, Pooling = false };
            using (var connection = new SqlConnection(builder.ConnectionString))
            using (var service = DapperServiceFactory.Create(connection))
            {
                for (int round = 0; round < 2; round++)
                {
                    var tasks = Enumerable.Range(0, 50).Select(_ => service.ExecuteScalarFunctionAsync<int>(
                        "POWER", new { Base = 2, Exponent = 3 })).ToArray();
                    Assert.All(await Task.WhenAll(tasks), value => Assert.Equal(8, value));
                }
                CreateRows(connection);
                var queries = Enumerable.Range(0, 20).Select(_ => service.Query<Row>().ExecuteAsync()).ToArray();
                Assert.All(await Task.WhenAll(queries), rows => Assert.Equal(3, rows.Count()));
            }
        }

        [SqlServerFact]
        public async Task FailedAndCancelledOperations_ReleaseTheServiceGate()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                await Assert.ThrowsAsync<SqlException>(() => service.ExecuteScalarFunctionAsync<int>("NonexistentAuditFunction"));
                var active = service.ExecuteStoredProcedureAsync<Row>("sys.sp_executesql", new
                {
                    stmt = "WAITFOR DELAY '00:00:00.2'; SELECT 1 AS Id, N'waiting' AS Name, 1 AS Age, CAST(NULL AS BIT) AS Flag;"
                });
                using (var cancelled = new CancellationTokenSource())
                {
                    cancelled.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.ExecuteScalarFunctionAsync<int>(
                        "POWER", new { Base = 2, Exponent = 3 }, cancelled.Token));
                }
                Assert.Single(await active);
                Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 }));
            }
        }

        [SqlServerFact]
        public async Task MultiResultCallbacks_CanTrackAndRunCommandsAfterReadingResults()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = await service.ExecuteMultiResultStoredProcedureAsync("sys.sp_executesql", async reader =>
                {
                    var values = (await reader.ReadAsync<int>()).ToArray();
                    var attached = new Row { Id = 1, Name = "callback tracking" };
                    service.Attach(attached);
                    service.Detach(attached);
                    Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>(
                        "POWER", new { Base = 2, Exponent = 3 }));
                    return values;
                }, new { stmt = "SELECT 42;" });
                Assert.Equal(new[] { 42 }, result);
                Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 }));
            }
        }

        [SqlServerFact]
        public async Task QueryAsync_UsesAsynchronousConnectionOpening()
        {
            using (var connection = new AsyncOnlyConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Assert.Equal(3, (await service.Query<Row>().ExecuteAsync()).Count());
                Assert.Equal(1, connection.AsyncOpenCalls);
            }
        }

        [Table("#EDRegressionRows")]
        public class Row
        {
            [PrimaryKey] public int Id { get; set; }
            [Column("StoredName")] public string Name { get; set; }
            public int Age { get; set; }
            public bool? Flag { get; set; }
        }

        [Table("#EDRegressionBulk")]
        public class BulkRow
        {
            [PrimaryKey, Identity] public int Id { get; set; }
            public string Name { get; set; }
        }

        [Table("#EDRegressionNullable")]
        public class NullableRow
        {
            [PrimaryKey, Identity, Column("Audit Id")] public int? Id { get; set; }
            public string Name { get; set; }
        }

        [Table("#EDRegressionLong")]
        public class LongRow
        {
            [PrimaryKey, Identity] public long Id { get; set; }
            public string Name { get; set; }
        }

        [Table("#EDRegressionIdentityOnly")]
        public class IdentityOnlyRow { [PrimaryKey, Identity] public int Id { get; set; } }

        [Table("#EDRegressionKey")]
        public class RenamedKeyRow
        {
            [PrimaryKey] public int PersonId { get; set; }
            public int Order { get; set; }
        }

        [Table("#EDRegressionChild")]
        public class Child
        {
            [PrimaryKey] public int Id { get; set; }
            public int ParentId { get; set; }
            public int Amount { get; set; }
        }

        public class AppliedRow { public int Id { get; set; } public int? Amount { get; set; } }
        public class RenamedProjection { public string Renamed { get; set; } }
        public class SplitRow { public string Value { get; set; } }

        private sealed class AsyncOnlyConnection : DbConnection
        {
            private readonly SqlConnection _inner = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            public int AsyncOpenCalls { get; private set; }
            public override string ConnectionString { get => _inner.ConnectionString; set => _inner.ConnectionString = value; }
            public override string Database => _inner.Database;
            public override string DataSource => _inner.DataSource;
            public override string ServerVersion => _inner.ServerVersion;
            public override ConnectionState State => _inner.State;
            public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);
            public override void Close() => _inner.Close();
            public override void Open() => throw new InvalidOperationException("This connection requires asynchronous opening.");
            public override async Task OpenAsync(CancellationToken cancellationToken)
            {
                AsyncOpenCalls++;
                await _inner.OpenAsync(cancellationToken).ConfigureAwait(false);
                CreateRows(_inner);
            }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => _inner.BeginTransaction(isolationLevel);
            protected override DbCommand CreateDbCommand() => _inner.CreateCommand();
            protected override void Dispose(bool disposing)
            {
                if (disposing) _inner.Dispose();
                base.Dispose(disposing);
            }
        }
    }
}
