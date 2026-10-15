using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class BulkExecutionSqlTests
    {
#if NET8_0
        private const string AtomicTable = "__EDAtomicN8_064764e48e264403a0e5a06d1a529121";
#else
        private const string AtomicTable = "__EDAtomicN462_064764e48e264403a0e5a06d1a529121";
#endif
        private const string AtomicRule = AtomicTable + "_Rule";

        [SqlServerFact]
        public void OrderedIdentityFallback_IsAtomicAndPreservesExistingOuterChanges()
        {
            WithRuleTable((connection, manager, bulk) =>
            {
                foreach (var asynchronous in new[] { false, true })
                {
                    var rows = new[] { new AtomicRow { Name = "valid" }, new AtomicRow { Name = null } };
                    var error = Assert.Throws<SqlException>(() =>
                    {
                        if (asynchronous) bulk.InsertListAsync(rows, true).GetAwaiter().GetResult();
                        else bulk.InsertList(rows, true);
                    });
                    Assert.Equal(513, error.Number);
                    Assert.Equal(0, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dbo.[" + AtomicTable + "]"));
                }

                manager.BeginTransaction();
                try
                {
                    connection.Execute("INSERT dbo.[" + AtomicTable + "] (Name) VALUES(N'outer');", transaction: manager.CurrentTransaction);
                    foreach (var asynchronous in new[] { false, true })
                    {
                        var rows = new[] { new AtomicRow { Name = "valid" }, new AtomicRow { Name = null } };
                        Assert.Throws<SqlException>(() =>
                        {
                            if (asynchronous) bulk.InsertListAsync(rows, true).GetAwaiter().GetResult();
                            else bulk.InsertList(rows, true);
                        });
                        Assert.Equal(1, manager.TransactionCount);
                        Assert.Equal(1, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dbo.[" + AtomicTable + "]", transaction: manager.CurrentTransaction));
                        Assert.Equal("outer", connection.QueryFirst<string>("SELECT Name FROM dbo.[" + AtomicTable + "]", transaction: manager.CurrentTransaction));
                    }
                }
                finally { manager.RollbackTransaction(); }
            });
        }

        [SqlServerFact]
        public void OrderedIdentityFallback_CancellationDoesNotPersistPartialRows()
        {
            WithRuleTable((connection, manager, bulk) =>
            {
                connection.Execute("CREATE TRIGGER dbo.[" + AtomicTable + "_Wait] ON dbo.[" + AtomicTable + "] AFTER INSERT AS BEGIN SET NOCOUNT ON; WAITFOR DELAY '00:00:02'; END;");
                using (var cancellation = new CancellationTokenSource(100))
                {
                    var failure = Assert.ThrowsAny<Exception>(() => bulk.InsertListAsync(
                        new[] { new AtomicRow { Name = "valid" } }, true, cancellation.Token).GetAwaiter().GetResult());
                    Assert.True(failure is OperationCanceledException || failure is SqlException);
                }
                Assert.Equal(0, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dbo.[" + AtomicTable + "]"));
                Assert.Equal(0, manager.TransactionCount);
            });
        }

        [SqlServerFact]
        public async Task BulkInputCallbacks_RunOutsideTheConnectionOperationLease()
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Open();
                connection.Execute("CREATE TABLE #EDBulkCallbacks (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));");
                foreach (var asynchronous in new[] { false, true })
                {
                    foreach (var generateIdentities in new[] { false, true })
                    {
                        var row = new CallbackRow { Name = "captured" };
                        row.ReadHook = () => RunSuppressedQuery(service);
                        row.IdentityHook = () => RunSuppressedQuery(service);
                        var entities = EnumerateWithCallback(row, () => RunSuppressedQuery(service));
                        var affected = asynchronous
                            ? await service.InsertListAsync(entities, generateIdentities)
                            : service.InsertList(entities, generateIdentities);
                        Assert.Equal(1, affected);
                        Assert.Equal(1, row.GetterCalls);
                        Assert.Equal(generateIdentities ? 1 : 0, row.IdentitySetterCalls);
                        if (generateIdentities) Assert.True(row.Id > 0);
                    }
                }
                Assert.Equal(4, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM #EDBulkCallbacks"));
            }
        }

        private static IEnumerable<CallbackRow> EnumerateWithCallback(CallbackRow row, Action callback)
        {
            callback();
            yield return row;
        }

        private static void RunSuppressedQuery(IDapperService service)
        {
            Task<int> query;
            using (ExecutionContext.SuppressFlow())
                query = Task.Run(() => service.ExecuteScalarFunctionAsync<int>("POWER", new { A = 2, B = 3 }));
            Assert.True(query.Wait(500), "A bulk input callback must not wait for a lease held by its own caller.");
            Assert.Equal(8, query.GetAwaiter().GetResult());
        }

        private static void WithRuleTable(Action<SqlConnection, global::EasyDapper.ConnectionManager, BulkOperations> action)
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            {
                connection.Open();
                connection.ChangeDatabase("tempdb");
                var tableCreated = false;
                var ruleCreated = false;
                try
                {
                    connection.Execute("CREATE TABLE dbo.[" + AtomicTable + "] (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));");
                    tableCreated = true;
                    connection.Execute("CREATE RULE dbo.[" + AtomicRule + "] AS @value IS NOT NULL;");
                    ruleCreated = true;
                    connection.Execute("EXEC sys.sp_bindrule @rulename=@RuleName, @objname=@ColumnName", new { RuleName = "dbo." + AtomicRule, ColumnName = "dbo." + AtomicTable + ".Name" });
                    using (var manager = new global::EasyDapper.ConnectionManager(connection))
                    {
                        var cache = new global::EasyDapper.QueryCache();
                        action(connection, manager, new BulkOperations(manager, cache, new global::EasyDapper.SqlBuilder(cache)));
                    }
                }
                finally
                {
                    if (tableCreated) connection.Execute("DROP TABLE dbo.[" + AtomicTable + "];");
                    if (ruleCreated) connection.Execute("DROP RULE dbo.[" + AtomicRule + "];");
                }
            }
        }

        [Table(AtomicTable)]
        public class AtomicRow
        {
            [PrimaryKey, Identity] public int Id { get; set; }
            public string Name { get; set; }
        }

        [Table("#EDBulkCallbacks")]
        public class CallbackRow
        {
            private int _id;
            private string _name;
            public Action ReadHook;
            public Action IdentityHook;
            public int GetterCalls;
            public int IdentitySetterCalls;

            [PrimaryKey, Identity]
            public int Id
            {
                get => _id;
                set { IdentitySetterCalls++; IdentityHook?.Invoke(); _id = value; }
            }

            public string Name
            {
                get { GetterCalls++; ReadHook?.Invoke(); return _name; }
                set { _name = value; }
            }
        }
    }
}
