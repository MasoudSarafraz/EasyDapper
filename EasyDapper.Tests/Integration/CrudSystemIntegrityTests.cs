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
    public class CrudSystemIntegrityTests
    {
#if NET8_0
        private const string TriggerTable = "__EDZeroSystemN8_064764e48e264403a0e5a06d1a529121";
#else
        private const string TriggerTable = "__EDZeroSystemN462_064764e48e264403a0e5a06d1a529121";
#endif
        private const string TriggerLog = TriggerTable + "_Log";

        [SqlServerFact]
        public async Task Insert_ValueGettersAndGeneratedIdentitySetters_CanQueryWithoutExecutionContextFlow()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    var row = new CallbackRow { Name = "inserted", Age = 10 };
                    row.NameRead = () => RunWithoutFlow(service);
                    row.IdWrite = () => RunWithoutFlow(service);
                    Assert.Equal(1, asynchronous ? await service.InsertAsync(row) : service.Insert(row));
                    Assert.True(row.Id > 0);
                    Assert.Equal("inserted", service.GetById<CallbackRow>(row.Id).Name);
                    Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 }));
                }
            }
        }

        [SqlServerFact]
        public async Task Update_ValueGettersOfAttachedAndUnattachedEntities_CanQueryWithoutExecutionContextFlow()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                foreach (var tracked in new[] { false, true })
                {
                    using (var connection = OpenConnection())
                    using (var service = DapperServiceFactory.Create(connection))
                    {
                        CreateTable(connection);
                        connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                        var row = new CallbackRow { Id = 1, Name = "A", Age = 10 };
                        if (tracked) service.Attach(row);
                        row.Name = "B";
                        row.NameRead = () => RunWithoutFlow(service);
                        Assert.Equal(1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                        Assert.Equal("B", service.GetById<CallbackRow>(1).Name);
                        if (tracked)
                            Assert.Equal(0, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task GetByIdAndDelete_EntityKeyGetters_CanQueryWithoutExecutionContextFlow()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                    var row = new CallbackRow { Id = 1 };
                    row.IdRead = () => RunWithoutFlow(service);
                    var found = asynchronous ? await service.GetByIdAsync(row) : service.GetById(row);
                    Assert.Equal("A", found.Name);
                    Assert.Equal(1, asynchronous ? await service.DeleteAsync(row) : service.Delete(row));
                    Assert.Null(service.GetById<CallbackRow>(1));
                }
            }
        }

        [SqlServerFact]
        public async Task GetById_ResultConstructorsAndSetters_PreserveNestedQueriesWithMars()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                foreach (var constructor in new[] { false, true })
                {
                    var connectionString = new SqlConnectionStringBuilder(
                        Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"))
                    {
                        MultipleActiveResultSets = true
                    };
                    using (var connection = new SqlConnection(connectionString.ConnectionString))
                    using (var service = DapperServiceFactory.Create(connection))
                    {
                        connection.Open();
                        CreateTable(connection);
                        connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                        if (constructor) MaterializedRow.ConstructorHook = () => RunWithoutFlow(service);
                        else MaterializedRow.NameSetterHook = () => RunWithoutFlow(service);
                        try
                        {
                            var row = asynchronous
                                ? await service.GetByIdAsync<MaterializedRow>(1)
                                : service.GetById<MaterializedRow>(1);
                            Assert.Equal("A", row.Name);
                            Assert.Equal(10, row.Age);
                        }
                        finally
                        {
                            MaterializedRow.ConstructorHook = null;
                            MaterializedRow.NameSetterHook = null;
                        }
                    }
                }
            }
        }

        [SqlServerFact]
        public void AttachAndDetach_EntityGetters_CanQueryWithoutExecutionContextFlow()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateTable(connection);
                connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                var row = new CallbackRow { Id = 1, Name = "A", Age = 10 };
                row.NameRead = () => RunWithoutFlow(service);
                service.Attach(row);
                row.IdRead = () => RunWithoutFlow(service);
                service.Detach(row);
                row.Name = "B";
                Assert.Equal(1, service.Update(row));
                Assert.Equal("B", service.GetById<CallbackRow>(1).Name);
            }
        }

        [SqlServerFact]
        public async Task UpdateListAndDeleteList_LazyEnumeration_CanQueryWithoutExecutionContextFlow()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                    var row = new CallbackRow { Id = 1, Name = "B", Age = 10 };
                    var updates = EnumerateWithCallback(row, () => RunWithoutFlow(service));
                    Assert.Equal(1, asynchronous ? await service.UpdateListAsync(updates) : service.UpdateList(updates));
                    Assert.Equal("B", service.GetById<CallbackRow>(1).Name);
                    var deletes = EnumerateWithCallback(row, () => RunWithoutFlow(service));
                    Assert.Equal(1, asynchronous ? await service.DeleteListAsync(deletes) : service.DeleteList(deletes));
                    Assert.Null(service.GetById<CallbackRow>(1));
                }
            }
        }

        [SqlServerFact]
        public async Task Lists_PreserveStreamingEnumerationAfterEachCompletedSqlCommand()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10),(N'B',20);");
                    var first = new CallbackRow { Id = 1, Name = "first updated", Age = 10 };
                    var second = new CallbackRow { Id = 2, Name = "second updated", Age = 20 };
                    var updates = StreamAfterFirst(first, second, () =>
                        Assert.Equal("first updated", service.GetById<CallbackRow>(first.Id).Name));
                    Assert.Equal(2, asynchronous ? await service.UpdateListAsync(updates) : service.UpdateList(updates));
                    var deletes = StreamAfterFirst(first, second, () =>
                        Assert.Null(service.GetById<CallbackRow>(first.Id)));
                    Assert.Equal(2, asynchronous ? await service.DeleteListAsync(deletes) : service.DeleteList(deletes));
                    Assert.Null(service.GetById<CallbackRow>(second.Id));
                }
            }
        }

        [SqlServerFact]
        public async Task Tracking_AcceptsTheValuesWrittenBySqlInsteadOfRereadingLiveGetters()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                    var row = new CallbackRow { Id = 1, Name = "A", Age = 10 };
                    service.Attach(row);
                    row.NameRead = () => row.Name = connection.ExecuteScalar<string>(
                        "SELECT Name FROM #EDCrudSystem WHERE Id=1;") == "A" ? "B" : "C";

                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                    Assert.Equal("B", service.GetById<CallbackRow>(1).Name);
                    // The getter now yields C. The accepted snapshot must still
                    // contain B, which was the value bound to the preceding SQL.
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                    Assert.Equal("C", service.GetById<CallbackRow>(1).Name);
                }
            }
        }

        [SqlServerFact]
        public async Task Tracking_UsesTargetRowsInsteadOfCountsFromUnrelatedTriggerWrites()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    connection.ChangeDatabase("tempdb");
                    var targetCreated = false;
                    var logCreated = false;
                    try
                    {
                        connection.Execute("CREATE TABLE dbo.[" + TriggerTable + "] (Id INT PRIMARY KEY, Name NVARCHAR(100));");
                        targetCreated = true;
                        connection.Execute("CREATE TABLE dbo.[" + TriggerLog + "] (Id INT IDENTITY PRIMARY KEY);");
                        logCreated = true;
                        connection.Execute("CREATE TRIGGER dbo.[" + TriggerTable + "_Trigger] ON dbo.[" + TriggerTable +
                            "] AFTER UPDATE AS INSERT dbo.[" + TriggerLog + "] DEFAULT VALUES;");
                        var row = new TriggerRow { Id = 1, Name = "A" };
                        service.Attach(row);
                        row.Name = "B";

                        // SQL Server reports the trigger's audit INSERT even
                        // though no row of the target table was updated.
                        Assert.Equal(1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                        Assert.Equal(0, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM dbo.[" + TriggerTable + "]"));
                        connection.Execute("INSERT dbo.[" + TriggerTable + "] VALUES(1,N'A');");
                        Assert.Equal(2, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                        Assert.Equal("B", service.GetById<TriggerRow>(1).Name);
                    }
                    finally
                    {
                        if (targetCreated) connection.Execute("DROP TABLE dbo.[" + TriggerTable + "];");
                        if (logCreated) connection.Execute("DROP TABLE dbo.[" + TriggerLog + "];");
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task Tracking_AcceptsSuccessfulUpdatesWhenNoCountSuppressesAffectedRowCounts()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10); SET NOCOUNT ON;");
                    var row = new CallbackRow { Id = 1, Name = "A", Age = 10 };
                    service.Attach(row);
                    row.Name = "B";
                    Assert.Equal(-1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                    Assert.Equal("B", service.GetById<CallbackRow>(1).Name);
                    row.Name = "A";
                    Assert.Equal(-1, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                    Assert.Equal("A", service.GetById<CallbackRow>(1).Name);
                    Assert.Equal(0, asynchronous ? await service.UpdateAsync(row) : service.Update(row));
                }
            }
        }

        [SqlServerFact]
        public async Task UpdateList_CapturedValues_PreserveDeclaredNullTypesHandlersAndGetterCounts()
        {
            try
            {
                foreach (var asynchronous in new[] { false, true })
                {
                    foreach (var tracked in new[] { false, true })
                    {
                        using (var connection = OpenConnection())
                        using (var service = DapperServiceFactory.Create(connection))
                        {
                            connection.Execute("CREATE TABLE #EDTypedCapture (Id INT PRIMARY KEY, Code NVARCHAR(100), GuidValue UNIQUEIDENTIFIER NULL, NullableFlag SMALLINT NULL, OptionalNumber INT NULL, Text NVARCHAR(MAX) NULL, ZProbe INT); INSERT #EDTypedCapture(Id,Code) VALUES(1,N'old');");
                            SqlMapper.AddTypeHandler(new CapturedCodeHandler(() => RunWithoutFlow(service)));
                            var row = new CapturedTypedRow { Id = 1, Code = new CapturedCode("old"), ZProbe = new MetadataProbe(service) };
                            if (tracked) service.Attach(row);
                            row.IdReads = 0;
                            row.TextReads = 0;
                            row.Code = new CapturedCode("new");
                            Assert.Equal(1, asynchronous ? await service.UpdateListAsync(new[] { row }) : service.UpdateList(new[] { row }));
                            Assert.Equal(1, row.IdReads);
                            Assert.Equal(1, row.TextReads);
                            Assert.Equal("new", connection.QueryFirst<string>("SELECT Code FROM #EDTypedCapture WHERE Id=1;"));
                            Assert.Equal(7, connection.ExecuteScalar<int>("SELECT ZProbe FROM #EDTypedCapture WHERE Id=1;"));
                            Assert.Equal(4, connection.ExecuteScalar<int>("SELECT CASE WHEN GuidValue IS NULL THEN 1 ELSE 0 END + CASE WHEN NullableFlag IS NULL THEN 1 ELSE 0 END + CASE WHEN OptionalNumber IS NULL THEN 1 ELSE 0 END + CASE WHEN Text IS NULL THEN 1 ELSE 0 END FROM #EDTypedCapture WHERE Id=1;"));
                        }
                    }
                }
            }
            finally { SqlMapper.AddTypeHandler(typeof(CapturedCode), null); }
        }

        [Fact]
        public void PreparedSnapshot_DoesNotReplaceAnExplicitReattachment()
        {
            var tracker = new EntityTracker();
            var keys = new List<System.Reflection.PropertyInfo> { typeof(CallbackRow).GetProperty("Id") };
            var original = new CallbackRow { Id = 1, Name = "A", Age = 10 };
            tracker.Attach(original);
            var key = tracker.CreateCompositeKey(original, keys);
            Assert.True(tracker.TryGetAttachment(key, out var attachment));
            original.Name = "B";
            var values = EntityTracker.CaptureValues(original, typeof(CallbackRow).GetProperties());
            var pending = tracker.PrepareSnapshot<CallbackRow>(values);

            tracker.Detach(original);
            var replacement = new CallbackRow { Id = 1, Name = "A" };
            tracker.Attach(replacement);
            tracker.AcceptPreparedSnapshot(attachment, pending);
            replacement.Name = "B";
            Assert.True(tracker.TryGetAttachment(key, out var current));
            var changed = tracker.GetChangedProperties<CallbackRow>(current,
                EntityTracker.CaptureValues(replacement, typeof(CallbackRow).GetProperties()));
            Assert.Equal(new[] { "Name" }, changed);
        }

        [SqlServerFact]
        public async Task AsyncCrud_OpeningCallbacks_CanQueryAndDisposeAlreadyAdmittedCalls()
        {
            foreach (var operation in new[] { "Insert", "Update", "DeleteList" })
            {
                foreach (var dispose in new[] { false, true })
                {
                    using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
                    using (var service = DapperServiceFactory.Create(connection))
                    {
                        var row = new CallbackRow { Id = 1, Name = "A", Age = 10 };
                        if (operation == "Update") service.Attach(row);
                        row.Name = "B";
                        StateChangeEventHandler handler = null;
                        handler = (sender, args) =>
                        {
                            if (args.CurrentState != ConnectionState.Open) return;
                            connection.StateChange -= handler;
                            RunWithoutFlow(service);
                            CreateTable(connection);
                            connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                            if (dispose) service.Dispose();
                        };
                        connection.StateChange += handler;
                        try
                        {
                            var affected = operation == "Insert" ? await service.InsertAsync(row)
                                : operation == "Update" ? await service.UpdateAsync(row)
                                : await service.DeleteListAsync(new[] { row });
                            Assert.Equal(1, affected);
                            var count = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM #EDCrudSystem;");
                            Assert.Equal(operation == "Insert" ? 2 : operation == "Update" ? 1 : 0, count);
                            if (operation == "Update")
                                Assert.Equal("B", connection.QueryFirst<string>("SELECT Name FROM #EDCrudSystem WHERE Id=1;"));
                        }
                        finally { connection.StateChange -= handler; }
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task DeleteList_IteratorDisposal_PreservesItsAlreadyOpenedBorrowedConnection()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    connection.Execute("INSERT #EDCrudSystem(Name,Age) VALUES(N'A',10);");
                    var rows = EnumerateWithCallback(new CallbackRow { Id = 1 }, service.Dispose);
                    Assert.Equal(1, asynchronous ? await service.DeleteListAsync(rows) : service.DeleteList(rows));
                    Assert.Equal(0, connection.ExecuteScalar<int>("SELECT COUNT(*) FROM #EDCrudSystem;"));
                }
            }
        }

        [SqlServerFact]
        public async Task UpdateList_EntityDynamicParameters_PreserveTheirOriginalSingleNativeBinding()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                foreach (var tracked in new[] { false, true })
                {
                    using (var connection = OpenConnection())
                    using (var service = DapperServiceFactory.Create(connection))
                    {
                        connection.Execute("CREATE TABLE #EDNativeEntity (Id INT PRIMARY KEY, Name NVARCHAR(100)); INSERT #EDNativeEntity VALUES(1,N'A');");
                        var row = new NativeEntityRow { Id = 1, Name = "A" };
                        if (tracked) service.Attach(row);
                        row.Name = "B";
                        Assert.Equal(1, asynchronous ? await service.UpdateListAsync(new[] { row }) : service.UpdateList(new[] { row }));
                        Assert.Equal(1, row.Bindings);
                        Assert.Equal("B!", connection.QueryFirst<string>("SELECT Name FROM #EDNativeEntity WHERE Id=1;"));
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task UpdateList_EntityDynamicParameters_PreserveStoredProcedureCommandOverrides()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                foreach (var tracked in new[] { false, true })
                {
                    using (var connection = OpenConnection())
                    using (var service = DapperServiceFactory.Create(connection))
                    {
                        connection.Execute("CREATE TABLE #EDNativeOverride (Id INT PRIMARY KEY, Name NVARCHAR(100)); INSERT #EDNativeOverride VALUES(1,N'A');");
                        var row = new NativeOverrideRow { Id = 1, Name = "A" };
                        if (tracked) service.Attach(row);
                        row.Name = "B";
                        Assert.Equal(1, asynchronous ? await service.UpdateListAsync(new[] { row }) : service.UpdateList(new[] { row }));
                        Assert.Equal(1, row.Bindings);
                        Assert.Equal("B!", connection.QueryFirst<string>("SELECT Name FROM #EDNativeOverride WHERE Id=1;"));
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task BulkCrudTrackingAndNestedTransactions_PreserveRowsSnapshotsAndPartialUpdates()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    CreateTable(connection);
                    var rows = new[]
                    {
                        new CallbackRow { Name = "A", Age = 10 },
                        new CallbackRow { Name = "B", Age = 20 }
                    };
                    service.BeginTransaction();
                    Assert.Equal(2, asynchronous ? await service.InsertListAsync(rows, true) : service.InsertList(rows, true));
                    Assert.NotEqual(rows[0].Id, rows[1].Id);
                    Assert.Equal("A", service.GetById<CallbackRow>(rows[0].Id).Name);
                    Assert.Equal("B", service.GetById<CallbackRow>(rows[1].Id).Name);

                    // Age is deliberately absent from this consumer's attached state.
                    // Only Name changes may be sent by subsequent tracked updates.
                    var partial = new CallbackRow { Id = rows[0].Id, Name = "A" };
                    service.Attach(partial);
                    partial.Name = "outer";
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    Assert.Equal(0, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    Assert.Equal(10, service.GetById<CallbackRow>(partial.Id).Age);

                    service.BeginTransaction();
                    partial.Name = "inner";
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    rows[1].Name = "nested list";
                    var repeated = new[] { rows[1], rows[1] };
                    Assert.Equal(2, asynchronous ? await service.UpdateListAsync(repeated) : service.UpdateList(repeated));
                    var nestedRows = new[] { new CallbackRow { Name = "C", Age = 30 }, new CallbackRow { Name = "D", Age = 40 } };
                    Assert.Equal(2, asynchronous ? await service.InsertListAsync(nestedRows, true) : service.InsertList(nestedRows, true));
                    Assert.Equal(1, asynchronous ? await service.DeleteAsync(rows[0]) : service.Delete(rows[0]));
                    Assert.Null(service.GetById<CallbackRow>(rows[0].Id));
                    service.RollbackTransaction();
                    Assert.Equal(1, service.TransactionCount());
                    Assert.Equal("outer", service.GetById<CallbackRow>(partial.Id).Name);
                    Assert.Equal("B", service.GetById<CallbackRow>(rows[1].Id).Name);
                    Assert.Null(service.GetById<CallbackRow>(nestedRows[0].Id));
                    Assert.Null(service.GetById<CallbackRow>(nestedRows[1].Id));
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    service.CommitTransaction();
                    Assert.Equal(0, service.TransactionCount());
                    Assert.Equal("inner", service.GetById<CallbackRow>(partial.Id).Name);

                    connection.Execute("UPDATE #EDCrudSystem SET Age=99 WHERE Id=@Id;", new { partial.Id });
                    partial.Name = "after commit";
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    Assert.Equal(99, service.GetById<CallbackRow>(partial.Id).Age);

                    service.BeginTransaction();
                    service.BeginTransaction();
                    partial.Name = "nested committed";
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    service.CommitTransaction();
                    service.RollbackTransaction();
                    Assert.Equal("after commit", service.GetById<CallbackRow>(partial.Id).Name);
                    Assert.Equal(1, asynchronous ? await service.UpdateAsync(partial) : service.Update(partial));
                    Assert.Equal(99, service.GetById<CallbackRow>(partial.Id).Age);

                    var deletes = new[] { partial, partial };
                    Assert.Equal(1, asynchronous ? await service.DeleteListAsync(deletes) : service.DeleteList(deletes));
                    Assert.Equal(0, asynchronous ? await service.DeleteAsync(partial) : service.Delete(partial));
                    Assert.Null(service.GetById<CallbackRow>(partial.Id));
                }
            }
        }

        private static SqlConnection OpenConnection()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            return connection;
        }

        private static void CreateTable(SqlConnection connection)
        {
            connection.Execute("CREATE TABLE #EDCrudSystem (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100), Age INT);");
        }

        private static IEnumerable<CallbackRow> EnumerateWithCallback(CallbackRow row, Action callback)
        {
            callback();
            yield return row;
        }

        private static IEnumerable<CallbackRow> StreamAfterFirst(CallbackRow first, CallbackRow second, Action afterFirst)
        {
            yield return first;
            // The existing list APIs execute each item before asking for the
            // next; input iterators may use that result to construct later items.
            afterFirst();
            yield return second;
        }

        private static void RunWithoutFlow(IDapperService service)
        {
            // Cancellation makes a lease regression fail within a bounded period
            // instead of leaving the test process blocked indefinitely.
            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
            {
                Task<int> nested;
                using (ExecutionContext.SuppressFlow())
                    nested = Task.Run(() => service.ExecuteScalarFunctionAsync<int>("POWER",
                        new { Base = 2, Exponent = 3 }, cancellation.Token));
                Assert.Equal(8, nested.GetAwaiter().GetResult());
            }
        }

        [Table("#EDCrudSystem")]
        public class CallbackRow
        {
            private int _id;
            private string _name;
            public Action IdRead;
            public Action IdWrite;
            public Action NameRead;

            [PrimaryKey, Identity]
            public int Id
            {
                get { IdRead?.Invoke(); return _id; }
                set { IdWrite?.Invoke(); _id = value; }
            }

            public string Name
            {
                get { NameRead?.Invoke(); return _name; }
                set { _name = value; }
            }

            public int Age { get; set; }
        }

        [Table("#EDCrudSystem")]
        public class MaterializedRow
        {
            public static Action ConstructorHook;
            public static Action NameSetterHook;
            private string _name;

            public MaterializedRow() { ConstructorHook?.Invoke(); }

            [PrimaryKey, Identity]
            public int Id { get; set; }

            public string Name
            {
                get { return _name; }
                set { NameSetterHook?.Invoke(); _name = value; }
            }

            public int Age { get; set; }
        }

        [Table(TriggerTable)]
        public class TriggerRow
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public enum CapturedFlag : short { One = 1 }

        public sealed class CapturedCode
        {
            public readonly string Value;
            public CapturedCode(string value) { Value = value; }
            public override bool Equals(object value) => value is CapturedCode other && Value == other.Value;
            public override int GetHashCode() => Value.GetHashCode();
        }

        private sealed class CapturedCodeHandler : SqlMapper.TypeHandler<CapturedCode>
        {
            private readonly Action _callback;
            public CapturedCodeHandler(Action callback) { _callback = callback; }
            public override void SetValue(IDbDataParameter parameter, CapturedCode value)
            {
                _callback();
                parameter.DbType = DbType.String;
                parameter.Size = 100;
                parameter.Value = value.Value;
            }
            public override CapturedCode Parse(object value) => new CapturedCode((string)value);
        }

        public sealed class MetadataProbe : SqlMapper.ICustomQueryParameter
        {
            private readonly IDapperService _service;
            public MetadataProbe(IDapperService service) { _service = service; }
            public void AddParameter(IDbCommand command, string name)
            {
                RunWithoutFlow(_service);
                Assert.Equal(DbType.Guid, ((IDbDataParameter)command.Parameters["GuidValue"]).DbType);
                Assert.Equal(DbType.Int16, ((IDbDataParameter)command.Parameters["NullableFlag"]).DbType);
                Assert.Equal(DbType.Int32, ((IDbDataParameter)command.Parameters["OptionalNumber"]).DbType);
                Assert.Equal(DbType.String, ((IDbDataParameter)command.Parameters["Text"]).DbType);
                Assert.Equal(0, ((IDbDataParameter)command.Parameters["Text"]).Size);
                var parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.DbType = DbType.Int32;
                parameter.Value = 7;
                command.Parameters.Add(parameter);
            }
        }

        [Table("#EDTypedCapture")]
        public class CapturedTypedRow
        {
            private int _id;
            private string _text;
            public int IdReads;
            public int TextReads;
            [PrimaryKey] public int Id { get { IdReads++; return _id; } set { _id = value; } }
            public CapturedCode Code { get; set; }
            public Guid? GuidValue { get; set; }
            public CapturedFlag? NullableFlag { get; set; }
            public int? OptionalNumber { get; set; }
            public string Text { get { TextReads++; return _text; } set { _text = value; } }
            public MetadataProbe ZProbe { get; set; }
        }

        [Table("#EDNativeEntity")]
        public class NativeEntityRow : SqlMapper.IDynamicParameters
        {
            [PrimaryKey] public int Id { get; set; }
            public string Name { get; set; }
            public int Bindings;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
                var id = command.CreateParameter();
                id.ParameterName = "Id"; id.DbType = DbType.Int32; id.Value = Id; command.Parameters.Add(id);
                var name = command.CreateParameter();
                name.ParameterName = "Name"; name.DbType = DbType.String; name.Size = 100; name.Value = Name + "!"; command.Parameters.Add(name);
            }
        }

        [Table("#EDNativeOverride")]
        public class NativeOverrideRow : SqlMapper.IDynamicParameters
        {
            [PrimaryKey] public int Id { get; set; }
            public string Name { get; set; }
            public int Bindings;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
                command.CommandType = CommandType.StoredProcedure;
                command.CommandText = "sys.sp_executesql";
                var statement = command.CreateParameter();
                statement.ParameterName = "stmt"; statement.DbType = DbType.String; statement.Size = -1;
                statement.Value = "UPDATE #EDNativeOverride SET Name=N'B!' WHERE Id=1;";
                command.Parameters.Add(statement);
            }
        }
    }
}
