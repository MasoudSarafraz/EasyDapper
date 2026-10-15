using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class CommandLifecycleIntegrityTests
    {
        [SqlServerFact]
        public async Task Procedure_CustomBinderReceivesOriginalQueryIdentity()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new ProcedureParameters("SELECT 11 AS Id;");
                    if (asynchronous)
                        await connection.QueryAsync<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure);
                    else
                        connection.Query<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure).ToArray();
                    var actual = new ProcedureParameters("SELECT 11 AS Id;");
                    var rows = asynchronous
                        ? await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", actual)
                        : service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", actual);
                    Assert.Equal(11, rows.Single().Id);
                    AssertIdentity(expected.Identity, actual.Identity);
                    Assert.Equal(1, actual.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task MultiResult_CustomBinderReceivesOriginalGridIdentity()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new ProcedureParameters("SELECT 11 AS Id;");
                    using (var reader = asynchronous
                        ? await connection.QueryMultipleAsync("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure)
                        : connection.QueryMultiple("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure))
                        reader.Read<IdentityRow>().ToArray();
                    var actual = new ProcedureParameters("SELECT 11 AS Id;");
                    var row = asynchronous
                        ? await service.ExecuteMultiResultStoredProcedureAsync<IdentityRow>("sys.sp_executesql",
                            async reader => (await reader.ReadAsync<IdentityRow>()).Single(), actual)
                        : service.ExecuteMultiResultStoredProcedure<IdentityRow>("sys.sp_executesql",
                            reader => reader.Read<IdentityRow>().Single(), actual);
                    Assert.Equal(11, row.Id);
                    AssertIdentity(expected.Identity, actual.Identity);
                    Assert.Equal(1, actual.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task Scalar_CustomBinderReceivesOriginalTextIdentity()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new ScalarParameters();
                    if (asynchronous)
                        await connection.ExecuteScalarAsync<int>("SELECT POWER(@Base, @Exponent)", expected);
                    else
                        connection.ExecuteScalar<int>("SELECT POWER(@Base, @Exponent)", expected);
                    var actual = new ScalarParameters();
                    Assert.Equal(8, asynchronous
                        ? await service.ExecuteScalarFunctionAsync<int>("POWER", actual)
                        : service.ExecuteScalarFunction<int>("POWER", actual));
                    AssertIdentity(expected.Identity, actual.Identity);
                    Assert.Equal(1, actual.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task TableFunction_CustomBinderReceivesOriginalResultIdentity()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new TableParameters();
                    if (asynchronous)
                        await connection.QueryAsync<PermissionRow>("SELECT * FROM sys.fn_builtin_permissions(@permissionClass)", expected);
                    else
                        connection.Query<PermissionRow>("SELECT * FROM sys.fn_builtin_permissions(@permissionClass)", expected).ToArray();
                    var actual = new TableParameters();
                    var rows = asynchronous
                        ? await service.ExecuteTableFunctionAsync<PermissionRow>("sys.fn_builtin_permissions", actual)
                        : service.ExecuteTableFunction<PermissionRow>("sys.fn_builtin_permissions", actual);
                    Assert.NotEmpty(rows);
                    AssertIdentity(expected.Identity, actual.Identity);
                    Assert.Equal(1, actual.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task CustomBinder_CanUseDappersGeneratedBinderForItsOriginalReceiver()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var values = new GeneratedParameters();
                    var rows = asynchronous
                        ? await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", values)
                        : service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", values);
                    Assert.Equal(17, rows.Single().Id);
                    Assert.Equal(1, values.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task CustomBinder_PreservesNativeCommandChangesMadeDuringBinding()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new CommandChangingParameters();
                    var direct = asynchronous
                        ? await connection.QueryAsync<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure)
                        : connection.Query<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure);
                    Assert.Equal(19, direct.Single().Id);
                    var actual = new CommandChangingParameters();
                    var rows = asynchronous
                        ? await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", actual)
                        : service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", actual);
                    Assert.Equal(19, rows.Single().Id);
                    Assert.Equal(1, actual.Bindings);
                }
            }
        }

        [SqlServerFact]
        public async Task MarsBufferedProcedure_DrainsFurtherResultsAndCompletesOutputsOnce()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection(true))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var parameters = new CompletionParameters();
                    var rows = asynchronous
                        ? await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", parameters)
                        : service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", parameters);
                    Assert.Equal(11, rows.Single().Id);
                    Assert.Equal(29, parameters.Output.Value);
                    Assert.Equal(1, parameters.Completions);
                    Assert.Equal(1, parameters.Bindings);
                    Assert.Equal(ConnectionState.Open, connection.State);
                    Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 }));
                }
            }
        }

        [SqlServerFact]
        public async Task MultiResult_DynamicOutputCallbacksRunBeforeTheMapperContinues()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var target = new OutputTarget { Value = 2 };
                    var parameters = new DynamicParameters(new
                    {
                        stmt = "SELECT 11 AS Id; SET @Value=29; SELECT 23 AS Id;",
                        @params = "@Value int OUTPUT",
                        Value = target.Value
                    });
                    parameters.Output(target, value => value.Value, DbType.Int32);
                    var row = asynchronous
                        ? await service.ExecuteMultiResultStoredProcedureAsync<IdentityRow>("sys.sp_executesql", async reader =>
                        {
                            var first = (await reader.ReadAsync<IdentityRow>()).Single();
                            Assert.Equal(2, target.Value);
                            Assert.Equal(23, (await reader.ReadAsync<IdentityRow>()).Single().Id);
                            Assert.Equal(29, target.Value);
                            return first;
                        }, parameters)
                        : service.ExecuteMultiResultStoredProcedure<IdentityRow>("sys.sp_executesql", reader =>
                        {
                            var first = reader.Read<IdentityRow>().Single();
                            Assert.Equal(2, target.Value);
                            Assert.Equal(23, reader.Read<IdentityRow>().Single().Id);
                            Assert.Equal(29, target.Value);
                            return first;
                        }, parameters);
                    Assert.Equal(11, row.Id);
                    Assert.Equal(29, target.Value);
                }
            }
        }

        [SqlServerFact]
        public async Task MarsBufferedProcedure_FailedHydrationSkipsCompletionLikeDapperQuery()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection(true))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    const string statement = "SELECT N'invalid' AS Id; SET @output=29;";
                    var expected = new CompletionParameters(statement);
                    var actual = new CompletionParameters(statement);
                    Exception directError, serviceError;
                    if (asynchronous)
                    {
                        directError = await Record.ExceptionAsync(() => connection.QueryAsync<IdentityRow>(
                            "sys.sp_executesql", expected, commandType: CommandType.StoredProcedure));
                        serviceError = await Record.ExceptionAsync(() => service.ExecuteStoredProcedureAsync<IdentityRow>(
                            "sys.sp_executesql", actual));
                    }
                    else
                    {
                        directError = Record.Exception(() => connection.Query<IdentityRow>(
                            "sys.sp_executesql", expected, commandType: CommandType.StoredProcedure));
                        serviceError = Record.Exception(() => service.ExecuteStoredProcedure<IdentityRow>(
                            "sys.sp_executesql", actual));
                    }
                    Assert.NotNull(directError);
                    Assert.NotNull(serviceError);
                    Assert.Equal(directError.GetType(), serviceError.GetType());
                    Assert.Equal(0, expected.Completions);
                    Assert.Equal(expected.Completions, actual.Completions);
                    Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 }));
                }
            }
        }

        [SqlServerFact]
        public async Task MarsProcedure_WithoutColumnsPreservesDapperResultsAndErrors()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection(true))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var directParameters = new EmptyResultParameters();
                    var serviceParameters = new EmptyResultParameters();
                    IdentityRow[] directRows = null, serviceRows = null;
                    Exception directError, serviceError;
                    if (asynchronous)
                    {
                        directError = await Record.ExceptionAsync(async () => directRows =
                            (await connection.QueryAsync<IdentityRow>("sys.sp_executesql", directParameters,
                                commandType: CommandType.StoredProcedure)).ToArray());
                        serviceError = await Record.ExceptionAsync(async () => serviceRows =
                            (await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", serviceParameters)).ToArray());
                    }
                    else
                    {
                        directError = Record.Exception(() => directRows = connection.Query<IdentityRow>(
                            "sys.sp_executesql", directParameters, commandType: CommandType.StoredProcedure).ToArray());
                        serviceError = Record.Exception(() => serviceRows = service.ExecuteStoredProcedure<IdentityRow>(
                            "sys.sp_executesql", serviceParameters).ToArray());
                    }
                    if (directError == null)
                    {
                        Assert.Null(serviceError);
                        Assert.Empty(directRows);
                        Assert.Empty(serviceRows);
                    }
                    else
                    {
                        Assert.NotNull(serviceError);
                        Assert.Equal(directError.GetType(), serviceError.GetType());
                    }
                    Assert.Equal(directParameters.Completions, serviceParameters.Completions);
                    Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 }));
                }
            }
        }

        [SqlServerFact]
        public async Task CompletionCallbacks_CanReadOriginalNativeOutputCollection()
        {
            foreach (var asynchronous in new[] { false, true })
            foreach (var scalar in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var expected = new NativeCollectionParameters(scalar);
                    var actual = new NativeCollectionParameters(scalar);
                    if (scalar)
                    {
                        Assert.Equal(8, asynchronous
                            ? await connection.ExecuteScalarAsync<int>("SELECT POWER(@Base, @Exponent)", expected)
                            : connection.ExecuteScalar<int>("SELECT POWER(@Base, @Exponent)", expected));
                        Assert.Equal(8, asynchronous
                            ? await service.ExecuteScalarFunctionAsync<int>("POWER", actual)
                            : service.ExecuteScalarFunction<int>("POWER", actual));
                    }
                    else
                    {
                        var direct = asynchronous
                            ? await connection.QueryAsync<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure)
                            : connection.Query<IdentityRow>("sys.sp_executesql", expected, commandType: CommandType.StoredProcedure);
                        var rows = asynchronous
                            ? await service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", actual)
                            : service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", actual);
                        Assert.Equal(11, direct.Single().Id);
                        Assert.Equal(11, rows.Single().Id);
                    }
                    Assert.Equal(29, expected.Result);
                    Assert.Equal(expected.Result, actual.Result);
                    Assert.Equal(1, actual.Completions);
                    Assert.Equal(1, actual.Bindings);
                    Assert.Equal(expected.Command.Parameters.Count, actual.Command.Parameters.Count);
                }
            }
        }

        [SqlServerFact]
        public async Task PreparedWrite_RejectsTransactionCommittedDuringParameterBinding()
        {
            foreach (var asynchronous in new[] { false, true })
            {
                using (var connection = OpenConnection())
                using (var service = DapperServiceFactory.Create(connection))
                {
                    connection.Execute("CREATE TABLE #EDLifecycle (Id int PRIMARY KEY, Value int NOT NULL); INSERT #EDLifecycle VALUES(1,0);");
                    service.BeginTransaction();
                    var parameters = new TransactionChangingParameters(service);
                    if (asynchronous)
                        await Assert.ThrowsAsync<InvalidOperationException>(() =>
                            service.ExecuteStoredProcedureAsync<IdentityRow>("sys.sp_executesql", parameters));
                    else
                        Assert.Throws<InvalidOperationException>(() =>
                            service.ExecuteStoredProcedure<IdentityRow>("sys.sp_executesql", parameters));
                    Assert.Equal(1, parameters.StatementReads);
                    Assert.Equal(0, connection.ExecuteScalar<int>("SELECT Value FROM #EDLifecycle WHERE Id=1"));
                    Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 }));
                }
            }
        }

        [SqlServerFact]
        public async Task MarsResultMaterialization_ConstructorsAndSettersCanQueryWithoutContextFlow()
        {
            foreach (var asynchronous in new[] { false, true })
            foreach (var constructor in new[] { false, true })
            {
                using (var connection = OpenConnection(true))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    var nestedResult = 0;
                    Action callback = () => nestedResult = NestedWithoutFlow(service);
                    if (constructor) MaterializedRow.ConstructorHook = callback;
                    else MaterializedRow.SetterHook = callback;
                    try
                    {
                        var rows = asynchronous
                            ? await service.ExecuteStoredProcedureAsync<MaterializedRow>("sys.sp_executesql", new { stmt = "SELECT 11 AS Id;" })
                            : service.ExecuteStoredProcedure<MaterializedRow>("sys.sp_executesql", new { stmt = "SELECT 11 AS Id;" });
                        Assert.Equal(11, rows.Single().Id);
                        Assert.Equal(2, nestedResult);
                    }
                    finally
                    {
                        MaterializedRow.ConstructorHook = null;
                        MaterializedRow.SetterHook = null;
                    }
                }
            }
        }

        private static void AssertIdentity(SqlMapper.Identity expected, SqlMapper.Identity actual)
        {
            Assert.NotNull(actual);
            Assert.Equal(expected.sql, actual.sql);
            Assert.Equal(expected.commandType, actual.commandType);
            Assert.Equal(expected.connectionString, actual.connectionString);
            Assert.Equal(expected.type, actual.type);
            Assert.Equal(expected.parametersType, actual.parametersType);
            Assert.Equal(expected.gridIndex, actual.gridIndex);
            Assert.Equal(expected.hashCode, actual.hashCode);
        }

        private static SqlConnection OpenConnection(bool mars = false)
        {
            var settings = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"))
            { MultipleActiveResultSets = mars };
            var connection = new SqlConnection(settings.ConnectionString);
            connection.Open();
            return connection;
        }

        private static int NestedWithoutFlow(IDapperService service)
        {
            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
            {
                Task<int> nested;
                using (ExecutionContext.SuppressFlow())
                    nested = Task.Run(() => service.ExecuteScalarFunctionAsync<int>("POWER",
                        new { Base = 2, Exponent = 1 }, cancellation.Token));
                return nested.GetAwaiter().GetResult();
            }
        }

        private abstract class IdentityParameters : SqlMapper.IDynamicParameters
        {
            internal SqlMapper.Identity Identity;
            internal int Bindings;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Identity = identity;
                Bindings++;
                AddNativeParameters(Assert.IsType<SqlCommand>(command));
            }
            protected abstract void AddNativeParameters(SqlCommand command);
        }

        private sealed class ProcedureParameters : IdentityParameters
        {
            private readonly string _statement;
            internal ProcedureParameters(string statement) { _statement = statement; }
            protected override void AddNativeParameters(SqlCommand command)
                => command.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar) { Value = _statement });
        }

        private sealed class ScalarParameters : IdentityParameters
        {
            public int Base => 2;
            public int Exponent => 3;
            protected override void AddNativeParameters(SqlCommand command)
            {
                command.Parameters.Add(new SqlParameter("Base", SqlDbType.Int) { Value = Base });
                command.Parameters.Add(new SqlParameter("Exponent", SqlDbType.Int) { Value = Exponent });
            }
        }

        private sealed class TableParameters : IdentityParameters
        {
            public string permissionClass => "SERVER";
            protected override void AddNativeParameters(SqlCommand command)
                => command.Parameters.Add(new SqlParameter("permissionClass", SqlDbType.NVarChar) { Value = permissionClass });
        }

        private sealed class GeneratedParameters : SqlMapper.IDynamicParameters
        {
            public string stmt => "SELECT 17 AS Id;";
            internal int Bindings;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
#pragma warning disable 618
                SqlMapper.CreateParamInfoGenerator(identity, false, true)(command, this);
#pragma warning restore 618
                Assert.Equal(CommandType.StoredProcedure, command.CommandType);
                Assert.Equal("sys.sp_executesql", command.CommandText);
                Assert.Equal("SELECT 17 AS Id;", ((IDataParameter)command.Parameters["stmt"]).Value);
            }
        }

        private sealed class CommandChangingParameters : SqlMapper.IDynamicParameters
        {
            internal int Bindings;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
                command.CommandType = CommandType.Text;
                command.CommandText = "SELECT 19 AS Id;";
            }
        }

        private sealed class CompletionParameters : SqlMapper.IDynamicParameters, SqlMapper.IParameterCallbacks
        {
            internal int Bindings, Completions;
            internal SqlParameter Output;
            private readonly string _statement;
            internal CompletionParameters(string statement = "SELECT 11 AS Id; SELECT 23 AS Ignored; SET @output=29;")
                => _statement = statement;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
                var native = Assert.IsType<SqlCommand>(command);
                native.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)
                { Value = _statement });
                native.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar) { Value = "@output int OUTPUT" });
                Output = new SqlParameter("output", SqlDbType.Int) { Direction = ParameterDirection.Output };
                native.Parameters.Add(Output);
            }
            public void OnCompleted() { Completions++; Assert.Equal(29, Output.Value); }
        }

        private sealed class NativeCollectionParameters : SqlMapper.IDynamicParameters, SqlMapper.IParameterCallbacks
        {
            private readonly bool _scalar;
            private SqlParameter _output;
            internal SqlCommand Command;
            internal int Bindings, Completions, Result;
            internal NativeCollectionParameters(bool scalar) { _scalar = scalar; }
            public int Base => 2;
            public int Exponent => 3;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Command = Assert.IsType<SqlCommand>(command);
                Bindings++;
                if (_scalar)
                {
                    command.CommandText = "SET @Result=29; SELECT POWER(@Base,@Exponent);";
                    Command.Parameters.Add(new SqlParameter("Base", SqlDbType.Int) { Value = Base });
                    Command.Parameters.Add(new SqlParameter("Exponent", SqlDbType.Int) { Value = Exponent });
                }
                else
                {
                    Command.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar)
                    { Value = "SET @Result=29; SELECT 11 AS Id;" });
                    Command.Parameters.Add(new SqlParameter("params", SqlDbType.NVarChar) { Value = "@Result int OUTPUT" });
                }
                _output = new SqlParameter("Result", SqlDbType.Int) { Direction = ParameterDirection.Output };
                Command.Parameters.Add(_output);
            }
            public void OnCompleted()
            {
                Completions++;
                Assert.Same(_output, Command.Parameters["Result"]);
                Result = (int)Command.Parameters["Result"].Value;
            }
        }

        private sealed class EmptyResultParameters : SqlMapper.IDynamicParameters, SqlMapper.IParameterCallbacks
        {
            internal int Completions;
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
                => Assert.IsType<SqlCommand>(command).Parameters.Add(
                    new SqlParameter("stmt", SqlDbType.NVarChar) { Value = "PRINT N'no rows';" });
            public void OnCompleted() { Completions++; }
        }

        private sealed class TransactionChangingParameters
        {
            private readonly IDapperService _service;
            internal int StatementReads;
            internal TransactionChangingParameters(IDapperService service) { _service = service; }
            public string stmt
            {
                get
                {
                    StatementReads++;
                    Task commit;
                    using (ExecutionContext.SuppressFlow()) commit = Task.Run(() => _service.CommitTransaction());
                    Assert.True(commit.Wait(TimeSpan.FromSeconds(3)), "Commit was blocked by unlocked parameter preparation.");
                    commit.GetAwaiter().GetResult();
                    return "UPDATE #EDLifecycle SET Value=1 WHERE Id=1; SELECT 1 AS Id;";
                }
            }
        }

        public class IdentityRow { public int Id { get; set; } }
        public class OutputTarget { public int Value { get; set; } }
        public class PermissionRow { public string permission_name { get; set; } }
        public class MaterializedRow
        {
            internal static Action ConstructorHook, SetterHook;
            private int _id;
            public MaterializedRow() { ConstructorHook?.Invoke(); }
            public int Id { get => _id; set { _id = value; SetterHook?.Invoke(); } }
        }
    }
}
