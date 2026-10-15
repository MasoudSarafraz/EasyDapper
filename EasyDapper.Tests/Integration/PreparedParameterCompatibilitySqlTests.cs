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
    public class PreparedParameterCompatibilitySqlTests
    {
        [SqlServerFact]
        public Task SyncFunction_GetterCanWaitForSuppressedContextCommand() => VerifyGetter(false, false);

        [SqlServerFact]
        public Task AsyncFunction_GetterCanWaitForSuppressedContextCommand() => VerifyGetter(true, false);

        [SqlServerFact]
        public Task SyncProcedure_GetterCanWaitForSuppressedContextCommand() => VerifyGetter(false, true);

        [SqlServerFact]
        public Task AsyncProcedure_GetterCanWaitForSuppressedContextCommand() => VerifyGetter(true, true);

        [SqlServerFact]
        public Task SyncFunction_CustomParameterReceivesNativeCommandOnce() => VerifyNativeParameter(false);

        [SqlServerFact]
        public Task AsyncFunction_CustomParameterReceivesNativeCommandOnce() => VerifyNativeParameter(true);

        [SqlServerFact]
        public Task SyncProcedure_CompletionCanWaitForSuppressedContextCommand() => VerifyCompletion(false);

        [SqlServerFact]
        public Task AsyncProcedure_CompletionCanWaitForSuppressedContextCommand() => VerifyCompletion(true);

        [SqlServerFact]
        public void Procedure_DynamicOutputParametersKeepAttachedValues()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parameters = new DynamicParameters();
                parameters.Add("stmt", "SET @value = POWER(@value,3); SELECT @value AS Id;");
                parameters.Add("params", "@value int OUTPUT");
                parameters.Add("value", 2, DbType.Int32, ParameterDirection.InputOutput);
                Assert.Equal(8, service.ExecuteStoredProcedure<Person>("sys.sp_executesql", parameters).Single().Id);
                Assert.Equal(8, parameters.Get<int>("value"));
                parameters.Add("value", 2, DbType.Int32, ParameterDirection.InputOutput);
                service.ExecuteMultiResultStoredProcedure<Person>("sys.sp_executesql", reader =>
                {
                    var row = reader.Read<Person>().Single();
                    Assert.Equal(8, parameters.Get<int>("value"));
                    return row;
                }, parameters);
            }
        }

        [SqlServerFact]
        public void Function_NullArgumentKeepsDeclaredType()
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                // A declared nullable parameter must still reach SQL as NULL.
                Assert.Null(service.ExecuteScalarFunction<int?>("POWER", new { Base = (int?)null, Exponent = 3 }));
            }
        }

        private static async Task VerifyGetter(bool asynchronous, bool procedure)
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var values = new ComputedArguments(service);
                if (procedure)
                {
                    var rows = asynchronous
                        ? await service.ExecuteStoredProcedureAsync<Person>("sys.sp_executesql", values)
                        : service.ExecuteStoredProcedure<Person>("sys.sp_executesql", values);
                    Assert.Equal(8, rows.Single().Id);
                    Assert.Equal(1, values.StatementReads);
                }
                else
                {
                    var result = asynchronous
                        ? await service.ExecuteScalarFunctionAsync<int>("POWER", new FunctionArguments(values))
                        : service.ExecuteScalarFunction<int>("POWER", new FunctionArguments(values));
                    Assert.Equal(8, result);
                    Assert.Equal(1, values.BaseReads);
                }
            }
        }

        private static async Task VerifyNativeParameter(bool asynchronous)
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parameter = new NativeParameter(service);
                var arguments = new { Base = parameter, Exponent = 3 };
                Assert.Equal(8, asynchronous
                    ? await service.ExecuteScalarFunctionAsync<int>("POWER", arguments)
                    : service.ExecuteScalarFunction<int>("POWER", arguments));
                Assert.Equal(1, parameter.Bindings);
            }
        }

        private static async Task VerifyCompletion(bool asynchronous)
        {
            using (var connection = OpenConnection())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parameters = new CallbackParameters(service);
                var rows = asynchronous
                    ? await service.ExecuteStoredProcedureAsync<Person>("sys.sp_executesql", parameters)
                    : service.ExecuteStoredProcedure<Person>("sys.sp_executesql", parameters);
                Assert.Equal(11, rows.Single().Id);
                Assert.Equal(1, parameters.Bindings);
                Assert.Equal(1, parameters.Completions);
                Assert.Equal(2, parameters.NestedResult);
            }
        }

        private static SqlConnection OpenConnection()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            return connection;
        }

        private static int NestedWithoutFlow(IDapperService service)
        {
            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                Task<int> nested;
                using (ExecutionContext.SuppressFlow())
                    nested = Task.Run(() => service.ExecuteScalarFunctionAsync<int>("POWER",
                        new { Base = 2, Exponent = 1 }, cancellation.Token));
                return nested.GetAwaiter().GetResult();
            }
        }

        private sealed class ComputedArguments
        {
            private readonly IDapperService _service;
            internal int BaseReads, StatementReads;
            public ComputedArguments(IDapperService service) { _service = service; }
            internal int ReadBase() { BaseReads++; return NestedWithoutFlow(_service); }
            public string stmt { get { StatementReads++; return "SELECT " + (int)Math.Pow(NestedWithoutFlow(_service), 3) + " AS Id;"; } }
        }

        private sealed class FunctionArguments
        {
            private readonly ComputedArguments _values;
            public FunctionArguments(ComputedArguments values) { _values = values; }
            public int Base => _values.ReadBase();
            public int Exponent => 3;
        }

        private sealed class NativeParameter : SqlMapper.ICustomQueryParameter
        {
            private readonly IDapperService _service;
            internal int Bindings;
            public NativeParameter(IDapperService service) { _service = service; }
            public void AddParameter(IDbCommand command, string name)
            {
                Bindings++;
                var native = Assert.IsType<SqlCommand>(command);
                Assert.Equal(CommandType.Text, native.CommandType);
                native.Parameters.Add(new SqlParameter(name, SqlDbType.Int) { Value = NestedWithoutFlow(_service) });
            }
        }

        private sealed class CallbackParameters : SqlMapper.IDynamicParameters, SqlMapper.IParameterCallbacks
        {
            private readonly IDapperService _service;
            internal int Bindings, Completions, NestedResult;
            public CallbackParameters(IDapperService service) { _service = service; }
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                Bindings++;
                var native = Assert.IsType<SqlCommand>(command);
                Assert.Equal(CommandType.StoredProcedure, native.CommandType);
                native.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar) { Value = "SELECT 11 AS Id;" });
            }
            public void OnCompleted() { Completions++; NestedResult = NestedWithoutFlow(_service); }
        }
    }
}
