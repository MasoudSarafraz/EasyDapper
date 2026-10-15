using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class MultiResultMapperCompatibilityTests
    {
        [SqlServerFact]
        public void SyncMapper_WithMars_CanRunAnotherCommandBeforeReadingResults()
            => VerifySyncMapper(true);

        [SqlServerFact]
        public void SyncMapper_WithoutMars_CanRunAnotherCommandAfterReadingResults()
            => VerifySyncMapper(false);

        [SqlServerFact]
        public Task AsyncMapper_WithMars_CanRunAnotherCommandBeforeReadingResults()
            => VerifyAsyncMapper(true);

        [SqlServerFact]
        public Task AsyncMapper_WithoutMars_CanRunAnotherCommandAfterReadingResults()
            => VerifyAsyncMapper(false);

        [SqlServerFact]
        public Task AsyncMapper_CanAwaitTaskRunWithFlowingExecutionContext()
            => VerifyTaskRunMapper(false);

        [SqlServerFact]
        public Task AsyncMapper_CanAwaitTaskRunWithSuppressedExecutionContext()
            => VerifyTaskRunMapper(true);

        [SqlServerFact]
        public void Mapper_CanDisposeServiceBeforeReadingExternallyOwnedConnection()
            => VerifyDisposalMapper(true);

        [SqlServerFact]
        public void Mapper_CanDisposeServiceAfterReadingExternallyOwnedConnection()
            => VerifyDisposalMapper(false);

        [SqlServerFact]
        public void FunctionParameters_CanComputeValuesThroughAnotherServiceCommand()
        {
            using (var connection = OpenConnection(false))
            using (var service = DapperServiceFactory.Create(connection))
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new ComputedParameters(service, false)));
        }

        [SqlServerFact]
        public void FunctionParameters_CanWaitForAnAsynchronousNestedServiceCommand()
        {
            using (var connection = OpenConnection(false))
            using (var service = DapperServiceFactory.Create(connection))
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new ComputedParameters(service, true)));
        }

        [SqlServerFact]
        public Task ConnectionStateChange_CanRunServiceCommandDuringSynchronousOpening()
            => VerifyStateChangeCallback(false);

        [SqlServerFact]
        public Task ConnectionStateChange_CanRunServiceCommandDuringAsynchronousOpening()
            => VerifyStateChangeCallback(true);

        [SqlServerFact]
        public Task ConnectionStateChange_CanDisposeExternalServiceDuringSynchronousOpening()
            => VerifyStateChangeDisposal(false);

        [SqlServerFact]
        public Task ConnectionStateChange_CanDisposeExternalServiceDuringAsynchronousOpening()
            => VerifyStateChangeDisposal(true);

        private static SqlConnection OpenConnection(bool mars)
        {
            var builder = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"))
            { MultipleActiveResultSets = mars };
            var connection = new SqlConnection(builder.ConnectionString);
            connection.Open();
            return connection;
        }

        private static void VerifySyncMapper(bool mars)
        {
            using (var connection = OpenConnection(mars))
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = service.ExecuteMultiResultStoredProcedure<List<int>>("sys.sp_executesql", reader =>
                {
                    var rows = mars ? null : reader.Read<int>().ToList();
                    var nested = service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 });
                    return new List<int> { nested, (rows ?? reader.Read<int>().ToList())[0] };
                }, new { stmt = "SELECT 11;" });
                Assert.Equal(new[] { 8, 11 }, result);
                Assert.Equal(8, service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 }));
            }
        }

        private static async Task VerifyAsyncMapper(bool mars)
        {
            using (var connection = OpenConnection(mars))
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = await service.ExecuteMultiResultStoredProcedureAsync<List<int>>("sys.sp_executesql", async reader =>
                {
                    await Task.Yield();
                    var rows = mars ? null : (await reader.ReadAsync<int>()).ToList();
                    var nested = await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 });
                    return new List<int> { nested, (rows ?? (await reader.ReadAsync<int>()).ToList())[0] };
                }, new { stmt = "SELECT 11;" });
                Assert.Equal(new[] { 8, 11 }, result);
                Assert.Equal(8, await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 }));
            }
        }

        private static async Task VerifyTaskRunMapper(bool suppressFlow)
        {
            using (var connection = OpenConnection(true))
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = await service.ExecuteMultiResultStoredProcedureAsync<List<int>>("sys.sp_executesql", async reader =>
                {
                    using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
                    {
                        Task<int> nested;
                        if (suppressFlow)
                        {
                            using (ExecutionContext.SuppressFlow())
                                nested = Task.Run(() => service.ExecuteScalarFunctionAsync<int>(
                                    "POWER", new { Base = 2, Exponent = 3 }, cancellation.Token));
                        }
                        else
                            nested = Task.Run(() => service.ExecuteScalarFunctionAsync<int>(
                                "POWER", new { Base = 2, Exponent = 3 }, cancellation.Token));
                        return new List<int> { await nested, (await reader.ReadAsync<int>()).First() };
                    }
                }, new { stmt = "SELECT 11;" });
                Assert.Equal(new[] { 8, 11 }, result);
            }
        }

        private static void VerifyDisposalMapper(bool beforeReading)
        {
            using (var connection = OpenConnection(true))
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = service.ExecuteMultiResultStoredProcedure<List<int>>("sys.sp_executesql", reader =>
                {
                    var rows = beforeReading ? null : reader.Read<int>().ToList();
                    service.Dispose();
                    return rows ?? reader.Read<int>().ToList();
                }, new { stmt = "SELECT 11;" });
                Assert.Equal(new[] { 11 }, result);
                Assert.Equal(ConnectionState.Open, connection.State);
            }
        }

        private static async Task VerifyStateChangeCallback(bool asynchronousOpening)
        {
            var builder = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"))
            { MultipleActiveResultSets = false };
            using (var connection = new SqlConnection(builder.ConnectionString))
            using (var service = DapperServiceFactory.Create(connection))
            {
                var nestedResult = 0;
                connection.StateChange += (sender, args) =>
                {
                    if (args.CurrentState == ConnectionState.Open)
                        nestedResult = service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 1 });
                };
                var outerResult = asynchronousOpening
                    ? await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 })
                    : service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 });
                Assert.Equal(8, outerResult);
                Assert.Equal(2, nestedResult);
            }
        }

        private sealed class ComputedParameters
        {
            private readonly IDapperService _service;
            private readonly bool _asynchronous;
            public ComputedParameters(IDapperService service, bool asynchronous)
            {
                _service = service;
                _asynchronous = asynchronous;
            }
            public int Base => _asynchronous
                ? _service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 1 }).GetAwaiter().GetResult()
                : _service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 1 });
            public int Exponent => 3;
        }

        private static async Task VerifyStateChangeDisposal(bool asynchronousOpening)
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.StateChange += (sender, args) =>
                {
                    if (args.CurrentState == ConnectionState.Open) service.Dispose();
                };
                var result = asynchronousOpening
                    ? await service.ExecuteScalarFunctionAsync<int>("POWER", new { Base = 2, Exponent = 3 })
                    : service.ExecuteScalarFunction<int>("POWER", new { Base = 2, Exponent = 3 });
                Assert.Equal(8, result);
                Assert.Equal(ConnectionState.Open, connection.State);
            }
        }
    }
}
