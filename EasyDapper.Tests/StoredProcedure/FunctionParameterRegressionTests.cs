using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using Xunit;

namespace EasyDapper.Tests.StoredProcedure
{
    public class FunctionParameterRegressionTests
    {
        [Fact]
        public void DynamicParameterTemplate_MaterializesWithoutExecutingOrOpeningConnection()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters(new { Base = 2, Exponent = 3 });
            using (var cache = new global::EasyDapper.QueryCache())
            {
                var builder = new global::EasyDapper.SqlBuilder(cache);
                Assert.Same(parameters, builder.NormalizeFunctionParameters(parameters, connection));
                Assert.Contains("Base", parameters.ParameterNames);
                Assert.Contains("Exponent", parameters.ParameterNames);
                Assert.Equal(ConnectionState.Closed, connection.State);
                Assert.False(connection.WasOpened);
                Assert.Empty(connection.ExecutedCommands);
            }
        }

        [Fact]
        public void ScalarFunction_DynamicParameterTemplate_BindsAllArguments()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters(new { Base = 2, Exponent = 3 });
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);
                Assert.Equal("SELECT POWER(@Base, @Exponent)", connection.LastCommandText);
                Assert.Equal(2, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
                Assert.Single(connection.ExecutedCommands);
            }
        }

        [Fact]
        public void ScalarFunction_MixedTemplateAndExplicitParameter_PreservesValuesAndMetadata()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters(new { Base = 2 });
            parameters.Add("Exponent", 3, DbType.Int32, ParameterDirection.Input, size: 7);
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);
                Assert.Equal("SELECT POWER(@Base, @Exponent)", connection.LastCommandText);
                Assert.Equal(2, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
                var exponent = (IDbDataParameter)connection.LastCommand.Parameters["Exponent"];
                Assert.Equal(DbType.Int32, exponent.DbType);
                Assert.Equal(7, exponent.Size);
                Assert.Single(connection.ExecutedCommands);
            }
        }

        [Fact]
        public void ScalarFunction_AnonymousTemplate_PreservesDeclaredArgumentOrder()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters(new { B = 3, A = 2 });
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);
                Assert.Equal("SELECT POWER(@B, @A)", connection.LastCommandText);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["B"]);
                Assert.Equal(2, connection.LastCommand.GetParameterSnapshot()["A"]);
            }
        }

        [Fact]
        public void ScalarFunction_TemplateWithExplicitOverride_UsesOverrideInOriginalArgumentSlot()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters(new { Base = 2, Exponent = 3 });
            parameters.Add("Base", 4);
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);
                Assert.Equal("SELECT POWER(@Base, @Exponent)", connection.LastCommandText);
                Assert.Equal(4, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
            }
        }

        [Fact]
        public void ScalarFunction_DynamicParameters_UsesActualNamesAndValues()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters();
            parameters.Add("@Base", 10);
            parameters.Add("Exponent", 3);
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);

                Assert.Equal("SELECT POWER(@Base, @Exponent)", connection.LastCommandText);
                Assert.Equal(10, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
            }
        }

        [Fact]
        public void TableFunction_DynamicParameters_UsesActualNames()
        {
            var connection = new SpyDbConnection();
            var parameters = new DynamicParameters();
            parameters.Add("MinAge", 18);
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteTableFunction<Person>("dbo.UsersByAge", parameters);

                Assert.Equal("SELECT * FROM dbo.UsersByAge(@MinAge)", connection.LastCommandText);
                Assert.Equal(18, connection.LastCommand.GetParameterSnapshot()["MinAge"]);
            }
        }

        [Fact]
        public void ScalarFunction_DictionaryWithTypedValues_BindsEntriesInsteadOfContainerProperties()
        {
            var connection = new SpyDbConnection();
            var parameters = new Dictionary<string, int> { { "Base", 10 }, { "Exponent", 3 } };
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);

                Assert.Contains("@Base", connection.LastCommandText);
                Assert.Contains("@Exponent", connection.LastCommandText);
                Assert.DoesNotContain("@Count", connection.LastCommandText);
                Assert.Equal(10, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
            }
        }

        [Fact]
        public void ScalarFunction_NonGenericDictionary_BindsItsNamedValues()
        {
            var connection = new SpyDbConnection();
            var parameters = new Hashtable { { "Base", 10 }, { "Exponent", 3 } };
            using (var service = DapperServiceFactory.Create(connection))
            {
                service.ExecuteScalarFunction<int>("POWER", parameters);
                Assert.Equal(2, connection.LastCommand.GetParameterSnapshot().Count);
                Assert.Equal(10, connection.LastCommand.GetParameterSnapshot()["Base"]);
                Assert.Equal(3, connection.LastCommand.GetParameterSnapshot()["Exponent"]);
            }
        }

        [Theory]
        [InlineData("A); SELECT 1--")]
        [InlineData("A\n")]
        [InlineData("")]
        public void FunctionParameter_UnsafeNames_AreRejectedBeforeAnyCommandRuns(string name)
        {
            var connection = new SpyDbConnection();
            var parameters = new Dictionary<string, object> { { name, 1 } };
            using (var service = DapperServiceFactory.Create(connection))
            {
                Assert.Throws<ArgumentException>(() => service.ExecuteScalarFunction<int>("POWER", parameters));
                Assert.Empty(connection.ExecutedCommands);
            }
        }

        [Fact]
        public void MultiResultProcedure_NullMapper_IsRejectedBeforeCommandRuns()
        {
            var connection = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(connection))
            {
                Assert.Throws<ArgumentNullException>(() => service.ExecuteMultiResultStoredProcedure<Person>("dbo.Users", null));
                Assert.Empty(connection.ExecutedCommands);
            }
        }

        [Fact]
        public async Task AsyncValidation_PreservesFaultedTaskContract()
        {
            var connection = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(connection))
            {
                Task scalar = null, table = null, procedure = null, multi = null;
                Assert.Null(Record.Exception(() => { scalar = service.ExecuteScalarFunctionAsync<int>("invalid name"); }));
                Assert.Null(Record.Exception(() => { table = service.ExecuteTableFunctionAsync<Person>("invalid name", null); }));
                Assert.Null(Record.Exception(() => { procedure = service.ExecuteStoredProcedureAsync<Person>("invalid name"); }));
                Assert.Null(Record.Exception(() => { multi = service.ExecuteMultiResultStoredProcedureAsync<Person>("dbo.Valid", null); }));
                await Assert.ThrowsAsync<ArgumentException>(() => scalar);
                await Assert.ThrowsAsync<ArgumentException>(() => table);
                await Assert.ThrowsAsync<ArgumentException>(() => procedure);
                await Assert.ThrowsAsync<ArgumentNullException>(() => multi);
                Assert.Empty(connection.ExecutedCommands);
            }
        }
    }
}
