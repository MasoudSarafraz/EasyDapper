using System;
using Xunit;

namespace EasyDapper.Tests.StoredProcedure
{
    public class StoredProcedureExecutorValidationTests
    {
        private global::EasyDapper.StoredProcedureExecutor CreateExecutor()
            => new global::EasyDapper.StoredProcedureExecutor(
                new global::EasyDapper.ConnectionManager(new SpyDbConnection()),
                new global::EasyDapper.SqlBuilder(new global::EasyDapper.QueryCache()));

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void ExecuteStoredProcedure_NullOrEmptyName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteStoredProcedure<Person>(name));
        }

        [Theory]
        [InlineData("usp; DROP TABLE")]
        [InlineData("usp'--")]
        [InlineData("usp EXEC")]
        [InlineData("usp/*comment*/")]
        [InlineData("usp name with space")]
        public void ExecuteStoredProcedure_InvalidName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteStoredProcedure<Person>(name));
        }

        [Theory]
        [InlineData("usp_GetUser")]
        [InlineData("dbo.usp_GetUser")]
        [InlineData("schema_name.procedure_name")]
        [InlineData("Proc123")]
        public void ExecuteStoredProcedure_ValidName_DoesNotThrowArgumentException(string name)
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var exec = new global::EasyDapper.StoredProcedureExecutor(
                new global::EasyDapper.ConnectionManager(spy),
                new global::EasyDapper.SqlBuilder(new global::EasyDapper.QueryCache()));
            var ex = Record.Exception(() => exec.ExecuteStoredProcedure<Person>(name));
            Assert.False(ex is ArgumentException, $"Expected no ArgumentException but got: {ex?.GetType().Name}");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteScalarFunction_NullOrEmptyName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteScalarFunction<int>(name));
        }

        [Theory]
        [InlineData("fn; DROP TABLE")]
        [InlineData("fn'--")]
        public void ExecuteScalarFunction_InvalidName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteScalarFunction<int>(name));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteTableFunction_NullOrEmptyName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteTableFunction<Person>(name, new { }));
        }

        [Theory]
        [InlineData("fn; DROP TABLE")]
        [InlineData("fn'--")]
        public void ExecuteTableFunction_InvalidName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteTableFunction<Person>(name, new { }));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteMultiResultStoredProcedure_NullOrEmptyName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteMultiResultStoredProcedure<Person>(
                name, gr => null, new { }));
        }

        [Theory]
        [InlineData("usp; DROP")]
        [InlineData("usp'--")]
        public void ExecuteMultiResultStoredProcedure_InvalidName_Throws(string name)
        {
            var exec = CreateExecutor();
            Assert.Throws<ArgumentException>(() => exec.ExecuteMultiResultStoredProcedure<Person>(
                name, gr => null, new { }));
        }
    }
}
