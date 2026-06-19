using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceStoredProceduresAdvancedTests
    {
        private global::EasyDapper.DapperService CreateService(SpyDbConnection spy = null)
        {
            spy = spy ?? new SpyDbConnection();
            spy.Open();
            return new global::EasyDapper.DapperService(spy);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void ExecuteStoredProcedure_InvalidName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteStoredProcedure<Person>(name));
            }
        }

        [Theory]
        [InlineData("usp; DROP TABLE")]
        [InlineData("usp'--")]
        [InlineData("usp EXEC")]
        [InlineData("usp name with space")]
        public void ExecuteStoredProcedure_MaliciousName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteStoredProcedure<Person>(name));
            }
        }

        [Theory]
        [InlineData("usp_GetUser")]
        [InlineData("dbo.usp_GetUser")]
        [InlineData("schema.procedure_name")]
        [InlineData("Proc123")]
        public void ExecuteStoredProcedure_ValidName_DoesNotThrowValidation(string name)
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var ex = Record.Exception(() => svc.ExecuteStoredProcedure<Person>(name));
                Assert.Null(ex as ArgumentException);
            }
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteScalarFunction_InvalidName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteScalarFunction<int>(name));
            }
        }

        [Theory]
        [InlineData("fn; DROP TABLE")]
        [InlineData("fn'--")]
        public void ExecuteScalarFunction_MaliciousName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteScalarFunction<int>(name));
            }
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteTableFunction_InvalidName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteTableFunction<Person>(name, new { }));
            }
        }

        [Theory]
        [InlineData("fn; DROP TABLE")]
        [InlineData("fn'--")]
        public void ExecuteTableFunction_MaliciousName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteTableFunction<Person>(name, new { }));
            }
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ExecuteMultiResultStoredProcedure_InvalidName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteMultiResultStoredProcedure<Person>(
                    name, gr => null, new { }));
            }
        }

        [Theory]
        [InlineData("usp; DROP")]
        [InlineData("usp'--")]
        public void ExecuteMultiResultStoredProcedure_MaliciousName_Throws(string name)
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.ExecuteMultiResultStoredProcedure<Person>(
                    name, gr => null, new { }));
            }
        }

        [Fact]
        public void ExecuteStoredProcedure_WithParameters_ExecutesCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextDataReader = new SpyDbConnection.EmptyDataReader();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.ExecuteStoredProcedure<Person>("usp_GetUser", new { UserId = 123 });

                Assert.Equal(1, spy.ExecutedCommands.Count);
                Assert.Equal("usp_GetUser", spy.LastCommandText);
            }
        }

        [Fact]
        public void ExecuteStoredProcedure_WithoutParameters_ExecutesCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextDataReader = new SpyDbConnection.EmptyDataReader();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.ExecuteStoredProcedure<Person>("usp_GetAllUsers");

                Assert.Equal(1, spy.ExecutedCommands.Count);
                Assert.Equal("usp_GetAllUsers", spy.LastCommandText);
            }
        }

        [Fact]
        public void ExecuteScalarFunction_WithParameters_ExecutesCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 42;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var result = svc.ExecuteScalarFunction<int>("fn_CalculateSum", new { A = 5, B = 10 });

                Assert.Equal(42, result);
                Assert.Equal(1, spy.ExecutedCommands.Count);
                Assert.Contains("fn_CalculateSum", spy.LastCommandText);
            }
        }

        [Fact]
        public void ExecuteTableFunction_WithParameters_ExecutesCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextDataReader = new SpyDbConnection.EmptyDataReader();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.ExecuteTableFunction<Person>("fn_GetActiveUsers", new { MinAge = 18 });

                Assert.Equal(1, spy.ExecutedCommands.Count);
                Assert.Contains("fn_GetActiveUsers", spy.LastCommandText);
            }
        }

        [Fact]
        public void ExecuteStoredProcedure_WithinTransaction_ParticipatesInTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextDataReader = new SpyDbConnection.EmptyDataReader();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.ExecuteStoredProcedure<Person>("usp_GetUser", new { UserId = 1 });

                Assert.NotNull(spy.LastCommand.Transaction);
            }
        }

        [Fact]
        public void ExecuteScalarFunction_WithinTransaction_ParticipatesInTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.ExecuteScalarFunction<int>("fn_Test", new { });

                Assert.NotNull(spy.LastCommand.Transaction);
            }
        }

        [Fact]
        public void ExecuteTableFunction_WithinTransaction_ParticipatesInTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextDataReader = new SpyDbConnection.EmptyDataReader();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.ExecuteTableFunction<Person>("fn_Test", new { });

                Assert.NotNull(spy.LastCommand.Transaction);
            }
        }

        [Fact]
        public void ExecuteStoredProcedure_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.ExecuteStoredProcedure<Person>("usp_Test"));
        }

        [Fact]
        public void ExecuteScalarFunction_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.ExecuteScalarFunction<int>("fn_Test"));
        }

        [Fact]
        public void ExecuteTableFunction_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.ExecuteTableFunction<Person>("fn_Test", new { }));
        }

        [Fact]
        public void ExecuteMultiResultStoredProcedure_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.ExecuteMultiResultStoredProcedure<Person>(
                "usp_Test", gr => null, new { }));
        }

        [Theory]
        [InlineData("usp_GetUser", true)]
        [InlineData("dbo.usp_GetUser", true)]
        [InlineData("a", true)]
        [InlineData("usp; DROP", false)]
        [InlineData("usp'", false)]
        public void StoredProcedure_Validation_AllCases(string name, bool shouldPass)
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                if (shouldPass)
                {
                    var ex = Record.Exception(() => svc.ExecuteStoredProcedure<Person>(name));
                    Assert.Null(ex as ArgumentException);
                }
                else
                {
                    Assert.Throws<ArgumentException>(() => svc.ExecuteStoredProcedure<Person>(name));
                }
            }
        }

        [Theory]
        [InlineData("fn_Sum", true)]
        [InlineData("dbo.fn_Sum", true)]
        [InlineData("a", true)]
        [InlineData("fn; DROP", false)]
        [InlineData("fn'", false)]
        public void ScalarFunction_Validation_AllCases(string name, bool shouldPass)
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                if (shouldPass)
                {
                    var ex = Record.Exception(() => svc.ExecuteScalarFunction<int>(name));
                    Assert.Null(ex as ArgumentException);
                }
                else
                {
                    Assert.Throws<ArgumentException>(() => svc.ExecuteScalarFunction<int>(name));
                }
            }
        }

        [Theory]
        [InlineData("fn_GetUsers", true)]
        [InlineData("dbo.fn_GetUsers", true)]
        [InlineData("a", true)]
        [InlineData("fn; DROP", false)]
        public void TableFunction_Validation_AllCases(string name, bool shouldPass)
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                if (shouldPass)
                {
                    var ex = Record.Exception(() => svc.ExecuteTableFunction<Person>(name, new { }));
                    Assert.Null(ex as ArgumentException);
                }
                else
                {
                    Assert.Throws<ArgumentException>(() => svc.ExecuteTableFunction<Person>(name, new { }));
                }
            }
        }
    }
}
