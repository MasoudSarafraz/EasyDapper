using System.Linq;
using Dapper;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperSpyCompatibilityTests
    {
        [Fact]
        public void Dapper_Execute_Calls_Connection_CreateCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 42;
            var result = spy.Execute("INSERT INTO Foo (Bar) VALUES (@Bar)", new { Bar = "baz" });
            Assert.Equal(42, result);
            Assert.Equal(1, spy.ExecutedCommands.Count);
            Assert.Contains("INSERT INTO Foo", spy.LastCommandText);
        }

        [Fact]
        public void Dapper_ExecuteScalar_Calls_Connection_CreateCommand()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 99;
            var result = spy.ExecuteScalar<int>("SELECT COUNT(*) FROM Foo");
            Assert.Equal(99, result);
            Assert.Equal(1, spy.ExecutedCommands.Count);
        }

        [Fact]
        public void Dapper_QueryFirstOrDefault_WithEmptyReader_ReturnsDefault()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var result = spy.QueryFirstOrDefault<Person>("SELECT * FROM Person WHERE Id = @Id", new { Id = 1 });
            Assert.Null(result);
            Assert.Equal(1, spy.ExecutedCommands.Count);
        }
    }
}
