using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using EasyDapper.Attributes;
using EasyDapper.Tests.Integration;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderCompatibilityTests
    {
        [Theory]
        [InlineData("LEN(Name), Id")]
        [InlineData("CASE WHEN Age >= 20 THEN 0 ELSE 1 END, Age DESC")]
        [InlineData("Name COLLATE Latin1_General_100_BIN2 ASC")]
        [InlineData("2 DESC")]
        [InlineData("Age % 10, Id")]
        [InlineData("(SELECT 1), Id")]
        [InlineData("COALESCE(Name,N''), Id")]
        [InlineData("CHARINDEX(';',Name), Id")]
        [InlineData("CHARINDEX('--',Name), Id")]
        [InlineData("  Name DESC /* caller-owned SQL */  ")]
        public void RawOrderBy_PreservesExistingTrustedSql(string clause)
        {
            var query = new global::EasyDapper.QueryBuilder<Person>(new StubConnection()).OrderBy(clause);
            Assert.EndsWith("ORDER BY " + clause, query.BuildQuery());
        }

        [SqlServerFact]
        public void LegacyOrderExpressionsAndJoinHints_StillExecute()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var clauses = new[] { "LEN(Name), Id", "CASE WHEN Age >= 20 THEN 0 ELSE 1 END, Age DESC",
                    "Name COLLATE Latin1_General_100_BIN2 ASC", "2 DESC", "Age % 10, Id", "(SELECT 1), Id",
                    "COALESCE(Name,N''), Id", "CHARINDEX(';',Name), Id", "CHARINDEX('--',Name), Id" };
                var results = new[] { new[] { 2, 3, 1 }, new[] { 3, 2, 1 }, new[] { 2, 1, 3 }, new[] { 2, 3, 1 },
                    new[] { 1, 2, 3 }, new[] { 1, 2, 3 }, new[] { 1, 3, 2 }, new[] { 1, 2, 3 }, new[] { 1, 2, 3 } };
                for (int i = 0; i < clauses.Length; i++)
                    Assert.Equal(results[i], service.Query<CompatibilityRow>().OrderBy(clauses[i]).Execute().Select(row => row.Id));
                foreach (var hint in new[] { "INNER HASH JOIN", "INNER LOOP JOIN", "LEFT OUTER MERGE JOIN" })
                {
                    var actual = service.Query<CompatibilityRow>().CustomJoin<CompatibilityRow, CompatibilityChild>(hint,
                        (row, child) => row.Id == child.ParentId).Execute().Select(row => row.Id).OrderBy(id => id);
                    Assert.Equal(hint.StartsWith("LEFT", StringComparison.Ordinal) ? new[] { 1, 2, 3 } : new[] { 1, 2 }, actual);
                }
            }
        }

        [SqlServerFact]
        public void UnnamedAggregatesAndDuplicateSetProjection_KeepLegacyResults()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var result = (IDictionary<string, object>)service.Query<CompatibilityRow>().Sum(row => row.Age).Execute<dynamic>().Cast<object>().Single();
                Assert.Equal(new[] { "" }, result.Keys);
                Assert.Equal(28, result[""]);
                Assert.Equal(28, service.Query<CompatibilityRow>().Sum(row => row.Age).Execute<int>().Single());
                var repeated = service.Query<CompatibilityRow>().Select(row => row.Id, row => row.Id)
                    .Union(service.Query<CompatibilityRow>().Select(row => row.Id, row => row.Id));
                Assert.Equal(new[] { 1, 2, 3 }, repeated.Execute<int>().OrderBy(id => id));
                var aggregateSet = service.Query<CompatibilityRow>().Sum(row => row.Age)
                    .Union(service.Query<CompatibilityRow>().Sum(row => row.Age));
                var setDynamic = (IDictionary<string, object>)aggregateSet.Execute<dynamic>().Cast<object>().Single();
                Assert.Equal(new[] { "" }, setDynamic.Keys);
                Assert.Equal(28, aggregateSet.Top(1).Execute<int>().Single());
            }
        }

        [SqlServerFact]
        public void LegacyStringOverloadNullSearchAndCapturedGetter_KeepTheirBehavior()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Assert.Equal(new[] { 1 }, service.Query<CompatibilityRow>()
                    .Where(row => row.Name.StartsWith("A", StringComparison.OrdinalIgnoreCase)).Execute().Select(row => row.Id));
                string value = null;
                Assert.Empty(service.Query<CompatibilityRow>().Where(row => row.Name.Contains(value)).Execute());
                var counter = new GetterCounter();
                Assert.Equal(new[] { 1 }, service.Query<CompatibilityRow>().Where(row => row.Age == counter.Value).Execute().Select(row => row.Id));
                Assert.Equal(1, counter.Calls);
            }
        }

        [SqlServerFact]
        public void NullableValueProjection_KeepsHistoricalOutputColumnName()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var first = service.Query<CompatibilityRow>().Select(row => new { RenamedTime = row.CreatedAt.Value });
                var projected = (IDictionary<string, object>)first.Execute<dynamic>().Cast<object>().First();
                Assert.Equal(new[] { "CreatedAt" }, projected.Keys);
                var combined = first.Union(service.Query<CompatibilityRow>().Select(row => new { RenamedTime = row.CreatedAt.Value }))
                    .Where(row => row.CreatedAt.HasValue);
                var setResult = (IDictionary<string, object>)combined.Execute<dynamic>().Cast<object>().Single();
                Assert.Equal(new[] { "CreatedAt" }, setResult.Keys);
            }
        }

        private static SqlConnection OpenFixture()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            connection.Execute(@"CREATE TABLE #CompatQueryRows (Id INT PRIMARY KEY, Name NVARCHAR(30) COLLATE Latin1_General_100_CI_AS, Age INT, CreatedAt DATETIME NULL);
                INSERT #CompatQueryRows VALUES (1,N'alpha',1,'20260101'),(2,N'Z',2,'20260101'),(3,N'b;--',25,'20260101');
                CREATE TABLE #CompatQueryChildren (Id INT PRIMARY KEY, ParentId INT);
                INSERT #CompatQueryChildren VALUES (10,1),(20,2);");
            return connection;
        }

        [Table("#CompatQueryRows")]
        public class CompatibilityRow { public int Id { get; set; } public string Name { get; set; } public int Age { get; set; } public DateTime? CreatedAt { get; set; } }
        [Table("#CompatQueryChildren")]
        public class CompatibilityChild { public int Id { get; set; } public int ParentId { get; set; } }
        private sealed class GetterCounter { public int Calls; public int Value { get { return ++Calls; } } }
    }
}
