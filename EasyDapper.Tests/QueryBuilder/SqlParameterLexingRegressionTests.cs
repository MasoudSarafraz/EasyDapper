using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using EasyDapper.Tests.Integration;
using Xunit;
using Xunit.Abstractions;

namespace EasyDapper.Tests.QueryBuilder
{
    public class SqlParameterLexingRegressionTests
    {
        private readonly ITestOutputHelper _output;
        public SqlParameterLexingRegressionTests(ITestOutputHelper output) { _output = output; }
        [Theory]
        [InlineData("SELECT /* ' */ @p1, 'literal', @p2", "SELECT /* ' */ @p2, 'literal', @p3")]
        [InlineData("SELECT -- ' @p1\n @p1, 'literal', @p2", "SELECT -- ' @p1\n @p2, 'literal', @p3")]
        [InlineData("SELECT /* outer /* nested */ ' @p1 */ @p1, 'literal', @p2", "SELECT /* outer /* nested */ ' @p1 */ @p2, 'literal', @p3")]
        [InlineData("SELECT @p1 /* @p2 */, @p2 -- @p1", "SELECT @p2 /* @p2 */, @p3 -- @p1")]
        [InlineData("SELECT '/* @p1 */ -- ''x''', @p1, '@p2'", "SELECT '/* @p1 */ -- ''x''', @p2, '@p2'")]
        [InlineData("SELECT [@p1]]--/*], @p1, \"@p2\"\"/*--\"", "SELECT [@p1]]--/*], @p2, \"@p2\"\"/*--\"")]
        [InlineData("SELECT @@p1, @p10, name@p1, @p1 /* keep */", "SELECT @@p1, @p10, name@p1, @p2 /* keep */")]
        [InlineData("SELECT @p1 -- ' @p2\r\n, @p2, 'x'", "SELECT @p2 -- ' @p2\r\n, @p3, 'x'")]
        public void ParameterImport_PreservesCommentsAndRewritesOnlySqlParameters(string sql, string expected)
        {
            var replacements = new Dictionary<string, string> { { "@p1", "@p2" }, { "@p2", "@p3" } };
            Assert.Equal(expected, global::EasyDapper.QueryBuilderCore<LexerRow>.ReplaceParameters(sql, replacements));
        }

        [Fact]
        public void ConcurrentImports_KeepEachCallMappingAndProtectedTextIndependent()
        {
            const string sql = "SELECT /* ' /* @p1 */ tail */ @p1, '@p2', @p2 -- @p1";
            Parallel.For(0, 512, new ParallelOptions { MaxDegreeOfParallelism = 8 }, index =>
            {
                string first = "@q" + (index * 2);
                string second = "@q" + (index * 2 + 1);
                var replacements = new Dictionary<string, string> { { "@p1", first }, { "@p2", second } };
                string expected = "SELECT /* ' /* @p1 */ tail */ " + first + ", '@p2', " + second + " -- @p1";
                Assert.Equal(expected, global::EasyDapper.QueryBuilderCore<LexerRow>.ReplaceParameters(sql, replacements));
            });
        }

        [SqlServerFact]
        public void NestedUnion_WithAQuoteInsideComment_PreservesAllBranchValues()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var child = BuildCommentedSet(service);
                var query = service.Query<LexerRow>().Where(row => row.Id == 1).UnionAll(child)
                    .Where(row => row.Id >= 1);
                _output.WriteLine(query.BuildQuery());
                var actual = query.Execute().Select(row => row.Id).OrderBy(id => id).ToArray();
                Assert.Equal(new[] { 1, 2, 3 }, actual);
            }
        }

        [SqlServerFact]
        public async Task ApplyOfACommentedSet_PreservesImportedParametersAndChildReuse()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var child = BuildCommentedSet(service);
                var childSql = child.BuildQuery();
                var query = service.Query<LexerParent>().Where(parent => parent.Id == 1)
                    .Select(parent => new { ParentId = parent.Id })
                    .OuterApply<LexerRow>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<LexerRow>(item => new { ChildId = item.Id });
                _output.WriteLine(query.BuildQuery());
                var actual = (await query.ExecuteAsync<JoinedRow>()).OrderBy(row => row.ChildId).ToArray();
                Assert.Equal(new[] { 2, 3 }, actual.Select(row => row.ChildId));
                Assert.All(actual, row => Assert.Equal(1, row.ParentId));
                Assert.Equal(childSql, child.BuildQuery());
                Assert.Equal(new[] { 2, 3 }, child.Execute().Select(row => row.Id).OrderBy(id => id));
            }
        }

        [SqlServerTheory]
        [InlineData("UNION", false)]
        [InlineData("UNION ALL", false)]
        [InlineData("INTERSECT", false)]
        [InlineData("EXCEPT", false)]
        [InlineData("UNION", true)]
        [InlineData("UNION ALL", true)]
        [InlineData("INTERSECT", true)]
        [InlineData("EXCEPT", true)]
        public void OrderedSetOperands_ExecuteStandalone_WithAndWithoutTop(string operation, bool top)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var left = service.Query<LexerRow>().Where(row => row.Id <= 2).OrderByDescending(row => row.Id);
                var right = service.Query<LexerRow>().Where(row => row.Id >= 2).OrderByAscending(row => row.Id);
                if (top) { left.Top(1); right.Top(1); }
                switch (operation)
                {
                    case "UNION": left.Union(right); break;
                    case "UNION ALL": left.UnionAll(right); break;
                    case "INTERSECT": left.Intersect(right); break;
                    case "EXCEPT": left.Except(right); break;
                }
                var projection = top ? "TOP (1) " : "";
                var offset = top ? "" : " OFFSET 0 ROWS";
                var direct = connection.Query<int>("SELECT Id FROM (SELECT " + projection
                    + "Id FROM #EDReviewParameterLexing WHERE Id <= 2 ORDER BY Id DESC" + offset + ") AS L "
                    + operation + " SELECT Id FROM (SELECT " + projection
                    + "Id FROM #EDReviewParameterLexing WHERE Id >= 2 ORDER BY Id ASC" + offset + ") AS R")
                    .OrderBy(id => id).ToArray();
                _output.WriteLine(left.BuildQuery());
                Assert.Equal(direct, left.Execute().Select(row => row.Id).OrderBy(id => id));
            }
        }

        private static IQueryBuilder<LexerRow> BuildCommentedSet(IDapperService service)
            => service.Query<LexerRow>().Where(row => row.Id == 2).OrderBy("Id /* ' */")
                .UnionAll(service.Query<LexerRow>().Where(row => row.Id == 3)
                    .OrderBy("CASE WHEN 'x' = 'x' THEN Id ELSE Id END"));

        [SqlServerTheory]
        [InlineData("Id -- trailing", false)]
        [InlineData("Id -- '", false)]
        [InlineData("Id -- trailing", true)]
        [InlineData("Id -- '", true)]
        public void TrailingLineComment_DoesNotHideEmbeddingSyntax(string order, bool apply)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                if (apply)
                {
                    var query = service.Query<LexerParent>().Where(parent => parent.Id == 1)
                        .Select(parent => new { ParentId = parent.Id })
                        .OuterApply<LexerRow>((parent, item) => parent.Id == item.ParentId,
                            child => child.Where(item => item.Id >= 2).Top(1).OrderBy(order))
                        .Select<LexerRow>(item => new { ChildId = item.Id });
                    Assert.Equal(2, Assert.Single(query.Execute<JoinedRow>()).ChildId);
                }
                else
                {
                    var query = service.Query<LexerRow>().Where(row => row.Id == 2).OrderBy(order)
                        .UnionAll(service.Query<LexerRow>().Where(row => row.Id == 3));
                    Assert.Equal(new[] { 2, 3 }, query.Execute().Select(row => row.Id).OrderBy(id => id));
                }
            }
        }

        private static SqlConnection OpenFixture()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            try
            {
                connection.Open();
                connection.Execute(@"CREATE TABLE #EDReviewParameterLexing(Id INT PRIMARY KEY, ParentId INT);
                    INSERT #EDReviewParameterLexing VALUES (1,0),(2,1),(3,1);");
                return connection;
            }
            catch { connection.Dispose(); throw; }
        }

        [Table("#EDReviewParameterLexing")]
        public class LexerRow { public int Id { get; set; } public int ParentId { get; set; } }

        [Table("#EDReviewParameterLexing")]
        public class LexerParent { public int Id { get; set; } }

        public class JoinedRow { public int ParentId { get; set; } public int ChildId { get; set; } }
    }

    public sealed class SqlServerTheoryAttribute : TheoryAttribute
    {
        public SqlServerTheoryAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
                Skip = "Set EASYDAPPER_SQL_TEST_CONNECTION to run SQL Server integration tests.";
        }
    }
}
