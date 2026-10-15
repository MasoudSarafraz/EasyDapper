using System;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using EasyDapper.Attributes;
using EasyDapper.Tests.Integration;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class AliasLifecycleIntegrityTests
    {
        [SqlServerFact]
        public void RenameAfterTypedWhereSelectJoinOrApply_IsRejectedWithoutChangingTheQuery()
        {
            using (var connection = OpenFixture())
            {
                foreach (var alias in new[] { "root", "order" })
                foreach (var clause in new[] { "where", "select", "join", "apply" })
                {
                    var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                    query.WithTableAlias<MainRow>(alias);
                    if (clause == "where") query.Where(row => row.Id == 1);
                    if (clause == "select") query.Select(row => row.Id);
                    if (clause == "join") query.InnerJoin<MainRow, SideRow>((row, side) => row.Id == side.ParentId);
                    if (clause == "apply") query.OuterApply<SideRow>((row, side) => row.Id == side.ParentId,
                        sub => sub.Where(side => side.Id > 0));
                    var before = query.GetRawSql();

                    Assert.Throws<InvalidOperationException>(() => query.WithTableAlias<MainRow>("renamed"));
                    Assert.Equal(before, query.GetRawSql());
                    var expected = clause == "where" || clause == "join" ? new[] { 1 } : new[] { 1, 2, 3 };
                    Assert.Equal(expected, query.Execute<int>().OrderBy(id => id));
                }
            }
        }

        [SqlServerFact]
        public void EarlyAndIdempotentRenames_RemainUsable()
        {
            using (var connection = OpenFixture())
            {
                var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                query.WithTableAlias<MainRow>("first");
                query.WithTableAlias<MainRow>("second");
                query.Where(row => row.Id == 1);
                var before = query.GetRawSql();
                query.WithTableAlias<MainRow>("second");
                Assert.Equal(before, query.GetRawSql());
                Assert.Equal(new[] { 1 }, query.Execute<int>());
            }
        }

        [SqlServerFact]
        public void FrozenSetScope_DoesNotPreventRenamingAnInactiveTableAlias()
        {
            using (var connection = OpenFixture())
            {
                var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                query.WithTableAlias<MainRow>("frozen");
                query.Where(row => row.Id == 1).Union(new global::EasyDapper.QueryBuilder<MainRow>(connection)
                    .Where(row => row.Id == 2));
                var before = query.GetRawSql();
                query.WithTableAlias<MainRow>("future");
                Assert.Equal(before, query.GetRawSql());
                Assert.Equal(new[] { 1, 2 }, query.OrderByAscending(row => row.Id).Execute<int>());
            }
        }

        [SqlServerFact]
        public void IndependentApplyInnerScope_DoesNotBindTheParentsTableRegistry()
        {
            using (var connection = OpenFixture())
            {
                var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                query.OuterApply<SideRow>((row, side) => row.Id == side.ParentId,
                    sub => sub.Where(side => side.Id > 0));
                query.WithTableAlias<SideRow>("unusedParentScopeAlias");
                Assert.Equal(new[] { 1, 2, 3 }, query.Execute<int>().OrderBy(id => id));
            }
        }

        [SqlServerFact]
        public void AliasIndependentClausesAndFailedParsing_DoNotBindAnAlias()
        {
            using (var connection = OpenFixture())
            {
                var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                query.WithTableAlias<MainRow>("root");
                Assert.Throws<NotSupportedException>(() => query.Where(row => row.Id > 0 && Unsupported(row.Id)));
                query.Count().Top(1);
                query.WithTableAlias<MainRow>("renamed");
                Assert.Equal(new[] { 3 }, query.Execute<int>());

                var page = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                page.Paging(1, 2).OrderBy("Id ASC");
                page.WithTableAlias<MainRow>("page");
                Assert.Equal(new[] { 2 }, page.Execute<int>());

                var constant = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                constant.WithTableAlias<MainRow>("constant");
                var empty = new System.Collections.Generic.List<int>();
                constant.Where(row => empty.Contains(row.Id));
                constant.WithTableAlias<MainRow>("stillFree");
                Assert.Empty(constant.Execute<int>());
            }
        }

        [SqlServerFact]
        public void IdenticallyNamedColumnAndSqlLiteral_DoNotBecomeAliasReferences()
        {
            using (var connection = OpenFixture())
            {
                var query = new global::EasyDapper.QueryBuilder<MainRow>(connection);
                query.WithTableAlias<MainRow>("root");
                query.OrderBy("[root] ASC, CHARINDEX('root.', Name)");
                query.WithTableAlias<MainRow>("renamed");
                Assert.Equal(new[] { 3, 2, 1 }, query.Execute<int>());
            }
        }

        private static bool Unsupported(int value) => true;

        private static SqlConnection OpenFixture()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            connection.Execute(@"CREATE TABLE #AliasLifeRows(Id INT PRIMARY KEY, Name NVARCHAR(30), [root] INT);
                INSERT #AliasLifeRows VALUES(1,N'one',3),(2,N'two',2),(3,N'three',1);
                CREATE TABLE #AliasLifeSide(Id INT PRIMARY KEY, ParentId INT);
                INSERT #AliasLifeSide VALUES(11,1);");
            return connection;
        }

        [Table("#AliasLifeRows")]
        public class MainRow
        {
            public int Id { get; set; }
            public string Name { get; set; }
            [Column("root")] public int SortKey { get; set; }
        }
        [Table("#AliasLifeSide")]
        public class SideRow { public int Id { get; set; } public int ParentId { get; set; } }
    }
}
