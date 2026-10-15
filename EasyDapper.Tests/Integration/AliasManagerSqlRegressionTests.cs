using System;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class AliasManagerSqlRegressionTests
    {
        [SqlServerFact]
        public void DistinctClrRolesOnMainTable_UseDistinctAliasesAndRightProjection()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<MainRole>()
                    .InnerJoin<MainRole, OtherRole>((parent, child) => parent.Id == child.ParentId)
                    .Where(parent => parent.Id == 1)
                    .Select(parent => parent.Id)
                    .Select<OtherRole>(child => new { ChildId = child.Id });
                var rows = query.Execute<ProjectedRow>().OrderBy(row => row.ChildId).ToArray();
                Assert.Equal(new[] { 2, 3, 10 }, rows.Select(row => row.ChildId));
                Assert.All(rows, row => Assert.Equal(1, row.Id));
            }
        }

        [SqlServerFact]
        public void SameTableRoleProjectionBeforeJoin_UsesTheJoinedRole()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<MainRole>().Select(parent => parent.Id)
                    .Select<OtherRole>(child => new { ChildId = child.Id })
                    .InnerJoin<MainRole, OtherRole>((parent, child) => parent.Id == child.ParentId)
                    .Where(parent => parent.Id == 1).Execute<ProjectedRow>().OrderBy(row => row.ChildId).ToArray();
                Assert.Equal(new[] { 2, 3, 10 }, rows.Select(row => row.ChildId));
                Assert.All(rows, row => Assert.Equal(1, row.Id));
            }
        }

        [SqlServerFact]
        public void AlternateClrViewWithoutJoin_StillUsesTheMainTable()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<MainRole>().Where(parent => parent.Id == 1)
                    .Select<OtherRole>(view => new { ChildId = view.Id }).Execute<ProjectedRow>().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.ChildId));
            }
        }

        [SqlServerFact]
        public void GeneratedNumericAlias_IsValidInEveryColumnReference()
        {
            using (var connection = OpenFixture())
            {
                var manager = new global::EasyDapper.AliasManager();
                var alias = manager.GenerateAlias("[dbo].[123MappedRows]");
                var query = new global::EasyDapper.QueryBuilder<MainRole>(connection)
                    .WithTableAlias<MainRole>(alias).Where(row => row.Id == 1).OrderByAscending(row => row.Id);
                Assert.Equal(new[] { 1 }, query.Execute().Select(row => row.Id));
            }
        }

        [SqlServerFact]
        public void InternalExplicitAliases_AreQuotedConsistentlyWithoutChangingTheirNames()
        {
            using (var connection = OpenFixture())
            {
                foreach (var alias in new[] { "order", "alias with space", "alias]bracket", "علیاس" })
                {
                    var query = new global::EasyDapper.QueryBuilder<MainRole>(connection)
                        .WithTableAlias<MainRole>(alias).Where(row => row.Id == 1).Select(row => row.Id);
                    Assert.Equal(new[] { 1 }, query.Execute<ProjectedRow>().Select(row => row.Id));
                }
            }
        }

        [SqlServerFact]
        public void JoiningMainTypeThroughAnotherLeftType_DoesNotReuseFromAlias()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<MainRole>()
                    .InnerJoin<MainRole, SideRole>((parent, side) => parent.Id == side.ParentId)
                    .InnerJoin<SideRole, MainRole>((side, otherParent) => side.ParentId == otherParent.Id)
                    .Execute().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.Id));
            }
        }

        [SqlServerFact]
        public void CaseOnlyMappedNames_InCorrelatedApply_DoNotShadowOuterAlias()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                // In a case-sensitive tempdb this is a distinct table with the same
                // fixture rows; in a case-insensitive tempdb both CLR types bind one table.
                if (connection.ExecuteScalar<int>("SELECT CASE WHEN OBJECT_ID('tempdb..#amrolerows') IS NULL THEN 1 ELSE 0 END") == 1)
                    connection.Execute(@"CREATE TABLE #amrolerows (Id INT PRIMARY KEY, ParentId INT NULL, Value INT);
                        INSERT #amrolerows VALUES (1,NULL,1),(2,1,10),(3,1,20),(10,1,100);");
                var rows = service.Query<MainRole>().Where(parent => parent.Id == 1)
                    .OuterApply<CaseRole>((parent, child) => parent.Id == child.ParentId,
                        sub => sub.Where(child => child.Id < 10))
                    .Select(parent => parent.Id).Select<CaseRole>(child => child.Value)
                    .Execute<ProjectedRow>().OrderBy(row => row.Value).ToArray();
                Assert.Equal(new int?[] { 10, 20 }, rows.Select(row => row.Value));
                Assert.All(rows, row => Assert.Equal(1, row.Id));
            }
        }

        [SqlServerFact]
        public void RepeatedSameClrJoin_KeepsFirstRoleProjectionAndDistinctJoinAliases()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<SideRole>()
                    .InnerJoin<SideRole, MainRole>((side, parent) => side.ParentId == parent.Id)
                    .InnerJoin<SideRole, MainRole>((side, secondParent) => side.Id == secondParent.Id)
                    .Select<MainRole>(parent => parent.Value).Execute<ProjectedRow>().ToArray();
                Assert.Equal(new int?[] { 1 }, rows.Select(row => row.Value));
            }
        }

        [SqlServerFact]
        public void SameClrViewSelfJoin_PreservesTheExistingPrimaryViewProjection()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<MainRole>().Where(parent => parent.Id == 1)
                    .InnerJoin<OtherRole, OtherRole>((primaryView, child) => primaryView.Id == child.ParentId)
                    .Select<OtherRole>(primaryView => new { ChildId = primaryView.Id })
                    .Execute<ProjectedRow>().ToArray();
                Assert.Equal(new[] { 1, 1, 1 }, rows.Select(row => row.ChildId));
            }
        }

        [SqlServerFact]
        public void FailedJoin_DoesNotRebindAPreviouslyUsableProjection()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<MainRole>().Where(parent => parent.Id == 1)
                    .Select<OtherRole>(view => new { ChildId = view.Id });
                Assert.Throws<NotSupportedException>(() => query.InnerJoin<MainRole, OtherRole>(
                    (parent, child) => parent.Id.ToString() == child.Id.ToString()));
                Assert.Equal(new[] { 1 }, query.Execute<ProjectedRow>().Select(row => row.ChildId));
            }
        }

        [SqlServerFact]
        public void Apply_QualifiedLegacyOrderByKeepsDistinctTableAliasNumbering()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<SideRole>()
                    .OuterApply<MainRole>((side, parent) => side.ParentId == parent.Id,
                        sub => sub.Top(1).OrderBy("#AMRoleRow_A1.[Id] DESC"))
                    .Select(side => side.Id).Select<MainRole>(parent => parent.Value)
                    .Execute<ProjectedRow>().ToArray();
                Assert.Equal(new[] { 10 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 1 }, rows.Select(row => row.Value));
            }
        }

        [SqlServerFact]
        public void NestedCorrelatedApply_ReservesEveryAncestorAlias()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<MainRole>().Where(parent => parent.Id == 1)
                    .OuterApply<SideRole>((parent, side) => parent.Id == side.ParentId,
                        sub => sub.OuterApply<MainRole>((side, nestedParent) => side.ParentId == nestedParent.Id,
                            nested => nested.Where(parent => parent.Value == 1)).Where(side => side.Value > 0))
                    .Select(parent => parent.Id).Select<SideRole>(side => side.Value)
                    .Execute<ProjectedRow>().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 99 }, rows.Select(row => row.Value));
            }
        }

        private static SqlConnection OpenFixture()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            connection.Open();
            connection.Execute(@"CREATE TABLE #AMRoleRows (Id INT PRIMARY KEY, ParentId INT NULL, Value INT);
                INSERT #AMRoleRows VALUES (1,NULL,1),(2,1,10),(3,1,20),(10,1,100);
                CREATE TABLE #AMSides (Id INT PRIMARY KEY, ParentId INT, Value INT);
                INSERT #AMSides VALUES (10,1,99);");
            return connection;
        }

        [Table("#AMRoleRows")]
        public class MainRole { public int Id { get; set; } public int? ParentId { get; set; } public int Value { get; set; } }
        [Table("#AMRoleRows")]
        public class OtherRole { public int Id { get; set; } public int? ParentId { get; set; } public int Value { get; set; } }
        [Table("#amrolerows")]
        public class CaseRole { public int Id { get; set; } public int? ParentId { get; set; } public int Value { get; set; } }
        [Table("#AMSides")]
        public class SideRole { public int Id { get; set; } public int ParentId { get; set; } public int Value { get; set; } }
        public class ProjectedRow { public int Id { get; set; } public int ChildId { get; set; } public int? Value { get; set; } }
    }
}
