using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using EasyDapper.Tests.Integration;
using Xunit;
using Xunit.Abstractions;

namespace EasyDapper.Tests.QueryBuilder
{
    public class DeepCompositionRegressionTests
    {
        private readonly ITestOutputHelper _output;
        public DeepCompositionRegressionTests(ITestOutputHelper output) { _output = output; }

        [SqlServerTheory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task JoinDuringCapturedGetter_AllocatesDistinctAliasesAndPreservesSelectedRole(bool failingOuter, bool sameThread)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<Parent>().Select(parent => parent.Id)
                    .Select<Item>(item => new { ChildAmount = item.Amount });
                Task nested = null;
                var captured = new CallbackValue(() =>
                {
                    if (sameThread)
                    {
                        query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId);
                        return;
                    }
                    using (ExecutionContext.SuppressFlow())
                        nested = Task.Run(() => query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId));
                    if (!nested.Wait(TimeSpan.FromSeconds(3))) throw new TimeoutException("Nested JOIN was blocked.");
                });
                try
                {
                    if (failingOuter)
                    {
                        Assert.Throws<NotSupportedException>(() => query.InnerJoin<Parent, Item>((parent, item) =>
                            parent.Id == item.ParentId && item.Amount > captured.Value && Unsupported(item.Id)));
                        query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId && item.Amount > 8);
                    }
                    else query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId && item.Amount > captured.Value);

                    _output.WriteLine(query.BuildQuery());
                    var expected = connection.Query<Joined>(@"SELECT p.Id, first.Amount AS ChildAmount
                        FROM #EDDeepParents p JOIN #EDDeepItems first ON p.Id=first.ParentId
                        JOIN #EDDeepItems second ON p.Id=second.ParentId AND second.Amount>8")
                        .OrderBy(row => row.ChildAmount).ToArray();
                    var actual = query.Execute<Joined>().OrderBy(row => row.ChildAmount).ToArray();
                    Assert.Equal(expected.Select(row => row.Id), actual.Select(row => row.Id));
                    Assert.Equal(expected.Select(row => row.ChildAmount), actual.Select(row => row.ChildAmount));
                    Assert.Equal(1, captured.Reads);
                }
                finally { if (nested != null) await nested; }
            }
        }

        [SqlServerFact]
        public void ParallelJoinsDuringCapturedGetter_ReserveEveryRoleUntilCommit()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<Parent>().Select(parent => parent.Id);
                var captured = new CallbackValue(() => Parallel.For(0, 32, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                    unused => query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId && item.Amount > 8)));
                query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId && item.Amount > captured.Value);
                Assert.Equal(new[] { 1 }, query.Execute<int>());
                Assert.Equal(1, captured.Reads);
            }
        }

        [SqlServerFact]
        public void FailedJoinWithoutNestedWrites_ReleasesItsAliasForRetry()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<Parent>().Select(parent => parent.Id);
                var before = query.BuildQuery();
                Assert.Throws<NotSupportedException>(() => query.InnerJoin<Parent, Item>((parent, item) =>
                    parent.Id == item.ParentId && item.Amount > 0 && Unsupported(item.Id)));
                var aliases = ((global::EasyDapper.QueryBuilder<Parent>)query).GetAliasManager();
                var candidate = aliases.GetAliasForType(typeof(Item));
                Assert.Equal(before, query.BuildQuery());
                query.InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId);
                Assert.Contains("AS [" + candidate + "] ON", query.BuildQuery());
                Assert.Equal(new[] { 1, 1, 2, 3 }, query.Execute<int>().OrderBy(value => value));
            }
        }

        [SqlServerTheory]
        [InlineData(false)]
        [InlineData(true)]
        public void DuplicateProjectionNames_InSet_BindEachMemberToItsOwnColumn(bool nested)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Func<IQueryBuilder<Item>> branch = () => service.Query<Item>()
                    .Select(item => new { Value = item.Id }).Select(item => new { Value = item.Amount });
                var query = branch().Union(branch());
                if (nested) query = branch().Where(item => item.Id < 0).UnionAll(query);
                query.Where(item => item.Amount > 8).Select(item => new { Key = item.Id, Value = item.Amount });
                _output.WriteLine(query.BuildQuery());
                var actual = Assert.Single(query.Execute<Renamed>());
                Assert.Equal(12, actual.Key);
                Assert.Equal(9, actual.Value);
            }
        }

        [SqlServerTheory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        public void DuplicateProjectionNames_InApply_BindEachMemberToItsOwnColumn(int mode)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var child = service.Query<Item>().Select(item => new { Value = item.ParentId })
                    .Select(item => new { Value = item.Amount });
                if (mode != 0) child.Row_Number(item => item.ParentId, item => item.Id, "Value");
                if (mode == 2) child.Union(service.Query<Item>().Where(item => item.Id < 0)
                    .Select(item => new { Value = item.ParentId }).Select(item => new { Value = item.Amount })
                    .Row_Number(item => item.ParentId, item => item.Id, "Value"));
                var originalSql = child.BuildQuery();
                var query = service.Query<Parent>().Select(parent => parent.Id)
                    .OuterApply<Item>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<Item>(item => new { ChildAmount = item.Amount });
                var expected = connection.Query<Joined>(@"SELECT p.Id,i.Amount AS ChildAmount FROM #EDDeepParents p
                    LEFT JOIN #EDDeepItems i ON p.Id=i.ParentId").OrderBy(row => row.Id).ThenBy(row => row.ChildAmount).ToArray();
                _output.WriteLine(query.BuildQuery());
                var actual = query.Execute<Joined>().OrderBy(row => row.Id).ThenBy(row => row.ChildAmount).ToArray();
                Assert.Equal(expected.Select(row => row.Id), actual.Select(row => row.Id));
                Assert.Equal(expected.Select(row => row.ChildAmount), actual.Select(row => row.ChildAmount));
                Assert.Equal(originalSql, child.BuildQuery());
            }
        }

        [SqlServerTheory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void DuplicateAggregateAndSelectedNames_WithRowNumber_KeepOutputPositions(bool rowNumber, bool aggregateLast)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Func<IQueryBuilder<Item>> branch = () =>
                {
                    var builder = service.Query<Item>().GroupBy(item => item.ParentId);
                    if (!aggregateLast) builder.Sum(item => item.Amount, "Value");
                    builder.Select(item => new { Value = item.ParentId });
                    if (aggregateLast) builder.Sum(item => item.Amount, "Value");
                    if (rowNumber) builder.Row_Number(item => item.ParentId, item => item.ParentId, "Value");
                    return builder;
                };
                var query = branch().Union(branch()).Where(item => item.Amount > 10)
                    .Select(item => new { Key = item.ParentId, Value = item.Amount });
                var actual = Assert.Single(query.Execute<Renamed>());
                var expected = Assert.Single(connection.Query<Renamed>(@"SELECT ParentId AS [Key],SUM(Amount) AS Value
                    FROM #EDDeepItems GROUP BY ParentId HAVING SUM(Amount)>10"));
                Assert.Equal(expected.Key, actual.Key);
                Assert.Equal(expected.Value, actual.Value);
            }
        }

        [SqlServerTheory]
        [InlineData(false)]
        [InlineData(true)]
        public void RepeatedMemberAndCaseInsensitiveOutputCollision_UseLatestSelectedPosition(bool caseDifference)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Func<IQueryBuilder<Item>> branch = () =>
                {
                    var builder = service.Query<Item>().Select(item => new { First = item.Amount, Value = item.Id });
                    if (caseDifference) builder.Select(item => new { value = item.Amount });
                    else builder.Select(item => new { Value = item.Amount });
                    return builder;
                };
                var query = branch().UnionAll(branch()).Where(item => item.Amount > 8)
                    .Select(item => new { Key = item.Id, Value = item.Amount });
                var actual = query.Execute<Renamed>().ToArray();
                Assert.Equal(new[] { 12, 12 }, actual.Select(row => row.Key));
                Assert.Equal(new[] { 9, 9 }, actual.Select(row => row.Value));
            }
        }

        [SqlServerFact]
        public void JoinedSourceOutput_WithMatchingRootMemberAndRowNumberName_PreservesColumnPosition()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                Func<IQueryBuilder<Parent>> branch = () => service.Query<Parent>()
                    .InnerJoin<Parent, Item>((parent, item) => parent.Id == item.ParentId)
                    .Select<Item>(item => new { Id = item.Amount })
                    .Row_Number(parent => parent.Id, parent => parent.Id, "Id");
                var query = branch().Union(branch()).Where(parent => parent.Id > 8).Select(parent => parent.Id);
                Assert.Equal(new[] { 9 }, query.Execute<int>());
            }
        }

        public static IEnumerable<object[]> SetCases()
        {
            foreach (string operation in new[] { "UNION", "UNION ALL", "INTERSECT", "EXCEPT" })
            foreach (int mode in new[] { 0, 1, 2 })
            foreach (bool nested in new[] { false, true })
                yield return new object[] { operation, mode, nested };
        }

        [SqlServerTheory]
        [MemberData(nameof(SetCases))]
        public void RenamedOrderedSet_WithTopPagingAndNestedReuse_MatchesDirectSql(string operation, int mode, bool nested)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var left = service.Query<Item>().Where(item => item.Id <= 21)
                    .Select(item => new { Key = item.Id, Value = item.Amount }).OrderByDescending(item => item.Id);
                var right = service.Query<Item>().Where(item => item.Id >= 12)
                    .Select(item => new { Key = item.Id, Value = item.Amount }).OrderByAscending(item => item.Id);
                if (mode == 1) { left.Top(2); right.Top(2); }
                if (mode == 2) { left.Paging(2, 2); right.Paging(2); }
                switch (operation)
                {
                    case "UNION": left.Union(right); break;
                    case "UNION ALL": left.UnionAll(right); break;
                    case "INTERSECT": left.Intersect(right); break;
                    case "EXCEPT": left.Except(right); break;
                }
                var combined = left;
                var originalSql = left.BuildQuery();
                var top = mode == 1 ? "TOP (2) " : "";
                var leftPage = mode == 1 ? "" : mode == 2 ? " OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY" : " OFFSET 0 ROWS";
                var rightPage = mode == 1 ? "" : mode == 2 ? " OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY" : " OFFSET 0 ROWS";
                var control = "SELECT * FROM (SELECT " + top + "Id AS [Key],Amount AS Value FROM #EDDeepItems WHERE Id<=21 ORDER BY Id DESC"
                    + leftPage + ") L " + operation + " SELECT * FROM (SELECT " + top
                    + "Id AS [Key],Amount AS Value FROM #EDDeepItems WHERE Id>=12 ORDER BY Id ASC" + rightPage + ") R";
                if (nested)
                {
                    left = service.Query<Item>().Where(item => item.Id < 0)
                        .Select(item => new { Key = item.Id, Value = item.Amount }).UnionAll(left);
                    Assert.Equal(originalSql, combined.BuildQuery());
                }
                var expected = connection.Query<Renamed>("SELECT * FROM (" + control + ") result WHERE Value>=7").OrderBy(row => row.Key).ToArray();
                var actual = left.Where(item => item.Amount >= 7).OrderByAscending(item => item.Id).Execute<Renamed>().ToArray();
                Assert.Equal(expected.Select(row => row.Key), actual.Select(row => row.Key));
                Assert.Equal(expected.Select(row => row.Value), actual.Select(row => row.Value));
                if (nested)
                {
                    var directChild = connection.Query<Renamed>(control).OrderBy(row => row.Key).ToArray();
                    var reusedChild = combined.Execute<Renamed>().OrderBy(row => row.Key).ToArray();
                    Assert.Equal(directChild.Select(row => row.Key), reusedChild.Select(row => row.Key));
                    Assert.Equal(directChild.Select(row => row.Value), reusedChild.Select(row => row.Value));
                    Assert.Equal(originalSql, combined.BuildQuery());
                }
            }
        }

        [SqlServerTheory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public void OrderedApply_WithoutTop_PreservesCorrelationAndChildReuse(bool outer, bool paging)
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var child = service.Query<Item>().Where(item => item.Amount >= 7).OrderByDescending(item => item.Amount);
                if (paging) child.Paging(1);
                var originalSql = child.BuildQuery();
                var query = service.Query<Parent>().Select(parent => parent.Id);
                if (outer) query.OuterApply<Item>((parent, item) => parent.Id == item.ParentId, unused => child);
                else query.CrossApply<Item>((parent, item) => parent.Id == item.ParentId, unused => child);
                query.Select<Item>(item => new { ChildAmount = item.Amount });
                _output.WriteLine(query.BuildQuery());
                var expected = connection.Query<Joined>("SELECT p.Id, child.Amount AS ChildAmount FROM #EDDeepParents p "
                    + (outer ? "OUTER" : "CROSS") + @" APPLY (SELECT i.Amount FROM #EDDeepItems i
                        WHERE p.Id=i.ParentId AND i.Amount>=7 ORDER BY i.Amount DESC OFFSET 0 ROWS"
                    + (paging ? " FETCH NEXT 1 ROWS ONLY" : "") + ") child");
                Func<Joined, string> key = row => row.Id + ":" + row.ChildAmount;
                Assert.Equal(expected.Select(key).OrderBy(value => value), query.Execute<Joined>().Select(key).OrderBy(value => value));
                Assert.Equal(originalSql, child.BuildQuery());
                Assert.Equal(paging ? new[] { 9 } : new[] { 9, 7, 7 }, child.Execute().Select(item => item.Amount));
            }
        }

        private static bool Unsupported(int value) => value > 0;

        private static SqlConnection OpenFixture()
        {
            var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            try
            {
                connection.Open();
                connection.Execute(@"CREATE TABLE #EDDeepParents(Id INT PRIMARY KEY);
                    INSERT #EDDeepParents VALUES(1),(2),(3);
                    CREATE TABLE #EDDeepItems(Id INT PRIMARY KEY,ParentId INT,Amount INT);
                    INSERT #EDDeepItems VALUES(11,1,7),(12,1,9),(21,2,7),(31,3,5);");
                return connection;
            }
            catch { connection.Dispose(); throw; }
        }

        [Table("#EDDeepParents")]
        public class Parent { public int Id { get; set; } }
        [Table("#EDDeepItems")]
        public class Item { public int Id { get; set; } public int ParentId { get; set; } public int Amount { get; set; } }
        public class Joined { public int Id { get; set; } public int? ChildAmount { get; set; } }
        public class Renamed { public int Key { get; set; } public int Value { get; set; } }
        private sealed class CallbackValue
        {
            private readonly Action _callback;
            public CallbackValue(Action callback) { _callback = callback; }
            public int Reads { get; private set; }
            public int Value { get { Reads++; _callback(); return 8; } }
        }
    }
}
