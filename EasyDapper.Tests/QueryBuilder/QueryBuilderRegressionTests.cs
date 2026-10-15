using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderRegressionTests
    {
        private static global::EasyDapper.QueryBuilder<T> Create<T>()
            => new global::EasyDapper.QueryBuilder<T>(new StubConnection());

        private static IDictionary<string, object> Parameters<T>(IQueryBuilder<T> query)
            => ((global::EasyDapper.QueryBuilder<T>)query).GetParameterBuilder().GetParameters();

        private static object PredicateValue<T>(IQueryBuilder<T> query, string column, string op)
        {
            var match = Regex.Match(query.BuildQuery(), @"\.\[" + column + @"\]\s*" + Regex.Escape(op) + @"\s*(@p\d+)");
            Assert.True(match.Success, query.BuildQuery());
            return Parameters(query)[match.Groups[1].Value];
        }

        [Fact]
        public void Union_KeepsEachBranchesParameterValues()
        {
            var query = Create<Person>().Where(p => p.Age > 30)
                .Union(Create<Person>().Where(p => p.Age < 20));
            Assert.Equal(30, PredicateValue(query, "Age", ">"));
            Assert.Equal(20, PredicateValue(query, "Age", "<"));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void Apply_KeepsSubqueryAndCorrelationAndOuterParameters(bool whereFirst)
        {
            IQueryBuilder<Order> query = Create<Order>();
            if (whereFirst) query.Where(o => o.Id > 30);
            query.OuterApply<OrderItem>((o, item) => o.Id == item.OrderId && item.Id > 3,
                sub => sub.Where(item => item.Quantity > 5));
            if (!whereFirst) query.Where(o => o.Id > 30);
            Assert.Equal(5, PredicateValue(query, "Quantity", ">"));
            var values = Parameters(query).Values.OrderBy(v => (int)v).ToArray();
            Assert.Equal(new object[] { 3, 5, 30 }, values);
            var sqlParameters = Regex.Matches(query.BuildQuery(), @"@p\d+").Cast<Match>().Select(m => m.Value).Distinct().ToArray();
            Assert.Equal(3, sqlParameters.Length);
        }

        [Fact]
        public void ParameterRenaming_IsSimultaneous_AndSkipsLiteralsAndIdentifiers()
        {
            var replacements = new Dictionary<string, string> { { "@p1", "@p2" }, { "@p2", "@p3" } };
            var sql = global::EasyDapper.QueryBuilderCore<Person>.ReplaceParameters(
                "SELECT '@p1', [@p2], @p1, @p2, @p10", replacements);
            Assert.Equal("SELECT '@p1', [@p2], @p2, @p3, @p10", sql);
        }

        [Fact]
        public void CapturedNull_UsesNullPredicates()
        {
            string value = null;
            Assert.Contains("[CurrentFirstName] IS NULL", Create<Person>().Where(p => p.Name == value).BuildQuery());
            Assert.Contains("[CurrentFirstName] IS NOT NULL", Create<Person>().Where(p => p.Name != value).BuildQuery());
        }

        [Fact]
        public void NegatedAndComparedHasValue_UseUnderlyingColumn()
        {
            var negated = Create<Person>().Where(p => !p.CreatedAt.HasValue).BuildQuery();
            Assert.Contains("[CreatedAt] IS NULL", negated);
            Assert.DoesNotContain("[HasValue]", negated);
            Assert.Contains("[CreatedAt] IS NULL", Create<Person>().Where(p => p.CreatedAt.HasValue == false).BuildQuery());
            Assert.Contains("[CreatedAt] IS NOT NULL", Create<Person>().Where(p => p.CreatedAt.HasValue != false).BuildQuery());
        }

        [Fact]
        public void Contains_EmptyCollection_IsAlwaysFalse()
        {
            var ids = new List<int>();
            var sql = Create<Person>().Where(p => ids.Contains(p.Id)).BuildQuery();
            Assert.Contains("WHERE (1 = 0)", sql);
            Assert.DoesNotContain("IN ()", sql);
        }

        [Theory]
        [InlineData("%", "%[%]%")]
        [InlineData("_", "%[_]%")]
        [InlineData("[", "%[[]%")]
        [InlineData("[a]%_", "%[[]a][%][_]%")]
        public void Contains_EscapesSqlWildcards(string value, string expected)
        {
            var query = Create<Person>().Where(p => p.Name.Contains(value));
            Assert.Equal(expected, Assert.Single(Parameters(query)).Value);
        }

        [Fact]
        public void FluentOperationsAfterSet_ApplyToCombinedProjection()
        {
            var query = Create<Person>().Select(p => p.Name)
                .Union(Create<Person>().Select(p => p.Name))
                .Where(p => p.Name == "Alice").Top(1).OrderByAscending(p => p.Name);
            var sql = query.BuildQuery();
            Assert.StartsWith("SELECT TOP (1) Person_SQ1.* FROM (", sql);
            var outerSql = sql.Substring(sql.LastIndexOf(") AS [Person_SQ1]", StringComparison.Ordinal));
            Assert.Contains("WHERE Person_SQ1.[Name] =", outerSql);
            Assert.Contains("ORDER BY Person_SQ1.[Name] ASC", outerSql);
            Assert.DoesNotContain("CurrentFirstName", outerSql);
            Assert.DoesNotContain("Person_SQ1.[Id]", sql);
        }

        [Fact]
        public void DifferentSetOperations_PreserveCallOrder()
        {
            var sql = Create<Person>().Except(Create<Person>()).Union(Create<Person>()).BuildQuery();
            Assert.True(sql.IndexOf("EXCEPT", StringComparison.Ordinal) < sql.LastIndexOf("UNION", StringComparison.Ordinal));
        }

        [Theory]
        [InlineData("Name; DELETE FROM Person")]
        [InlineData("Name -- comment")]
        [InlineData("Name /* comment */")]
        [InlineData("LEN(Name)")]
        [InlineData("Name OFFSET 0 ROWS")]
        public void ColumnOrderBy_RejectsSqlFragments(string clause)
            => Assert.Throws<ArgumentException>(() => Create<Person>().OrderByColumns(clause));

        [Theory]
        [InlineData("Name ASC, Age DESC")]
        [InlineData("p.[CurrentFirstName] DESC, [Age]")]
        [InlineData("[p].[Name with spaces] ASC")]
        public void ColumnOrderBy_AcceptsColumnIdentifiers(string clause)
            => Assert.Contains("ORDER BY " + clause, Create<Person>().OrderByColumns(clause).BuildQuery());

        [Theory]
        [InlineData("INNER HASH JOIN")]
        [InlineData("INNER LOOP JOIN")]
        [InlineData("LEFT OUTER MERGE JOIN")]
        [InlineData("left /* legacy hint note */ join")]
        public void CustomJoin_PreservesLegacyTrustedFragments(string join)
            => Assert.Contains(join + " [dbo].[Customer]", Create<Order>().CustomJoin<Order, Customer>(join, (o, c) => o.CustomerId == c.Id).BuildQuery());

        [Fact]
        public void CrossJoin_DoesNotGenerateInvalidOnClause()
        {
            var sql = Create<Order>().CustomJoin<Order, Customer>("CROSS JOIN", (o, c) => o.CustomerId == c.Id).BuildQuery();
            Assert.Contains("CROSS JOIN", sql);
            Assert.DoesNotContain(" ON ", sql);
            Assert.Contains("WHERE", sql);
        }

        [Fact]
        public void Apply_ReservesAliasesWhenDifferentTableNamesSharePrefix()
        {
            var sql = Create<PrefixParent>().OuterApply<PrefixChild>((parent, child) => parent.Id == child.ParentId,
                sub => sub.Where(child => child.Amount > 5)).BuildQuery();
            Assert.Contains("LongPrefix_A1.[Id] = LongPrefix_A2.[ParentId]", sql);
        }

        [Fact]
        public void SelfApply_SubqueryHasDistinctAlias_AndLaterLambdaStillUsesMainSource()
        {
            var sql = Create<Person>().OuterApply<Person>((parent, child) => parent.Id == child.Id,
                sub => sub.Where(child => child.Age > 5)).Where(parent => parent.Age > 30).Select(parent => parent.Id).BuildQuery();
            Assert.Contains("Person_A1.[Id] = Person_A2.[Id]", sql);
            Assert.StartsWith("SELECT Person_A1.[Id] AS [Id]", sql);
            Assert.Matches(@"\) AS \[Person_SQ1\] \([^)]*\) WHERE Person_A1\.\[Age\] > @p\d+", sql);
        }

        [Fact]
        public void ReservedOutputNames_AreQuotedInProjectionsAndSetPredicates()
        {
            Assert.Contains("[Order] AS [Order]", Create<ReservedOutput>().Select(row => row.Order).BuildQuery());
            var sql = Create<ReservedOutput>().Union(Create<ReservedOutput>()).Where(row => row.Order == 3).BuildQuery();
            Assert.Matches(@"WHERE \w+_SQ\d+\.\[Order\] = @p\d+", sql);
        }

        [Fact]
        public void SetOperations_MapRenamedProjectionToOriginalMember()
        {
            var query = Create<Person>().Select(person => new { Renamed = person.Name })
                .Union(Create<Person>().Select(person => new { Renamed = person.Name }))
                .Where(person => person.Name == "Alice").OrderByAscending(person => person.Name);
            Assert.Contains("WHERE Person_SQ1.[Renamed] =", query.BuildQuery());
            Assert.Contains("ORDER BY Person_SQ1.[Renamed] ASC", query.BuildQuery());
        }

        [Fact]
        public void SetOperations_NameImplicitAggregates_AndRejectMissingOutputMembers()
        {
            var sql = Create<Person>().Sum(person => person.Age).Union(Create<Person>().Sum(person => person.Age)).BuildQuery();
            Assert.Contains("SUM(Person_A1.[Age])", sql);
            Assert.DoesNotContain("AS [SUM_Age]", sql);
            var wrapped = Create<Person>().Sum(person => person.Age).Union(Create<Person>().Sum(person => person.Age)).Top(1).BuildQuery();
            Assert.Contains("AS [Person_SQ1] ([SUM_Age])", wrapped);
            var partial = Create<Person>().Select(person => person.Name).Union(Create<Person>().Select(person => person.Name));
            Assert.Throws<InvalidOperationException>(() => partial.Where(person => person.Age > 18));
        }

        [Fact]
        public async Task PredicateParsingAndSetTransition_AreAtomic()
        {
            var query = Create<Person>();
            using (var entered = new ManualResetEventSlim())
            using (var resume = new ManualResetEventSlim())
            {
                var value = new BlockingValue(entered, resume);
                var predicateTask = Task.Run(() => query.Where(person => person.Age == value.Value));
                Assert.True(entered.Wait(TimeSpan.FromSeconds(3)), "Predicate did not start.");
                var setTask = Task.Run(() => query.Union(Create<Person>()));
                try { Assert.False(setTask.Wait(TimeSpan.FromMilliseconds(50)), "Set transition raced with predicate parsing."); }
                finally { resume.Set(); }
                await Task.WhenAll(predicateTask, setTask);
                var sql = query.BuildQuery();
                Assert.Contains("WHERE Person_A1.[Age] =", sql);
                Assert.DoesNotContain(") AS [Person_SQ1]", sql);
            }
        }

        private sealed class BlockingValue
        {
            private readonly ManualResetEventSlim _entered;
            private readonly ManualResetEventSlim _resume;
            internal BlockingValue(ManualResetEventSlim entered, ManualResetEventSlim resume) { _entered = entered; _resume = resume; }
            public int Value { get { _entered.Set(); if (!_resume.Wait(TimeSpan.FromSeconds(5))) throw new TimeoutException(); return 20; } }
        }

        [global::EasyDapper.Attributes.Table("LongPrefixParent")]
        private sealed class PrefixParent { public int Id { get; set; } }
        [global::EasyDapper.Attributes.Table("LongPrefixChild")]
        private sealed class PrefixChild { public int ParentId { get; set; } public int Amount { get; set; } }
        private sealed class ReservedOutput { public int Order { get; set; } }

        [Fact]
        public async Task ExecuteAsync_OpensDbConnectionAsynchronously()
        {
            using (var connection = new AsyncOpenConnection())
            {
                var query = new global::EasyDapper.QueryBuilder<Person>(connection);
                await Assert.ThrowsAsync<NotSupportedException>(() => query.ExecuteAsync());
                Assert.Equal(1, connection.AsyncOpenCalls);
                Assert.Equal(0, connection.SyncOpenCalls);
            }
        }

        private sealed class AsyncOpenConnection : DbConnection
        {
            private ConnectionState _state;
            public int AsyncOpenCalls;
            public int SyncOpenCalls;
            public override string ConnectionString { get; set; }
            public override string Database => "Test";
            public override string DataSource => "Test";
            public override string ServerVersion => "1";
            public override ConnectionState State => _state;
            public override void ChangeDatabase(string databaseName) { }
            public override void Close() { _state = ConnectionState.Closed; }
            public override void Open() { SyncOpenCalls++; throw new InvalidOperationException("Synchronous open was used."); }
            public override async Task OpenAsync(CancellationToken cancellationToken)
            {
                AsyncOpenCalls++;
                await Task.Delay(1, cancellationToken);
                _state = ConnectionState.Open;
            }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
            protected override DbCommand CreateDbCommand() => throw new NotSupportedException("Stop after opening the connection.");
        }
    }
}
