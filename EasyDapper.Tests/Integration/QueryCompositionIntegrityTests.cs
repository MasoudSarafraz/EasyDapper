using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class QueryCompositionIntegrityTests
    {
        [SqlServerFact]
        public void JoinApplyWithSetSubquery_ThenRenamedSetFilterAndPaging_PreservesValues()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                int? missingParent = null;
                var left = service.Query<ParentRow>()
                    .Where(parent => parent.Id <= 2 && parent.Name.Contains("%") && parent.ParentId == missingParent)
                    .InnerJoin<ParentRow, ItemRow>((parent, item) => parent.Id == item.RowId && item.Amount > 3)
                    .OuterApply<ItemRow>((parent, item) => parent.Id == item.RowId,
                        sub => sub.Where(item => item.Amount > 8).Select(item => new { item.RowId, item.Amount })
                            .Union(service.Query<ItemRow>().Where(item => item.Amount == 7)
                                .Select(item => new { item.RowId, item.Amount })))
                    .Select(parent => new { parent.Id, Label = parent.Name })
                    .Select<ItemRow>(item => new { AppliedAmount = item.Amount });
                var right = service.Query<ParentRow>().Where(parent => parent.Id == 3)
                    .Select(parent => new { parent.Id, Label = parent.Name, AppliedAmount = parent.Age });
                var rows = left.Union(right).Where(parent => parent.Name.Contains("%"))
                    .OrderBy("Id ASC, AppliedAmount DESC").Paging(2).Execute<ComposedRow>().ToArray();
                Assert.Equal(new[] { 1, 1 }, rows.Select(row => row.Id));
                Assert.Equal(new[] { 9, 7 }, rows.Select(row => row.AppliedAmount));
                Assert.All(rows, row => Assert.Equal("100%", row.Label));
            }
        }

        [SqlServerFact]
        public void ApplyBeforeSet_ThenNewJoinOfSameClrRole_UsesCurrentOuterSource()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>()
                    .OuterApply<ItemRow>((parent, item) => parent.Id == item.RowId, sub => sub.Where(item => item.Amount > 0))
                    .Union(service.Query<ParentRow>())
                    .InnerJoin<ParentRow, ItemRow>((parent, item) => parent.Id == item.RowId)
                    .Select(parent => parent.Id).Select<ItemRow>(item => item.Amount);
                var rows = query.Execute<JoinedRow>().OrderBy(row => row.Id).ThenBy(row => row.Amount).ToArray();
                Assert.Equal(new[] { 1, 1, 2, 3 }, rows.Select(row => row.Id));
                Assert.Equal(new[] { 7, 9, 7, 5 }, rows.Select(row => row.Amount));
            }
        }

        [SqlServerFact]
        public void FailedSelectArray_ThenFallbackAndSet_DoesNotCommitFirstColumn()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>();
                Assert.Throws<NotSupportedException>(() => query.Select(parent => parent.Id, parent => parent.Name.ToLower()));
                var rows = query.Select(parent => parent.Name)
                    .Union(service.Query<ParentRow>().Select(parent => parent.Name)).Execute<string>().OrderBy(name => name).ToArray();
                Assert.Equal(new[] { "100%", "bonus%", "plain" }, rows);
            }
        }

        [SqlServerFact]
        public void FailedGroupByArray_ThenFallback_DoesNotKeepPartialGrouping()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().Sum(parent => parent.Age);
                Assert.Throws<NotSupportedException>(() => query.GroupBy(parent => parent.Name, parent => parent.Id + 1));
                var sums = query.GroupBy(parent => parent.Age).Execute<int>().OrderBy(value => value).ToArray();
                Assert.Equal(new[] { 20, 30 }, sums);
            }
        }

        [SqlServerFact]
        public void FailedLargePredicates_ThenValidRetry_DoNotExhaustSqlParameterLimit()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>();
                var ids = Enumerable.Range(1, 1100).ToList();
                for (int i = 0; i < 2; i++)
                    Assert.Throws<NotSupportedException>(() => query.Where(parent => ids.Contains(parent.Id) && Unsupported(parent.Id)));
                var rows = query.Where(parent => parent.Id == 1).Execute().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.Id));
            }
        }

        [SqlServerFact]
        public void FailedPredicate_WithNestedCommittedWhere_PreservesNestedParametersForRetry()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().Select(parent => parent.Id);
                var captured = new MutatingValue(() => query.Where(parent => parent.Age == 10));
                Assert.Throws<NotSupportedException>(() => query.Where(
                    parent => parent.Id > 0 && parent.Id == captured.Value && Unsupported(parent.Id)));
                Assert.Equal(1, captured.Reads);

                // The getter's separate successful fluent call remains usable
                // after the enclosing translation fails.
                Assert.Equal(new[] { 1, 2 }, query.Execute<int>().OrderBy(id => id));
                Assert.Equal(new[] { 2 }, query.Where(parent => parent.Id == 2).Execute<int>());
            }
        }

        [SqlServerFact]
        public async Task CapturedGetter_CanCommitWhereOnAnotherThreadDuringWhereHavingJoinAndApply()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                foreach (var clause in new[] { "where", "having", "join", "apply" })
                {
                    var query = service.Query<ParentRow>();
                    if (clause == "having") query.GroupBy(parent => parent.Age).Sum(parent => parent.Age);
                    else query.Select(parent => parent.Id);
                    var background = new BackgroundMutation(() => query.Where(parent => parent.Age == 10));
                    var captured = new MutatingValue(background.Run);
                    try
                    {
                        if (clause == "where") query.Where(parent => parent.Id == captured.Value);
                        if (clause == "having") query.Having(parent => parent.Age >= captured.Value);
                        if (clause == "join") query.InnerJoin<ParentRow, ItemRow>(
                            (parent, item) => parent.Id == item.RowId && parent.Id == captured.Value);
                        if (clause == "apply") query.CrossApply<ItemRow>(
                            (parent, item) => parent.Id == item.RowId && parent.Id == captured.Value, sub => sub);
                        Assert.True(background.CompletedBeforeReturn);
                        Assert.Equal(1, captured.Reads);
                        var expected = clause == "having" ? new[] { 20 }
                            : clause == "where" ? new[] { 1 } : new[] { 1, 1 };
                        Assert.Equal(expected, query.Execute<int>());
                    }
                    finally { await background.DrainAsync(); }
                }
            }
        }

        [SqlServerFact]
        public async Task FailedPredicate_WithAnotherThreadsCommittedWhere_PreservesParametersForRetry()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().Select(parent => parent.Id);
                var background = new BackgroundMutation(() => query.Where(parent => parent.Age == 10));
                var captured = new MutatingValue(background.Run);
                try
                {
                    Assert.Throws<NotSupportedException>(() => query.Where(
                        parent => parent.Id > 0 && parent.Id == captured.Value && Unsupported(parent.Id)));
                    Assert.True(background.CompletedBeforeReturn);
                    Assert.Equal(1, captured.Reads);
                    Assert.Equal(new[] { 1, 2 }, query.Execute<int>().OrderBy(id => id));
                    Assert.Equal(new[] { 2 }, query.Where(parent => parent.Id == 2).Execute<int>());
                }
                finally { await background.DrainAsync(); }
            }
        }

        [SqlServerFact]
        public async Task WaitingSetTransition_AdmitsAnotherThreadsPredicateFromCapturedGetter()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            using (var writerStarted = new ManualResetEventSlim())
            {
                var query = service.Query<ParentRow>().Select(parent => parent.Id);
                var right = service.Query<ParentRow>().Where(parent => parent.Id == 3).Select(parent => parent.Id);
                var background = new BackgroundMutation(() => query.Where(parent => parent.Age == 10));
                Task writer = null;
                var captured = new MutatingValue(() =>
                {
                    using (ExecutionContext.SuppressFlow())
                        writer = Task.Run(() => { writerStarted.Set(); query.Union(right); });
                    Assert.True(writerStarted.Wait(TimeSpan.FromSeconds(2)));
                    Assert.False(writer.Wait(TimeSpan.FromMilliseconds(50)), "SET must wait for the active translation.");
                    // Admit this independent reader even though the exclusive
                    // scope transition was requested before its predicate.
                    background.Run();
                });
                try
                {
                    query.Where(parent => parent.Id > 0 && parent.Id != captured.Value);
                    Assert.True(background.CompletedBeforeReturn);
                    Assert.Equal(1, captured.Reads);
                    var completed = await Task.WhenAny(writer, Task.Delay(TimeSpan.FromSeconds(3)));
                    Assert.Same(writer, completed);
                    await writer;
                    Assert.Equal(new[] { 2, 3 }, query.OrderByAscending(parent => parent.Id).Execute<int>());
                }
                finally
                {
                    await background.DrainAsync();
                    if (writer != null)
                    {
                        var completed = await Task.WhenAny(writer, Task.Delay(TimeSpan.FromSeconds(3)));
                        Assert.Same(writer, completed);
                        await writer;
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task ContainsEnumeration_CanCommitWhereOnAnotherThreadFromEveryEnumeratorCallback()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                foreach (var stage in new[] { "GetEnumerator", "MoveNext", "Current", "Dispose" })
                {
                    var query = service.Query<ParentRow>().Select(parent => parent.Id);
                    var background = new BackgroundMutation(() => query.Where(parent => parent.Age == 10));
                    var values = new CallbackCollection(new[] { 1, 2 }, stage, background.Run);
                    try
                    {
                        query.Where(parent => values.Contains(parent.Id));
                        Assert.True(background.CompletedBeforeReturn);
                        Assert.Equal(new[] { 1, 2 }, query.Execute<int>().OrderBy(id => id));
                    }
                    finally { await background.DrainAsync(); }
                }
            }
        }

        [SqlServerFact]
        public void BoxedConstructorProjection_PreservesLegacyOutputAndComposesWithSet()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                // A constructor argument without an expression member list has
                // historically selected the boxed column without renaming it.
                var result = service.Query<ParentRow>().Where(parent => parent.Id == 1)
                    .Select(parent => new BoxedProjection((object)parent.Name))
                    .Execute<dynamic>().Cast<object>().Single();
                var columns = (IDictionary<string, object>)result;
                Assert.Equal(new[] { "StoredName" }, columns.Keys);
                Assert.Equal("100%", columns["StoredName"]);

                var names = service.Query<ParentRow>()
                    .Select(parent => new BoxedProjection((object)parent.Name))
                    .Union(service.Query<ParentRow>()
                        .Select(parent => new BoxedProjection((object)parent.Name)))
                    .Where(parent => parent.Name.Contains("%"))
                    .OrderByAscending(parent => parent.Name).Execute<string>().ToArray();
                Assert.Equal(new[] { "100%", "bonus%" }, names);
            }
        }

        [SqlServerFact]
        public void FailedApplyPredicate_ThenRetryAndSet_DoesNotImportAbandonedParameters()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().Where(parent => parent.Age >= 10);
                var ids = Enumerable.Range(1, 1100).ToList();
                for (int i = 0; i < 2; i++)
                    Assert.Throws<NotSupportedException>(() => query.OuterApply<ItemRow>(
                        (parent, item) => parent.Id == item.RowId && item.Amount > 0 && Unsupported(item.Id),
                        sub => sub.Where(item => ids.Contains(item.Id))));

                var rows = query.OuterApply<ItemRow>((parent, item) => parent.Id == item.RowId,
                        sub => sub.Where(item => item.Amount > 8))
                    .Select(parent => parent.Id).Select<ItemRow>(item => item.Amount)
                    .Union(service.Query<ParentRow>().Where(parent => parent.Id == 3)
                        .Select(parent => new { parent.Id, Amount = parent.Age }))
                    .Where(parent => parent.Id < 2).Execute<JoinedRow>().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.Id));
                Assert.Equal(new[] { 9 }, rows.Select(row => row.Amount));
            }
        }

        [SqlServerFact]
        public void FailedHavingPredicate_PreservesPreviousClauseAndAllowsValidRetry()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().GroupBy(parent => parent.Age)
                    .Sum(parent => parent.Age).Having(parent => parent.Age > 10);
                var ids = Enumerable.Range(1, 1100).ToList();
                for (int i = 0; i < 2; i++)
                    Assert.Throws<NotSupportedException>(() => query.Having(
                        parent => ids.Contains(parent.Age) && Unsupported(parent.Age)));

                Assert.Equal(new[] { 30 }, query.Execute<int>());
                var sums = query.Having(parent => parent.Age >= 10).Execute<int>().OrderBy(value => value).ToArray();
                Assert.Equal(new[] { 20, 30 }, sums);
            }
        }

        [SqlServerFact]
        public void ApplyBeforeSet_ThenFreshApplyOfSameRole_UsesLatestProjectedColumns()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var rows = service.Query<ParentRow>()
                    .OuterApply<ItemRow>((parent, item) => parent.Id == item.RowId,
                        sub => sub.Where(item => item.Amount == 7))
                    .Union(service.Query<ParentRow>())
                    .OuterApply<ItemRow>((parent, item) => parent.Id == item.RowId,
                        sub => sub.Where(item => item.Amount > 8)
                            .Select(item => new { item.RowId, LatestAmount = item.Amount }))
                    .Where(parent => parent.Id == 1)
                    .Select(parent => parent.Id).Select<ItemRow>(item => item.Amount)
                    .Execute<JoinedRow>().ToArray();
                Assert.Equal(new[] { 1 }, rows.Select(row => row.Id));
                Assert.Equal(new[] { 9 }, rows.Select(row => row.Amount));
            }
        }

        [SqlServerFact]
        public void ApplyReturningIndependentBuilder_WithSharedMappedPrefix_PreservesCorrelation()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var rows = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedItem>((parent, item) => parent.Id == item.ParentId,
                        unused => service.Query<ScopedItem>())
                    .Select<ScopedItem>(item => item.Amount).Execute<NullableAmountRow>()
                    .OrderBy(row => row.Id).ToArray();
                Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
            }
        }

        [SqlServerFact]
        public async Task SetDuringApplySubbuilder_WithSharedMappedPrefix_PreservesBothSourceScopes()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var query = service.Query<ScopedParent>().Where(parent => parent.Id == 1).Select(parent => parent.Id);
                Task transition = null;
                try
                {
                    query.OuterApply<ScopedItem>((parent, item) => parent.Id == item.ParentId, sub =>
                    {
                        using (ExecutionContext.SuppressFlow())
                            transition = Task.Run(() => query.Union(service.Query<ScopedParent>()
                                .Where(parent => parent.Id == 2).Select(parent => parent.Id)));
                        if (!transition.Wait(TimeSpan.FromSeconds(2)))
                            throw new TimeoutException("A subbuilder could not finish its parent SET transition.");
                        return sub.Union(service.Query<ScopedItem>());
                    }).Select(parent => parent.Id).Select<ScopedItem>(item => item.Amount);
                    var rows = query.Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                    Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id));
                    Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                }
                finally
                {
                    if (transition != null)
                    {
                        var completed = await Task.WhenAny(transition, Task.Delay(TimeSpan.FromSeconds(3)));
                        Assert.Same(transition, completed);
                        await transition;
                    }
                }
            }
        }

        [SqlServerFact]
        public void ReusedIndependentApply_WithTopNestedApplyAndRawQualifiedOrder_LeavesOriginalQueryUnchanged()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var captured = new MutatingValue(() => { });
                var child = service.Query<ScopedItem>().Where(item => item.Amount > captured.Value)
                    .CrossApply<ScopedExtra>((item, extra) => item.Id == extra.ItemId && extra.Weight > 0,
                        sub => sub.Where(extra => extra.Weight > 4)).Select(item => item.Amount).Top(1);
                var childAlias = ((global::EasyDapper.QueryBuilder<ScopedItem>)child).GetSourceAlias();
                var literal = "N'[" + childAlias + "].'";
                child.OrderBy("CHARINDEX(" + literal + ", CONVERT(nvarchar(40), " + childAlias + ".[Amount])), "
                    + "(SELECT MAX([" + childAlias + "].[Amount]) FROM #QCScopedParentItems AS [" + childAlias + "]), "
                    + childAlias + ".[Amount] DESC /* " + childAlias + ".Amount */");
                var originalSql = child.GetRawSql();

                var parent = service.Query<ScopedParent>().Select(row => row.Id)
                    .OuterApply<ScopedItem>((row, item) => row.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount);
                var rows = parent.Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                Assert.Contains(literal, parent.GetRawSql());
                Assert.Contains("AS [" + childAlias + "]", parent.GetRawSql());
                Assert.Contains("/* " + childAlias + ".Amount */", parent.GetRawSql());
                Assert.Equal(originalSql, child.GetRawSql());
                Assert.Equal(new[] { 9 }, child.Execute<int>());

                var second = service.Query<ScopedParent>().Where(row => row.Id == 2)
                    .OuterApply<ScopedItem>((row, item) => row.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount).Execute<int>().ToArray();
                Assert.Equal(new[] { 9 }, second);
                Assert.Equal(originalSql, child.GetRawSql());
                Assert.Equal(1, captured.Reads);
            }
        }

        [SqlServerFact]
        public void IndependentApplyAliasMatchingFunctionSchema_PreservesSchemaAndColumnArgument()
        {
            using (var connection = OpenFixture())
            {
                CreateSharedPrefixRows(connection);
                var parent = new global::EasyDapper.QueryBuilder<ScopedParent>(connection);
                parent.WithTableAlias<ScopedParent>("sys");
                parent.Select(row => row.Id);
                var child = new global::EasyDapper.QueryBuilder<ScopedItem>(connection);
                child.WithTableAlias<ScopedItem>("sys");
                child.Top(1).OrderBy("sys.fn_varbintohexstr(CONVERT(varbinary(4), [sys].[Id])) DESC");
                var originalSql = child.GetRawSql();
                parent.OuterApply<ScopedItem>((row, item) => row.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount);
                var rows = parent.Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                Assert.Contains("sys.fn_varbintohexstr(", parent.GetRawSql());
                Assert.Equal(originalSql, child.GetRawSql());
            }
        }

        [SqlServerFact]
        public void IndependentApply_WithLowercaseRawQualifierOnCiDatabase_RemainsValid()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                if (connection.ExecuteScalar<int>("SELECT CASE WHEN 'A' = 'a' THEN 1 ELSE 0 END") != 1) return;
                CreateSharedPrefixRows(connection);
                var child = service.Query<ScopedItem>().Top(1);
                var alias = ((global::EasyDapper.QueryBuilder<ScopedItem>)child).GetSourceAlias();
                child.OrderBy(alias.ToLowerInvariant() + ".Amount DESC");
                var before = child.GetRawSql();
                var rows = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedItem>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount).Execute<NullableAmountRow>().ToArray();
                Assert.Equal(new int?[] { 7, 9 }, rows.OrderBy(row => row.Id).Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());

                var uncorrelated = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedItem>((parent, item) => item.Amount > 0, unused => child)
                    .Select<ScopedItem>(item => item.Amount);
                Assert.Equal(new int?[] { 9, 9 }, uncorrelated.Execute<NullableAmountRow>().Select(row => row.Amount));
                Assert.DoesNotContain("CROSS APPLY (SELECT", uncorrelated.GetRawSql());
            }
        }

        [SqlServerFact]
        public void IndependentApply_WithRawXmlColumnMethod_PreservesColumnReceiver()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var child = service.Query<ScopedXmlItem>().Top(1);
                var alias = ((global::EasyDapper.QueryBuilder<ScopedXmlItem>)child).GetSourceAlias();
                child.OrderBy("[" + alias + "].[Payload].value('(/value/text())[1]', 'int') DESC");
                var before = child.GetRawSql();
                var rows = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedXmlItem>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<ScopedXmlItem>(item => item.Amount).Execute<NullableAmountRow>().ToArray();
                Assert.Equal(new int?[] { 7, 9 }, rows.OrderBy(row => row.Id).Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());
            }
        }

        [SqlServerFact]
        public void IndependentApply_WithCaseDifferentNestedRawAliasOnCiDatabase_PreservesLocalScope()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                if (connection.ExecuteScalar<int>("SELECT CASE WHEN 'A' = 'a' THEN 1 ELSE 0 END") != 1) return;
                CreateSharedPrefixRows(connection);
                var child = service.Query<ScopedItem>().Top(1);
                var alias = ((global::EasyDapper.QueryBuilder<ScopedItem>)child).GetSourceAlias();
                child.OrderBy("(SELECT MAX([" + alias + "].Amount) FROM #QCScopedParentItems AS ["
                    + alias.ToUpperInvariant() + "]), [" + alias + "].Amount DESC");
                var before = child.GetRawSql();
                var rows = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedItem>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount).Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());
            }
        }

        [SqlServerFact]
        public void IndependentApply_WithClauseKeywordsInRawComments_CorrelatesBeforeTop()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var child = service.Query<ScopedItem>().Top(1);
                var alias = ((global::EasyDapper.QueryBuilder<ScopedItem>)child).GetSourceAlias();
                child.OrderBy("[" + alias + "].Amount DESC /* WHERE marker /* GROUP BY */ OFFSET */\n-- HAVING marker\n");
                var before = child.GetRawSql();
                var rows = service.Query<ScopedParent>().Select(parent => parent.Id)
                    .OuterApply<ScopedItem>((parent, item) => parent.Id == item.ParentId, unused => child)
                    .Select<ScopedItem>(item => item.Amount).Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());
            }
        }

        [SqlServerFact]
        public void ApplyWithCollidingRenamedSetScopes_PreservesMapsDuringCapturedNestedPredicate()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                CreateSharedPrefixRows(connection);
                var parent = service.Query<ScopedParent>().Where(row => row.Id == 1)
                    .Select(row => new { ParentKey = row.Id });
                var captured = new MutatingValue(() => parent.Where(row => row.Id > 0));
                var child = service.Query<ScopedItem>()
                    .Select(item => new { ChildParentKey = item.ParentId, item.Amount })
                    .Union(service.Query<ScopedItem>().Select(item => new { ChildParentKey = item.ParentId, item.Amount }));
                var before = child.GetRawSql();
                parent.OuterApply<ScopedItem>((row, item) => row.Id == item.ParentId && item.Amount > captured.Value, unused =>
                {
                    parent.Union(service.Query<ScopedParent>().Where(row => row.Id == 2)
                        .Select(row => new { ParentKey = row.Id }));
                    return child;
                }).Select(row => row.Id).Select<ScopedItem>(item => item.Amount);
                var rows = parent.Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 7, 9 }, rows.Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());
                Assert.Equal(1, captured.Reads);
            }
        }

        [SqlServerFact]
        public void IndependentApply_WithNestedApplyAliasMatchingParentSet_CorrelatesToOuterParent()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var parent = service.Query<ParentRow>().Where(row => row.Id <= 2).Select(row => row.Id)
                    .Union(service.Query<ParentRow>().Where(row => row.Id == 1).Select(row => row.Id));
                var child = service.Query<ItemRow>()
                    .CrossApply<ParentRow>((item, lookup) => item.RowId == lookup.Id, sub => sub.Select(row => row.Id))
                    .Select(item => new { item.RowId, item.Amount }).Top(1).OrderByDescending(item => item.Amount);
                var before = child.GetRawSql();
                parent.OuterApply<ItemRow>((row, item) => row.Id == item.RowId, unused => child)
                    .Select(row => row.Id).Select<ItemRow>(item => item.Amount);
                var rows = parent.Execute<NullableAmountRow>().OrderBy(row => row.Id).ToArray();
                Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id));
                Assert.Equal(new int?[] { 9, 7 }, rows.Select(row => row.Amount));
                Assert.Equal(before, child.GetRawSql());
                Assert.Equal(9, child.Execute<ItemRow>().Single().Amount);
            }
        }

        [SqlServerFact]
        public void FailedJoinPredicate_ThenDistinctRoleRetry_KeepsProjectionBinding()
        {
            using (var connection = OpenFixture())
            using (var service = DapperServiceFactory.Create(connection))
            {
                var query = service.Query<ParentRow>().Select<OtherParentRole>(child => new { ChildId = child.Id });
                Assert.Throws<NotSupportedException>(() => query.InnerJoin<ParentRow, OtherParentRole>(
                    (parent, child) => parent.Id == child.ParentId && Unsupported(child.Id)));
                var rows = query.InnerJoin<ParentRow, OtherParentRole>((parent, child) => parent.Id == child.ParentId)
                    .Where(parent => parent.Id == 1).Execute<ChildProjection>().OrderBy(row => row.ChildId).ToArray();
                Assert.Equal(new[] { 2, 3 }, rows.Select(row => row.ChildId));
            }
        }

        [SqlServerFact]
        public async Task MarsQueryMaterialization_CanUseSuppressedFlowNestedServiceQuery()
        {
            foreach (bool asynchronous in new[] { false, true })
            using (var connection = OpenFixture(true))
            using (var service = DapperServiceFactory.Create(connection))
            {
                Task<int> pending = null;
                HydratedRow.OnNameSet = row =>
                {
                    using (ExecutionContext.SuppressFlow())
                        pending = Task.Run(() => service.Query<ParentRow>().Where(parent => parent.Id == row.Id).Count().Execute<int>().Single());
                    if (!pending.Wait(TimeSpan.FromSeconds(1))) throw new TimeoutException("Nested MARS query was blocked during materialization.");
                    Assert.Equal(1, pending.Result);
                };
                try
                {
                    var query = service.Query<HydratedRow>().Where(row => row.Id == 1);
                    var result = asynchronous ? (await query.ExecuteAsync()).Single() : query.Execute().Single();
                    Assert.Equal("100%", result.Name);
                }
                finally
                {
                    HydratedRow.OnNameSet = null;
                    if (pending != null)
                    {
                        var completed = await Task.WhenAny(pending, Task.Delay(TimeSpan.FromSeconds(3)));
                        Assert.Same(pending, completed);
                        await pending;
                    }
                }
            }
        }

        [SqlServerFact]
        public async Task AsyncOpenCallbacks_CanWaitForSuppressedFlowNestedQueryOrDispose()
        {
            foreach (bool disposeInCallback in new[] { false, true })
            {
                using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    Task<int> pending = null;
                    int callbackEntered = 0;
                    System.Data.StateChangeEventHandler callback = (sender, change) =>
                    {
                        if (change.CurrentState != System.Data.ConnectionState.Open
                            || Interlocked.Exchange(ref callbackEntered, 1) != 0) return;
                        using (ExecutionContext.SuppressFlow())
                            pending = Task.Run(() =>
                            {
                                if (disposeInCallback) { service.Dispose(); return 1; }
                                return service.ExecuteScalarFunction<int>("ABS", new { value = -1 });
                            });
                        if (!pending.Wait(TimeSpan.FromSeconds(2)))
                            throw new TimeoutException("Nested operation was blocked by the async open callback.");
                        Assert.Equal(1, pending.Result);
                    };
                    connection.StateChange += callback;
                    try
                    {
                        Assert.Equal(2, await service.ExecuteScalarFunctionAsync<int>("ABS", new { value = -2 }));
                        Assert.Equal(1, callbackEntered);
                        // An in-flight call historically completes even if its
                        // callback disposes the service owning a borrowed connection.
                        Assert.Equal(System.Data.ConnectionState.Open, connection.State);
                    }
                    finally
                    {
                        connection.StateChange -= callback;
                        if (pending != null)
                        {
                            var completed = await Task.WhenAny(pending, Task.Delay(TimeSpan.FromSeconds(3)));
                            Assert.Same(pending, completed);
                            await pending;
                        }
                    }
                }
            }
        }

        private static bool Unsupported(int value) => true;

        private static SqlConnection OpenFixture(bool mars = false)
        {
            var settings = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION"));
            if (mars) settings.MultipleActiveResultSets = true;
            var connection = new SqlConnection(settings.ConnectionString);
            connection.Open();
            connection.Execute(@"CREATE TABLE #QCParents (Id INT PRIMARY KEY, StoredName NVARCHAR(40), Age INT, ParentId INT NULL);
                INSERT #QCParents VALUES (1,N'100%',10,NULL),(2,N'plain',10,1),(3,N'bonus%',30,1);
                CREATE TABLE #QCItems (Id INT PRIMARY KEY, RowId INT, Amount INT);
                INSERT #QCItems VALUES (11,1,7),(12,1,9),(21,2,7),(31,3,5);");
            return connection;
        }

        private static void CreateSharedPrefixRows(SqlConnection connection)
        {
            connection.Execute(@"CREATE TABLE #QCScopedParents(Id INT PRIMARY KEY);
                INSERT #QCScopedParents VALUES(1),(2);
                CREATE TABLE #QCScopedParentItems(Id INT PRIMARY KEY, ParentId INT, Amount INT, Payload XML);
                INSERT #QCScopedParentItems VALUES(11,1,7,N'<value>7</value>'),(21,2,9,N'<value>9</value>');
                CREATE TABLE #QCScopedParentExtra(Id INT PRIMARY KEY, ItemId INT, Weight INT);
                INSERT #QCScopedParentExtra VALUES(101,11,5),(102,21,6);");
        }

        [Table("#QCParents")]
        public class ParentRow
        {
            public int Id { get; set; }
            [Column("StoredName")] public string Name { get; set; }
            public int Age { get; set; }
            public int? ParentId { get; set; }
        }
        [Table("#QCParents")]
        public class OtherParentRole { public int Id { get; set; } public int? ParentId { get; set; } }
        [Table("#QCItems")]
        public class ItemRow { public int Id { get; set; } public int RowId { get; set; } public int Amount { get; set; } }
        public class ComposedRow { public int Id { get; set; } public string Label { get; set; } public int AppliedAmount { get; set; } }
        public class JoinedRow { public int Id { get; set; } public int Amount { get; set; } }
        public class ChildProjection { public int ChildId { get; set; } }
        [Table("#QCScopedParents")]
        public class ScopedParent { public int Id { get; set; } }
        [Table("#QCScopedParentItems")]
        public class ScopedItem { public int Id { get; set; } public int ParentId { get; set; } public int Amount { get; set; } }
        [Table("#QCScopedParentItems")]
        public class ScopedXmlItem { public int Id { get; set; } public int ParentId { get; set; } public int Amount { get; set; } public string Payload { get; set; } }
        [Table("#QCScopedParentExtra")]
        public class ScopedExtra { public int Id { get; set; } public int ItemId { get; set; } public int Weight { get; set; } }
        public class NullableAmountRow { public int Id { get; set; } public int? Amount { get; set; } }
        public class BoxedProjection
        {
            public BoxedProjection(object value) { Value = value; }
            public object Value { get; }
        }
        public class MutatingValue
        {
            private readonly Action _onRead;
            public MutatingValue(Action onRead) { _onRead = onRead; }
            public int Reads { get; private set; }
            public int Value { get { Reads++; _onRead(); return 1; } }
        }
        public class BackgroundMutation
        {
            private readonly Action _mutation;
            public BackgroundMutation(Action mutation) { _mutation = mutation; }
            public Task Pending { get; private set; }
            public bool CompletedBeforeReturn { get; private set; }
            public void Run()
            {
                using (ExecutionContext.SuppressFlow()) Pending = Task.Run(_mutation);
                if (!Pending.Wait(TimeSpan.FromSeconds(2)))
                    throw new TimeoutException("A captured callback could not mutate the same query on another thread.");
                CompletedBeforeReturn = true;
            }
            public async Task DrainAsync()
            {
                if (Pending == null) return;
                var completed = await Task.WhenAny(Pending, Task.Delay(TimeSpan.FromSeconds(3)));
                Assert.Same(Pending, completed);
                await Pending;
            }
        }
        public class CallbackCollection : IEnumerable<int>
        {
            private readonly int[] _values;
            private readonly string _stage;
            private readonly Action _callback;
            private bool _called;
            public CallbackCollection(int[] values, string stage, Action callback)
            { _values = values; _stage = stage; _callback = callback; }
            public bool Contains(int value) => _values.Contains(value);
            private void Invoke(string stage)
            {
                if (_called || _stage != stage) return;
                _called = true;
                _callback();
            }
            public IEnumerator<int> GetEnumerator()
            {
                Invoke("GetEnumerator");
                return new Enumerator(this, ((IEnumerable<int>)_values).GetEnumerator());
            }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
            private class Enumerator : IEnumerator<int>
            {
                private readonly CallbackCollection _owner;
                private readonly IEnumerator<int> _inner;
                public Enumerator(CallbackCollection owner, IEnumerator<int> inner) { _owner = owner; _inner = inner; }
                public int Current { get { _owner.Invoke("Current"); return _inner.Current; } }
                object System.Collections.IEnumerator.Current => Current;
                public bool MoveNext() { _owner.Invoke("MoveNext"); return _inner.MoveNext(); }
                public void Reset() => _inner.Reset();
                public void Dispose() { _owner.Invoke("Dispose"); _inner.Dispose(); }
            }
        }
        [Table("#QCParents")]
        public class HydratedRow
        {
            public static Action<HydratedRow> OnNameSet;
            private string _name;
            public int Id { get; set; }
            [Column("StoredName")]
            public string Name { get { return _name; } set { _name = value; var callback = OnNameSet; if (callback != null) callback(this); } }
        }
    }
}
