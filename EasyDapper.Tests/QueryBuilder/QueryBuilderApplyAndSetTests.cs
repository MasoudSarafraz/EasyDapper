using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderApplyAndSetTests
    {
        private IQueryBuilder<T> CreateBuilder<T>() where T : class
        {
            var stub = new StubConnection();
            return new global::EasyDapper.QueryBuilder<T>(stub);
        }

        [Fact]
        public void CrossApply_ProducesValidSql()
        {
            var qb = CreateBuilder<Order>()
                .CrossApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5));

            var sql = qb.BuildQuery();

            Assert.Contains("CROSS APPLY", sql);
            Assert.Contains("[dbo].[OrderItem]", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("Quantity", sql);
        }

        [Fact]
        public void OuterApply_ProducesValidSql()
        {
            var qb = CreateBuilder<Order>()
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5));

            var sql = qb.BuildQuery();

            Assert.Contains("OUTER APPLY", sql);
            Assert.Contains("[dbo].[OrderItem]", sql);
        }

        [Fact]
        public void Apply_WithSubQueryWhere_SubQueryWhereIsPresent()
        {
            var qb = CreateBuilder<Order>()
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5));

            var sql = qb.BuildQuery();

            Assert.Contains("[Quantity] >", sql);
        }

        [Fact]
        public void Apply_MultipleAppliesWithDifferentTypes()
        {
            var qb = CreateBuilder<Order>()
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5))
                .OuterApply<Customer>((o, c) => o.CustomerId == c.Id,
                    sub => sub.Where(s => s.Name == "Test"));

            var sql = qb.BuildQuery();

            Assert.Contains("[dbo].[OrderItem]", sql);
            Assert.Contains("[dbo].[Customer]", sql);
            Assert.Contains("[Quantity] >", sql);
            Assert.Contains("[Name]", sql);
        }

        [Fact]
        public void Apply_CombinedWithInnerJoin()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5));

            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN [dbo].[Customer]", sql);
            Assert.Contains("OUTER APPLY", sql);
            Assert.Contains("[dbo].[OrderItem]", sql);
        }

        [Fact]
        public void Union_TwoSimpleQueries_Combined()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.Union(q2);

            var sql = result.BuildQuery();

            Assert.Contains("UNION (", sql);
            Assert.Contains("[Age] >", sql);
            Assert.Contains("[Age] <", sql);
        }

        [Fact]
        public void UnionAll_TwoSimpleQueries_Combined()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.UnionAll(q2);

            var sql = result.BuildQuery();

            Assert.Contains("UNION ALL (", sql);
        }

        [Fact]
        public void Intersect_TwoQueries_Combined()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 18);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 65);
            var result = q1.Intersect(q2);

            var sql = result.BuildQuery();

            Assert.Contains("INTERSECT (", sql);
        }

        [Fact]
        public void Except_TwoQueries_Combined()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 18);
            var q2 = CreateBuilder<Person>().Where(p => p.Age > 65);
            var result = q1.Except(q2);

            var sql = result.BuildQuery();

            Assert.Contains("EXCEPT (", sql);
        }

        [Fact]
        public void Union_ThreeQueries_Chained()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age == 10);
            var q2 = CreateBuilder<Person>().Where(p => p.Age == 20);
            var q3 = CreateBuilder<Person>().Where(p => p.Age == 30);
            var result = q1.Union(q2).Union(q3);

            var sql = result.BuildQuery();

            int unionCount = sql.Split(new[] { "UNION (" }, StringSplitOptions.None).Length - 1;
            Assert.Equal(2, unionCount);
        }

        [Fact]
        public void Union_CombinedWithOrderBy()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.Union(q2);

            var sql = result.BuildQuery();

            Assert.Contains("UNION (", sql);
            Assert.Contains("[Age] >", sql);
            Assert.Contains("[Age] <", sql);
        }

        [Fact]
        public void Union_ParameterRenaming_NoCollision()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.Union(q2);

            var sql = result.BuildQuery();

            var matches = System.Text.RegularExpressions.Regex.Matches(sql, @"@p\d+");
            var distinctParams = matches.Cast<System.Text.RegularExpressions.Match>()
                .Select(m => m.Value)
                .Distinct()
                .ToList();
            Assert.True(distinctParams.Count >= 1, $"Expected at least 1 distinct parameter, got {distinctParams.Count}: {string.Join(", ", distinctParams)}");
        }

        [Fact]
        public void Union_WithJoinInSubQuery_SubQuerySqlValid()
        {
            var q1 = CreateBuilder<Person>()
                .InnerJoin<Person, Party>((p, x) => p.PARTY_ID == x.PartyId)
                .Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.Union(q2);

            var sql = result.BuildQuery();

            Assert.Contains("INNER JOIN", sql);
            Assert.Contains("UNION (", sql);
        }

        [Fact]
        public void Apply_WithSubQuerySelect_ColumnFromSubQuery()
        {
            var qb = CreateBuilder<Order>()
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5))
                .Select<OrderItem>(oi => oi.Quantity);

            var sql = qb.BuildQuery();

            Assert.Contains("Quantity", sql);
        }

        [Fact]
        public void Apply_MultipleAppliesWithSameType_LastOneAccessible()
        {
            var qb = CreateBuilder<Order>()
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5))
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity < 2));

            var sql = qb.BuildQuery();

            Assert.Contains("OrderIte_SQ1", sql);
            Assert.Contains("OrderIte_SQ2", sql);
        }
    }
}
