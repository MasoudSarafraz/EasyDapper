using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderAggregationAndPagingTests
    {
        private IQueryBuilder<T> CreateBuilder<T>() where T : class
        {
            var stub = new StubConnection();
            return new global::EasyDapper.QueryBuilder<T>(stub);
        }

        [Fact]
        public void Sum_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Order>()
                .Sum(o => o.Id, "TotalIds")
                .GroupBy(o => o.CustomerId);

            var sql = qb.BuildQuery();

            Assert.Contains("SUM(", sql);
            Assert.Contains("AS [TotalIds]", sql);
            Assert.Contains("GROUP BY", sql);
            Assert.Contains("[CustomerId]", sql);
        }

        [Fact]
        public void Avg_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Avg(p => p.Age, "AverageAge")
                .GroupBy(p => p.IsActive);

            var sql = qb.BuildQuery();

            Assert.Contains("AVG(", sql);
            Assert.Contains("AS [AverageAge]", sql);
            Assert.Contains("GROUP BY", sql);
            Assert.Contains("IsActive", sql);
        }

        [Fact]
        public void Min_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Min(p => p.Age, "MinAge")
                .GroupBy(p => p.IsActive);

            var sql = qb.BuildQuery();

            Assert.Contains("MIN(", sql);
            Assert.Contains("AS [MinAge]", sql);
        }

        [Fact]
        public void Max_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Max(p => p.Age, "MaxAge")
                .GroupBy(p => p.IsActive);

            var sql = qb.BuildQuery();

            Assert.Contains("MAX(", sql);
            Assert.Contains("AS [MaxAge]", sql);
        }

        [Fact]
        public void Count_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Count(p => p.Id, "IdCount")
                .GroupBy(p => p.IsActive);

            var sql = qb.BuildQuery();

            Assert.Contains("COUNT(", sql);
            Assert.Contains("AS [IdCount]", sql);
        }

        [Fact]
        public void MultipleAggregates_CombinedInSelect()
        {
            var qb = CreateBuilder<Person>()
                .Sum(p => p.Age, "TotalAge")
                .Avg(p => p.Age, "AvgAge")
                .Min(p => p.Age, "MinAge")
                .Max(p => p.Age, "MaxAge")
                .Count(p => p.Id, "Count")
                .GroupBy(p => p.IsActive);

            var sql = qb.BuildQuery();

            Assert.Contains("SUM(", sql);
            Assert.Contains("AVG(", sql);
            Assert.Contains("MIN(", sql);
            Assert.Contains("MAX(", sql);
            Assert.Contains("COUNT(", sql);
        }

        [Fact]
        public void Having_WithAggregate_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Sum(p => p.Age, "TotalAge")
                .GroupBy(p => p.IsActive)
                .Having(p => p.Age > 5);

            var sql = qb.BuildQuery();

            Assert.Contains("HAVING", sql);
        }

        [Fact]
        public void Aggregate_WithoutGroupByAndWithSelectedColumns_Throws()
        {
            var qb = CreateBuilder<Person>()
                .Sum(p => p.Age)
                .Select(p => p.Name);

            Assert.Throws<InvalidOperationException>(() => qb.BuildQuery());
        }

        [Fact]
        public void Aggregate_WithoutGroupByAndWithoutSelectedColumns_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>().Sum(p => p.Age);
            var sql = qb.BuildQuery();

            Assert.Contains("SUM(", sql);
            Assert.DoesNotContain("GROUP BY", sql);
        }

        [Fact]
        public void Paging_FirstPage_ProducesOffsetZero()
        {
            var qb = CreateBuilder<Person>()
                .Paging(10, 1)
                .OrderByAscending(p => p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("OFFSET 0 ROWS", sql);
            Assert.Contains("FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void Paging_SecondPage_ProducesOffsetTen()
        {
            var qb = CreateBuilder<Person>()
                .Paging(10, 2)
                .OrderByAscending(p => p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("OFFSET 10 ROWS", sql);
            Assert.Contains("FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void Paging_TenthPage_ProducesOffsetNinety()
        {
            var qb = CreateBuilder<Person>()
                .Paging(10, 10)
                .OrderByAscending(p => p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("OFFSET 90 ROWS", sql);
            Assert.Contains("FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void Paging_LargePageSize_ProducesCorrectOffset()
        {
            var qb = CreateBuilder<Person>()
                .Paging(100, 5)
                .OrderByAscending(p => p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("OFFSET 400 ROWS", sql);
            Assert.Contains("FETCH NEXT 100 ROWS ONLY", sql);
        }

        [Fact]
        public void Paging_WithMultipleOrderBy_OrderIsCorrect()
        {
            var qb = CreateBuilder<Person>()
                .Paging(10, 1)
                .OrderByAscending(p => p.Name)
                .OrderByDescending(p => p.Age);

            var sql = qb.BuildQuery();

            Assert.Contains("[CurrentFirstName] ASC", sql);
            Assert.Contains("[Age] DESC", sql);
            Assert.Contains("OFFSET 0 ROWS", sql);
        }

        [Fact]
        public void Row_Number_WithPartition_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Row_Number(p => p.IsActive, p => p.Age);

            var sql = qb.BuildQuery();

            Assert.Contains("ROW_NUMBER() OVER", sql);
            Assert.Contains("PARTITION BY", sql);
            Assert.Contains("ORDER BY", sql);
        }

        [Fact]
        public void Row_Number_WithCustomAlias_AliasPresent()
        {
            var qb = CreateBuilder<Person>()
                .Row_Number(p => p.IsActive, p => p.Age, "RowNum");

            var sql = qb.BuildQuery();

            Assert.Contains("AS [RowNum]", sql);
        }

        [Fact]
        public void Row_Number_DefaultAlias_IsRowNumber()
        {
            var qb = CreateBuilder<Person>()
                .Row_Number(p => p.IsActive, p => p.Age);

            var sql = qb.BuildQuery();

            Assert.Contains("AS [RowNumber]", sql);
        }

        [Fact]
        public void Distinct_WithTop_BothPresent()
        {
            var qb = CreateBuilder<Person>()
                .Distinct()
                .Top(10)
                .Select(p => p.Name);

            var sql = qb.BuildQuery();

            Assert.Contains("SELECT DISTINCT TOP (10)", sql);
        }

        [Fact]
        public void Count_NoParameters_ProducesCountStar()
        {
            var qb = CreateBuilder<Person>().Count();
            var sql = qb.BuildQuery();

            Assert.Contains("SELECT COUNT(*) AS TotalCount", sql);
        }

        [Fact]
        public void ComplexQuery_JoinGroupByHavingOrderByPaging()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .Where(o => o.Id > 0)
                .Sum(o => o.Id, "TotalOrders")
                .GroupBy(o => o.CustomerId)
                .Having(o => o.CustomerId > 0)
                .OrderBy("Customer DESC")
                .Paging(10, 1);

            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("SUM(", sql);
            Assert.Contains("GROUP BY", sql);
            Assert.Contains("HAVING", sql);
            Assert.Contains("ORDER BY Customer DESC", sql);
            Assert.Contains("OFFSET 0 ROWS", sql);
            Assert.Contains("FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void ComplexQuery_DistinctTopWhereOrderBy()
        {
            var qb = CreateBuilder<Person>()
                .Distinct()
                .Top(50)
                .Select(p => p.Name)
                .Where(p => p.Age > 18 && p.IsActive)
                .OrderByAscending(p => p.Name)
                .OrderByDescending(p => p.Age);

            var sql = qb.BuildQuery();

            Assert.Contains("SELECT DISTINCT TOP (50)", sql);
            Assert.Contains("[CurrentFirstName] AS Name", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("AND", sql);
            Assert.Contains("[CurrentFirstName] ASC", sql);
            Assert.Contains("[Age] DESC", sql);
        }

        [Fact]
        public void MultipleWheres_CombinedWithAnd()
        {
            var qb = CreateBuilder<Person>()
                .Where(p => p.Age > 18)
                .Where(p => p.IsActive)
                .Where(p => p.Name == "John");

            var sql = qb.BuildQuery();

            Assert.Contains("WHERE", sql);
            Assert.Contains("AND", sql);
        }

        [Fact]
        public void Where_WithComplexExpression_ParenthesesCorrect()
        {
            var qb = CreateBuilder<Person>()
                .Where(p => (p.Age > 18 && p.IsActive) || p.Name == "Admin");

            var sql = qb.BuildQuery();

            Assert.Contains("(", sql);
            Assert.Contains("OR", sql);
            Assert.Contains("AND", sql);
        }
    }
}
