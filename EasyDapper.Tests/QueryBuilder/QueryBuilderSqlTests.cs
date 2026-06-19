using System;
using System.Collections.Generic;
using System.Data;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderSqlTests
    {
        private IQueryBuilder<T> CreateBuilder<T>() where T : class
        {
            var stub = new StubConnection();
            return new global::EasyDapper.QueryBuilder<T>(stub);
        }

        [Fact]
        public void Simple_Where_ProducesExpectedSql()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Age > 18);
            var sql = qb.BuildQuery();
            Assert.Contains("FROM [dbo].[Person] AS", sql);
            Assert.Contains("WHERE", sql);
            Assert.Contains("[Age]", sql);
            Assert.Contains(">", sql);
        }

        [Fact]
        public void FirstAlias_IsSuffixedWith_A1()
        {
            var qb = CreateBuilder<Person>();
            var sql = qb.BuildQuery();
            Assert.Contains("Person_A1", sql);
            Assert.DoesNotContain("Person_A2", sql);
        }

        [Fact]
        public void SelfJoin_SelectReferencesFromAlias_NotJoinAlias()
        {
            var qb = CreateBuilder<Employee>()
                .InnerJoin<Employee, Employee>((e, mgr) => e.ManagerId == mgr.Id);
            var sql = qb.BuildQuery();

            Assert.Contains("SELECT Employee_A1.[Id] AS [Id]", sql);
            Assert.Contains("FROM [dbo].[Employee] AS [Employee_A1]", sql);
            Assert.Contains("INNER JOIN [dbo].[Employee] AS [Employee_A2]", sql);

            Assert.Contains("Employee_A1.[ManagerId] = Employee_A2.[Id]", sql);
        }

        [Fact]
        public void Join_TwoTables_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .InnerJoin<Person, Party>((p, x) => p.PARTY_ID == x.PartyId);
            var sql = qb.BuildQuery();

            Assert.Contains("FROM [dbo].[Person] AS [Person_A1]", sql);
            Assert.Contains("INNER JOIN [dbo].[Party] AS [Party_A2]", sql);
            Assert.Contains("Person_A1.[PARTY_ID] = Party_A2.[PartyId]", sql);
        }

        [Fact]
        public void Join_MultipleTables_ProducesValidSql()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Product>((o, p) => o.ProductId == p.Id);
            var sql = qb.BuildQuery();

            Assert.Contains("FROM [dbo].[Order] AS [Order_A1]", sql);
            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A2]", sql);
            Assert.Contains("INNER JOIN [dbo].[Product] AS [Product_A3]", sql);
        }

        [Fact]
        public void Join_RepeatedSameTable_SecondJoinUsesFreshAlias()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Customer>((o, c) => o.BillingCustomerId == c.Id);
            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A2]", sql);
            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A3]", sql);
            Assert.Contains("Order_A1.[CustomerId] = Customer_A2.[Id]", sql);
            Assert.Contains("Order_A1.[BillingCustomerId] = Customer_A3.[Id]", sql);
        }

        [Fact]
        public void JoinAndApply_SelectFromMultipleSources()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .OuterApply<OrderItem>((o, oi) => o.Id == oi.OrderId,
                    sub => sub.Where(s => s.Quantity > 5))
                .Select<Order>(o => o.OrderDate)
                .Select<Customer>(c => c.Name)
                .Select<OrderItem>(oi => oi.Quantity);
            var sql = qb.BuildQuery();

            Assert.Contains("Order_A1.[OrderDate] AS OrderDate", sql);
            Assert.Contains("Customer_A2.[Name] AS Name", sql);

            Assert.Contains("OrderIte_SQ1.Quantity AS Quantity", sql);
            Assert.Contains("OUTER APPLY", sql);
        }

        [Fact]
        public void Union_TwoQueries_BothUseAliasA1InSeparateScopes()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);
            var result = q1.Union(q2);
            var sql = result.BuildQuery();

            Assert.Contains("Person_A1.[Age] > @p1", sql);
            Assert.Contains("UNION (", sql);
            Assert.Contains("Person_A1.[Age] <", sql);

            Assert.Matches(@"Person_A1\.\[Age\] < @p\d+", sql);
        }

        [Fact]
        public void Where_NullableValueAccess_CompilesToColumn()
        {
            var qb = CreateBuilder<Person>().Where(p => p.CreatedAt.Value > DateTime.MinValue);
            var sql = qb.BuildQuery();
            Assert.Contains("[CreatedAt]", sql);

            Assert.DoesNotContain("Value", sql);
        }

        [Fact]
        public void Where_InheritedEntity_UsesParamType()
        {
            var qb = CreateBuilder<DerivedItem>().Where(d => d.Name == "X");
            var sql = qb.BuildQuery();
            Assert.Contains("[dbo].[DerivedItem]", sql);

            Assert.Contains("DerivedIte_A1.[Name]", sql);
        }

        [Fact]
        public void Distinct_AddsDistinctKeywordToSelect()
        {
            var qb = CreateBuilder<Person>()
                .Select(p => p.Name)
                .Distinct();
            var sql = qb.BuildQuery();
            Assert.Contains("SELECT DISTINCT", sql);
        }

        [Fact]
        public void Top_AddsTopKeywordToSelect()
        {
            var qb = CreateBuilder<Person>()
                .Top(10);
            var sql = qb.BuildQuery();
            Assert.Contains("SELECT TOP (10)", sql);
        }

        [Fact]
        public void Top_Zero_Throws()
        {
            var qb = CreateBuilder<Person>();
            Assert.Throws<ArgumentOutOfRangeException>(() => qb.Top(0));
        }

        [Fact]
        public void Top_Negative_Throws()
        {
            var qb = CreateBuilder<Person>();
            Assert.Throws<ArgumentOutOfRangeException>(() => qb.Top(-5));
        }

        [Fact]
        public void OrderBy_RawString_IsPassedThrough()
        {
            var qb = CreateBuilder<Person>().OrderBy("Name ASC, Age DESC");
            var sql = qb.BuildQuery();
            Assert.Contains("ORDER BY Name ASC, Age DESC", sql);
        }

        [Fact]
        public void OrderByAscending_ProducesAscSuffix()
        {
            var qb = CreateBuilder<Person>().OrderByAscending(p => p.Name);
            var sql = qb.BuildQuery();
            Assert.Contains("ORDER BY", sql);
            Assert.Contains("CurrentFirstName", sql);
            Assert.Contains("ASC", sql);
        }

        [Fact]
        public void OrderByDescending_ProducesDescSuffix()
        {
            var qb = CreateBuilder<Person>().OrderByDescending(p => p.Name);
            var sql = qb.BuildQuery();
            Assert.Contains("ORDER BY", sql);
            Assert.Contains("CurrentFirstName", sql);
            Assert.Contains("DESC", sql);
        }

        [Fact]
        public void Paging_ProducesOffsetFetchClause()
        {
            var qb = CreateBuilder<Person>()
                .Paging(10, 2)
                .OrderByAscending(p => p.Id);
            var sql = qb.BuildQuery();
            Assert.Contains("OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void Paging_ZeroPageSize_Throws()
        {
            var qb = CreateBuilder<Person>();
            Assert.Throws<ArgumentException>(() => qb.Paging(0, 1));
        }

        [Fact]
        public void Paging_ZeroPageNumber_Throws()
        {
            var qb = CreateBuilder<Person>();
            Assert.Throws<ArgumentException>(() => qb.Paging(10, 0));
        }

        [Fact]
        public void Paging_WithoutOrderBy_AutoGeneratesSyntheticOrderBy()
        {
            var qb = CreateBuilder<Person>().Paging(10, 1);
            var sql = qb.BuildQuery();
            Assert.Contains("ORDER BY (SELECT 1)", sql);
            Assert.Contains("OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", sql);
        }

        [Fact]
        public void Where_StringContains_TranslatesToLike()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Name.Contains("John"));
            var sql = qb.BuildQuery();
            Assert.Contains("LIKE", sql);
            Assert.Matches(@"@p\d+", sql);
        }

        [Fact]
        public void Where_StringStartsWith_TranslatesToLike()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Name.StartsWith("Jo"));
            var sql = qb.BuildQuery();
            Assert.Contains("LIKE", sql);
            Assert.Matches(@"@p\d+", sql);
        }

        [Fact]
        public void Where_StringEndsWith_TranslatesToLike()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Name.EndsWith("hn"));
            var sql = qb.BuildQuery();
            Assert.Contains("LIKE", sql);
            Assert.Matches(@"@p\d+", sql);
        }

        [Fact]
        public void Where_InequalityOperator_ProducesNotEqual()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Age != 18);
            var sql = qb.BuildQuery();
            Assert.Contains("[Age] <>", sql);
        }

        [Fact]
        public void Where_BooleanMember_ProducesEqualsOne()
        {
            var qb = CreateBuilder<Person>().Where(p => p.IsActive);
            var sql = qb.BuildQuery();
            Assert.Contains("[IsActive] = 1", sql);
        }

        [Fact]
        public void Where_NegatedBooleanMember_ProducesEqualsZero()
        {
            var qb = CreateBuilder<Person>().Where(p => !p.IsActive);
            var sql = qb.BuildQuery();
            Assert.Contains("[IsActive] = 0", sql);
        }

        [Fact]
        public void Where_NullComparison_ProducesIsNull()
        {
            var qb = CreateBuilder<Person>().Where(p => p.CreatedAt == null);
            var sql = qb.BuildQuery();
            Assert.Contains("[CreatedAt] IS NULL", sql);
        }

        [Fact]
        public void Where_NotNullComparison_ProducesIsNotNull()
        {
            var qb = CreateBuilder<Person>().Where(p => p.CreatedAt != null);
            var sql = qb.BuildQuery();
            Assert.Contains("[CreatedAt] IS NOT NULL", sql);
        }

        [Fact]
        public void Where_LogicalAnd_CombinesWithAnd()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Age > 18 && p.IsActive);
            var sql = qb.BuildQuery();
            Assert.Contains("AND", sql);
        }

        [Fact]
        public void Where_LogicalOr_CombinesWithOr()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Age > 18 || p.Age < 5);
            var sql = qb.BuildQuery();
            Assert.Contains("OR", sql);
        }

        [Fact]
        public void Where_WithParameter_ProducesNamedParameter()
        {
            var qb = CreateBuilder<Person>().Where(p => p.Age == 42);
            var sql = qb.BuildQuery();
            Assert.Matches(@"@p\d+", sql);
        }

        [Fact]
        public void Select_SpecificColumns_OnlyThoseColumnsAppearInSelect()
        {
            var qb = CreateBuilder<Person>()
                .Select(p => p.Name, p => p.Age);
            var sql = qb.BuildQuery();

            Assert.Contains("[CurrentFirstName] AS Name", sql);
            Assert.Contains("[Age] AS Age", sql);

            Assert.DoesNotContain("[Id] AS Id", sql);
        }

        [Fact]
        public void Count_ProducesCountStarSelect()
        {
            var qb = CreateBuilder<Person>().Count();
            var sql = qb.BuildQuery();
            Assert.Contains("SELECT COUNT(*) AS TotalCount", sql);
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
        public void Aggregate_WithGroupBy_ProducesValidSql()
        {
            var qb = CreateBuilder<Person>()
                .Sum(p => p.Age, "TotalAge")
                .GroupBy(p => p.IsActive);
            var sql = qb.BuildQuery();
            Assert.Contains("SUM(", sql);
            Assert.Contains("AS [TotalAge]", sql);
            Assert.Contains("GROUP BY", sql);
        }

        [Fact]
        public void Row_Number_ProducesRowNumberOverClause()
        {
            var qb = CreateBuilder<Person>()
                .Row_Number(p => p.IsActive, p => p.Age);
            var sql = qb.BuildQuery();
            Assert.Contains("ROW_NUMBER() OVER (PARTITION BY", sql);
            Assert.Contains("ORDER BY", sql);
        }

        [Fact]
        public void MultipleWheres_AreCombinedWithAnd()
        {
            var qb = CreateBuilder<Person>()
                .Where(p => p.Age > 18)
                .Where(p => p.IsActive);
            var sql = qb.BuildQuery();

            Assert.Contains("Age", sql);
            Assert.Contains("IsActive", sql);
            Assert.Contains("AND", sql);
        }

        [Fact]
        public void BuildQuery_DoesNotOpenConnection()
        {
            var stub = new StubConnection();
            var qb = new global::EasyDapper.QueryBuilder<Person>(stub);

            var sql = qb.BuildQuery();
            Assert.False(stub.WasOpened);
        }

        [Fact]
        public void Constructor_DirectConnection_DoesNotOpenConnection()
        {
            var stub = new StubConnection();
            var qb = new global::EasyDapper.QueryBuilder<Person>(stub)
                .Where(p => p.Age > 18)
                .Select(p => p.Name)
                .OrderByAscending(p => p.Id);
            Assert.False(stub.WasOpened, "QueryBuilder construction must not open the connection");
        }
    }
}
