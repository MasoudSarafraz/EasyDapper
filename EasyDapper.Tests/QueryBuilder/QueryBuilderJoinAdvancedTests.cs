using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderJoinAdvancedTests
    {
        private IQueryBuilder<T> CreateBuilder<T>() where T : class
        {
            var stub = new StubConnection();
            return new global::EasyDapper.QueryBuilder<T>(stub);
        }

        [Fact]
        public void Join_ThreeLevelChain_AllAliasesPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Product>((o, p) => o.ProductId == p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("FROM [dbo].[Order] AS [Order_A1]", sql);
            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A2]", sql);
            Assert.Contains("INNER JOIN [dbo].[Product] AS [Product_A3]", sql);
            Assert.Contains("Order_A1.[CustomerId] = Customer_A2.[Id]", sql);
            Assert.Contains("Order_A1.[ProductId] = Product_A3.[Id]", sql);
        }

        [Fact]
        public void Join_MixedInnerAndLeft_AllClausesPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .LeftJoin<Order, Product>((o, p) => o.ProductId == p.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN [dbo].[Customer]", sql);
            Assert.Contains("LEFT JOIN [dbo].[Product]", sql);
        }

        [Fact]
        public void Join_AllFourJoinTypes_AllPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .LeftJoin<Order, Product>((o, p) => o.ProductId == p.Id)
                .RightJoin<Order, OrderItem>((o, oi) => o.Id == oi.OrderId)
                .FullJoin<Order, Party>((o, p) => o.Id == p.PartyId);

            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN [dbo].[Customer]", sql);
            Assert.Contains("LEFT JOIN [dbo].[Product]", sql);
            Assert.Contains("RIGHT JOIN [dbo].[OrderItem]", sql);
            Assert.Contains("FULL JOIN [dbo].[Party]", sql);
        }

        [Fact]
        public void Join_ConditionWithAndOperator_BothClausesPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id && o.Id > 0);

            var sql = qb.BuildQuery();

            Assert.Contains("AND", sql);
            Assert.Contains("Order_A1.[CustomerId] = Customer_A2.[Id]", sql);
            Assert.Contains("Order_A1.[Id] >", sql);
        }

        [Fact]
        public void Join_ConditionWithOrOperator_BothClausesPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id || o.BillingCustomerId == c.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("OR", sql);
            Assert.Contains("Order_A1.[CustomerId] = Customer_A2.[Id]", sql);
            Assert.Contains("Order_A1.[BillingCustomerId] = Customer_A2.[Id]", sql);
        }

        [Fact]
        public void SelfJoin_ThreeWay_Chain()
        {
            var qb = CreateBuilder<Employee>()
                .InnerJoin<Employee, Employee>((e, mgr) => e.ManagerId == mgr.Id)
                .InnerJoin<Employee, Employee>((e, dept) => e.Id == dept.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("FROM [dbo].[Employee] AS [Employee_A1]", sql);
            Assert.Contains("INNER JOIN [dbo].[Employee] AS [Employee_A2]", sql);
            Assert.Contains("INNER JOIN [dbo].[Employee] AS [Employee_A3]", sql);
            Assert.Contains("Employee_A1.[ManagerId] = Employee_A2.[Id]", sql);
        }

        [Fact]
        public void RepeatedJoin_WithSelect_LastJoinAliasUsed()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Customer>((o, c) => o.BillingCustomerId == c.Id)
                .Select<Customer>(c => c.Name);

            var sql = qb.BuildQuery();

            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A2]", sql);
            Assert.Contains("INNER JOIN [dbo].[Customer] AS [Customer_A3]", sql);
            Assert.Contains("Customer_A", sql);
            Assert.Contains("[Name]", sql);
        }

        [Fact]
        public void Join_WithWhereOnMainTable_Combined()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .Where(o => o.Id > 100);

            var sql = qb.BuildQuery();

            Assert.Contains("WHERE", sql);
            Assert.Contains("Order_A1.[Id] > @p1", sql);
        }

        [Fact]
        public void Join_WithWhereOnJoinedTable_Combined()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .Where(o => o.CustomerId == 5);

            var sql = qb.BuildQuery();

            Assert.Contains("WHERE", sql);
            Assert.Contains("Order_A1.[CustomerId] =", sql);
        }

        [Fact]
        public void CustomJoin_RawSql_PassedThrough()
        {
            var qb = CreateBuilder<Order>()
                .CustomJoin<Order, Customer>("CROSS JOIN", (o, c) => o.CustomerId == c.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("CROSS JOIN [dbo].[Customer]", sql);
        }

        [Fact]
        public void Join_FiveJoins_AllAliasesIncremented()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Product>((o, p) => o.ProductId == p.Id)
                .LeftJoin<Order, OrderItem>((o, oi) => o.Id == oi.OrderId)
                .LeftJoin<Order, Party>((o, p) => o.Id == p.PartyId)
                .InnerJoin<Order, Employee>((o, e) => o.Id == e.Id);

            var sql = qb.BuildQuery();

            Assert.Contains("Customer_A2", sql);
            Assert.Contains("Product_A3", sql);
            Assert.Contains("OrderItem_A4", sql);
            Assert.Contains("Party_A5", sql);
            Assert.Contains("Employee_A6", sql);
        }

        [Fact]
        public void Join_WithSelectFromMultipleTables_AllColumnsPresent()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                .InnerJoin<Order, Product>((o, p) => o.ProductId == p.Id)
                .Select<Order>(o => o.OrderDate)
                .Select<Customer>(c => c.Name)
                .Select<Product>(p => p.Price);

            var sql = qb.BuildQuery();

            Assert.Contains("Order_A1.[OrderDate] AS OrderDate", sql);
            Assert.Contains("Customer_A2.[Name] AS Name", sql);
            Assert.Contains("Product_A3.[Price] AS Price", sql);
        }

        [Fact]
        public void Join_WithComplexOnCondition_IncludesAllComparisons()
        {
            var qb = CreateBuilder<Order>()
                .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id && o.Id >= 1 && o.Id <= 1000);

            var sql = qb.BuildQuery();

            Assert.Contains(">=", sql);
            Assert.Contains("<=", sql);
            Assert.Contains("AND", sql);
        }
    }
}
