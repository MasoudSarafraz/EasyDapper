using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace EasyDapper.Tests.AliasManager
{
    public class AliasManagerSelfJoinTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        private void SimulateQueryBuilderConstructor<T>(global::EasyDapper.AliasManager manager)
        {
            var mainTableName = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(T));
            var mainAlias = manager.GenerateAlias(mainTableName);
            manager.SetTableAlias(mainTableName, mainAlias);
            manager.SetTypeAlias(typeof(T), mainAlias);
        }

        private (string leftAlias, string rightAlias) SimulateJoin<TLeft, TRight>(
            global::EasyDapper.AliasManager manager) where TLeft : class where TRight : class
        {
            var leftAlias = manager.GetAliasForType(typeof(TLeft));
            string rightAlias;

            bool isSelfJoin = typeof(TLeft) == typeof(TRight);
            int joinCount = 0;
            bool isRepeatedJoin = joinCount > 0;

            if (isSelfJoin || isRepeatedJoin)
                rightAlias = manager.GetUniqueAliasForType(typeof(TRight));
            else
                rightAlias = manager.GetAliasForType(typeof(TRight));

            return (leftAlias, rightAlias);
        }

        [Fact]
        public void SelfJoin_TwoAliasesAreDistinct()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var (leftAlias, rightAlias) = SimulateJoin<Employee, Employee>(manager);

            Assert.NotEqual(leftAlias, rightAlias);
            Assert.StartsWith("Employee_A", leftAlias);
            Assert.StartsWith("Employee_A", rightAlias);
        }

        [Fact]
        public void SelfJoin_LeftAliasIsThePrimaryFromAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var primaryAlias = manager.GetAliasForType(typeof(Employee));
            var (leftAlias, _) = SimulateJoin<Employee, Employee>(manager);

            Assert.Equal(primaryAlias, leftAlias);
        }

        [Fact]
        public void SelfJoin_RightAliasIsRegisteredInAllAliases()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var (_, rightAlias) = SimulateJoin<Employee, Employee>(manager);

            Assert.False(manager.IsSubqueryAlias(rightAlias));
        }

        [Fact]
        public void SelfJoin_GetAliasForTypeStillReturnsPrimaryAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var primaryAlias = manager.GetAliasForType(typeof(Employee));
            SimulateJoin<Employee, Employee>(manager);

            var aliasAfterSelfJoin = manager.GetAliasForType(typeof(Employee));
            Assert.Equal(primaryAlias, aliasAfterSelfJoin);
        }

        [Fact]
        public void SelfJoin_MultipleTimes_EachCallReturnsFreshAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var (_, alias1) = SimulateJoin<Employee, Employee>(manager);
            var (_, alias2) = SimulateJoin<Employee, Employee>(manager);
            var (_, alias3) = SimulateJoin<Employee, Employee>(manager);

            Assert.NotEqual(alias1, alias2);
            Assert.NotEqual(alias2, alias3);
            Assert.NotEqual(alias1, alias3);
        }

        [Fact]
        public void SelfJoin_PrimaryAliasIsReusableAsTableAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);
            var primaryAlias = manager.GetAliasForType(typeof(Employee));

            Assert.True(manager.TryGetTypeAlias(typeof(Employee), out var alias));
            Assert.Equal(primaryAlias, alias);
        }

        [Fact]
        public void SelfJoin_RightAliasNotInTypeToAliasRegistry()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Employee>(manager);

            var (_, rightAlias) = SimulateJoin<Employee, Employee>(manager);

            Assert.True(manager.TryGetTypeAlias(typeof(Employee), out var typeAlias));
            Assert.NotEqual(rightAlias, typeAlias);
        }
    }

    public class AliasManagerRepeatedJoinTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        private void SimulateQueryBuilderConstructor<T>(global::EasyDapper.AliasManager manager)
        {
            var mainTableName = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(T));
            var mainAlias = manager.GenerateAlias(mainTableName);
            manager.SetTableAlias(mainTableName, mainAlias);
            manager.SetTypeAlias(typeof(T), mainAlias);
        }

        private string SimulateRepeatedJoin<TRight>(global::EasyDapper.AliasManager manager, int existingJoinCount)
            where TRight : class
        {
            bool isRepeatedJoin = existingJoinCount > 0;
            if (isRepeatedJoin)
                return manager.GetUniqueAliasForType(typeof(TRight));
            return manager.GetAliasForType(typeof(TRight));
        }

        [Fact]
        public void RepeatedJoin_FirstCallReturnsPrimaryAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var firstAlias = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 0);

            Assert.True(manager.TryGetTypeAlias(typeof(Customer), out var primaryAlias));
            Assert.Equal(primaryAlias, firstAlias);
        }

        [Fact]
        public void RepeatedJoin_SecondCallReturnsDifferentAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var firstAlias = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 0);
            var secondAlias = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 1);

            Assert.NotEqual(firstAlias, secondAlias);
        }

        [Fact]
        public void RepeatedJoin_ThirdCallReturnsYetAnotherAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var first = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 0);
            var second = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 1);
            var third = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 2);

            Assert.NotEqual(first, second);
            Assert.NotEqual(second, third);
            Assert.NotEqual(first, third);
        }

        [Fact]
        public void RepeatedJoin_PrimaryAliasUnchangedAfterMultipleCalls()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 0);
            SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 1);
            SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 2);

            Assert.True(manager.TryGetTypeAlias(typeof(Customer), out var primaryAlias));
            Assert.StartsWith("Customer_A", primaryAlias);
        }

        [Fact]
        public void RepeatedJoin_AllAliasesRegisteredAsTableNotSubQuery()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var a1 = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 0);
            var a2 = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 1);
            var a3 = SimulateRepeatedJoin<Customer>(manager, existingJoinCount: 2);

            Assert.False(manager.IsSubqueryAlias(a1));
            Assert.False(manager.IsSubqueryAlias(a2));
            Assert.False(manager.IsSubqueryAlias(a3));
        }
    }

    public class AliasManagerApplySubQueryTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        private void SimulateQueryBuilderConstructor<T>(global::EasyDapper.AliasManager manager)
        {
            var mainTableName = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(T));
            var mainAlias = manager.GenerateAlias(mainTableName);
            manager.SetTableAlias(mainTableName, mainAlias);
            manager.SetTypeAlias(typeof(T), mainAlias);
        }

        [Fact]
        public void Apply_SubQueryAliasIsDistinctFromTableAlias()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var tableAlias = manager.GetAliasForType(typeof(OrderItem));
            var subQueryAlias = manager.GenerateSubQueryAlias(global::EasyDapper.QueryBuilderCache.GetTableName(typeof(OrderItem)));
            manager.SetSubQueryAlias(typeof(OrderItem), subQueryAlias);

            Assert.NotEqual(tableAlias, subQueryAlias);
            Assert.StartsWith("OrderItem_A", tableAlias);
            Assert.StartsWith("OrderIte_SQ", subQueryAlias);
        }

        [Fact]
        public void Apply_SubQueryAliasIsRecognizedAsSubQuery()
        {
            var manager = CreateManager();
            var alias = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), alias);

            Assert.True(manager.IsSubqueryAlias(alias));
        }

        [Fact]
        public void Apply_TryGetSubQueryAliasReturnsRegisteredAlias()
        {
            var manager = CreateManager();
            var alias = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), alias);

            Assert.True(manager.TryGetSubQueryAlias(typeof(OrderItem), out var retrieved));
            Assert.Equal(alias, retrieved);
        }

        [Fact]
        public void Apply_SubQueryAndTableAliasForSameTypeBothWork()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var tableAlias = manager.GetAliasForType(typeof(OrderItem));
            var subQueryAlias = manager.GenerateSubQueryAlias(global::EasyDapper.QueryBuilderCache.GetTableName(typeof(OrderItem)));
            manager.SetSubQueryAlias(typeof(OrderItem), subQueryAlias);

            Assert.True(manager.TryGetTypeAlias(typeof(OrderItem), out var retrievedTable));
            Assert.True(manager.TryGetSubQueryAlias(typeof(OrderItem), out var retrievedSubQuery));
            Assert.Equal(tableAlias, retrievedTable);
            Assert.Equal(subQueryAlias, retrievedSubQuery);
            Assert.NotEqual(tableAlias, subQueryAlias);
        }

        [Fact]
        public void Apply_MultipleSubQueriesForSameType_LastOneWins()
        {
            var manager = CreateManager();
            var alias1 = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), alias1);
            var alias2 = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), alias2);

            Assert.True(manager.TryGetSubQueryAlias(typeof(OrderItem), out var retrieved));
            Assert.Equal(alias2, retrieved);
            Assert.NotEqual(alias1, retrieved);
        }

        [Fact]
        public void Apply_SubQueryAliasDoesNotAffectTableAliasLookup()
        {
            var manager = CreateManager();
            SimulateQueryBuilderConstructor<Order>(manager);

            var tableAlias = manager.GetAliasForType(typeof(OrderItem));
            var subQueryAlias = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), subQueryAlias);

            Assert.True(manager.TryGetTypeAlias(typeof(OrderItem), out var tableLookup));
            Assert.Equal(tableAlias, tableLookup);
            Assert.NotEqual(subQueryAlias, tableLookup);
        }
    }

    public class AliasManagerEdgeCaseTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        [Fact]
        public void GenerateAlias_Exactly10Characters_NotTruncated()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[0123456789]");
            Assert.Equal("0123456789_A1", alias);
        }

        [Fact]
        public void GenerateAlias_11Characters_TruncatedTo10()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[01234567890]");
            Assert.Equal("0123456789_A1", alias);
        }

        [Fact]
        public void GenerateAlias_SingleCharacterTable_ProducesValidAlias()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[X]");
            Assert.Equal("X_A1", alias);
        }

        [Fact]
        public void GenerateAlias_TableWithSchema_UsesOnlyTableName()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[sales].[CustomerOrders]");
            Assert.StartsWith("CustomerOr_A", alias);
            Assert.DoesNotContain("sales", alias);
        }

        [Fact]
        public void GenerateAlias_NameWithBrackets_StripsBrackets()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[MyTable]");
            Assert.Equal("MyTable_A1", alias);
        }

        [Fact]
        public void GenerateAlias_NameWithoutBrackets_StillWorks()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("dbo.MyTable");
            Assert.Equal("MyTable_A1", alias);
        }

        [Fact]
        public void GenerateSubQueryAlias_Exactly8Characters_NotTruncated()
        {
            var manager = CreateManager();
            var alias = manager.GenerateSubQueryAlias("[dbo].[01234567]");
            Assert.Equal("01234567_SQ1", alias);
        }

        [Fact]
        public void GenerateSubQueryAlias_9Characters_TruncatedTo8()
        {
            var manager = CreateManager();
            var alias = manager.GenerateSubQueryAlias("[dbo].[012345678]");
            Assert.Equal("01234567_SQ1", alias);
        }

        [Fact]
        public void GenerateAlias_ThousandAliases_AllUnique()
        {
            var manager = CreateManager();
            var aliases = new HashSet<string>();
            for (int i = 0; i < 1000; i++)
            {
                var alias = manager.GenerateAlias("[dbo].[Person]");
                Assert.True(aliases.Add(alias), $"Duplicate alias at iteration {i}: {alias}");
            }
            Assert.Equal(1000, aliases.Count);
        }

        [Fact]
        public void GenerateSubQueryAlias_ThousandAliases_AllUnique()
        {
            var manager = CreateManager();
            var aliases = new HashSet<string>();
            for (int i = 0; i < 1000; i++)
            {
                var alias = manager.GenerateSubQueryAlias("[dbo].[Person]");
                Assert.True(aliases.Add(alias), $"Duplicate subquery alias at iteration {i}: {alias}");
            }
            Assert.Equal(1000, aliases.Count);
        }

        [Fact]
        public void GenerateAlias_MixedTables_AllUnique()
        {
            var manager = CreateManager();
            var aliases = new HashSet<string>();
            var tableNames = new[]
            {
                "[dbo].[Person]", "[dbo].[Party]", "[dbo].[Order]",
                "[dbo].[Customer]", "[dbo].[Product]", "[dbo].[OrderItem]",
                "[dbo].[Employee]", "[dbo].[Department]", "[dbo].[Address]"
            };
            for (int i = 0; i < 100; i++)
            {
                var tn = tableNames[i % tableNames.Length];
                Assert.True(aliases.Add(manager.GenerateAlias(tn)));
            }
        }

        [Fact]
        public void SetTableAlias_SameAliasForSameTable_IsIdempotent()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Person]", "p1");

            Assert.Equal("p1", manager.GetAliasForTable("[dbo].[Person]"));
        }

        [Fact]
        public void SetTypeAlias_SameAliasForSameType_IsIdempotent()
        {
            var manager = CreateManager();
            manager.SetTypeAlias(typeof(Person), "p1");
            manager.SetTypeAlias(typeof(Person), "p1");

            Assert.Equal("p1", manager.GetAliasForType(typeof(Person)));
        }

        [Fact]
        public void SetSubQueryAlias_SameAliasForSameType_IsIdempotent()
        {
            var manager = CreateManager();
            manager.SetSubQueryAlias(typeof(Person), "p1");
            manager.SetSubQueryAlias(typeof(Person), "p1");

            Assert.True(manager.TryGetSubQueryAlias(typeof(Person), out var alias));
            Assert.Equal("p1", alias);
        }

        [Fact]
        public void SetSubQueryAlias_DuplicateAliasForDifferentType_Throws()
        {
            var manager = CreateManager();
            manager.SetSubQueryAlias(typeof(Person), "sq1");
            Assert.Throws<InvalidOperationException>(
                () => manager.SetSubQueryAlias(typeof(Party), "sq1"));
        }

        [Fact]
        public void SetSubQueryAlias_DuplicateAliasForTable_Throws()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "x1");
            Assert.Throws<InvalidOperationException>(
                () => manager.SetSubQueryAlias(typeof(Party), "x1"));
        }

        [Fact]
        public void SetTableAlias_DuplicateAliasForSubQuery_Throws()
        {
            var manager = CreateManager();
            manager.SetSubQueryAlias(typeof(Person), "x1");
            Assert.Throws<InvalidOperationException>(
                () => manager.SetTableAlias("[dbo].[Party]", "x1"));
        }

        [Fact]
        public void GetAliasForTable_DifferentSchemas_TreatedAsDifferentTables()
        {
            var manager = CreateManager();
            var a1 = manager.GetAliasForTable("[dbo].[Person]");
            var a2 = manager.GetAliasForTable("[sales].[Person]");
            Assert.NotEqual(a1, a2);
        }

        [Fact]
        public void GetAliasForType_DifferentTypes_ReturnDifferentAliases()
        {
            var manager = CreateManager();
            var a1 = manager.GetAliasForType(typeof(Person));
            var a2 = manager.GetAliasForType(typeof(Party));
            var a3 = manager.GetAliasForType(typeof(Order));
            Assert.NotEqual(a1, a2);
            Assert.NotEqual(a2, a3);
            Assert.NotEqual(a1, a3);
        }

        [Fact]
        public void ClearAliases_ResetsCounters()
        {
            var manager = CreateManager();
            manager.GenerateAlias("[dbo].[Foo]");
            manager.GenerateAlias("[dbo].[Foo]");
            manager.GenerateSubQueryAlias("[dbo].[Foo]");

            manager.ClearAliases();

            var alias = manager.GenerateAlias("[dbo].[Foo]");
            Assert.Equal("Foo_A1", alias);
        }

        [Fact]
        public void ClearAliases_AllowsReusingOldAliases()
        {
            var manager = CreateManager();
            var firstAlias = manager.GenerateAlias("[dbo].[Foo]");
            manager.SetTableAlias("[dbo].[Foo]", firstAlias);

            manager.ClearAliases();

            var secondAlias = manager.GenerateAlias("[dbo].[Foo]");
            Assert.Equal(firstAlias, secondAlias);
            manager.SetTableAlias("[dbo].[Foo]", secondAlias);
        }

        [Fact]
        public void GetAllTableAliases_AfterClear_ReturnsEmpty()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Party]", "p2");

            manager.ClearAliases();

            Assert.Empty(manager.GetAllTableAliases());
        }
    }

    public class AliasManagerAliasLifecycleTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        [Fact]
        public void Lifecycle_FullQuerySimulation_AliasesConsistent()
        {
            var manager = CreateManager();

            var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Order));
            var mainAlias = manager.GenerateAlias(mainTable);
            manager.SetTableAlias(mainTable, mainAlias);
            manager.SetTypeAlias(typeof(Order), mainAlias);

            var customerAlias = manager.GetAliasForType(typeof(Customer));
            var productAlias = manager.GetAliasForType(typeof(Product));

            var orderItemSubAlias = manager.GenerateSubQueryAlias(
                global::EasyDapper.QueryBuilderCache.GetTableName(typeof(OrderItem)));
            manager.SetSubQueryAlias(typeof(OrderItem), orderItemSubAlias);

            Assert.Equal(mainAlias, manager.GetAliasForType(typeof(Order)));
            Assert.Equal(customerAlias, manager.GetAliasForType(typeof(Customer)));
            Assert.Equal(productAlias, manager.GetAliasForType(typeof(Product)));
            Assert.True(manager.TryGetSubQueryAlias(typeof(OrderItem), out var subAlias));
            Assert.Equal(orderItemSubAlias, subAlias);

            Assert.NotEqual(mainAlias, customerAlias);
            Assert.NotEqual(customerAlias, productAlias);
            Assert.NotEqual(productAlias, orderItemSubAlias);
        }

        [Fact]
        public void Lifecycle_SelfJoinWithApply_AliasesConsistent()
        {
            var manager = CreateManager();

            var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Employee));
            var mainAlias = manager.GenerateAlias(mainTable);
            manager.SetTableAlias(mainTable, mainAlias);
            manager.SetTypeAlias(typeof(Employee), mainAlias);

            var selfJoinAlias = manager.GetUniqueAliasForType(typeof(Employee));

            var subAlias = manager.GenerateSubQueryAlias(mainTable);
            manager.SetSubQueryAlias(typeof(Employee), subAlias);

            Assert.Equal(mainAlias, manager.GetAliasForType(typeof(Employee)));
            Assert.NotEqual(mainAlias, selfJoinAlias);
            Assert.NotEqual(mainAlias, subAlias);
            Assert.NotEqual(selfJoinAlias, subAlias);

            Assert.True(manager.TryGetSubQueryAlias(typeof(Employee), out var retrievedSub));
            Assert.Equal(subAlias, retrievedSub);
        }

        [Fact]
        public void Lifecycle_RepeatedJoinWithApply_AliasesConsistent()
        {
            var manager = CreateManager();

            var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Order));
            var mainAlias = manager.GenerateAlias(mainTable);
            manager.SetTableAlias(mainTable, mainAlias);
            manager.SetTypeAlias(typeof(Order), mainAlias);

            var customer1 = manager.GetAliasForType(typeof(Customer));
            var customer2 = manager.GetUniqueAliasForType(typeof(Customer));

            var subAlias = manager.GenerateSubQueryAlias(
                global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Customer)));
            manager.SetSubQueryAlias(typeof(Customer), subAlias);

            Assert.Equal(customer1, manager.GetAliasForType(typeof(Customer)));
            Assert.NotEqual(customer1, customer2);
            Assert.NotEqual(customer1, subAlias);
            Assert.NotEqual(customer2, subAlias);
        }

        [Fact]
        public void Lifecycle_UnionScenario_AliasesIndependent()
        {
            var manager1 = new global::EasyDapper.AliasManager();
            var manager2 = new global::EasyDapper.AliasManager();

            var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Person));

            var alias1 = manager1.GenerateAlias(mainTable);
            manager1.SetTableAlias(mainTable, alias1);
            manager1.SetTypeAlias(typeof(Person), alias1);

            var alias2 = manager2.GenerateAlias(mainTable);
            manager2.SetTableAlias(mainTable, alias2);
            manager2.SetTypeAlias(typeof(Person), alias2);

            Assert.Equal(alias1, alias2);
            Assert.Equal(alias1, manager1.GetAliasForType(typeof(Person)));
            Assert.Equal(alias2, manager2.GetAliasForType(typeof(Person)));
        }
    }

    public class AliasManagerComplexScenarioTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        [Fact]
        public void ComplexScenario_DeepNestedApplyChain()
        {
            var manager = CreateManager();
            var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Order));
            var mainAlias = manager.GenerateAlias(mainTable);
            manager.SetTableAlias(mainTable, mainAlias);
            manager.SetTypeAlias(typeof(Order), mainAlias);

            var sub1 = manager.GenerateSubQueryAlias(
                global::EasyDapper.QueryBuilderCache.GetTableName(typeof(OrderItem)));
            manager.SetSubQueryAlias(typeof(OrderItem), sub1);

            var sub2 = manager.GenerateSubQueryAlias(
                global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Product)));
            manager.SetSubQueryAlias(typeof(Product), sub2);

            var sub3 = manager.GenerateSubQueryAlias(
                global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Customer)));
            manager.SetSubQueryAlias(typeof(Customer), sub3);

            Assert.True(manager.IsSubqueryAlias(sub1));
            Assert.True(manager.IsSubqueryAlias(sub2));
            Assert.True(manager.IsSubqueryAlias(sub3));
            Assert.NotEqual(sub1, sub2);
            Assert.NotEqual(sub2, sub3);
            Assert.NotEqual(sub1, sub3);
        }

        [Fact]
        public void ComplexScenario_MultipleSelfJoinsWithDifferentTypes()
        {
            var manager = CreateManager();

            foreach (var type in new[] { typeof(Employee), typeof(Person), typeof(Order) })
            {
                var mainTable = global::EasyDapper.QueryBuilderCache.GetTableName(type);
                var mainAlias = manager.GenerateAlias(mainTable);
                manager.SetTableAlias(mainTable, mainAlias);
                manager.SetTypeAlias(type, mainAlias);

                var selfJoinAlias1 = manager.GetUniqueAliasForType(type);
                var selfJoinAlias2 = manager.GetUniqueAliasForType(type);

                Assert.NotEqual(mainAlias, selfJoinAlias1);
                Assert.NotEqual(mainAlias, selfJoinAlias2);
                Assert.NotEqual(selfJoinAlias1, selfJoinAlias2);
            }
        }

        [Fact]
        public void ComplexScenario_MixedTableTypeAndSubQueryAliases()
        {
            var manager = CreateManager();

            var t1 = manager.GetAliasForType(typeof(Person));
            var t2 = manager.GetAliasForType(typeof(Party));
            var t3 = manager.GetAliasForType(typeof(Order));

            var u1 = manager.GetUniqueAliasForType(typeof(Person));
            var u2 = manager.GetUniqueAliasForType(typeof(Person));

            var sq1 = manager.GenerateSubQueryAlias("[dbo].[OrderItem]");
            manager.SetSubQueryAlias(typeof(OrderItem), sq1);

            var sq2 = manager.GenerateSubQueryAlias("[dbo].[Customer]");
            manager.SetSubQueryAlias(typeof(Customer), sq2);

            Assert.Equal(5, manager.GetAllTableAliases().Count());
            Assert.Equal(3, manager.GetAllTableAliases().Count(kvp => kvp.Key.ToString().Contains("Person")));
            Assert.NotEqual(t1, u1);
            Assert.NotEqual(u1, u2);
            Assert.NotEqual(sq1, sq2);
            Assert.True(manager.IsSubqueryAlias(sq1));
            Assert.True(manager.IsSubqueryAlias(sq2));
            Assert.False(manager.IsSubqueryAlias(t1));
            Assert.False(manager.IsSubqueryAlias(u1));
        }

        [Fact]
        public void ComplexScenario_AliasCollisionsAvoided()
        {
            var manager = CreateManager();
            var allAliases = new HashSet<string>();

            for (int i = 0; i < 100; i++)
            {
                var alias = manager.GenerateAlias("[dbo].[Foo]");
                Assert.True(allAliases.Add(alias), $"Collision at iteration {i}: {alias}");
            }

            for (int i = 0; i < 100; i++)
            {
                var alias = manager.GenerateSubQueryAlias("[dbo].[Foo]");
                Assert.True(allAliases.Add(alias), $"Collision at SQ iteration {i}: {alias}");
            }

            Assert.Equal(200, allAliases.Count);
        }

        [Fact]
        public void ComplexScenario_TableAliasSwitchPreservesOthers()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Party]", "party1");
            manager.SetTableAlias("[dbo].[Order]", "o1");

            manager.SetTableAlias("[dbo].[Person]", "p2");

            Assert.Equal("p2", manager.GetAliasForTable("[dbo].[Person]"));
            Assert.Equal("party1", manager.GetAliasForTable("[dbo].[Party]"));
            Assert.Equal("o1", manager.GetAliasForTable("[dbo].[Order]"));
        }

        [Fact]
        public void ComplexScenario_SwitchToAliasUsedByAnotherTable_Throws()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Party]", "party1");

            Assert.Throws<InvalidOperationException>(
                () => manager.SetTableAlias("[dbo].[Person]", "party1"));
        }
    }
}
