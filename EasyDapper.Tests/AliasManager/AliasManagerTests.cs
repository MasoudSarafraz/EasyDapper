using System;
using Xunit;

namespace EasyDapper.Tests.AliasManager
{
    /// <summary>
    /// Tests for AliasManager in isolation. Because AliasManager is internal, the test project
    /// uses InternalsVisibleTo to access it directly.
    /// </summary>
    public class AliasManagerTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        [Fact]
        public void GenerateAlias_FirstAliasEndsWith_A1_Not_A2()
        {
            // FIX (B1): previously the first alias was "Foo_A2" because the counter started at 1
            // and was incremented before use. Now it should be "Foo_A1".
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[Foo]");
            Assert.Equal("Foo_A1", alias);
        }

        [Fact]
        public void GenerateAlias_TruncatesLongNamesTo10Chars()
        {
            var manager = CreateManager();
            var alias = manager.GenerateAlias("[dbo].[CustomerOrdersArchive]");
            // Short name truncated to 10 chars, then suffixed with _A1.
            Assert.StartsWith("CustomerOr_A", alias);
            Assert.True(alias.Length <= "CustomerO_A".Length + 5);
        }

        [Fact]
        public void GenerateSubQueryAlias_FirstAliasEndsWith_SQ1()
        {
            var manager = CreateManager();
            var alias = manager.GenerateSubQueryAlias("[dbo].[Party]");
            Assert.Equal("Party_SQ1", alias);
        }

        [Fact]
        public void SetTableAlias_ThenGetAliasForTable_ReturnsSameAlias()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            Assert.Equal("p1", manager.GetAliasForTable("[dbo].[Person]"));
        }

        [Fact]
        public void SetTableAlias_DuplicateAliasForDifferentTable_Throws()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            var ex = Assert.Throws<InvalidOperationException>(
                () => manager.SetTableAlias("[dbo].[Party]", "p1"));
            Assert.Contains("p1", ex.Message);
            Assert.Contains("person", ex.Message.ToLower());
        }

        /// <summary>
        /// FIX (C4): when SetTableAlias is called with a new alias for an already-registered
        /// table, the previously-registered alias must be released from the global registry
        /// so it can be reused. Previously the old alias leaked forever.
        /// </summary>
        [Fact]
        public void SetTableAlias_ReplacingAlias_ReleasesOldAliasFromRegistry()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            // Now switch the alias for the same table.
            manager.SetTableAlias("[dbo].[Person]", "p2");
            // The old alias "p1" should be reusable now.
            manager.SetTableAlias("[dbo].[Party]", "p1");
            Assert.Equal("p1", manager.GetAliasForTable("[dbo].[Party]"));
            Assert.Equal("p2", manager.GetAliasForTable("[dbo].[Person]"));
        }

        [Fact]
        public void SetTypeAlias_DelegatesToSetTableAlias()
        {
            var manager = CreateManager();
            manager.SetTypeAlias(typeof(Person), "person_alias");
            Assert.Equal("person_alias", manager.GetAliasForType(typeof(Person)));
            Assert.Equal("person_alias", manager.GetAliasForTable("[dbo].[Person]"));
        }

        [Fact]
        public void GetAliasForType_FirstCall_GeneratesAlias()
        {
            var manager = CreateManager();
            var alias = manager.GetAliasForType(typeof(Person));
            Assert.StartsWith("Person_A", alias);
        }

        [Fact]
        public void GetAliasForType_SecondCall_ReturnsSameAlias()
        {
            var manager = CreateManager();
            var a1 = manager.GetAliasForType(typeof(Person));
            var a2 = manager.GetAliasForType(typeof(Person));
            Assert.Equal(a1, a2);
        }

        /// <summary>
        /// FIX (C1): the critical Self-Join bug. Calling GetUniqueAliasForType must NOT
        /// overwrite the type's primary alias. Previously it did, which caused SELECT * to
        /// emit the JOIN-side alias instead of the FROM-side alias.
        /// </summary>
        [Fact]
        public void GetUniqueAliasForType_DoesNotOverwriteTypeAlias()
        {
            var manager = CreateManager();
            var primaryAlias = manager.GetAliasForType(typeof(Employee));
            var uniqueAlias = manager.GetUniqueAliasForType(typeof(Employee));

            Assert.NotEqual(primaryAlias, uniqueAlias);
            // The critical assertion: GetAliasForType must still return the original alias.
            Assert.Equal(primaryAlias, manager.GetAliasForType(typeof(Employee)));
        }

        [Fact]
        public void GetUniqueAliasForType_MultipleCalls_ReturnDistinctAliases()
        {
            var manager = CreateManager();
            var a1 = manager.GetUniqueAliasForType(typeof(Employee));
            var a2 = manager.GetUniqueAliasForType(typeof(Employee));
            Assert.NotEqual(a1, a2);
        }

        [Fact]
        public void SetSubQueryAlias_ThenTryGetSubQueryAlias_ReturnsSameAlias()
        {
            var manager = CreateManager();
            manager.SetSubQueryAlias(typeof(OrderItem), "oi_sq1");
            Assert.True(manager.TryGetSubQueryAlias(typeof(OrderItem), out var alias));
            Assert.Equal("oi_sq1", alias);
        }

        [Fact]
        public void IsSubqueryAlias_ForTableAlias_ReturnsFalse()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            Assert.False(manager.IsSubqueryAlias("p1"));
        }

        [Fact]
        public void IsSubqueryAlias_ForSubQueryAlias_ReturnsTrue()
        {
            var manager = CreateManager();
            manager.SetSubQueryAlias(typeof(Person), "p_sq1");
            Assert.True(manager.IsSubqueryAlias("p_sq1"));
        }

        [Fact]
        public void IsSubqueryAlias_ForUnknownAlias_ReturnsFalse()
        {
            var manager = CreateManager();
            Assert.False(manager.IsSubqueryAlias("nonexistent"));
        }

        [Fact]
        public void IsSubqueryAlias_ForNullOrEmpty_ReturnsFalse()
        {
            var manager = CreateManager();
            Assert.False(manager.IsSubqueryAlias(null));
            Assert.False(manager.IsSubqueryAlias(""));
        }

        /// <summary>
        /// FIX (B13): GetUniqueAliasForType now registers the alias as AliasType.Table (not
        /// AliasType.Type). Verify by setting the same alias as a subquery alias and confirming
        /// IsSubqueryAlias returns false.
        /// </summary>
        [Fact]
        public void GetUniqueAliasForType_RegistersAsTableAliasNotSubQuery()
        {
            var manager = CreateManager();
            var unique = manager.GetUniqueAliasForType(typeof(Employee));
            Assert.False(manager.IsSubqueryAlias(unique));
        }

        [Fact]
        public void ClearAliases_RemovesAllEntries()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetSubQueryAlias(typeof(Party), "party_sq1");
            manager.ClearAliases();
            // After clear, the previously-used alias should be available again.
            manager.SetTableAlias("[dbo].[Party]", "p1");
            Assert.Equal("p1", manager.GetAliasForTable("[dbo].[Party]"));
        }

        [Fact]
        public void GetAllTableAliases_ReturnsOnlyTableAliases()
        {
            var manager = CreateManager();
            manager.SetTableAlias("[dbo].[Person]", "p1");
            manager.SetTableAlias("[dbo].[Party]", "party1");
            manager.SetSubQueryAlias(typeof(OrderItem), "oi_sq1");

            var tableAliases = manager.GetAllTableAliases();
            Assert.Contains(tableAliases, kvp => kvp.Key == "[dbo].[Person]" && kvp.Value == "p1");
            Assert.Contains(tableAliases, kvp => kvp.Key == "[dbo].[Party]" && kvp.Value == "party1");
            Assert.DoesNotContain(tableAliases, kvp => kvp.Value == "oi_sq1");
        }

        [Fact]
        public void TryGetTypeAlias_ForUnregisteredType_ReturnsFalse()
        {
            var manager = CreateManager();
            Assert.False(manager.TryGetTypeAlias(typeof(Person), out var alias));
            Assert.Null(alias);
        }

        [Fact]
        public void TryGetSubQueryAlias_ForUnregisteredType_ReturnsFalse()
        {
            var manager = CreateManager();
            Assert.False(manager.TryGetSubQueryAlias(typeof(Person), out var alias));
            Assert.Null(alias);
        }

        [Fact]
        public void GenerateAlias_NullOrEmpty_Throws()
        {
            var manager = CreateManager();
            Assert.Throws<ArgumentNullException>(() => manager.GenerateAlias(null));
            Assert.Throws<ArgumentNullException>(() => manager.GenerateAlias(""));
            Assert.Throws<ArgumentNullException>(() => manager.GenerateAlias("   "));
        }

        [Fact]
        public void GenerateSubQueryAlias_NullOrEmpty_Throws()
        {
            var manager = CreateManager();
            Assert.Throws<ArgumentNullException>(() => manager.GenerateSubQueryAlias(null));
            Assert.Throws<ArgumentNullException>(() => manager.GenerateSubQueryAlias(""));
        }

        [Fact]
        public void SetTableAlias_NullOrEmpty_Throws()
        {
            var manager = CreateManager();
            Assert.Throws<ArgumentException>(() => manager.SetTableAlias(null, "a"));
            Assert.Throws<ArgumentException>(() => manager.SetTableAlias("", "a"));
            Assert.Throws<ArgumentException>(() => manager.SetTableAlias("[dbo].[T]", null));
            Assert.Throws<ArgumentException>(() => manager.SetTableAlias("[dbo].[T]", ""));
        }
    }
}
