using System;
using System.Linq;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.AliasManager
{
    public class AliasManagerRegressionTests
    {
        [Fact]
        public void FailedTableReassignment_PreservesOriginalRegistryAndMappings()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTypeAlias(typeof(Person), "person");
            manager.SetTableAlias("[dbo].[Party]", "party");
            var before = manager.GetAllTableAliases().OrderBy(x => x.Key).ToArray();

            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("[dbo].[Person]", "party"));

            Assert.Equal(before, manager.GetAllTableAliases().OrderBy(x => x.Key).ToArray());
            Assert.Equal("person", manager.GetAliasForType(typeof(Person)));
            Assert.Equal("person", manager.GetAliasForTable("[dbo].[Person]"));
            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("[dbo].[Order]", "person"));
        }

        [Fact]
        public void FailedTableReassignment_ToSubqueryAlias_PreservesOriginalRegistry()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTableAlias("[dbo].[Person]", "person");
            manager.SetSubQueryAlias(typeof(Party), "party");

            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("[dbo].[Person]", "party"));

            Assert.Contains(manager.GetAllTableAliases(), x => x.Key == "[dbo].[Person]" && x.Value == "person");
            Assert.True(manager.IsSubqueryAlias("party"));
        }

        [Fact]
        public void TableReassignment_UpdatesEveryBoundTypeWithoutTouchingOtherTables()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTypeAlias(typeof(Person), "old");
            Assert.Equal("old", manager.GetAliasForType(typeof(AlternatePerson)));
            manager.SetTypeAlias(typeof(Party), "party");

            manager.SetTableAlias("[dbo].[Person]", "new");

            Assert.Equal("new", manager.GetAliasForTable("[dbo].[Person]"));
            Assert.Equal("new", manager.GetAliasForType(typeof(Person)));
            Assert.Equal("new", manager.GetAliasForType(typeof(AlternatePerson)));
            Assert.Equal("party", manager.GetAliasForType(typeof(Party)));
            manager.SetTableAlias("[dbo].[Order]", "old");
        }

        [Fact]
        public void TypeReassignment_ReleasesOnlyPreviousCanonicalAlias()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTypeAlias(typeof(Person), "old");
            var joinAlias = manager.GetUniqueAliasForType(typeof(Person));

            manager.SetTypeAlias(typeof(Person), "new");

            Assert.Equal("new", manager.GetAliasForType(typeof(Person)));
            Assert.Equal("new", manager.GetAliasForTable("[dbo].[Person]"));
            manager.SetTableAlias("[dbo].[Party]", "old");
            Assert.Contains(manager.GetAllTableAliases(), x => x.Key == "[dbo].[Person]" && x.Value == joinAlias);
            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("[dbo].[Order]", joinAlias));
        }

        [Fact]
        public void ExistingUniqueAlias_CanBecomeCanonicalThroughTypeRegistration()
        {
            var manager = new global::EasyDapper.AliasManager();
            var alias = manager.GetUniqueAliasForType(typeof(Person));

            manager.SetTypeAlias(typeof(Person), alias);

            Assert.Equal(alias, manager.GetAliasForTable("[dbo].[Person]"));
            Assert.Equal(alias, manager.GetAliasForType(typeof(Person)));
            Assert.Single(manager.GetAllTableAliases());
        }

        [Fact]
        public void ExistingUniqueAlias_TypeRolePreservesCanonicalTableAndOtherTypes()
        {
            var manager = new global::EasyDapper.AliasManager();
            var previous = manager.GetAliasForType(typeof(Person));
            var role = manager.GetUniqueAliasForType(typeof(AlternatePerson));

            manager.SetTypeAlias(typeof(AlternatePerson), role);

            Assert.Equal(previous, manager.GetAliasForType(typeof(Person)));
            Assert.Equal(previous, manager.GetAliasForTable("[dbo].[Person]"));
            Assert.Equal(role, manager.GetAliasForType(typeof(AlternatePerson)));
            manager.SetTableAlias("[dbo].[Person]", "renamed");
            Assert.Equal("renamed", manager.GetAliasForType(typeof(Person)));
            Assert.Equal(role, manager.GetAliasForType(typeof(AlternatePerson)));
        }

        [Fact]
        public void RepeatedSubqueryRegistration_KeepsEarlierOccurrenceReserved()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetSubQueryAlias(typeof(Person), "first");
            manager.SetSubQueryAlias(typeof(Person), "second");

            Assert.True(manager.TryGetSubQueryAlias(typeof(Person), out var current));
            Assert.Equal("second", current);
            Assert.True(manager.IsSubqueryAlias("first"));
            Assert.True(manager.IsSubqueryAlias("second"));
            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("[dbo].[Party]", "first"));
        }

        [Fact]
        public void FailedReassignment_ToOuterReservation_DoesNotFreeCurrentAlias()
        {
            var outer = new global::EasyDapper.AliasManager();
            outer.SetTypeAlias(typeof(Person), "outer");
            var inner = new global::EasyDapper.AliasManager();
            inner.ReserveOuterAliases(outer);
            inner.SetTableAlias("[dbo].[Party]", "inner");

            Assert.Throws<InvalidOperationException>(() => inner.SetTableAlias("[dbo].[Party]", "outer"));

            Assert.Contains(inner.GetAllTableAliases(), x => x.Key == "[dbo].[Party]" && x.Value == "inner");
            Assert.DoesNotContain(inner.GetAllTableAliases(), x => x.Value == "outer");
        }

        [Fact]
        public void ReservedOuterAliases_AreSkippedByAllRegisteredGenerators()
        {
            var outer = new global::EasyDapper.AliasManager();
            outer.SetTableAlias("[dbo].[Person]", "Person_A1");
            outer.SetSubQueryAlias(typeof(Person), "Person_SQ1");
            var inner = new global::EasyDapper.AliasManager();
            inner.ReserveOuterAliases(outer);

            Assert.Equal("Person_A2", inner.GetAliasForType(typeof(Person)));
            Assert.Equal("Person_A3", inner.GetUniqueAliasForType(typeof(Person)));
            Assert.Equal("Person_SQ2", inner.GenerateSubQueryAlias("[dbo].[Person]"));
            Assert.False(inner.IsSubqueryAlias("Person_SQ1"));
        }

        [Fact]
        public void SelfReservation_IsANoOpAndClearRemovesReservations()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTableAlias("[dbo].[Person]", "Person_A1");
            manager.ReserveOuterAliases(manager);
            Assert.Single(manager.GetAllTableAliases());
            manager.ClearAliases();
            Assert.Empty(manager.GetAllTableAliases());
            Assert.Equal("Person_A1", manager.GetAliasForTable("[dbo].[Person]"));
        }

        [Table("Person")]
        private sealed class AlternatePerson { }
    }
}
