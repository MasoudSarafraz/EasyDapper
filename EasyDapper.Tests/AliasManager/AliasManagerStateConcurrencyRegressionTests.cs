using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.AliasManager
{
    public class AliasManagerStateConcurrencyRegressionTests
    {
        [Fact]
        public void PrivateCorrelationReservation_PreservesOwnersAndGeneratedNumbering()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTypeAlias(typeof(SharedPrimary), "owned");

            Assert.False(manager.ReserveAliasName("OWNED"));
            Assert.True(manager.ReserveAliasName("__EasyDapperCorrelation_C1"));
            Assert.False(manager.ReserveAliasName("__easydappercorrelation_c1"));
            Assert.Equal("owned", manager.GetAliasForType(typeof(SharedPrimary)));
            Assert.Single(manager.GetAllTableAliases());
            Assert.Equal("Other_A1", manager.GenerateAlias("Other"));
            Assert.Equal("Other_SQ1", manager.GenerateSubQueryAlias("Other"));
            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("Other", "__EasyDapperCorrelation_C1"));

            manager.ClearAliases();
            Assert.True(manager.ReserveAliasName("__EasyDapperCorrelation_C1"));
        }

        [Fact]
        public void ConcurrentPrivateCorrelationReservations_HaveOneOwner()
        {
            var manager = new global::EasyDapper.AliasManager();
            var winners = 0;
            Parallel.For(0, 64, index =>
            {
                if (manager.ReserveAliasName(index % 2 == 0 ? "__EasyDapperCorrelation_C1" : "__easydappercorrelation_c1"))
                    Interlocked.Increment(ref winners);
            });

            Assert.Equal(1, winners);
            Assert.Empty(manager.GetAllTableAliases());
            Assert.Throws<InvalidOperationException>(() => manager.SetTableAlias("Other", "__EasyDapperCorrelation_C1"));
        }

        [Fact]
        public void ConcurrentReservationAndRegistration_HaveOnlyConsistentWinnerStates()
        {
            var outer = new global::EasyDapper.AliasManager();
            outer.SetTableAlias("[dbo].[Parent]", "shared");
            for (int trial = 0; trial < 1000; trial++)
            {
                var inner = new global::EasyDapper.AliasManager();
                var registered = false;
                using (var start = new Barrier(2))
                {
                    var registration = Task.Run(() =>
                    {
                        if (!start.SignalAndWait(TimeSpan.FromSeconds(5))) throw new TimeoutException();
                        try { inner.SetTableAlias("[dbo].[Child]", "shared"); registered = true; }
                        catch (InvalidOperationException) { }
                    });
                    var reservation = Task.Run(() =>
                    {
                        if (!start.SignalAndWait(TimeSpan.FromSeconds(5))) throw new TimeoutException();
                        inner.ReserveOuterAliases(outer);
                    });
                    Assert.True(Task.WaitAll(new[] { registration, reservation }, TimeSpan.FromSeconds(5)), "Registration and reservation must finish.");
                }

                // Either registration owns the name, or the outer reservation
                // rejects it. A successful lookup without an inventory entry
                // is never a valid outcome, regardless of which thread wins.
                if (registered)
                {
                    Assert.Equal("shared", inner.GetAliasForTable("[dbo].[Child]"));
                    var entry = Assert.Single(inner.GetAllTableAliases());
                    Assert.Equal("[dbo].[Child]", entry.Key);
                    Assert.Equal("shared", entry.Value);
                }
                else
                {
                    Assert.Empty(inner.GetAllTableAliases());
                    var generated = inner.GetAliasForTable("[dbo].[Child]");
                    Assert.NotEqual("shared", generated);
                    Assert.Contains(inner.GetAllTableAliases(), entry => entry.Key == "[dbo].[Child]" && entry.Value == generated);
                }
            }
        }

        [Fact]
        public async Task ReverseReservations_FinishWithoutNestedManagerLockDeadlock()
        {
            var first = new global::EasyDapper.AliasManager();
            var second = new global::EasyDapper.AliasManager();
            first.SetTableAlias("[dbo].[First]", "first");
            second.SetTableAlias("[dbo].[Second]", "second");
            using (var start = new Barrier(2))
            {
                var left = Task.Run(() =>
                {
                    if (!start.SignalAndWait(TimeSpan.FromSeconds(5))) throw new TimeoutException();
                    for (int i = 0; i < 2000; i++) first.ReserveOuterAliases(second);
                });
                var right = Task.Run(() =>
                {
                    if (!start.SignalAndWait(TimeSpan.FromSeconds(5))) throw new TimeoutException();
                    for (int i = 0; i < 2000; i++) second.ReserveOuterAliases(first);
                });
                var complete = Task.WhenAll(left, right);
                Assert.Same(complete, await Task.WhenAny(complete, Task.Delay(TimeSpan.FromSeconds(5))));
                await complete;
            }

            Assert.Equal("first", first.GetAliasForTable("[dbo].[First]"));
            Assert.Equal("second", second.GetAliasForTable("[dbo].[Second]"));
            Assert.Equal("first", Assert.Single(first.GetAllTableAliases()).Value);
            Assert.Equal("second", Assert.Single(second.GetAllTableAliases()).Value);
        }

        [Fact]
        public void ParallelCanonicalRenames_RefreshBoundTypesAndPreserveDistinctRoles()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTypeAlias(typeof(SharedPrimary), "initial");
            Assert.Equal("initial", manager.GetAliasForType(typeof(SharedProjection)));
            var roleAlias = manager.GetUniqueAliasForType(typeof(SharedJoinedRole));
            manager.SetTypeAlias(typeof(SharedJoinedRole), roleAlias);
            Assert.Equal("initial", manager.GetAliasForTable("[dbo].[AliasState]"));

            Parallel.For(0, 1000, i => manager.SetTableAlias("[dbo].[AliasState]", "renamed_" + i));

            var canonical = manager.GetAliasForTable("[dbo].[AliasState]");
            Assert.Equal(canonical, manager.GetAliasForType(typeof(SharedPrimary)));
            Assert.Equal(canonical, manager.GetAliasForType(typeof(SharedProjection)));
            Assert.Equal(roleAlias, manager.GetAliasForType(typeof(SharedJoinedRole)));
            var entries = manager.GetAllTableAliases().ToList();
            Assert.Equal(2, entries.Count);
            Assert.All(entries, entry => Assert.Equal("[dbo].[AliasState]", entry.Key));
            Assert.Contains(entries, entry => entry.Value == canonical);
            Assert.Contains(entries, entry => entry.Value == roleAlias);
        }

        [Fact]
        public void ExplicitCaseDistinctAliasesRemainValid_WhileGenerationAvoidsTheirNames()
        {
            var manager = new global::EasyDapper.AliasManager();
            manager.SetTableAlias("[dbo].[First]", "Foo_A1");
            manager.SetTableAlias("[dbo].[Second]", "foo_a1");
            manager.SetSubQueryAlias(typeof(SharedPrimary), "Foo_SQ1");
            manager.SetSubQueryAlias(typeof(SharedProjection), "foo_sq1");

            Assert.Equal("Foo_A1", manager.GetAliasForTable("[dbo].[First]"));
            Assert.Equal("foo_a1", manager.GetAliasForTable("[dbo].[Second]"));
            Assert.Equal(2, manager.GetAllTableAliases().Count());
            var tableCandidate = manager.GenerateAlias("[dbo].[Foo]");
            var subqueryCandidate = manager.GenerateSubQueryAlias("[dbo].[Foo]");
            Assert.False(string.Equals("Foo_A1", tableCandidate, StringComparison.OrdinalIgnoreCase));
            Assert.False(string.Equals("Foo_SQ1", subqueryCandidate, StringComparison.OrdinalIgnoreCase));
            Assert.True(manager.TryGetSubQueryAlias(typeof(SharedPrimary), out var firstSubquery));
            Assert.True(manager.TryGetSubQueryAlias(typeof(SharedProjection), out var secondSubquery));
            Assert.Equal("Foo_SQ1", firstSubquery);
            Assert.Equal("foo_sq1", secondSubquery);
        }

        [Fact]
        public void GeneratedNames_AvoidReservedOuterNamesAcrossCaseDifferences()
        {
            var outer = new global::EasyDapper.AliasManager();
            outer.SetTableAlias("[dbo].[Parent]", "foo_a1");
            outer.SetSubQueryAlias(typeof(SharedPrimary), "foo_sq1");
            var inner = new global::EasyDapper.AliasManager();
            inner.ReserveOuterAliases(outer);
            Assert.False(string.Equals("foo_a1", inner.GenerateAlias("[dbo].[Foo]"), StringComparison.OrdinalIgnoreCase));
            Assert.False(string.Equals("foo_sq1", inner.GenerateSubQueryAlias("[dbo].[Foo]"), StringComparison.OrdinalIgnoreCase));
            Assert.Empty(inner.GetAllTableAliases());
        }

        [Table("AliasState")]
        private class SharedPrimary { public int Id { get; set; } }
        [Table("AliasState")]
        private class SharedProjection { public int Id { get; set; } }
        [Table("AliasState")]
        private class SharedJoinedRole { public int Id { get; set; } }
    }
}
