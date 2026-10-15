using System;
using System.Linq;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Crud
{
    public class CrudRegressionTests
    {
        [Fact]
        public void AttachedEntities_WithSameIdInDifferentTypes_UpdateIndependently()
        {
            var spy = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(spy))
            {
                var person = new TrackedPerson { Id = 1, Name = "person" };
                var order = new TrackedOrder { Id = 1, Name = "order" };
                service.Attach(person);
                service.Attach(order);
                person.Name = "person changed";
                order.Name = "order changed";

                Assert.Equal(1, service.Update(person));
                Assert.Equal(1, service.Update(order));
                Assert.Equal(2, spy.ExecutedCommands.Count);
                Assert.Contains("[TrackedPerson]", spy.ExecutedCommands[0].CommandText);
                Assert.Contains("[TrackedOrder]", spy.ExecutedCommands[1].CommandText);
            }
        }

        [Fact]
        public void Detach_OneType_DoesNotRemoveOtherTypeWithSameId()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "person" };
            var order = new TrackedOrder { Id = 1, Name = "order" };
            tracker.Attach(person);
            tracker.Attach(order);
            tracker.Detach(order);
            var key = tracker.CreateCompositeKey(person, typeof(TrackedPerson).GetProperties()
                .Where(p => p.Name == "Id").ToList());
            Assert.True(tracker.TryGetAttached(key, out var snapshot));
            Assert.IsType<TrackedPerson>(snapshot);
        }

        [Fact]
        public void TrackedUpdate_RevertingOriginalValue_ExecutesSecondUpdate()
        {
            var spy = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(spy))
            {
                var person = new TrackedPerson { Id = 1, Name = "A" };
                service.Attach(person);
                person.Name = "B";
                Assert.Equal(1, service.Update(person));
                Assert.Equal(0, service.Update(person));
                person.Name = "A";
                Assert.Equal(1, service.Update(person));
                Assert.Equal(2, spy.ExecutedCommands.Count);
                Assert.Equal("A", spy.LastCommand.GetParameterSnapshot()["Name"]);
            }
        }

        [Fact]
        public void TrackedUpdate_ZeroRows_DoesNotAcceptChanges()
        {
            var spy = new SpyDbConnection { NextExecuteNonQueryResult = 0 };
            using (var service = DapperServiceFactory.Create(spy))
            {
                var person = new TrackedPerson { Id = 1, Name = "A" };
                service.Attach(person);
                person.Name = "B";
                Assert.Equal(0, service.Update(person));
                spy.NextExecuteNonQueryResult = 1;
                Assert.Equal(1, service.Update(person));
                Assert.Equal(2, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void UpdateList_RefreshesEachSuccessfulAttachedSnapshot()
        {
            var spy = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(spy))
            {
                var person = new TrackedPerson { Id = 1, Name = "A" };
                service.Attach(person);
                person.Name = "B";
                Assert.Equal(1, service.UpdateList(new[] { person }));
                Assert.Equal(0, service.Update(person));
                person.Name = "A";
                Assert.Equal(1, service.Update(person));
                Assert.Equal(2, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void Clear_DetachesSnapshotsSoLaterUpdatesCannotSkipRolledBackValues()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.Clear();
            var key = tracker.CreateCompositeKey(person, typeof(TrackedPerson).GetProperties()
                .Where(p => p.Name == "Id").ToList());
            Assert.False(tracker.TryGetAttached(key, out _));
        }

        [Fact]
        public void RollbackCheckpoint_RestoresTheOriginalAttachedSnapshot()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            tracker.BeginTransactionCheckpoint();
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.RollbackTransactionCheckpoint();
            var snapshot = AttachedPerson(tracker, person);
            Assert.Equal("A", snapshot.Name);
            Assert.Contains("Name", tracker.GetChangedProperties(snapshot, person));
        }

        [Fact]
        public void NestedCommit_LeavesOriginalSnapshotAvailableForOuterRollback()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            tracker.BeginTransactionCheckpoint();
            tracker.BeginTransactionCheckpoint();
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.CommitTransactionCheckpoint();
            tracker.RollbackTransactionCheckpoint();
            Assert.Equal("A", AttachedPerson(tracker, person).Name);
        }

        [Fact]
        public void NestedRollback_RestoresTheSavepointSnapshotBeforeOuterRollback()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            tracker.BeginTransactionCheckpoint();
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.BeginTransactionCheckpoint();
            person.Name = "C";
            tracker.AcceptChanges(person);
            tracker.RollbackTransactionCheckpoint();
            Assert.Equal("B", AttachedPerson(tracker, person).Name);
            tracker.RollbackTransactionCheckpoint();
            Assert.Equal("A", AttachedPerson(tracker, person).Name);
        }

        [Fact]
        public void RollbackCheckpoint_DoesNotReattachExplicitlyDetachedEntities()
        {
            var tracker = new EntityTracker();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            tracker.BeginTransactionCheckpoint();
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.Detach(person);
            tracker.RollbackTransactionCheckpoint();
            var key = tracker.CreateCompositeKey(person, typeof(TrackedPerson).GetProperties().Where(p => p.Name == "Id").ToList());
            Assert.False(tracker.TryGetAttached(key, out _));
        }

        [Fact]
        public void EntityAttachedInsideTransaction_KeepsItsOriginalSnapshotOnRollback()
        {
            var tracker = new EntityTracker();
            tracker.BeginTransactionCheckpoint();
            var person = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(person);
            person.Name = "B";
            tracker.AcceptChanges(person);
            tracker.RollbackTransactionCheckpoint();
            Assert.Equal("A", AttachedPerson(tracker, person).Name);
        }

        [Fact]
        public void DetachAndReattach_DoesNotRestoreAFormerAttachmentsJournal()
        {
            var tracker = new EntityTracker();
            var initial = new TrackedPerson { Id = 1, Name = "A" };
            tracker.Attach(initial);
            tracker.BeginTransactionCheckpoint();
            initial.Name = "B";
            tracker.AcceptChanges(initial);
            tracker.BeginTransactionCheckpoint();
            initial.Name = "C";
            tracker.AcceptChanges(initial);
            tracker.Detach(initial);
            var replacement = new TrackedPerson { Id = 1, Name = "replacement" };
            tracker.Attach(replacement);
            tracker.RollbackTransactionCheckpoint();
            tracker.RollbackTransactionCheckpoint();
            Assert.Equal("replacement", AttachedPerson(tracker, replacement).Name);
        }

        private static TrackedPerson AttachedPerson(EntityTracker tracker, TrackedPerson person)
        {
            var key = tracker.CreateCompositeKey(person, typeof(TrackedPerson).GetProperties().Where(p => p.Name == "Id").ToList());
            Assert.True(tracker.TryGetAttached(key, out var snapshot));
            return Assert.IsType<TrackedPerson>(snapshot);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GetById_RenamedSingleKey_BindsTheQueryParameter(bool entityOverload)
        {
            var spy = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(spy))
            {
                if (entityOverload) service.GetById(new RenamedKeyEntity { PersonId = 7 });
                else service.GetById<RenamedKeyEntity>(7);
                Assert.Contains("WHERE [PersonId] = @PersonId", spy.LastCommandText);
                Assert.Equal(7, spy.LastCommand.GetParameterSnapshot()["PersonId"]);
            }
        }

        [Fact]
        public void Insert_NullableIdentity_AssignsUnderlyingNumericValue()
        {
            var spy = new SpyDbConnection { NextExecuteScalarResult = 42m };
            using (var service = DapperServiceFactory.Create(spy))
            {
                var entity = new NullableIdentityEntity { Name = "test" };
                Assert.Equal(1, service.Insert(entity));
                Assert.Equal(42, entity.Id);
            }
        }

        [Fact]
        public void Insert_LongIdentity_PreservesValuesBeyondIntRange()
        {
            var spy = new SpyDbConnection { NextExecuteScalarResult = 2147483648m };
            using (var service = DapperServiceFactory.Create(spy))
            {
                var entity = new LongIdentityEntity { Name = "test" };
                Assert.Equal(1, service.Insert(entity));
                Assert.Equal(2147483648L, entity.Id);
                Assert.DoesNotContain("AS INT", spy.LastCommandText);
            }
        }

        [Fact]
        public void GetById_ReservedProperty_QuotesSelectAlias()
        {
            var spy = new SpyDbConnection();
            using (var service = DapperServiceFactory.Create(spy))
            {
                service.GetById<ReservedAliasEntity>(1);
                Assert.Contains("[Order] AS [Order]", spy.LastCommandText);
            }
        }

        [Fact]
        public void BinaryPrimaryKey_UsesImmutableStructuralValues()
        {
            var tracker = new EntityTracker();
            var properties = typeof(BinaryKeyEntity).GetProperties().Where(p => p.Name == "Id").ToList();
            var entity = new BinaryKeyEntity { Id = new byte[] { 1, 2 }, Name = "A" };
            var key = tracker.CreateCompositeKey(entity, properties);
            entity.Id[0] = 9;
            Assert.Equal(key, tracker.CreateCompositeKey(new BinaryKeyEntity { Id = new byte[] { 1, 2 } }, properties));
            Assert.NotEqual(key, tracker.CreateCompositeKey(entity, properties));
        }

        [Fact]
        public void DateTimePrimaryKey_DistinguishesFractionalSeconds()
        {
            var tracker = new EntityTracker();
            var properties = typeof(DateKeyEntity).GetProperties().Where(p => p.Name == "Id").ToList();
            var first = new DateKeyEntity { Id = new DateTime(2026, 10, 9, 12, 0, 0).AddTicks(1) };
            var second = new DateKeyEntity { Id = first.Id.AddTicks(1) };
            Assert.NotEqual(tracker.CreateCompositeKey(first, properties), tracker.CreateCompositeKey(second, properties));
        }

        [Fact]
        public void Insert_IdentityOnlyEntity_UsesDatabaseDefaultValues()
        {
            var spy = new SpyDbConnection { NextExecuteScalarResult = 42m };
            using (var service = DapperServiceFactory.Create(spy))
            {
                var entity = new IdentityOnlyEntity();
                Assert.Equal(1, service.Insert(entity));
                Assert.Equal(42L, entity.Id);
                Assert.Contains("DEFAULT VALUES", spy.LastCommandText);
            }
        }

        public class TrackedPerson { [PrimaryKey] public int Id { get; set; } public string Name { get; set; } }
        public class TrackedOrder { [PrimaryKey] public int Id { get; set; } public string Name { get; set; } }
        public class RenamedKeyEntity { [PrimaryKey] public int PersonId { get; set; } public string Name { get; set; } }
        public class NullableIdentityEntity { [PrimaryKey, Identity] public int? Id { get; set; } public string Name { get; set; } }
        public class LongIdentityEntity { [PrimaryKey, Identity] public long Id { get; set; } public string Name { get; set; } }
        public class ReservedAliasEntity { [PrimaryKey] public int Id { get; set; } public int Order { get; set; } }
        public class BinaryKeyEntity { [PrimaryKey] public byte[] Id { get; set; } public string Name { get; set; } }
        public class DateKeyEntity { [PrimaryKey] public DateTime Id { get; set; } public string Name { get; set; } }
        public class IdentityOnlyEntity { [PrimaryKey, Identity] public long? Id { get; set; } }
    }
}
