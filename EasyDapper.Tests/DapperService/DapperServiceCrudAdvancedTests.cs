using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceCrudAdvancedTests
    {
        private global::EasyDapper.DapperService CreateService(SpyDbConnection spy = null)
        {
            spy = spy ?? new SpyDbConnection();
            spy.Open();
            return new global::EasyDapper.DapperService(spy);
        }

        [Fact]
        public void Insert_WithComplexEntity_AllColumnsInParameter()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entity = new CompositeEntity { Key1 = 7, Key2 = "X", Data = "test" };
                svc.Insert(entity);

                Assert.Equal(1, spy.ExecutedCommands.Count);
                var sql = spy.LastCommandText;
                Assert.Contains("[Key1]", sql);
                Assert.Contains("[Key2]", sql);
                Assert.Contains("[Data]", sql);
                Assert.Contains("@Key1", sql);
                Assert.Contains("@Key2", sql);
                Assert.Contains("@Data", sql);
            }
        }

        [Fact]
        public void Insert_IdentityProperty_PopulatedAfterInsert()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 42;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Name = "Alice", Age = 30 };
                Assert.Equal(0, person.Id);

                svc.Insert(person);

                Assert.Equal(42, person.Id);
            }
        }

        [Fact]
        public void InsertList_EmptyList_ReturnsZeroAndNoCommands()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var result = svc.InsertList(new Person[0]);
                Assert.Equal(0, result);
                Assert.Equal(0, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void InsertList_NullEntities_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentNullException>(() => svc.InsertList<Person>(null));
            }
        }

        [Fact]
        public void Update_Attached_OnlyChangedColumnsInSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30, IsActive = true };
                svc.Attach(person);

                person.Age = 31;
                person.Name = "New";

                svc.Update(person);

                var sql = spy.LastCommandText;
                Assert.Contains("[Age] = @Age", sql);
                Assert.Contains("[CurrentFirstName] = @Name", sql);
                Assert.DoesNotContain("[IsActive]", sql);
                Assert.Contains("WHERE [Id] = @pk_Id", sql);
            }
        }

        [Fact]
        public void Update_Attached_OnlyOnePropertyChanged_OnlyThatColumnInSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30, IsActive = true };
                svc.Attach(person);

                person.Age = 99;

                svc.Update(person);

                var sql = spy.LastCommandText;
                Assert.Contains("[Age] = @Age", sql);
                Assert.DoesNotContain("[CurrentFirstName]", sql);
                Assert.DoesNotContain("[IsActive]", sql);
            }
        }

        [Fact]
        public void Update_Attached_BooleanChanged_IncludedInSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30, IsActive = true };
                svc.Attach(person);

                person.IsActive = false;

                svc.Update(person);

                var sql = spy.LastCommandText;
                Assert.Contains("[IsActive]", sql);
            }
        }

        [Fact]
        public void Update_NotAttached_PerformsFullUpdate()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30 };
                svc.Update(person);

                var sql = spy.LastCommandText;
                Assert.Contains("[CurrentFirstName] = @Name", sql);
                Assert.Contains("[Age] = @Age", sql);
                Assert.Contains("WHERE [Id] = @Id", sql);
            }
        }

        [Fact]
        public void UpdateList_MultipleEntities_ExecutesMultipleCommands()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entities = new[]
                {
                    new Person { Id = 1, Name = "A", Age = 1 },
                    new Person { Id = 2, Name = "B", Age = 2 },
                    new Person { Id = 3, Name = "C", Age = 3 }
                };
                var result = svc.UpdateList(entities);

                Assert.Equal(3, result);
                Assert.Equal(3, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void DeleteList_MultipleEntities_ExecutesMultipleCommands()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entities = new[]
                {
                    new Person { Id = 1, Name = "A", Age = 1 },
                    new Person { Id = 2, Name = "B", Age = 2 }
                };
                var result = svc.DeleteList(entities);

                Assert.Equal(2, result);
                Assert.Equal(2, spy.ExecutedCommands.Count);
                Assert.All(spy.ExecutedCommands, c => Assert.Contains("DELETE FROM", c.CommandText));
            }
        }

        [Fact]
        public void GetById_SingleKey_UsesAtIdParameter()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.GetById<Person>(42);

                var sql = spy.LastCommandText;
                Assert.Contains("WHERE [Id] = @Id", sql);
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithAnonymousObject_AllKeysPresent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.GetById<CompositeEntity>(new { Key1 = 1, Key2 = "X" });

                var sql = spy.LastCommandText;
                Assert.Contains("[Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithEntityInstance_AllKeysPresent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var probe = new CompositeEntity { Key1 = 1, Key2 = "X" };
                svc.GetById(probe);

                var sql = spy.LastCommandText;
                Assert.Contains("[Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithScalarId_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.GetById<CompositeEntity>(123));
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithMismatchedAnonymousObject_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.GetById<CompositeEntity>(new { WrongName = 1 }));
            }
        }

        [Fact]
        public void GetById_CompositeKey_PartialAnonymousObject_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<ArgumentException>(() => svc.GetById<CompositeEntity>(new { Key1 = 1 }));
            }
        }

        [Fact]
        public void Attach_Detach_Reattach_SnapshotIsRefreshed()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30 };
                svc.Attach(person);
                svc.Detach(person);

                person.Name = "New";
                svc.Attach(person);

                person.Name = "Newer";

                svc.Update(person);

                var sql = spy.LastCommandText;
                Assert.Contains("[CurrentFirstName] = @Name", sql);
            }
        }

        [Fact]
        public void Attach_AlreadyAttached_DoesNotRefreshSnapshot()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Old", Age = 30 };
                svc.Attach(person);

                person.Name = "Changed";
                svc.Attach(person);

                person.Name = "Old";

                var result = svc.Update(person);
                Assert.Equal(0, result);
            }
        }

        [Fact]
        public void Insert_ThenUpdate_IdentityPopulatedOnInsert()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 99;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Name = "Test", Age = 20 };
                svc.Insert(person);
                Assert.Equal(99, person.Id);

                spy.NextExecuteNonQueryResult = 1;
                person.Age = 21;
                svc.Update(person);

                Assert.Equal(2, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void MultipleOperations_SequenceOfCommands()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.Insert(new Person { Name = "A", Age = 1 });
                spy.NextExecuteScalarResult = 2;
                svc.Insert(new Person { Name = "B", Age = 2 });
                spy.NextExecuteNonQueryResult = 1;
                svc.Delete(new Person { Id = 1, Name = "A", Age = 1 });

                Assert.Equal(3, spy.ExecutedCommands.Count);
                Assert.Contains("INSERT", spy.ExecutedCommands[0].CommandText);
                Assert.Contains("INSERT", spy.ExecutedCommands[1].CommandText);
                Assert.Contains("DELETE", spy.ExecutedCommands[2].CommandText);
            }
        }

        [Fact]
        public void Insert_ThenAttach_ThenUpdate_OnlyChangedColumns()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 10;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Name = "Test", Age = 25 };
                svc.Insert(person);

                svc.Attach(person);

                person.Age = 26;

                spy.NextExecuteNonQueryResult = 1;
                svc.Update(person);

                var updateSql = spy.ExecutedCommands[1].CommandText;
                Assert.Contains("[Age] = @Age", updateSql);
                Assert.DoesNotContain("[CurrentFirstName]", updateSql);
            }
        }

        [Fact]
        public void Delete_EntityWithCompositeKey_AllKeysInWhereClause()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entity = new CompositeEntity { Key1 = 7, Key2 = "X" };
                svc.Delete(entity);

                var sql = spy.LastCommandText;
                Assert.Contains("DELETE FROM [dbo].[CompositeEntity]", sql);
                Assert.Contains("[Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }
    }
}
