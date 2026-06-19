using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceCrudTests
    {
        [Fact]
        public void Insert_NullEntity_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.Insert<Person>(null));
            }
        }

        [Fact]
        public void Insert_WithIdentity_ExecutesInsertWithScopeIdentity()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 42;

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Name = "Alice", Age = 30 };
                var result = svc.Insert(person);

                Assert.Equal(1, result);
                Assert.Equal(42, person.Id);
                Assert.Equal(1, spy.ExecutedCommands.Count);

                var sql = spy.LastCommandText;
                Assert.Contains("INSERT INTO [dbo].[Person]", sql);
                Assert.Contains("SELECT CAST(SCOPE_IDENTITY() AS INT)", sql);
            }
        }

        [Fact]
        public void Insert_WithoutIdentity_ExecutesPlainInsert()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entity = new CompositeEntity { Key1 = 1, Key2 = "A", Data = "test" };
                var result = svc.Insert(entity);

                Assert.Equal(1, result);
                var sql = spy.LastCommandText;
                Assert.Contains("INSERT INTO [dbo].[CompositeEntity]", sql);
                Assert.DoesNotContain("SCOPE_IDENTITY", sql);
            }
        }

        [Fact]
        public void Update_NullEntity_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.Update<Person>(null));
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
                var person = new Person { Id = 5, Name = "Bob", Age = 25 };
                var result = svc.Update(person);

                Assert.Equal(1, result);
                var sql = spy.LastCommandText;
                Assert.Contains("UPDATE [dbo].[Person]", sql);
                Assert.Contains("SET", sql);
                Assert.Contains("[CurrentFirstName] = @Name", sql);
                Assert.Contains("[Age] = @Age", sql);
                Assert.Contains("WHERE [Id] = @Id", sql);
            }
        }

        [Fact]
        public void Update_Attached_WithChanges_PerformsDynamicUpdate()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Bob", Age = 25 };
                svc.Attach(person);

                person.Age = 26;

                var result = svc.Update(person);

                Assert.Equal(1, result);
                var sql = spy.LastCommandText;
                Assert.Contains("UPDATE [dbo].[Person] SET", sql);
                Assert.Contains("[Age] = @Age", sql);
                Assert.DoesNotContain("[CurrentFirstName] = @Name", sql);
                Assert.Contains("WHERE [Id] = @pk_Id", sql);
            }
        }

        [Fact]
        public void Update_Attached_WithNoChanges_ReturnsZero()
        {
            var spy = new SpyDbConnection();
            spy.Open();

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Bob", Age = 25 };
                svc.Attach(person);

                var result = svc.Update(person);

                Assert.Equal(0, result);
                Assert.Equal(0, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void Delete_NullEntity_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.Delete<Person>(null));
            }
        }

        [Fact]
        public void Delete_ExecutesDeleteWithPrimaryKey()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var person = new Person { Id = 5, Name = "Bob", Age = 25 };
                var result = svc.Delete(person);

                Assert.Equal(1, result);
                var sql = spy.LastCommandText;
                Assert.Contains("DELETE FROM [dbo].[Person]", sql);
                Assert.Contains("WHERE [Id] = @Id", sql);
            }
        }

        [Fact]
        public void Delete_CompositeKey_WhereClauseIncludesBothKeys()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var entity = new CompositeEntity { Key1 = 7, Key2 = "X" };
                var result = svc.Delete(entity);

                Assert.Equal(1, result);
                var sql = spy.LastCommandText;
                Assert.Contains("DELETE FROM [dbo].[CompositeEntity]", sql);
                Assert.Contains("[Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }

        [Fact]
        public void GetById_SingleKey_ExecutesSelectWithIdParameter()
        {
            var spy = new SpyDbConnection();
            spy.Open();

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var result = svc.GetById<Person>(5);

                Assert.Null(result);
                var sql = spy.LastCommandText;
                Assert.Contains("SELECT", sql);
                Assert.Contains("FROM [dbo].[Person]", sql);
                Assert.Contains("WHERE [Id] = @Id", sql);
            }
        }

        [Fact]
        public void GetById_NullId_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.GetById<Person>((object)null));
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithMatchingAnonymousObject_ExecutesSelectWithBothKeys()
        {
            var spy = new SpyDbConnection();
            spy.Open();

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var result = svc.GetById<CompositeEntity>(new { Key1 = 1, Key2 = "A" });

                Assert.Null(result);
                var sql = spy.LastCommandText;
                Assert.Contains("WHERE [Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithEntityInstance_ExecutesSelectWithBothKeys()
        {
            var spy = new SpyDbConnection();
            spy.Open();

            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                var probe = new CompositeEntity { Key1 = 1, Key2 = "A" };
                var result = svc.GetById(probe);

                Assert.Null(result);
                var sql = spy.LastCommandText;
                Assert.Contains("WHERE [Key1] = @Key1", sql);
                Assert.Contains("[Key2] = @Key2", sql);
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithScalarId_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentException>(() => svc.GetById<CompositeEntity>(123));
            }
        }

        [Fact]
        public void GetById_CompositeKey_WithMismatchedAnonymousObject_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentException>(() => svc.GetById<CompositeEntity>(new { WrongName = 1 }));
            }
        }

        [Fact]
        public void DeleteList_NullEntities_Throws()
        {
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.DeleteList<Person>(null));
            }
        }

        [Fact]
        public void DeleteList_ExecutesOneDeletePerEntity()
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
                var result = svc.DeleteList(entities);

                Assert.Equal(3, result);
                Assert.Equal(3, spy.ExecutedCommands.Count);
                Assert.All(spy.ExecutedCommands, c => Assert.Contains("DELETE FROM", c.CommandText));
            }
        }

        [Fact]
        public void InsertList_EmptyList_ReturnsZero()
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
            using (var svc = new global::EasyDapper.DapperService(new SpyDbConnection()))
            {
                Assert.Throws<ArgumentNullException>(() => svc.InsertList<Person>(null));
            }
        }
    }
}
