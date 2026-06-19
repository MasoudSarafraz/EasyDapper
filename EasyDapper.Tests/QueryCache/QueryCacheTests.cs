using System;
using System.Linq;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.QueryCache
{
    public class QueryCacheTests
    {
        private global::EasyDapper.QueryCache CreateCache()
            => new global::EasyDapper.QueryCache();

        [Fact]
        public void GetTableName_WithTableAttribute_ReturnsBracketedSchemaAndTable()
        {
            var cache = CreateCache();
            var name = cache.GetTableName<Person>();
            Assert.Equal("[dbo].[Person]", name);
        }

        [Fact]
        public void GetTableName_WithCustomSchema_ReturnsBracketedSchemaAndTable()
        {
            var cache = CreateCache();
            var name = cache.GetTableName<Order>();
            Assert.Equal("[dbo].[Order]", name);
        }

        [Fact]
        public void GetTableName_CalledTwice_ReturnsSameInstance()
        {
            var cache = CreateCache();
            var name1 = cache.GetTableName<Person>();
            var name2 = cache.GetTableName<Person>();
            Assert.Same(name1, name2);
        }

        [Fact]
        public void GetInsertQuery_WithIdentity_ReturnsInsertWithScopeIdentity()
        {
            var cache = CreateCache();
            var query = cache.GetInsertQuery<Person>();
            Assert.Contains("INSERT INTO [dbo].[Person]", query);
            Assert.Contains("VALUES", query);
            Assert.Contains("SELECT CAST(SCOPE_IDENTITY() AS INT)", query);
            Assert.DoesNotContain("[Id]", query.Split(new[] { "VALUES" }, StringSplitOptions.None)[0]);
        }

        [Fact]
        public void GetInsertQuery_WithoutIdentity_ReturnsPlainInsert()
        {
            var cache = CreateCache();
            var query = cache.GetInsertQuery<CompositeEntity>();
            Assert.Contains("INSERT INTO [dbo].[CompositeEntity]", query);
            Assert.DoesNotContain("SCOPE_IDENTITY", query);
        }

        [Fact]
        public void GetUpdateQuery_WithRegularEntity_GeneratesSetAndWhere()
        {
            var cache = CreateCache();
            var query = cache.GetUpdateQuery<Person>();
            Assert.Contains("UPDATE [dbo].[Person]", query);
            Assert.Contains("SET", query);
            Assert.Contains("[CurrentFirstName] = @Name", query);
            Assert.Contains("[Age] = @Age", query);
            Assert.Contains("WHERE [Id] = @Id", query);
        }

        [Fact]
        public void GetDeleteQuery_WithSingleKey_GeneratesWhereWithKey()
        {
            var cache = CreateCache();
            var query = cache.GetDeleteQuery<Person>();
            Assert.Contains("DELETE FROM [dbo].[Person]", query);
            Assert.Contains("WHERE [Id] = @Id", query);
        }

        [Fact]
        public void GetDeleteQuery_WithCompositeKey_GeneratesWhereWithAllKeys()
        {
            var cache = CreateCache();
            var query = cache.GetDeleteQuery<CompositeEntity>();
            Assert.Contains("DELETE FROM [dbo].[CompositeEntity]", query);
            Assert.Contains("[Key1] = @Key1", query);
            Assert.Contains("[Key2] = @Key2", query);
        }

        [Fact]
        public void GetGetByIdQuery_WithSingleKey_UsesAtIdParameter()
        {
            var cache = CreateCache();
            var query = cache.GetGetByIdQuery<Person>();
            Assert.Contains("SELECT", query);
            Assert.Contains("FROM [dbo].[Person]", query);
            Assert.Contains("WHERE [Id] = @Id", query);
        }

        [Fact]
        public void GetGetByIdQuery_WithCompositeKey_UsesNamedParameters()
        {
            var cache = CreateCache();
            var query = cache.GetGetByIdQuery<CompositeEntity>();
            Assert.Contains("WHERE [Key1] = @Key1", query);
            Assert.Contains("[Key2] = @Key2", query);
        }

        [Fact]
        public void GetPrimaryKeyProperties_WithSingleKey_ReturnsOneProperty()
        {
            var cache = CreateCache();
            var pks = cache.GetPrimaryKeyProperties<Person>();
            Assert.Equal(1, pks.Count);
            Assert.Equal("Id", pks[0].Name);
        }

        [Fact]
        public void GetPrimaryKeyProperties_WithCompositeKey_ReturnsAllKeys()
        {
            var cache = CreateCache();
            var pks = cache.GetPrimaryKeyProperties<CompositeEntity>();
            Assert.Equal(2, pks.Count);
            Assert.Contains(pks, p => p.Name == "Key1");
            Assert.Contains(pks, p => p.Name == "Key2");
        }

        [Fact]
        public void GetPrimaryKeyProperties_NoPrimaryKey_Throws()
        {
            var cache = CreateCache();
            Assert.Throws<InvalidOperationException>(() => cache.GetPrimaryKeyProperties<NoKeyEntity>());
        }

        [Fact]
        public void GetIdentityProperty_WithIdentity_ReturnsProperty()
        {
            var cache = CreateCache();
            var identity = cache.GetIdentityProperty<Person>();
            Assert.NotNull(identity);
            Assert.Equal("Id", identity.Name);
        }

        [Fact]
        public void GetIdentityProperty_WithoutIdentity_ReturnsNull()
        {
            var cache = CreateCache();
            var identity = cache.GetIdentityProperty<CompositeEntity>();
            Assert.Null(identity);
        }

        [Fact]
        public void GetColumnName_WithColumnAttribute_ReturnsAttributeColumnName()
        {
            var cache = CreateCache();
            var prop = typeof(Person).GetProperty("Name");
            var name = cache.GetColumnName(prop);
            Assert.Equal("[CurrentFirstName]", name);
        }

        [Fact]
        public void GetColumnName_WithoutColumnAttribute_ReturnsPropertyName()
        {
            var cache = CreateCache();
            var prop = typeof(Person).GetProperty("Age");
            var name = cache.GetColumnName(prop);
            Assert.Equal("[Age]", name);
        }

        [Fact]
        public void GetTableName_InvalidIdentifier_Throws()
        {
            var cache = CreateCache();
            Assert.ThrowsAny<Exception>(() => cache.GetTableName<BadNameEntity>());
        }
    }

    public class NoKeyEntity
    {
        public string Name { get; set; }
    }

    [Table("Bad-Name", "dbo")]
    public class BadNameEntity
    {
        [PrimaryKey]
        public int Id { get; set; }
    }
}
