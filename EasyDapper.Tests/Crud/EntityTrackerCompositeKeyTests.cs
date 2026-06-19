using System;
using System.Collections.Generic;
using System.Reflection;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Crud
{
    public class EntityTrackerCompositeKeyTests
    {
        private global::EasyDapper.EntityTracker CreateTracker()
            => new global::EasyDapper.EntityTracker();

        [Fact]
        public void CreateCompositeKey_SingleColumn_PrefixedWithPropertyName()
        {
            var tracker = CreateTracker();
            var pks = new List<PropertyInfo>
            {
                typeof(Person).GetProperty("Id")
            };
            var entity = new Person { Id = 5 };
            var key = tracker.CreateCompositeKey(entity, pks);
            var keyStr = (string)key;
            Assert.Contains("Id", keyStr);
            Assert.Contains("5", keyStr);
        }

        [Fact]
        public void CreateCompositeKey_TwoColumns_ProducesDistinctKeysForCollidingValues()
        {
            var tracker = CreateTracker();
            var pks = new List<PropertyInfo>
            {
                typeof(CompositeEntity).GetProperty("Key1"),
                typeof(CompositeEntity).GetProperty("Key2")
            };

            var entity1 = new CompositeEntity { Key1 = 1, Key2 = "A|B" };

            var entity2 = new CompositeEntity { Key1 = 1, Key2 = "A||B" };

            var k1 = (string)tracker.CreateCompositeKey(entity1, pks);
            var k2 = (string)tracker.CreateCompositeKey(entity2, pks);

            Assert.NotEqual(k1, k2);
        }

        [Fact]
        public void CreateCompositeKey_SameValues_ProducesSameKey()
        {
            var tracker = CreateTracker();
            var pks = new List<PropertyInfo>
            {
                typeof(CompositeEntity).GetProperty("Key1"),
                typeof(CompositeEntity).GetProperty("Key2")
            };

            var e1 = new CompositeEntity { Key1 = 1, Key2 = "A" };
            var e2 = new CompositeEntity { Key1 = 1, Key2 = "A" };

            var k1 = tracker.CreateCompositeKey(e1, pks);
            var k2 = tracker.CreateCompositeKey(e2, pks);

            Assert.Equal(k1, k2);
        }

        [Fact]
        public void CreateCompositeKey_NullValue_ProducesNullPlaceholder()
        {
            var tracker = CreateTracker();
            var pks = new List<PropertyInfo>
            {
                typeof(CompositeEntity).GetProperty("Key1"),
                typeof(CompositeEntity).GetProperty("Key2")
            };
            var entity = new CompositeEntity { Key1 = 1, Key2 = null };
            var key = (string)tracker.CreateCompositeKey(entity, pks);
            Assert.Contains("NULL", key);
        }

        [Fact]
        public void Attach_Detach_RoundTrip()
        {
            var tracker = CreateTracker();
            var entity = new Person { Id = 7, Name = "Original", Age = 30 };
            tracker.Attach(entity);

            entity.Name = "Modified";
            entity.Age = 99;

            tracker.Attach(entity);

            tracker.Detach(entity);

            tracker.Attach(entity);
        }

        [Fact]
        public void GetChangedProperties_ReturnsOnlyChangedProperties()
        {
            var tracker = CreateTracker();
            var original = new Person { Id = 1, Name = "A", Age = 20, IsActive = true };
            var current = new Person { Id = 1, Name = "A", Age = 25, IsActive = true };

            var changed = tracker.GetChangedProperties(original, current);
            Assert.Contains("Age", changed);
            Assert.DoesNotContain("Name", changed);
            Assert.DoesNotContain("Id", changed);
        }

        [Fact]
        public void GetChangedProperties_ByteArray_SameContents_NotReportedAsChanged()
        {
            var tracker = CreateTracker();
            var original = new ByteArrayEntity { Id = 1, Data = new byte[] { 1, 2, 3 } };
            var current = new ByteArrayEntity { Id = 1, Data = new byte[] { 1, 2, 3 } };
            var changed = tracker.GetChangedProperties(original, current);
            Assert.DoesNotContain("Data", changed);
        }

        [Table("ByteArrayEntity", "dbo")]
        private class ByteArrayEntity
        {
            [PrimaryKey]
            public int Id { get; set; }
            public byte[] Data { get; set; }
        }
    }
}
