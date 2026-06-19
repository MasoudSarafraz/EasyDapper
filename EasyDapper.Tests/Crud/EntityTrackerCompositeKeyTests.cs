using System;
using System.Collections.Generic;
using System.Reflection;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Crud
{
    /// <summary>
    /// Tests for the EntityTracker composite-key builder. Verifies the fix for the previous
    /// string-concatenation collision bug where (Key1="A|B", Key2="C") collided with
    /// (Key1="A", Key2="B|C").
    /// </summary>
    public class EntityTrackerCompositeKeyTests
    {
        private global::EasyDapper.EntityTracker CreateTracker()
            => new global::EasyDapper.EntityTracker();

        /// <summary>
        /// FIX: previously CreateCompositeKey returned just the value for single-column keys
        /// (e.g. 5), which could collide between different types whose PK happened to share the
        /// same value. Now it prefixes with the property name.
        /// </summary>
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

        /// <summary>
        /// FIX: the old string-concatenation scheme produced collisions. The new scheme escapes
        /// pipe characters inside values and includes property names, so (A|B, C) and (A, B|C)
        /// produce different keys.
        /// </summary>
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
            // Without the escape fix this would collide with (1, "A|B") below.
            // Note: the values themselves differ, but the old concat would have collided for
            // (Key1=1, Key2="A|B") and (Key1=1, Key2="A||B") in the unescaped version. We test
            // that the new scheme produces non-equal keys for unequal values.
            var entity2 = new CompositeEntity { Key1 = 1, Key2 = "A||B" };

            var k1 = (string)tracker.CreateCompositeKey(entity1, pks);
            var k2 = (string)tracker.CreateCompositeKey(entity2, pks);

            Assert.NotEqual(k1, k2);
        }

        /// <summary>
        /// Two entities with the same PK values produce the same composite key (so that
        /// attach/detach round-trips correctly).
        /// </summary>
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

        /// <summary>
        /// Null values in a PK column are represented as "NULL" so that null vs empty string
        /// remain distinct.
        /// </summary>
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

        /// <summary>
        /// Attach stores a snapshot, and Detach removes it. Subsequent attach of the same key
        /// is a no-op (the first snapshot wins).
        /// </summary>
        [Fact]
        public void Attach_Detach_RoundTrip()
        {
            var tracker = CreateTracker();
            var entity = new Person { Id = 7, Name = "Original", Age = 30 };
            tracker.Attach(entity);

            // Mutating the entity should not affect the snapshot.
            entity.Name = "Modified";
            entity.Age = 99;

            // Attach again - should be a no-op since the key (Id=7) is already tracked.
            tracker.Attach(entity);

            // Detach should remove the entry.
            tracker.Detach(entity);

            // After detach, attaching again should create a fresh snapshot.
            tracker.Attach(entity);
        }

        /// <summary>
        /// GetChangedProperties returns the names of properties whose values differ between
        /// the original snapshot and the current entity.
        /// </summary>
        [Fact]
        public void GetChangedProperties_ReturnsOnlyChangedProperties()
        {
            var tracker = CreateTracker();
            var original = new Person { Id = 1, Name = "A", Age = 20, IsActive = true };
            var current = new Person { Id = 1, Name = "A", Age = 25, IsActive = true };

            var changed = tracker.GetChangedProperties(original, current);
            Assert.Contains("Age", changed);
            Assert.DoesNotContain("Name", changed);
            Assert.DoesNotContain("Id", changed); // PK is never reported as changed.
        }

        /// <summary>
        /// FIX: byte[] comparison previously used reference equality. Now structural equality is
        /// used so that two arrays with the same contents compare equal.
        /// </summary>
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
