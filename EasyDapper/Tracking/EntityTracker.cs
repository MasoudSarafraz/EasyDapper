using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal class EntityTracker : IDisposable
    {
        internal readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();
        private readonly object _snapshotLock = new object();
        private readonly Dictionary<object, Attachment> _attachments = new Dictionary<object, Attachment>();
        private readonly Dictionary<Type, int> _attachmentCounts = new Dictionary<Type, int>();
        private readonly Stack<Dictionary<object, Attachment>> _transactionSnapshots = new Stack<Dictionary<object, Attachment>>();
        private bool _disposed = false;

        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> _primaryKeyCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> _nonPrimaryKeyCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();

        private static List<PropertyInfo> GetPrimaryKeyProperties(Type type)
        {
            return _primaryKeyCache.GetOrAdd(type, t =>
                t.GetProperties()
                    .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                    .ToList());
        }

        private static List<PropertyInfo> GetNonPrimaryKeyProperties(Type type)
        {
            return _nonPrimaryKeyCache.GetOrAdd(type, t =>
                t.GetProperties()
                    .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) == null)
                    .ToList());
        }

        public void Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = GetPrimaryKeyProperties(typeof(T));
            var values = CaptureValues(entity, primaryKeys);
            var key = CreateCompositeKey<T>(values, primaryKeys);
            lock (_snapshotLock) { if (_attachments.ContainsKey(key)) return; }
            CaptureValues(entity, typeof(T).GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0), values);
            var snapshot = PrepareSnapshot<T>(values);
            lock (_snapshotLock)
            {
                if (_attachments.ContainsKey(key)) return;
                var attachment = new Attachment(key, snapshot, new object());
                _attachments.Add(key, attachment);
                _attachmentCounts.TryGetValue(typeof(T), out var count);
                _attachmentCounts[typeof(T)] = count + 1;
                _attachedEntities.TryAdd(key, snapshot.Entity);
            }
        }

        public void Detach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = GetPrimaryKeyProperties(typeof(T));
            var key = CreateCompositeKey(entity, primaryKeys);
            lock (_snapshotLock)
            {
                object _;
                _attachedEntities.TryRemove(key, out _);
                if (_attachments.Remove(key))
                {
                    var count = _attachmentCounts[typeof(T)] - 1;
                    if (count == 0) _attachmentCounts.Remove(typeof(T));
                    else _attachmentCounts[typeof(T)] = count;
                }
                foreach (var journal in _transactionSnapshots) journal.Remove(key);
            }
        }

        public bool TryGetAttached(object key, out object value) => _attachedEntities.TryGetValue(key, out value);

        internal void AcceptChanges<T>(T entity) where T : class
        {
            var values = CaptureValues(entity, typeof(T).GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0));
            var key = CreateCompositeKey<T>(values, GetPrimaryKeyProperties(typeof(T)));
            if (!TryGetAttachment(key, out var attachment)) return;
            var snapshot = PrepareSnapshot<T>(values);
            AcceptPreparedSnapshot(attachment, snapshot);
        }

        internal bool TryGetAttachment(object key, out Attachment attachment)
        {
            lock (_snapshotLock) { return _attachments.TryGetValue(key, out attachment); }
        }

        internal bool HasAttachments<T>()
        {
            lock (_snapshotLock) { return _attachmentCounts.ContainsKey(typeof(T)); }
        }

        internal bool IsCurrentAttachment(object key, Attachment expected)
        {
            lock (_snapshotLock)
            {
                _attachments.TryGetValue(key, out var current);
                return ReferenceEquals(current, expected);
            }
        }

        internal void AcceptPreparedSnapshot(Attachment expected, Snapshot snapshot)
        {
            lock (_snapshotLock)
            {
                if (!_attachments.TryGetValue(expected.Key, out var current) || !ReferenceEquals(current, expected)) return;
                if (_transactionSnapshots.Count > 0)
                {
                    var journal = _transactionSnapshots.Peek();
                    if (!journal.ContainsKey(expected.Key)) journal.Add(expected.Key, current);
                }
                var replacement = new Attachment(expected.Key, snapshot, expected.Owner);
                _attachments[expected.Key] = replacement;
                _attachedEntities[expected.Key] = snapshot.Entity;
            }
        }

        internal sealed class Attachment
        {
            internal readonly object Key;
            internal readonly Snapshot Snapshot;
            internal readonly object Owner;
            internal Attachment(object key, Snapshot snapshot, object owner) { Key = key; Snapshot = snapshot; Owner = owner; }
        }

        internal sealed class Snapshot
        {
            internal readonly object Entity;
            internal readonly Dictionary<string, object> Values;
            internal Snapshot(object entity, Dictionary<string, object> values) { Entity = entity; Values = values; }
        }

        internal static Dictionary<string, object> CaptureValues<T>(T entity, IEnumerable<PropertyInfo> properties,
            Dictionary<string, object> values = null) where T : class
        {
            if (values == null) values = new Dictionary<string, object>(StringComparer.Ordinal);
            foreach (var property in properties)
                if (!values.ContainsKey(property.Name)) values.Add(property.Name, property.GetValue(entity));
            return values;
        }

        internal Snapshot PrepareSnapshot<T>(Dictionary<string, object> values) where T : class
        {
            var frozen = values.ToDictionary(p => p.Key, p => CloneValue(p.Value), StringComparer.Ordinal);
            var clone = Activator.CreateInstance<T>();
            foreach (var property in typeof(T).GetProperties().Where(p => p.CanWrite && p.GetIndexParameters().Length == 0))
                if (frozen.TryGetValue(property.Name, out var value)) property.SetValue(clone, CloneValue(value));
            return new Snapshot(clone, frozen);
        }

        internal List<string> GetChangedProperties<T>(Attachment original, Dictionary<string, object> values)
        {
            return GetNonPrimaryKeyProperties(typeof(T)).Where(p =>
                !AreEqual(original.Snapshot.Values[p.Name], values[p.Name])).Select(p => p.Name).ToList();
        }

        internal void BeginTransactionCheckpoint()
        {
            lock (_snapshotLock) { _transactionSnapshots.Push(new Dictionary<object, Attachment>()); }
        }

        internal void CommitTransactionCheckpoint()
        {
            lock (_snapshotLock)
            {
                if (_transactionSnapshots.Count == 0) return;
                var journal = _transactionSnapshots.Pop();
                if (_transactionSnapshots.Count == 0) return;
                var parent = _transactionSnapshots.Peek();
                foreach (var entry in journal)
                    if (!parent.ContainsKey(entry.Key)) parent.Add(entry.Key, entry.Value);
            }
        }

        internal void RollbackTransactionCheckpoint()
        {
            lock (_snapshotLock)
            {
                if (_transactionSnapshots.Count == 0) return;
                foreach (var entry in _transactionSnapshots.Pop())
                {
                    if (_attachments.TryGetValue(entry.Key, out var current) && ReferenceEquals(current.Owner, entry.Value.Owner))
                    {
                        _attachments[entry.Key] = entry.Value;
                        _attachedEntities[entry.Key] = entry.Value.Snapshot.Entity;
                    }
                }
            }
        }

        internal void Clear()
        {
            lock (_snapshotLock)
            {
                _transactionSnapshots.Clear();
                _attachments.Clear();
                _attachmentCounts.Clear();
                _attachedEntities.Clear();
            }
        }

        internal object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            var values = new object[primaryKeys.Count];
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                var value = primaryKeys[i].GetValue(entity);
                values[i] = value is Array array ? array.Clone() : value;
            }
            return new EntityKey(typeof(T), values);
        }

        internal object CreateCompositeKey<T>(Dictionary<string, object> values, List<PropertyInfo> primaryKeys)
        {
            return new EntityKey(typeof(T), primaryKeys.Select(p =>
                values[p.Name] is Array array ? array.Clone() : values[p.Name]).ToArray());
        }

        private sealed class EntityKey : IEquatable<EntityKey>
        {
            private readonly Type _entityType;
            private readonly object[] _values;

            internal EntityKey(Type entityType, object[] values)
            {
                _entityType = entityType;
                _values = values;
            }

            public bool Equals(EntityKey other) => ReferenceEquals(this, other) || other != null &&
                _entityType == other._entityType &&
                StructuralComparisons.StructuralEqualityComparer.Equals(_values, other._values);

            public override bool Equals(object other) => Equals(other as EntityKey);

            public override int GetHashCode() => unchecked((_entityType.GetHashCode() * 397) ^
                StructuralComparisons.StructuralEqualityComparer.GetHashCode(_values));
        }

        internal List<string> GetChangedProperties<T>(T original, T current)
        {
            var props = GetNonPrimaryKeyProperties(typeof(T));
            var changed = new List<string>(props.Count);
            for (int i = 0; i < props.Count; i++)
            {
                var p = props[i];
                if (!AreEqual(p.GetValue(original), p.GetValue(current)))
                {
                    changed.Add(p.Name);
                }
            }
            return changed;
        }

        private static bool AreEqual(object a, object b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a is Array arrA && b is Array arrB)
            {
                if (arrA.Length != arrB.Length) return false;
                for (int i = 0; i < arrA.Length; i++)
                    if (!object.Equals(arrA.GetValue(i), arrB.GetValue(i))) return false;
                return true;
            }
            return object.Equals(a, b);
        }

        private static object CloneValue(object value)
        {
            if (value == null || value.GetType().IsValueType || value is string) return value;
            if (value is Array array) return array.Clone();
            return value.GetType().GetMethod("MemberwiseClone", BindingFlags.NonPublic | BindingFlags.Instance)
                .Invoke(value, null);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing) Clear();
            _disposed = true;
        }
    }
}
