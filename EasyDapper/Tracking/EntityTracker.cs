using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EasyDapper.Attributes;

namespace EasyDapper
{
    /// <summary>
    /// Tracks attached entities so that <see cref="DapperService.Update{T}"/> can emit an
    /// UPDATE statement that touches only the properties that have actually changed since
    /// <see cref="Attach{T}"/> was called.
    /// </summary>
    /// <remarks>
    /// For each attached entity the tracker keeps a private deep-ish copy of the original
    /// property values, keyed by a stable composite key derived from the entity's primary
    /// key values. Lookups are O(1) and the tracker is thread-safe.
    /// </remarks>
    internal class EntityTracker : IDisposable
    {
        internal readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();
        private bool _disposed = false;

        public void Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                .ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            // TryAdd ensures we keep the FIRST snapshot - subsequent attaches of the same key
            // are no-ops, mirroring EF Core's identity map behaviour.
            if (!_attachedEntities.ContainsKey(key))
            {
                var clone = CloneEntity(entity);
                _attachedEntities.TryAdd(key, clone);
            }
        }

        public void Detach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                .ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            object _;
            _attachedEntities.TryRemove(key, out _);
        }

        public bool TryGetAttached(object key, out object value) => _attachedEntities.TryGetValue((string)key, out value);

        /// <summary>
        /// Builds a stable, collision-free string key from the primary key values of an entity.
        /// Each key component is encoded so that values containing the separator cannot collide
        /// with multi-column keys.
        /// </summary>
        internal object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1)
            {
                var value = primaryKeys[0].GetValue(entity);
                // Prefix with the property name to disambiguate single-column keys of different types
                // (e.g. int 5 vs string "5") - the alias prefix carries type information.
                return primaryKeys[0].Name + ":" + (value ?? "NULL");
            }
            var parts = primaryKeys.Select(p =>
            {
                var v = p.GetValue(entity);
                // Escape the separator inside values so "A|B","C" cannot collide with "A","B|C".
                var vStr = v?.ToString()?.Replace("|", "||") ?? "NULL";
                return p.Name + "=" + vStr;
            });
            return string.Join("|", parts);
        }

        internal List<string> GetChangedProperties<T>(T original, T current)
        {
            return typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) == null
                            && !AreEqual(p.GetValue(original), p.GetValue(current)))
                .Select(p => p.Name)
                .ToList();
        }

        /// <summary>
        /// Compares two values using structural equality where appropriate (arrays, in particular
        /// <c>byte[]</c>) and falls back to <see cref="object.Equals(object, object)"/> otherwise.
        /// </summary>
        private static bool AreEqual(object a, object b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            // Structural comparison for arrays (e.g. byte[] for rowversion/varbinary).
            if (a is Array arrA && b is Array arrB)
            {
                if (arrA.Length != arrB.Length) return false;
                for (int i = 0; i < arrA.Length; i++)
                    if (!object.Equals(arrA.GetValue(i), arrB.GetValue(i))) return false;
                return true;
            }
            return object.Equals(a, b);
        }

        /// <summary>
        /// Produces a deep-ish copy of an entity. Primitive/value types and strings are copied by
        /// value; arrays are cloned element-by-element; other reference types are copied via
        /// <c>MemberwiseClone</c> when accessible. This is sufficient for change tracking because
        /// the tracker only inspects top-level property values.
        /// </summary>
        private T CloneEntity<T>(T entity)
        {
            var clone = Activator.CreateInstance<T>();
            foreach (var prop in typeof(T).GetProperties().Where(p => p.CanWrite))
            {
                var value = prop.GetValue(entity);
                if (value == null)
                {
                    prop.SetValue(clone, null);
                    continue;
                }
                if (prop.PropertyType.IsArray)
                {
                    var sourceArray = (Array)value;
                    var targetArray = (Array)Activator.CreateInstance(prop.PropertyType, sourceArray.Length);
                    Array.Copy(sourceArray, targetArray, sourceArray.Length);
                    prop.SetValue(clone, targetArray);
                    continue;
                }
                if (!prop.PropertyType.IsValueType && prop.PropertyType != typeof(string))
                {
                    var cloneMethod = prop.PropertyType.GetMethod("MemberwiseClone",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    if (cloneMethod != null)
                        value = cloneMethod.Invoke(value, null);
                }
                prop.SetValue(clone, value);
            }
            return clone;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing) _attachedEntities.Clear();
            _disposed = true;
        }
    }
}
