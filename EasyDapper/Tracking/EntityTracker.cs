using System;
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
            var key = CreateCompositeKey(entity, primaryKeys);

            if (!_attachedEntities.ContainsKey(key))
            {
                var clone = CloneEntity(entity);
                _attachedEntities.TryAdd(key, clone);
            }
        }

        public void Detach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = GetPrimaryKeyProperties(typeof(T));
            var key = CreateCompositeKey(entity, primaryKeys);
            object _;
            _attachedEntities.TryRemove(key, out _);
        }

        public bool TryGetAttached(object key, out object value) => _attachedEntities.TryGetValue((string)key, out value);

        internal object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1)
            {
                var value = primaryKeys[0].GetValue(entity);
                return primaryKeys[0].Name + ":" + (value ?? "NULL");
            }
            var parts = new string[primaryKeys.Count];
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                var p = primaryKeys[i];
                var v = p.GetValue(entity);
                var vStr = v != null ? (v.ToString() ?? "").Replace("|", "||") : "NULL";
                parts[i] = p.Name + "=" + vStr;
            }
            return string.Join("|", parts);
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

        private T CloneEntity<T>(T entity)
        {
            var clone = Activator.CreateInstance<T>();
            var props = typeof(T).GetProperties().Where(p => p.CanWrite).ToList();
            foreach (var prop in props)
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
