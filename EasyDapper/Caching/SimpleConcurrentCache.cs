using System;
using System.Collections.Concurrent;

namespace EasyDapper
{
    internal sealed class SimpleConcurrentCache<TKey, TValue> where TValue : class
    {
        private readonly ConcurrentDictionary<TKey, TValue> _cache = new ConcurrentDictionary<TKey, TValue>();

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory) => _cache.GetOrAdd(key, valueFactory);

        public bool TryGetValue(TKey key, out TValue value) => _cache.TryGetValue(key, out value);

        public void Clear() => _cache.Clear();

        public int Count => _cache.Count;
    }
}
