using System;
using System.Collections.Concurrent;

namespace EasyDapper
{
    /// <summary>
    /// A simple thread-safe cache wrapper around <see cref="ConcurrentDictionary{TKey, TValue}"/>.
    /// Used internally to cache SQL generation artifacts (table names, column names, primary key
    /// metadata, etc.) so that reflection is paid for only once per type.
    /// </summary>
    /// <typeparam name="TKey">The cache key type.</typeparam>
    /// <typeparam name="TValue">The cache value type (must be a reference type).</typeparam>
    internal sealed class SimpleConcurrentCache<TKey, TValue> where TValue : class
    {
        private readonly ConcurrentDictionary<TKey, TValue> _cache = new ConcurrentDictionary<TKey, TValue>();

        /// <summary>
        /// Gets the cached value for the supplied key, or creates and stores it using the supplied factory.
        /// </summary>
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory) => _cache.GetOrAdd(key, valueFactory);

        /// <summary>Attempts to retrieve a cached value.</summary>
        public bool TryGetValue(TKey key, out TValue value) => _cache.TryGetValue(key, out value);

        /// <summary>Removes all entries from the cache.</summary>
        public void Clear() => _cache.Clear();

        /// <summary>Returns the current number of entries in the cache.</summary>
        public int Count => _cache.Count;
    }
}
