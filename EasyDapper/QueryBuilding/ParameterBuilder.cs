using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    /// <summary>
    /// Accumulates named SQL parameters for a single <see cref="QueryBuilder{T}"/> instance.
    /// Parameter names are generated sequentially (<c>@p1</c>, <c>@p2</c>, ...) and the
    /// resulting set is exposed as a <see cref="ConcurrentDictionary{TKey, TValue}"/> for
    /// consumption by Dapper.
    /// </summary>
    /// <remarks>
    /// All public members are thread-safe. The order of parameter creation is recorded in
    /// <c>_orderOfCreation</c> so that callers can iterate parameters in insertion order when
    /// needed (e.g. when building a template's parameter list).
    /// </remarks>
    internal sealed class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>();
        private int _paramCounter = 0;
        // FIX (B5): the list of creation-order parameter names is protected by the same lock used
        // by GetUniqueParameterName, ensuring that AddParameter cannot race with
        // GetUniqueParameterName and corrupt the list.
        private readonly List<string> _orderOfCreation = new List<string>();
        private readonly object _orderLock = new object();

        public string GetUniqueParameterName()
        {
            lock (_orderLock)
            {
                var id = Interlocked.Increment(ref _paramCounter);
                var name = "@p" + id.ToString();
                _orderOfCreation.Add(name);
                return name;
            }
        }

        public void AddParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
            _parameters[name] = value;
            // FIX (B5): protect the list mutation with the same lock used by the generator.
            lock (_orderLock)
            {
                if (!_orderOfCreation.Contains(name)) _orderOfCreation.Add(name);
            }
        }

        /// <summary>
        /// Merges parameters from another builder (typically a sub-query's builder) into this
        /// builder. If a parameter name already exists in this builder, the source parameter is
        /// renamed to a fresh unique name and the rename mapping is returned so that the caller
        /// can patch the SQL text accordingly.
        /// </summary>
        public Dictionary<string, string> MergeParameters(ConcurrentDictionary<string, object> sourceParameters)
        {
            var renamings = new Dictionary<string, string>();
            if (sourceParameters == null) return renamings;
            foreach (var kv in sourceParameters)
            {
                string newName = kv.Key;
                if (_parameters.ContainsKey(kv.Key))
                {
                    newName = GetUniqueParameterName();
                    renamings[kv.Key] = newName;
                }
                _parameters[newName] = kv.Value;
            }
            return renamings;
        }

        public ConcurrentDictionary<string, object> GetParameters() => _parameters;

        public List<string> GetOrderedParameterNames()
        {
            lock (_orderLock) { return _orderOfCreation.ToList(); }
        }
    }
}
