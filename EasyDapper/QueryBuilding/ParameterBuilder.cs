using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    internal sealed class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>();
        private int _paramCounter = 0;

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

            lock (_orderLock)
            {
                if (!_orderOfCreation.Contains(name)) _orderOfCreation.Add(name);
            }
        }

        public Dictionary<string, string> MergeParameters(IDictionary<string, object> sourceParameters)
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

        public IDictionary<string, object> GetParameters()
        {
            var snapshot = new Dictionary<string, object>();
            foreach (var kv in _parameters) snapshot[kv.Key] = kv.Value;
            return snapshot;
        }

        internal ConcurrentDictionary<string, object> GetInternalParameters() => _parameters;

        public List<string> GetOrderedParameterNames()
        {
            lock (_orderLock) { return _orderOfCreation.ToList(); }
        }
    }
}
