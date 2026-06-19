using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    internal sealed class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
        private int _paramCounter = 0;

        public string GetUniqueParameterName()
        {
            var id = Interlocked.Increment(ref _paramCounter);
            return "@p" + id.ToString();
        }

        public void AddParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
            _parameters[name] = value;
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
            return new Dictionary<string, object>(_parameters, StringComparer.Ordinal);
        }

        internal ConcurrentDictionary<string, object> GetInternalParameters() => _parameters;

        public List<string> GetOrderedParameterNames()
        {
            return _parameters.Keys.ToList();
        }
    }
}
