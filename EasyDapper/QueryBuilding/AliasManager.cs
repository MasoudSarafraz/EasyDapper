using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    internal sealed class AliasManager
    {
        private sealed class AliasInfo { public AliasType Type { get; set; } public object Key { get; set; } }
        private enum AliasType { Table, Type, SubQuery }

        private readonly ConcurrentDictionary<string, AliasInfo> _allAliases = new ConcurrentDictionary<string, AliasInfo>();
        private readonly ConcurrentDictionary<string, string> _tableToAlias = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeToAlias = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeToAlias = new ConcurrentDictionary<Type, string>();

        private int _aliasCounter = 0;
        private int _subQueryCounter = 0;

        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return string.Format("{0}_A{1}", shortName, counter);
        }

        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 8) shortName = shortName.Substring(0, 8);
            var counter = Interlocked.Increment(ref _subQueryCounter);
            return string.Format("{0}_SQ{1}", shortName, counter);
        }

        public void SetTableAlias(string tableName, string alias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Table name and alias cannot be null or empty.");

            string oldAlias;
            if (_tableToAlias.TryGetValue(tableName, out oldAlias) && oldAlias != alias)
            {
                AliasInfo removed;
                _allAliases.TryRemove(oldAlias, out removed);
            }

            var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
            if (!_allAliases.TryAdd(alias, newInfo))
            {
                var existingInfo = _allAliases[alias];
                string existingKeyStr = (existingInfo.Key is string)
                    ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                    + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            _tableToAlias.AddOrUpdate(tableName, alias, (k, v) => alias);
        }

        public void SetTypeAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Type and alias cannot be null or empty.");
            var targetTableName = QueryBuilderCache.GetTableName(type);
            if (_allAliases.TryGetValue(alias, out var existingInfo))
            {
                if (existingInfo.Type == AliasType.Table
                    && existingInfo.Key is string existingTableName
                    && existingTableName == targetTableName)
                {
                    _typeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
                    return;
                }
                string existingKeyStr = (existingInfo.Key is string)
                    ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                    + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            else
            {
                SetTableAlias(targetTableName, alias);
                _typeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
            }
        }

        public void SetSubQueryAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Type and alias cannot be null or empty.");
            var newInfo = new AliasInfo { Type = AliasType.SubQuery, Key = type };
            if (!_allAliases.TryAdd(alias, newInfo))
            {
                var existingInfo = _allAliases[alias];
                string existingKeyStr = (existingInfo.Key is string)
                    ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                    + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            _subQueryTypeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
        }

        public string GetAliasForTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");

            return _tableToAlias.GetOrAdd(tableName, tn =>
            {
                string newAlias;
                while (true)
                {
                    newAlias = GenerateAlias(tn);
                    var newInfo = new AliasInfo { Type = AliasType.Table, Key = tn };
                    if (_allAliases.TryAdd(newAlias, newInfo))
                    {
                        return newAlias;
                    }
                }
            });
        }

        public string GetAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return _typeToAlias.GetOrAdd(type, t =>
            {
                var tableName = QueryBuilderCache.GetTableName(t);
                return GetAliasForTable(tableName);
            });
        }

        public string GetUniqueAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableName = QueryBuilderCache.GetTableName(type);
            string newAlias;
            while (true)
            {
                newAlias = GenerateAlias(tableName);

                var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
                if (_allAliases.TryAdd(newAlias, newInfo))
                {
                    return newAlias;
                }
            }
        }

        public bool TryGetTypeAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _typeToAlias.TryGetValue(type, out alias);
        }

        public bool TryGetSubQueryAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _subQueryTypeToAlias.TryGetValue(type, out alias);
        }

        public bool IsSubqueryAlias(string alias)
        {
            if (string.IsNullOrEmpty(alias)) return false;
            if (_allAliases.TryGetValue(alias, out var info))
            {
                return info.Type == AliasType.SubQuery;
            }
            return false;
        }

        public IEnumerable<KeyValuePair<string, string>> GetAllTableAliases()
        {
            return _allAliases
                .Where(kvp => kvp.Value.Type == AliasType.Table)
                .Select(kvp => new KeyValuePair<string, string>((string)kvp.Value.Key, kvp.Key));
        }

        public void ClearAliases()
        {
            _allAliases.Clear();
            _tableToAlias.Clear();
            _typeToAlias.Clear();
            _subQueryTypeToAlias.Clear();
            Interlocked.Exchange(ref _aliasCounter, 0);
            Interlocked.Exchange(ref _subQueryCounter, 0);
        }
    }
}
