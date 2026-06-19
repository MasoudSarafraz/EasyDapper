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
        private readonly object _registrationLock = new object();

        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = ExtractShortName(tableName, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return shortName + "_A" + counter.ToString();
        }

        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = ExtractShortName(tableName, 8);
            var counter = Interlocked.Increment(ref _subQueryCounter);
            return shortName + "_SQ" + counter.ToString();
        }

        private static string ExtractShortName(string tableName, int maxLength)
        {
            int lastDot = tableName.LastIndexOf('.');
            string name = lastDot >= 0 ? tableName.Substring(lastDot + 1) : tableName;
            name = name.Trim('[', ']');
            if (name.Length > maxLength) name = name.Substring(0, maxLength);
            return name;
        }

        public void SetTableAlias(string tableName, string alias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Table name and alias cannot be null or empty.");

            lock (_registrationLock)
            {
                string oldAlias;
                if (_tableToAlias.TryGetValue(tableName, out oldAlias) && oldAlias != alias)
                {
                    AliasInfo removed;
                    _allAliases.TryRemove(oldAlias, out removed);
                }

                var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
                AliasInfo existingInfo;
                if (_allAliases.TryGetValue(alias, out existingInfo))
                {
                    if (existingInfo.Type == AliasType.Table
                        && existingInfo.Key is string existingTableName
                        && existingTableName == tableName)
                    {
                        _tableToAlias[tableName] = alias;
                        return;
                    }
                    string existingKeyStr = (existingInfo.Key is string)
                        ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                        + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
                }
                _allAliases.TryAdd(alias, newInfo);
                _tableToAlias[tableName] = alias;
            }
        }

        public void SetTypeAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Type and alias cannot be null or empty.");
            var targetTableName = QueryBuilderCache.GetTableName(type);
            lock (_registrationLock)
            {
                AliasInfo existingInfo;
                if (_allAliases.TryGetValue(alias, out existingInfo))
                {
                    if (existingInfo.Type == AliasType.Table
                        && existingInfo.Key is string existingTableName
                        && existingTableName == targetTableName)
                    {
                        _typeToAlias[type] = alias;
                        return;
                    }
                    string existingKeyStr = (existingInfo.Key is string)
                        ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                        + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
                }
                else
                {
                    var newInfo = new AliasInfo { Type = AliasType.Table, Key = targetTableName };
                    _allAliases.TryAdd(alias, newInfo);
                    _tableToAlias[targetTableName] = alias;
                    _typeToAlias[type] = alias;
                }
            }
        }

        public void SetSubQueryAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Type and alias cannot be null or empty.");
            lock (_registrationLock)
            {
                var newInfo = new AliasInfo { Type = AliasType.SubQuery, Key = type };
                AliasInfo existingInfo;
                if (_allAliases.TryGetValue(alias, out existingInfo))
                {
                    if (existingInfo.Type == AliasType.SubQuery
                        && existingInfo.Key is Type existingType
                        && existingType == type)
                    {
                        _subQueryTypeToAlias[type] = alias;
                        return;
                    }
                    string existingKeyStr = (existingInfo.Key is string)
                        ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by "
                        + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
                }
                _allAliases.TryAdd(alias, newInfo);
                _subQueryTypeToAlias[type] = alias;
            }
        }

        public string GetAliasForTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");

            string existing;
            if (_tableToAlias.TryGetValue(tableName, out existing)) return existing;

            lock (_registrationLock)
            {
                if (_tableToAlias.TryGetValue(tableName, out existing)) return existing;

                string newAlias;
                while (true)
                {
                    newAlias = GenerateAlias(tableName);
                    var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
                    if (_allAliases.TryAdd(newAlias, newInfo))
                    {
                        _tableToAlias[tableName] = newAlias;
                        return newAlias;
                    }
                }
            }
        }

        public string GetAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");

            string existing;
            if (_typeToAlias.TryGetValue(type, out existing)) return existing;

            lock (_registrationLock)
            {
                if (_typeToAlias.TryGetValue(type, out existing)) return existing;

                var tableName = QueryBuilderCache.GetTableName(type);
                var alias = GetAliasForTable(tableName);
                _typeToAlias[type] = alias;
                return alias;
            }
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
            AliasInfo info;
            if (_allAliases.TryGetValue(alias, out info))
            {
                return info.Type == AliasType.SubQuery;
            }
            return false;
        }

        public IEnumerable<KeyValuePair<string, string>> GetAllTableAliases()
        {
            return _allAliases
                .Where(kvp => kvp.Value.Type == AliasType.Table)
                .Select(kvp => new KeyValuePair<string, string>((string)kvp.Value.Key, kvp.Key))
                .ToList();
        }

        public void ClearAliases()
        {
            lock (_registrationLock)
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
}
