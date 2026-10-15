using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace EasyDapper
{
    internal sealed class AliasManager
    {
        private sealed class AliasInfo { public AliasType Type { get; set; } public object Key { get; set; } }
        private enum AliasType { Table, Type, SubQuery, Reserved }

        private readonly ConcurrentDictionary<string, AliasInfo> _allAliases = new ConcurrentDictionary<string, AliasInfo>();
        private readonly ConcurrentDictionary<string, string> _tableToAlias = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeToAlias = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeToAlias = new ConcurrentDictionary<Type, string>();
        // Generated names must also avoid collisions in case-insensitive databases.
        // Explicit aliases retain their original ordinal identity for CS databases.
        private readonly HashSet<string> _reservedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private int _aliasCounter = 0;
        private int _subQueryCounter = 0;
        private readonly object _registrationLock = new object();
        private static readonly Regex OrdinaryGeneratedName = new Regex(
            @"\A[\p{L}_#][\p{L}\p{N}_#$]*(?:_A|_SQ)[0-9]+\z", RegexOptions.CultureInvariant);

        internal static string SqlReference(string alias)
        {
            if (string.IsNullOrEmpty(alias)) return alias;
            // Keep ordinary generated SQL text stable. Other names can include a
            // leading digit, a keyword or delimited-identifier characters.
            return OrdinaryGeneratedName.IsMatch(alias) ? alias : "[" + alias.Replace("]", "]]") + "]";
        }

        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = ExtractShortName(tableName, 10);
            lock (_registrationLock)
            {
                string alias;
                do { alias = shortName + "_A" + Interlocked.Increment(ref _aliasCounter).ToString(); }
                while (_reservedNames.Contains(alias));
                return alias;
            }
        }

        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = ExtractShortName(tableName, 8);
            lock (_registrationLock)
            {
                string alias;
                do { alias = shortName + "_SQ" + Interlocked.Increment(ref _subQueryCounter).ToString(); }
                while (_reservedNames.Contains(alias));
                return alias;
            }
        }

        internal void ReserveOuterAliases(AliasManager outer)
        {
            if (outer == null || ReferenceEquals(outer, this)) return;
            string[] aliases;
            // Never hold two managers' locks at once: nested scopes can reserve
            // each other concurrently, and a reservation must observe one snapshot.
            lock (outer._registrationLock) { aliases = outer._allAliases.Keys.ToArray(); }
            lock (_registrationLock)
            {
                foreach (var alias in aliases)
                    RegisterAlias(alias, new AliasInfo { Type = AliasType.Reserved, Key = alias });
            }
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
                RegisterTableAlias(tableName, alias);
            }
        }

        private void RegisterTableAlias(string tableName, string alias)
        {
            AliasInfo existingInfo;
            if (_allAliases.TryGetValue(alias, out existingInfo) &&
                (existingInfo.Type != AliasType.Table || !Equals(existingInfo.Key, tableName)))
                throw AliasConflict(alias, existingInfo);

            // Validate the new owner before releasing any existing registration.
            RegisterAlias(alias, new AliasInfo { Type = AliasType.Table, Key = tableName });
            string oldAlias;
            if (_tableToAlias.TryGetValue(tableName, out oldAlias) && oldAlias != alias)
            {
                AliasInfo removed;
                _allAliases.TryRemove(oldAlias, out removed);
                if (!_allAliases.Keys.Any(name => StringComparer.OrdinalIgnoreCase.Equals(name, oldAlias)))
                    _reservedNames.Remove(oldAlias);
                foreach (var entry in _typeToAlias.Where(entry => entry.Value == oldAlias).ToArray())
                    _typeToAlias[entry.Key] = alias;
            }
            _tableToAlias[tableName] = alias;
        }

        private void RegisterAlias(string alias, AliasInfo info)
        {
            _allAliases.TryAdd(alias, info);
            _reservedNames.Add(alias);
        }

        private static InvalidOperationException AliasConflict(string alias, AliasInfo info)
        {
            var key = info.Key is string ? (string)info.Key : ((Type)info.Key).Name;
            return new InvalidOperationException("Alias '" + alias + "' is already used by "
                + info.Type.ToString().ToLower() + " '" + key + "'.");
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
                        if (!_tableToAlias.ContainsKey(targetTableName)) _tableToAlias[targetTableName] = alias;
                        _typeToAlias[type] = alias;
                        return;
                    }
                    throw AliasConflict(alias, existingInfo);
                }
                else
                {
                    RegisterTableAlias(targetTableName, alias);
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
                    throw AliasConflict(alias, existingInfo);
                }
                RegisterAlias(alias, newInfo);
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

                var newAlias = GenerateAlias(tableName);
                RegisterAlias(newAlias, new AliasInfo { Type = AliasType.Table, Key = tableName });
                _tableToAlias[tableName] = newAlias;
                return newAlias;
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
            lock (_registrationLock)
            {
                var newAlias = GenerateAlias(tableName);
                var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
                RegisterAlias(newAlias, newInfo);
                return newAlias;
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

        internal bool TryGetTableAlias(string tableName, out string alias)
            => _tableToAlias.TryGetValue(tableName, out alias);

        internal bool IsReservedName(string alias)
        {
            lock (_registrationLock) return _reservedNames.Contains(alias);
        }

        internal bool ReserveAliasName(string alias)
        {
            if (string.IsNullOrEmpty(alias))
                throw new ArgumentException("Alias cannot be null or empty.", nameof(alias));

            lock (_registrationLock)
            {
                if (_reservedNames.Contains(alias)) return false;
                RegisterAlias(alias, new AliasInfo { Type = AliasType.Reserved, Key = alias });
                return true;
            }
        }

        internal void ClearSubQueryTypeBindings()
        {
            // SET captures previous scopes in SQL, but later clauses belong to a
            // new source scope. Historical names remain reserved, not active roles.
            lock (_registrationLock) _subQueryTypeToAlias.Clear();
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
            lock (_registrationLock) return _allAliases
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
                _reservedNames.Clear();
                Interlocked.Exchange(ref _aliasCounter, 0);
                Interlocked.Exchange(ref _subQueryCounter, 0);
            }
        }
    }
}
