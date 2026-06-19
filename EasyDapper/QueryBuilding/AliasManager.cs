using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    /// <summary>
    /// Allocates and tracks table/subquery aliases for a single <see cref="QueryBuilder{T}"/>
    /// instance. Each <see cref="QueryBuilder{T}"/> owns its own <see cref="AliasManager"/> so
    /// that aliases from sibling query builders cannot collide.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three categories of aliases are tracked:
    /// <list type="bullet">
    /// <item><description><c>Table</c> aliases identify a physical table referenced in FROM or JOIN.
    /// Stored in <c>_tableToAlias</c> keyed by the full <c>[schema].[table]</c> name.</description></item>
    /// <item><description><c>Type</c> aliases identify a CLR type. Stored in <c>_typeToAlias</c>.
    /// When a type is mapped to a table the type alias is identical to the table alias.</description></item>
    /// <item><description><c>SubQuery</c> aliases identify a derived table produced by CROSS/OUTER
    /// APPLY. Stored in <c>_subQueryTypeToAlias</c> keyed by the subquery's element
    /// type.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// A single registry (<c>_allAliases</c>) prevents collisions across all three categories.
    /// </para>
    /// <para>
    /// <b>Important:</b> When the same type is JOINed multiple times (self-join or repeated join)
    /// <see cref="GetUniqueAliasForType"/> allocates a fresh alias WITHOUT overwriting the type's
    /// primary alias. This means that subsequent references to the type via
    /// <see cref="GetAliasForType"/> still resolve to the original (FROM) alias, which is what
    /// callers expect when building <c>SELECT t.*</c> style queries.
    /// </para>
    /// </remarks>
    internal sealed class AliasManager
    {
        private sealed class AliasInfo { public AliasType Type { get; set; } public object Key { get; set; } }
        private enum AliasType { Table, Type, SubQuery }

        private readonly ConcurrentDictionary<string, AliasInfo> _allAliases = new ConcurrentDictionary<string, AliasInfo>();
        private readonly ConcurrentDictionary<string, string> _tableToAlias = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeToAlias = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeToAlias = new ConcurrentDictionary<Type, string>();

        // FIX (B1): initial value is 0 so that the first Interlocked.Increment returns 1 (not 2),
        // producing aliases like "Foo_A1" instead of "Foo_A2" for the first reference.
        private int _aliasCounter = 0;
        private int _subQueryCounter = 0;

        /// <summary>
        /// Generates a fresh, unique table-style alias derived from the supplied table name.
        /// The short name is taken from the last path segment of the table name (after the final
        /// dot) and truncated to 10 characters to keep aliases readable. A monotonically
        /// increasing counter guarantees uniqueness within this manager.
        /// </summary>
        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return string.Format("{0}_A{1}", shortName, counter);
        }

        /// <summary>
        /// Generates a fresh, unique subquery-style alias. Short name is truncated to 8 chars
        /// and suffixed with <c>_SQ&lt;n&gt;</c>.
        /// </summary>
        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 8) shortName = shortName.Substring(0, 8);
            var counter = Interlocked.Increment(ref _subQueryCounter);
            return string.Format("{0}_SQ{1}", shortName, counter);
        }

        /// <summary>
        /// Registers an explicit alias for a table. If a different alias was previously registered
        /// for the same table, that previous alias is released from the global registry so that
        /// it can be reused by a future call to <see cref="GenerateAlias"/> (prevents the leak
        /// described in scenario C4 of the code review).
        /// </summary>
        public void SetTableAlias(string tableName, string alias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(alias))
                throw new ArgumentException("Table name and alias cannot be null or empty.");

            // FIX (C4): release the previously-registered alias for this table (if any) so that
            // it does not leak in _allAliases forever.
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
                // Idempotent: re-setting the same alias for the same table is fine.
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

        /// <summary>
        /// Registers an alias for a subquery (produced by CROSS/OUTER APPLY). Subquery aliases
        /// are tracked separately from table aliases so that <see cref="TryGetSubQueryAlias"/>
        /// can resolve them independently when both a JOIN and an APPLY target the same type.
        /// </summary>
        /// <remarks>
        /// If multiple APPLYs target the same type, only the most recently-registered alias is
        /// returned by <see cref="TryGetSubQueryAlias"/>. Callers that need to reference an
        /// earlier subquery alias should capture it explicitly when building the APPLY.
        /// </remarks>
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
            // Note: ConcurrentDictionary.GetOrAdd may invoke the factory more than once under
            // concurrent access. The factory below uses an inner retry loop that only succeeds
            // when the generated alias is unique in _allAliases, so duplicate factory invocations
            // are harmless (they simply allocate and discard a few strings).
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

        /// <summary>
        /// Allocates a fresh alias for a type that has already been registered with a different
        /// alias (used for self-joins and repeated joins). The newly-allocated alias is NOT
        /// registered as the type's primary alias, so subsequent <see cref="GetAliasForType"/>
        /// calls continue to resolve to the original (FROM) alias. This is the fix for the
        /// critical Self-Join bug (scenario C1) where SELECT clauses were incorrectly emitting
        /// the JOIN-side alias instead of the FROM-side alias.
        /// </summary>
        public string GetUniqueAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableName = QueryBuilderCache.GetTableName(type);
            string newAlias;
            while (true)
            {
                newAlias = GenerateAlias(tableName);
                // FIX (B13): register as Table so that alias-lookup helpers like
                // IsSubqueryAlias correctly distinguish this from a SubQuery alias.
                var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
                if (_allAliases.TryAdd(newAlias, newInfo))
                {
                    // FIX (C1): do NOT call _typeToAlias.AddOrUpdate here. Doing so would
                    // overwrite the FROM-side alias and cause SELECT * to emit the wrong alias.
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
