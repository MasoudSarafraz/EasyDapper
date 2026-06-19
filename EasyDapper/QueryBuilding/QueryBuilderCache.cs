using System;
using System.Collections.Concurrent;
using System.Reflection;
using EasyDapper.Attributes;

namespace EasyDapper
{
    /// <summary>
    /// Process-wide (static) cache for table- and column-name resolution. Static caching is safe
    /// because the mapping from CLR type/member to SQL identifier is deterministic and depends
    /// only on attribute metadata that cannot change at runtime.
    /// </summary>
    internal static class QueryBuilderCache
    {
        private static readonly ConcurrentDictionary<Type, string> _tableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> _columnNameCache = new ConcurrentDictionary<MemberInfo, string>();

        // Characters that are unsafe to interpolate into a SQL identifier even when wrapped in
        // brackets. Brackets themselves are excluded (they would close the wrapping). Hyphen, slash,
        // star, semicolon, quote and double-quote are excluded because they have special meaning
        // in T-SQL and could enable injection if a downstream component forgets to bracket.
        private static readonly char[] InvalidIdentifierChars =
            new[] { ';', '-', '/', '*', '\'', '"', '[', ']', '(', ')', '&', '|', '^', '%', '~',
                    '`', '$', '{', '}', '<', '>', '?', '!', '=', '+', ',', ':', '\\', ' ', '\t', '\n', '\r' };

        public static string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return _tableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema)
                    ? "[" + Sanitize(tableAttr.Schema) + "]" : "[dbo]";
                var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName)
                    ? "[" + Sanitize(tableAttr.TableName) + "]" : "[" + Sanitize(t.Name) + "]";
                return schema + "." + name;
            });
        }

        public static string GetColumnName(MemberInfo member)
        {
            if (member == null) throw new ArgumentNullException("member", "MemberInfo cannot be null");
            return _columnNameCache.GetOrAdd(member, m =>
            {
                var column = m.GetCustomAttribute<ColumnAttribute>();
                return column != null && !string.IsNullOrWhiteSpace(column.ColumnName)
                    ? Sanitize(column.ColumnName) : Sanitize(m.Name);
            });
        }

        private static string Sanitize(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                throw new ArgumentException("Identifier cannot be null or empty.", "identifier");
            if (identifier.IndexOfAny(InvalidIdentifierChars) >= 0)
                throw new ArgumentException("Identifier contains invalid characters: " + identifier, "identifier");
            return identifier;
        }
    }
}
