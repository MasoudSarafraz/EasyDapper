using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal class QueryCache : IDisposable
    {
        private readonly SimpleConcurrentCache<Type, string> InsertQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> UpdateQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> DeleteQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> GetByIdQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, List<PropertyInfo>> PrimaryKeyCache = new SimpleConcurrentCache<Type, List<PropertyInfo>>();
        private readonly SimpleConcurrentCache<Type, PropertyInfo> IdentityPropertyCache = new SimpleConcurrentCache<Type, PropertyInfo>();
        private readonly SimpleConcurrentCache<Type, string> TableNameCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<PropertyInfo, string> ColumnNameCache = new SimpleConcurrentCache<PropertyInfo, string>();
        private const string DEFAULT_SCHEMA = "dbo";
        private static readonly char[] InvalidIdentifierChars = new[] { ';', '-', '/', '*', '\'', '"', '[', ']' };

        private string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
                throw new ArgumentException("Identifier cannot be null or empty.", "identifier");
            if (identifier.IndexOfAny(InvalidIdentifierChars) >= 0)
                throw new ArgumentException("Identifier contains invalid characters.", "identifier");
            return identifier.Replace("]", "]]");
        }

        public string GetTableName<T>()
        {
            var type = typeof(T);
            return TableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>(true);
                var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema)
                    ? SanitizeIdentifier(tableAttr.Schema) : DEFAULT_SCHEMA;
                var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName)
                    ? SanitizeIdentifier(tableAttr.TableName) : SanitizeIdentifier(t.Name);
                return "[" + schema + "].[" + name + "]";
            });
        }

        public string GetColumnName(PropertyInfo property)
        {
            if (property == null) throw new ArgumentNullException("property");
            return ColumnNameCache.GetOrAdd(property, p =>
            {
                var columnAttr = p.GetCustomAttribute<ColumnAttribute>(true);
                var name = columnAttr != null && !string.IsNullOrWhiteSpace(columnAttr.ColumnName)
                    ? SanitizeIdentifier(columnAttr.ColumnName) : SanitizeIdentifier(p.Name);
                return "[" + name + "]";
            });
        }

        public List<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            return PrimaryKeyCache.GetOrAdd(typeof(T), type =>
            {
                var properties = type.GetProperties()
                    .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                    .ToList();
                if (properties.Count == 0)
                    throw new InvalidOperationException($"No primary key defined for {type.Name}");
                if (properties.Count(p => p.GetCustomAttribute<IdentityAttribute>(true) != null) > 1)
                    throw new InvalidOperationException("Multiple Identity primary keys are not supported");
                return properties;
            });
        }

        public PropertyInfo GetIdentityProperty<T>()
        {
            return IdentityPropertyCache.GetOrAdd(typeof(T), type =>
                type.GetProperties().FirstOrDefault(p =>
                    p.GetCustomAttribute<IdentityAttribute>(true) != null &&
                    p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null));
        }

        public string GetInsertQuery<T>() => InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
        public string GetUpdateQuery<T>() => UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
        public string GetDeleteQuery<T>() => DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
        public string GetGetByIdQuery<T>() => GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);

        private string BuildInsertQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(GetColumnName));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {
                return $"INSERT INTO {tableName} ({columns}) VALUES ({values}); SELECT CAST(SCOPE_IDENTITY() AS INT);";
            }
            return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        }

        private string BuildUpdateQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var primaryKeys = GetPrimaryKeyProperties<T>();
            var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p) && !IsIdentity(p)).ToList();
            if (properties.Count > 0)
            {
                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            }
            var updatablePrimaryKeys = primaryKeys.Where(p => !IsIdentity(p)).ToList();
            if (updatablePrimaryKeys.Count == 0)
                throw new InvalidOperationException($"Cannot update type {type.Name}. All properties are identity primary keys.");
            var setClauseForPrimaryKeys = string.Join(", ", updatablePrimaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            var whereClauseForPrimaryKeys = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @old_{p.Name}"));
            return $"UPDATE {tableName} SET {setClauseForPrimaryKeys} WHERE {whereClauseForPrimaryKeys}";
        }

        private string BuildDeleteQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var primaryKeys = GetPrimaryKeyProperties<T>();
            var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            return $"DELETE FROM {tableName} WHERE {whereClause}";
        }

        private string BuildGetByIdQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var primaryKeys = GetPrimaryKeyProperties<T>();
            string whereClause;
            if (primaryKeys.Count == 1)
            {
                var pk = primaryKeys.First();
                whereClause = $"{GetColumnName(pk)} = @Id";
            }
            else
            {
                whereClause = string.Join(" AND ", primaryKeys.Select(pk => $"{GetColumnName(pk)} = @{pk.Name}"));
            }
            var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
            return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() =>
            typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);

        private bool IsPrimaryKey(PropertyInfo property) =>
            property.GetCustomAttribute<PrimaryKeyAttribute>(true) != null;

        private bool IsIdentity(PropertyInfo property) =>
            property.GetCustomAttribute<IdentityAttribute>(true) != null;

        public void Dispose() { /* SimpleConcurrentCache doesn't need explicit disposal */ }
    }
}
