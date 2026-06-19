using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Dapper;

namespace EasyDapper
{
    /// <summary>
    /// Builds SQL fragments and parameter bags for dynamic operations such as
    /// change-tracked UPDATEs, scalar/table-valued function calls and bulk-copy data tables.
    /// </summary>
    internal class SqlBuilder
    {
        private readonly QueryCache _queryCache;

        public SqlBuilder(QueryCache queryCache)
        {
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            _queryCache = queryCache;
        }

        /// <summary>
        /// Builds an UPDATE statement that only modifies the supplied changed properties.
        /// </summary>
        public string BuildDynamicUpdateQuery<T>(List<string> changedProps, List<PropertyInfo> primaryKeys)
        {
            var tableName = _queryCache.GetTableName<T>();
            var changedProperties = changedProps
                .Select(p => typeof(T).GetProperty(p))
                .Where(p => p != null)
                .ToList();
            var setClause = string.Join(", ", changedProperties.Select(p => $"{_queryCache.GetColumnName(p)} = @{p.Name}"));
            var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{_queryCache.GetColumnName(p)} = @pk_{p.Name}"));
            return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
        }

        public DynamicParameters BuildParameters<T>(T entity, List<PropertyInfo> primaryKeys, List<string> changedProps)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in primaryKeys) parameters.Add($"pk_{pk.Name}", pk.GetValue(entity));
            foreach (var propName in changedProps)
            {
                var prop = typeof(T).GetProperty(propName);
                if (prop != null) parameters.Add(prop.Name, prop.GetValue(entity));
            }
            return parameters;
        }

        public DynamicParameters CreatePrimaryKeyParameters<T>(T entity)
        {
            var parameters = new DynamicParameters();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            foreach (var pk in primaryKeys) parameters.Add(pk.Name, pk.GetValue(entity));
            return parameters;
        }

        public DynamicParameters GetPrimaryKeyValues<T>(T entity)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in _queryCache.GetPrimaryKeyProperties<T>())
            {
                var value = pk.GetValue(entity);
                parameters.Add(pk.Name, value);
            }
            return parameters;
        }

        public string BuildScalarFunctionQuery(string functionName, object parameters)
            => $"SELECT {functionName}({BuildParameters(parameters)})";

        public string BuildTableFunctionQuery(string functionName, object parameters)
            => $"SELECT * FROM {functionName}({BuildParameters(parameters)})";

        private string BuildParameters(object parameters)
        {
            if (parameters == null) return "";
            return string.Join(", ", parameters.GetType().GetProperties().Select(p => $"@{p.Name}"));
        }

        public void ExecuteRawCommand(IDbConnection connection, IDbTransaction transaction, string commandText)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentNullException("commandText");
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Converts a sequence of entities into a <see cref="DataTable"/> suitable for
        /// <see cref="SqlBulkCopy"/>. Column types are derived from the CLR property types,
        /// with <see cref="Nullable{T}"/> unwrapped to its underlying type so that ADO.NET
        /// can map values to SQL Server types correctly.
        /// </summary>
        public DataTable ToDataTable<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> properties)
        {
            var dataTable = new DataTable();
            var propertyList = properties as IList<PropertyInfo> ?? properties.ToList();
            foreach (var property in propertyList)
            {
                var columnName = _queryCache.GetColumnName(property);
                var rawColumnName = columnName.Trim('[', ']');
                dataTable.Columns.Add(rawColumnName,
                    Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }
            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();
                foreach (var property in propertyList)
                {
                    var columnName = _queryCache.GetColumnName(property);
                    var rawColumnName = columnName.Trim('[', ']');
                    row[rawColumnName] = property.GetValue(entity) ?? DBNull.Value;
                }
                dataTable.Rows.Add(row);
            }
            return dataTable;
        }
    }
}
