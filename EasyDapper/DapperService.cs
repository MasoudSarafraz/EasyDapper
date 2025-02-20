using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Interfaces;
using EasyDapper.Attributes;
using EasyDapper.Factories;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.SqlTypes;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;
using System.Xml.Linq;


namespace EasyDapper.Implementations
{
    internal sealed class DapperService : IDapperService, IDisposable
    {
        private readonly IDbConnection _externalConnection;
        private readonly Lazy<IDbConnection> _lazyConnection;
        private IDbTransaction _transaction;
        private static readonly ConcurrentDictionary<Type, string> InsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> UpdateQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> DeleteQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> GetByIdQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> BulkInsertQueryCache = new ConcurrentDictionary<Type, string>();

        public DapperService(string connectionString)
        {
            _lazyConnection = new Lazy<IDbConnection>(() => new SqlConnection(connectionString));
        }
        public DapperService(IDbConnection externalConnection)
        {
            _externalConnection = externalConnection ?? throw new ArgumentNullException(nameof(externalConnection));
        }
        public void BeginTransaction()
        {
            if (_transaction != null)
                throw new InvalidOperationException("A transaction is already in progress.");

            var connection = GetOpenConnection();
            _transaction = connection.BeginTransaction();
        }
        public async Task BeginTransactionAsync()
        {
            if (_transaction != null)
                throw new InvalidOperationException("A transaction is already in progress.");

            var connection = await GetOpenConnectionAsync();
            _transaction = connection.BeginTransaction();
        }
        public void CommitTransaction()
        {
            if (_transaction == null)
                throw new InvalidOperationException("No transaction is in progress.");

            try
            {
                _transaction.Commit();
            }
            catch
            {
                _transaction.Rollback();
                throw;
            }
            finally
            {
                CleanupTransaction();
            }
        }
        public void RollbackTransaction()
        {
            if (_transaction == null)
                throw new InvalidOperationException("No transaction is in progress.");

            try
            {
                _transaction.Rollback();
            }
            finally
            {
                CleanupTransaction();
            }
        }
        //public int Insert<T>(T entity) where T : class
        //{
        //    var connection = GetOpenConnection();
        //    var query = InsertQueryCache.GetOrAdd(typeof(T), type =>
        //    {
        //        var tableName = GetTableName<T>();
        //        var properties = GetInsertProperties<T>();
        //        var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
        //        var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
        //        return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        //    });
        //    return connection.Execute(query, entity, _transaction);
        //}
        public int Insert<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp != null)
            {
                var newId = connection.ExecuteScalar<int>(query, entity, _transaction);
                identityProp.SetValue(entity, newId);
                return 1;
            }
            return connection.Execute(query, entity, _transaction);
        }
        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync<int>(query, entity, _transaction);
                identityProp.SetValue(entity, newId);
                return 1;
            }
            return await connection.ExecuteAsync(query, entity, _transaction);
        }
        public int InsertList<T>(IEnumerable<T> entities) where T : class
        {
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;
            var connection = GetOpenConnection() as SqlConnection;
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {  
                var query = BuildOptimizedBatchInsertQuery<T>(entityList.Count);
                var parameters = CreateOptimizedParameters(entityList);
                var generatedIds = connection.Query<int>(query, parameters, _transaction).ToList();
                Parallel.For(0, entityList.Count, i =>
                {
                    identityProp.SetValue(entityList[i], generatedIds[i]);
                });
                return entityList.Count;
            }
            //return connection.Execute(BuildBulkInsertQuery<T>(), entityList, _transaction);
            var bulkQuery = BulkInsertQueryCache.GetOrAdd(typeof(T), BuildBulkInsertQuery<T>);
            return connection.Execute(bulkQuery, entityList, _transaction);
        }
        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities) where T : class
        {
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;
            var connection = await GetOpenConnectionAsync() as SqlConnection;
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {                
                var query = BuildOptimizedBatchInsertQuery<T>(entityList.Count);                
                var parameters = CreateParametersAsync(entityList);                
                var generatedIds = (await connection.QueryAsync<int>(query, parameters, _transaction)).ToList();
                for (int i = 0; i < entityList.Count; i++)
                {
                    identityProp.SetValue(entityList[i], generatedIds[i]);
                }
                return entityList.Count;
            }
            //return await connection.ExecuteAsync(BuildBulkInsertQuery<T>(), entityList, _transaction);
            var bulkQuery = BulkInsertQueryCache.GetOrAdd(typeof(T), BuildBulkInsertQuery<T>);
            return await connection.ExecuteAsync(bulkQuery, entityList, _transaction);
        }
        public int Update<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            return connection.Execute(query, entity, _transaction);
        }
        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            return await connection.ExecuteAsync(query, entity, _transaction);
        }
        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            return connection.Execute(query, entities, _transaction);
        }
        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            return await connection.ExecuteAsync(query, entities, _transaction);
        }
        public int Delete<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);
            return connection.Execute(query, parameters, _transaction);
        }
        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters, _transaction);
        }
        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = entities.Select(CreatePrimaryKeyParameters);
            return connection.Execute(query, parameters, _transaction);
        }
        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = entities.Select(CreatePrimaryKeyParameters);
            return await connection.ExecuteAsync(query, parameters, _transaction);
        }
        public T GetById<T>(object id) where T : class
        {
            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);
            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _transaction);
        }
        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);
            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _transaction);
        }
        public T GetById<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = GetSelectColumns<T>();
                var whereClause = BuildWhereClause<T>();
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });
            var parameters = GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters, _transaction);
        }
        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = string.Join(", ", typeof(T).GetProperties()
                    .Select(p => $"{GetColumnName(p)} AS {p.Name}"));
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyProperties<T>().ToDictionary(p => p.Name, p => p.GetValue(entity));

            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _transaction);
        }
        public IQueryBuilder<T> QueryBuilder<T>()
        {
            if (_externalConnection != null)
            {
                throw new InvalidOperationException("QueryBuilder is not supported when using an external connection.");
            }
            return new QueryBuilder<T>(_lazyConnection.Value.ConnectionString);
        }
        public void Dispose()
        {
            _transaction?.Dispose();
            if (_lazyConnection?.IsValueCreated == true)
            {
                _lazyConnection.Value.Dispose();
            }
        }
        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            //openConnection.Open();
            return openConnection.Query<T>(
            procedureName,
            param: parameters,
            commandType: CommandType.StoredProcedure
            );
        }
        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            //await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return await openConnection.QueryAsync<T>(
            new CommandDefinition(
                procedureName,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken
            )
            ).ConfigureAwait(false);
        }        
        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(
                procedureName,
                parameters,
                transaction,
                commandTimeout ?? openConnection.ConnectionTimeout,
                CommandType.StoredProcedure))
            {
                return mapper(multi);
            }
        }
        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            using (var multi = await openConnection.QueryMultipleAsync(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction,
                    commandTimeout ?? openConnection.ConnectionTimeout,
                    CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false))
            {
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }
        private string GetTableName<T>()
        {
            var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
            return tableAttr == null
                ? $"[{typeof(T).Name}]"
                : $"[{tableAttr.Schema}].[{tableAttr.TableName}]";
        }
        private string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttribute<ColumnAttribute>();
            return columnAttr == null
                ? $"[{property.Name}]"
                : $"[{columnAttr.ColumnName}]";
        }
        private IEnumerable<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            var properties = typeof(T).GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>() != null).ToList();
            if (properties.Count == 0)
            {
                throw new InvalidOperationException($"No primary key defined for type {typeof(T).Name}");
            }
            var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>() != null);
            if (identityPk > 1)
            {
                throw new InvalidOperationException("Multiple Identity primary keys are not supported");
            }
            return properties;
        }
        private bool IsPrimaryKey(PropertyInfo property) => property.GetCustomAttribute<PrimaryKeyAttribute>() != null;
        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>() == null);
        }
        private IDbConnection GetOpenConnection()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                {
                    _externalConnection.Open();
                }
                return _externalConnection;
            }
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }
        private async Task<IDbConnection> GetOpenConnectionAsync()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                {
                    _externalConnection.Open();
                }                    
                return _externalConnection;
            }
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                if (connection is SqlConnection sqlConnection)
                {
                    await sqlConnection.OpenAsync().ConfigureAwait(false);
                }                    
                else
                {
                    connection.Open();
                }                    
            }
            return connection;
        }
        private string BuildWhereClause<T>()
        {
            var primaryKeys = GetPrimaryKeyProperties<T>();
            return string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
        }
        private DynamicParameters GetPrimaryKeyValues<T>(T entity)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in GetPrimaryKeyProperties<T>())
            {
                var value = pk.GetValue(entity);
                parameters.Add(pk.Name, value);
            }
            return parameters;
        }
        private string GetSelectColumns<T>()
        {
            return string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
        }
        private PropertyInfo GetIdentityProperty<T>()
        {
            return typeof(T).GetProperties()
                .FirstOrDefault(p =>
                    p.GetCustomAttribute<IdentityAttribute>() != null &&
                    p.GetCustomAttribute<PrimaryKeyAttribute>() != null);
        }
        private void IsValidProcedureName(string procedureName)
        {
            //if (string.IsNullOrWhiteSpace(procedureName))
            //{
            //    throw new ArgumentException(
            //        "Procedure name cannot be null or whitespace.",
            //        nameof(procedureName)
            //    );
            //}
            if (!Regex.IsMatch(procedureName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Procedure name is not valid", nameof(procedureName));
            }
        }
        private void CleanupTransaction()
        {
            _transaction?.Dispose();
            _transaction = null;
        }
        private List<T> ValidateAndPrepareEntities<T>(IEnumerable<T> entities)
        {
            if (entities == null || !entities.Any())
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));

            return entities.ToList();
        }
        private string BuildInsertQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(GetColumnName));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));

            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {
                return $@"INSERT INTO {tableName} ({columns}) 
                   VALUES ({values});
                   SELECT CAST(SCOPE_IDENTITY() AS INT);";
            }

            return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        }
        private string BuildBulkInsertQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(GetColumnName));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        }
        private string BuildUpdateQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var primaryKeys = GetPrimaryKeyProperties<T>();
            var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p));
            var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
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
            var primaryKey = GetPrimaryKeyProperties<T>().Single();
            var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
            return $"SELECT {columns} FROM {tableName} WHERE {GetColumnName(primaryKey)} = @Id";
        }
        private DynamicParameters CreatePrimaryKeyParameters<T>(T entity)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in GetPrimaryKeyProperties<T>())
            {
                parameters.Add(pk.Name, pk.GetValue(entity));
            }
            return parameters;
        }
        private string BuildBatchInsertWithOutputQuery<T>()//در SQL 2016 به بعد ساپورت میشود
        {
            return InsertQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var properties = GetInsertProperties<T>();
                var columns = string.Join(", ", properties.Select(GetColumnName));
                var values = string.Join(", ", properties.Select(p => $"e.{GetColumnName(p)}"));
                var outputColumn = GetColumnName(GetIdentityProperty<T>());
                return $@"
                    DECLARE @InsertedRows TABLE (Id INT);            
                    INSERT INTO {tableName} ({columns})
                    OUTPUT INSERTED.{outputColumn} INTO @InsertedRows
                    SELECT {values}
                    FROM OPENJSON(@Entities)
                    WITH (
                        {string.Join(", ", properties.Select(p => $"{GetColumnName(p)} {GetSqlType(p.PropertyType)}"))}
                    ) AS e;            
                    SELECT Id FROM @InsertedRows;";
            });
        }
        private string GetSqlType(Type type)
        {            
            if (type == typeof(int)) return "INT";
            if (type == typeof(long)) return "BIGINT";
            if (type == typeof(short)) return "SMALLINT";
            if (type == typeof(byte)) return "TINYINT";
            if (type == typeof(bool)) return "BIT";
            if (type == typeof(string)) return "NVARCHAR(MAX)";
            if (type == typeof(char)) return "NCHAR(1)";
            if (type == typeof(DateTime)) return "DATETIME";
            if (type == typeof(DateTimeOffset)) return "DATETIMEOFFSET";
            if (type == typeof(decimal)) return "DECIMAL(18, 2)";
            if (type == typeof(float)) return "REAL";
            if (type == typeof(double)) return "FLOAT";
            if (type == typeof(Guid)) return "UNIQUEIDENTIFIER";
            if (type == typeof(byte[])) return "VARBINARY(MAX)";
            if (type == typeof(TimeSpan)) return "TIME";
            if (type == typeof(object)) return "SQL_VARIANT";
            if (Nullable.GetUnderlyingType(type) != null)
            {
                return GetSqlType(Nullable.GetUnderlyingType(type));
            }
            if (type.IsEnum)
            {
                return "INT";
            }
            throw new NotSupportedException($"Type {type.Name} is not supported");
        }
        private string BuildOptimizedBatchInsertQuery<T>(int count)
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>().ToList();
            var outputColumn = GetColumnName(GetIdentityProperty<T>());
            var columns = string.Join(", ", properties.Select(GetColumnName));
            var values = new StringBuilder("VALUES ");
            for (int i = 0; i < count; i++)
            {
                var rowValues = string.Join(", ", properties.Select(p => $"@p{i}_{p.Name}"));
                values.AppendLine($"({rowValues}){(i < count - 1 ? "," : "")}");
            }
            return $@"
                DECLARE @InsertedRows TABLE (Id INT);
        
                INSERT INTO {tableName} ({columns})
                OUTPUT INSERTED.{outputColumn} INTO @InsertedRows
                {values}
        
                SELECT Id FROM @InsertedRows 
                ORDER BY Id ASC;"; // حفظ ترتیب درج
        }
        private DynamicParameters CreateOptimizedParameters<T>(List<T> entities)
        {
            var parameters = new DynamicParameters();
            var properties = GetInsertProperties<T>().ToList();
            Parallel.For(0, entities.Count, i =>
            {
                foreach (var prop in properties)
                {
                    parameters.Add($"p{i}_{prop.Name}", prop.GetValue(entities[i]));
                }
            });
            return parameters;
        }
        private DynamicParameters CreateParametersAsync<T>(List<T> entities)
        {
            var parameters = new DynamicParameters();
            var properties = GetInsertProperties<T>().ToList();
            for (int i = 0; i < entities.Count; i++)
            {
                foreach (var prop in properties)
                {
                    parameters.Add($"p{i}_{prop.Name}", prop.GetValue(entities[i]));
                }
            }
            return parameters;
        }
    }
}