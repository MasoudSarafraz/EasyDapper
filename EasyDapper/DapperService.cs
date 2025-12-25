using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal sealed class SimpleConcurrentCache<TKey, TValue> where TValue : class
    {
        private readonly ConcurrentDictionary<TKey, TValue> _cache = new ConcurrentDictionary<TKey, TValue>();
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory) => _cache.GetOrAdd(key, valueFactory);
        public bool TryGetValue(TKey key, out TValue value) => _cache.TryGetValue(key, out value);
        public void Clear() => _cache.Clear();
        public int Count => _cache.Count;
    }

    internal sealed class DapperService : IDapperService, IDisposable
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly CrudOperations _crudOperations;
        private readonly BulkOperations _bulkOperations;
        private readonly StoredProcedureExecutor _storedProcedureExecutor;
        private readonly EntityTracker _entityTracker;
        private readonly SqlBuilder _sqlBuilder;
        private bool _disposed = false;

        public int TransactionCount() => _connectionManager.TransactionCount;

        public DapperService(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException("connectionString");
            _connectionManager = new ConnectionManager(connectionString);
            _queryCache = new QueryCache();
            _sqlBuilder = new SqlBuilder(_queryCache);
            _entityTracker = new EntityTracker();
            _crudOperations = new CrudOperations(_connectionManager, _queryCache, _sqlBuilder, _entityTracker);
            _bulkOperations = new BulkOperations(_connectionManager, _queryCache, _sqlBuilder);
            _storedProcedureExecutor = new StoredProcedureExecutor(_connectionManager, _sqlBuilder);
        }

        public DapperService(IDbConnection externalConnection)
        {
            if (externalConnection == null) throw new ArgumentNullException("externalConnection");
            _connectionManager = new ConnectionManager(externalConnection);
            _queryCache = new QueryCache();
            _sqlBuilder = new SqlBuilder(_queryCache);
            _entityTracker = new EntityTracker();
            _crudOperations = new CrudOperations(_connectionManager, _queryCache, _sqlBuilder, _entityTracker);
            _bulkOperations = new BulkOperations(_connectionManager, _queryCache, _sqlBuilder);
            _storedProcedureExecutor = new StoredProcedureExecutor(_connectionManager, _sqlBuilder);
        }

        public void BeginTransaction() => _connectionManager.BeginTransaction();
        public void CommitTransaction() => _connectionManager.CommitTransaction();
        public void RollbackTransaction() => _connectionManager.RollbackTransaction();
        public int Insert<T>(T entity) where T : class => _crudOperations.Insert(entity);
        public Task<int> InsertAsync<T>(T entity) where T : class => _crudOperations.InsertAsync(entity);
        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class => _bulkOperations.InsertList(entities, generateIdentities);
        public Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class => _bulkOperations.InsertListAsync(entities, generateIdentities, cancellationToken);
        public int Update<T>(T entity) where T : class => _crudOperations.Update(entity);
        public Task<int> UpdateAsync<T>(T entity) where T : class => _crudOperations.UpdateAsync(entity);
        public int UpdateList<T>(IEnumerable<T> entities) where T : class => _crudOperations.UpdateList(entities);
        public Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class => _crudOperations.UpdateListAsync(entities, cancellationToken);
        public int Delete<T>(T entity) where T : class => _crudOperations.Delete(entity);
        public Task<int> DeleteAsync<T>(T entity) where T : class => _crudOperations.DeleteAsync(entity);
        public int DeleteList<T>(IEnumerable<T> entities) where T : class => _crudOperations.DeleteList(entities);
        public Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class => _crudOperations.DeleteListAsync(entities, cancellationToken);
        public T GetById<T>(object id) where T : class => _crudOperations.GetById<T>(id);
        public Task<T> GetByIdAsync<T>(object id) where T : class => _crudOperations.GetByIdAsync<T>(id);
        public T GetById<T>(T entity) where T : class => _crudOperations.GetById(entity);
        public Task<T> GetByIdAsync<T>(T entity) where T : class => _crudOperations.GetByIdAsync(entity);
        public IQueryBuilder<T> Query<T>() => new QueryBuilder<T>(_connectionManager.GetOpenConnection());
        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class => _storedProcedureExecutor.ExecuteStoredProcedure<T>(procedureName, parameters);
        public Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class => _storedProcedureExecutor.ExecuteStoredProcedureAsync<T>(procedureName, parameters, cancellationToken);
        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class => _storedProcedureExecutor.ExecuteMultiResultStoredProcedure(procedureName, mapper, parameters, transaction, commandTimeout);
        public Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class => _storedProcedureExecutor.ExecuteMultiResultStoredProcedureAsync(procedureName, asyncMapper, parameters, transaction, commandTimeout, cancellationToken);
        public void Attach<T>(T entity) where T : class => _entityTracker.Attach(entity);
        public void Detach<T>(T entity) where T : class => _entityTracker.Detach(entity);
        public T ExecuteScalarFunction<T>(string functionName, object parameters = null) => _storedProcedureExecutor.ExecuteScalarFunction<T>(functionName, parameters);
        public Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default) => _storedProcedureExecutor.ExecuteScalarFunctionAsync<T>(functionName, parameters, cancellationToken);
        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters) => _storedProcedureExecutor.ExecuteTableFunction<T>(functionName, parameters);
        public Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default) => _storedProcedureExecutor.ExecuteTableFunctionAsync<T>(functionName, parameters, cancellationToken);
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing) { _connectionManager?.Dispose(); _entityTracker?.Dispose(); _queryCache?.Dispose(); }
            _disposed = true;
        }
    }

    internal class ConnectionManager : IDisposable
    {
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private readonly ConcurrentStack<string> _savePointStack = new ConcurrentStack<string>();
        private readonly int _timeOut;
        private const int DEFAULT_TIMEOUT = 30;
        private bool _disposed = false;
        private readonly object _lock = new object();

        public int TransactionCount => _transaction != null ? 1 : 0;
        public IDbTransaction CurrentTransaction => _transaction;
        public int CommandTimeout => _timeOut;

        public ConnectionManager(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException("connectionString");
            _connectionString = connectionString;
            _timeOut = GetConnectionTimeout();
        }
        public ConnectionManager(IDbConnection externalConnection)
        {
            if (externalConnection == null) throw new ArgumentNullException("externalConnection");
            _externalConnection = externalConnection;
            _timeOut = GetExternalConnectionTimeout();
        }

        private int GetConnectionTimeout()
        {
            try { using (var tempConnection = new SqlConnection(_connectionString)) { tempConnection.Open(); return tempConnection.ConnectionTimeout; } }
            catch { return DEFAULT_TIMEOUT; }
        }
        private int GetExternalConnectionTimeout()
        {
            try { return _externalConnection.ConnectionTimeout; }
            catch { return DEFAULT_TIMEOUT; }
        }

        public IDbConnection GetOpenConnection()
        {
            lock (_lock)
            {
                if (_externalConnection != null) return _externalConnection;
                if (_connection == null) _connection = new SqlConnection(_connectionString);
                EnsureConnectionOpen();
                return _connection;
            }
        }

        public async Task<IDbConnection> GetOpenConnectionAsync()
        {
            // Ensure we don't lock during async await, but protect the creation logic
            if (_externalConnection != null) return _externalConnection;

            bool needOpen = false;
            SqlConnection localConn = null;

            lock (_lock)
            {
                if (_connection == null)
                {
                    _connection = new SqlConnection(_connectionString);
                    needOpen = true;
                }
                else if (_connection.State != ConnectionState.Open)
                {
                    needOpen = true;
                }
                localConn = (SqlConnection)_connection;
            }

            if (needOpen) await localConn.OpenAsync().ConfigureAwait(false);
            return localConn;
        }

        private void EnsureConnectionOpen()
        {
            if (_connection.State == ConnectionState.Broken)
            {
                _connection.Close();
                _connection.Open();
            }
            else if (_connection.State != ConnectionState.Open)
            {
                _connection.Open();
            }
        }

        public void BeginTransaction()
        {
            lock (_lock)
            {
                var connection = GetOpenConnection();
                if (_transaction != null)
                {
                    var savePointName = $"SP_{Guid.NewGuid():N}";
                    ExecuteTransactionCommand($"SAVE TRANSACTION {savePointName}");
                    _savePointStack.Push(savePointName);
                }
                else
                {
                    _transaction = connection.BeginTransaction();
                }
            }
        }

        public void CommitTransaction()
        {
            lock (_lock)
            {
                if (_transaction == null) throw new InvalidOperationException("No transaction is in progress");
                if (_savePointStack.TryPop(out var _)) return;
                try { _transaction.Commit(); }
                finally { CleanupTransaction(); }
            }
        }

        public void RollbackTransaction()
        {
            lock (_lock)
            {
                if (_transaction == null) throw new InvalidOperationException("No transaction is in progress");
                try
                {
                    if (_savePointStack.TryPop(out var savePointName)) ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    else _transaction.Rollback();
                }
                finally { CleanupTransaction(); }
            }
        }

        private void ExecuteTransactionCommand(string commandText)
        {
            if (_transaction == null || _transaction.Connection == null) throw new InvalidOperationException("No active transaction");
            using (var command = _transaction.Connection.CreateCommand())
            {
                command.Transaction = _transaction;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void CleanupTransaction()
        {
            try
            {
                _transaction?.Dispose();
                _transaction = null;
                _savePointStack.Clear();
            }
            catch { /* Ignore cleanup errors */ }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                try { if (_transaction != null) _transaction.Rollback(); } catch { }
                try { if (_connection != null && _connection != _externalConnection) { if (_connection.State == ConnectionState.Open) _connection.Close(); _connection.Dispose(); } } catch { }
                // Never dispose external connection
                _transaction = null;
                _connection = null;
            }
            _disposed = true;
        }
    }

    internal class QueryCache : IDisposable
    {
        private readonly SimpleConcurrentCache<Type, string> InsertQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> UpdateQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> DeleteQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, string> GetByIdQueryCache = new SimpleConcurrentCache<Type, string>();
        private readonly SimpleConcurrentCache<Type, List<PropertyInfo>> PrimaryKeyCache = new SimpleConcurrentCache<Type, List<PropertyInfo>>();
        private readonly SimpleConcurrentCache<Type, PropertyInfo> IdentityPropertyCache = new SimpleConcurrentCache<Type, PropertyInfo>();
        private readonly SimpleConcurrentCache<string, string> TableNameCache = new SimpleConcurrentCache<string, string>();
        private readonly SimpleConcurrentCache<string, string> ColumnNameCache = new SimpleConcurrentCache<string, string>();
        private const string DEFAULT_SCHEMA = "dbo";
        private static readonly char[] InvalidIdentifierChars = new[] { ';', '-', '-', '/', '*', '\'', '"', '[', ']' };

        private string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier)) throw new ArgumentException("Identifier cannot be null or empty.", "identifier");
            if (identifier.IndexOfAny(InvalidIdentifierChars) >= 0) throw new ArgumentException("Identifier contains invalid characters.", "identifier");
            return identifier.Replace("]", "]]");
        }

        public string GetTableName<T>()
        {
            var type = typeof(T);
            var cacheKey = $"{type.FullName}_{DEFAULT_SCHEMA}";
            return TableNameCache.GetOrAdd(cacheKey, key =>
            {
                var tableAttr = type.GetCustomAttribute<TableAttribute>(true);
                var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? SanitizeIdentifier(tableAttr.Schema) : DEFAULT_SCHEMA;
                var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? SanitizeIdentifier(tableAttr.TableName) : SanitizeIdentifier(type.Name);
                return $"[{schema}].[{name}]";
            });
        }

        public string GetColumnName(PropertyInfo property)
        {
            if (property == null) throw new ArgumentNullException("property");
            var cacheKey = $"{property.DeclaringType?.FullName}_{property.Name}";
            return ColumnNameCache.GetOrAdd(cacheKey, key =>
            {
                var columnAttr = property.GetCustomAttribute<ColumnAttribute>(true);
                var name = columnAttr != null && !string.IsNullOrWhiteSpace(columnAttr.ColumnName) ? SanitizeIdentifier(columnAttr.ColumnName) : SanitizeIdentifier(property.Name);
                return $"[{name}]";
            });
        }

        public List<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            return PrimaryKeyCache.GetOrAdd(typeof(T), type =>
            {
                var properties = type.GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null).ToList();
                if (properties.Count == 0) throw new InvalidOperationException($"No primary key defined for {type.Name}");
                if (properties.Count(p => p.GetCustomAttribute<IdentityAttribute>(true) != null) > 1) throw new InvalidOperationException("Multiple Identity primary keys are not supported");
                return properties;
            });
        }

        public PropertyInfo GetIdentityProperty<T>()
        {
            return IdentityPropertyCache.GetOrAdd(typeof(T), type => type.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<IdentityAttribute>(true) != null && p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null));
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
            if (identityProp != null) { return $"INSERT INTO {tableName} ({columns}) VALUES ({values}); SELECT CAST(SCOPE_IDENTITY() AS INT);"; }
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
            if (updatablePrimaryKeys.Count == 0) throw new InvalidOperationException($"Cannot update type {type.Name}. All properties are identity primary keys.");
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

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() => typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
        private bool IsPrimaryKey(PropertyInfo property) => property.GetCustomAttribute<PrimaryKeyAttribute>(true) != null;
        private bool IsIdentity(PropertyInfo property) => property.GetCustomAttribute<IdentityAttribute>(true) != null;

        public void Dispose() { /* SimpleConcurrentCache doesn't need explicit disposal */ }
    }

    internal class CrudOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private readonly EntityTracker _entityTracker;

        public CrudOperations(ConnectionManager connectionManager, QueryCache queryCache, SqlBuilder sqlBuilder, EntityTracker entityTracker)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            if (entityTracker == null) throw new ArgumentNullException("entityTracker");
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
            _entityTracker = entityTracker;
        }

        public int Insert<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetInsertQuery<T>();
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = connection.ExecuteScalar(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }
            return connection.Execute(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetInsertQuery<T>();
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }
            return await connection.ExecuteAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int Update<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker.TryGetAttached(key, out var original)) return BaseUpdate(entity);
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;
            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = _connectionManager.GetOpenConnection();
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker.TryGetAttached(key, out var original)) return await BaseUpdateAsync(entity).ConfigureAwait(false);
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;
            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return UpdateListWithCompositeKeys(entities, query);
            return connection.Execute(query, entities, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateListWithCompositeKeysAsync(entities, query, cancellationToken).ConfigureAwait(false);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: entities, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        public int Delete<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        public T GetById<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetGetByIdQuery<T>();
            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public T GetById<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        private int BaseUpdate<T>(T entity) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return UpdateSingleWithCompositeKeys(entity, query);
            return connection.Execute(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> BaseUpdateAsync<T>(T entity) where T : class
        {
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateSingleWithCompositeKeysAsync(entity, query).ConfigureAwait(false);
            return await connection.ExecuteAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        private int UpdateListWithCompositeKeys<T>(IEnumerable<T> entities, string query) where T : class
        {
            var totalAffected = 0;
            foreach (var entity in entities) totalAffected += UpdateSingleWithCompositeKeys(entity, query);
            return totalAffected;
        }

        private async Task<int> UpdateListWithCompositeKeysAsync<T>(IEnumerable<T> entities, string query, CancellationToken cancellationToken) where T : class
        {
            var totalAffected = 0;
            foreach (var entity in entities) totalAffected += await UpdateSingleWithCompositeKeysAsync(entity, query, cancellationToken).ConfigureAwait(false);
            return totalAffected;
        }

        private int UpdateSingleWithCompositeKeys<T>(T entity, string query) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys) oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys) if (!IsIdentity(pk)) newParams.Add(pk.Name, pk.GetValue(entity));
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);
            return connection.Execute(query, combinedParams, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> UpdateSingleWithCompositeKeysAsync<T>(T entity, string query, CancellationToken cancellationToken = default) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys) oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys) if (!IsIdentity(pk)) newParams.Add(pk.Name, pk.GetValue(entity));
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: combinedParams, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        private void MergeDynamicParameters(DynamicParameters source, DynamicParameters destination)
        {
            if (source == null) return;
            foreach (var paramName in source.ParameterNames) destination.Add(paramName, source.Get<object>(paramName));
        }
        private bool IsIdentity(PropertyInfo property) => property.GetCustomAttribute<IdentityAttribute>(true) != null;
    }

    internal class BulkOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private const int DEFAULT_BATCH_SIZE = 100;

        public BulkOperations(ConnectionManager connectionManager, QueryCache queryCache, SqlBuilder sqlBuilder)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
        }

        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var entityList = entities as IList<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities) return InsertListWithIdentity(entityList);
            InsertBulkCopy(entityList);
            return entityList.Count;
        }

        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var entityList = entities as IList<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities) return await InsertListWithIdentityAsync(entityList, cancellationToken).ConfigureAwait(false);
            await InsertBulkCopyAsync(entityList, cancellationToken).ConfigureAwait(false);
            return entityList.Count;
        }

        private void InsertBulkCopy<T>(IEnumerable<T> entities) where T : class
        {
            var tableName = _queryCache.GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy((SqlConnection)_connectionManager.GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null) bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                }
                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task InsertBulkCopyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            var tableName = _queryCache.GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy((SqlConnection)_connectionManager.GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null) bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                }
                await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
            }
        }

        private int InsertListWithIdentity<T>(IList<T> entities) where T : class
        {
            var identities = InsertBulkCopyWithIdentity(entities);
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && identityProp.CanWrite)
            {
                for (int i = 0; i < entities.Count && i < identities.Count; i++)
                {
                    try
                    {
                        object convertedValue = Convert.ChangeType(identities[i], identityProp.PropertyType);
                        identityProp.SetValue(entities[i], convertedValue);
                    }
                    catch (Exception ex) { throw new InvalidOperationException($"Failed to set identity value for entity at index {i}. Identity value: {identities[i]}, Target type: {identityProp.PropertyType}", ex); }
                }
            }
            return entities.Count;
        }

        private async Task<int> InsertListWithIdentityAsync<T>(IList<T> entities, CancellationToken cancellationToken) where T : class
        {
            var identities = await InsertBulkCopyWithIdentityAsync(entities, cancellationToken).ConfigureAwait(false);
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && identityProp.CanWrite)
            {
                for (int i = 0; i < entities.Count && i < identities.Count; i++)
                {
                    try
                    {
                        object convertedValue = Convert.ChangeType(identities[i], identityProp.PropertyType);
                        identityProp.SetValue(entities[i], convertedValue);
                    }
                    catch (Exception ex) { throw new InvalidOperationException($"Failed to set identity value for entity at index {i}. Identity value: {identities[i]}, Target type: {identityProp.PropertyType}", ex); }
                }
            }
            return entities.Count;
        }

        private List<object> InsertBulkCopyWithIdentity<T>(IEnumerable<T> entities) where T : class
        {
            var tempTableName = $"#Temp_{Guid.NewGuid():N}";
            var identities = new List<object>();
            var connection = (SqlConnection)_connectionManager.GetOpenConnection();
            try
            {
                CreateTempTableWithOrderColumn<T>(tempTableName, connection);
                BulkCopyToTempTableWithOrder<T>(entities, tempTableName, connection);
                identities = InsertFromTempAndRetrieveIdentitiesWithOrder<T>(tempTableName, connection);
            }
            finally { DropTempTable(tempTableName, connection); }
            return identities;
        }

        private async Task<List<object>> InsertBulkCopyWithIdentityAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            var tempTableName = $"#Temp_{Guid.NewGuid():N}";
            var identities = new List<object>();
            var connection = (SqlConnection)await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            try
            {
                await CreateTempTableWithOrderColumnAsync<T>(tempTableName, connection, cancellationToken).ConfigureAwait(false);
                await BulkCopyToTempTableWithOrderAsync<T>(entities, tempTableName, connection, cancellationToken).ConfigureAwait(false);
                identities = await InsertFromTempAndRetrieveIdentitiesWithOrderAsync<T>(tempTableName, connection, cancellationToken).ConfigureAwait(false);
            }
            finally { await DropTempTableAsync(tempTableName, connection, cancellationToken).ConfigureAwait(false); }
            return identities;
        }

        private void CreateTempTableWithOrderColumn<T>(string tempTableName, SqlConnection connection)
        {
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var createTempTableQuery = $"SELECT TOP 0 {columns}, 0 AS [TempOrder] INTO {tempTableName} FROM {_queryCache.GetTableName<T>()};";
            _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, createTempTableQuery);
        }

        private async Task CreateTempTableWithOrderColumnAsync<T>(string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var createTempTableQuery = $"SELECT TOP 0 {columns}, 0 AS [TempOrder] INTO {tempTableName} FROM {_queryCache.GetTableName<T>()};";
            await connection.ExecuteAsync(new CommandDefinition(createTempTableQuery, transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        private void BulkCopyToTempTableWithOrder<T>(IEnumerable<T> entities, string tempTableName, SqlConnection connection)
        {
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);
            dataTable.Columns.Add("TempOrder", typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++) dataTable.Rows[i]["TempOrder"] = i;

            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null) bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                    else if (column.ColumnName == "TempOrder") bulkCopy.ColumnMappings.Add(column.ColumnName, "TempOrder");
                }
                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task BulkCopyToTempTableWithOrderAsync<T>(IEnumerable<T> entities, string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);
            dataTable.Columns.Add("TempOrder", typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++) dataTable.Rows[i]["TempOrder"] = i;

            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null) bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                    else if (column.ColumnName == "TempOrder") bulkCopy.ColumnMappings.Add(column.ColumnName, "TempOrder");
                }
                await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
            }
        }

        private List<object> InsertFromTempAndRetrieveIdentitiesWithOrder<T>(string tempTableName, SqlConnection connection) where T : class
        {
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>();
            var tableName = _queryCache.GetTableName<T>();
            var identityColumnName = _queryCache.GetColumnName(identityProp).Trim('[', ']');
            var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));

            var insertQuery = $@"
        CREATE TABLE #OutputIds (TempOrder INT, Id BIGINT);
        INSERT INTO {tableName} ({columnList}) OUTPUT INSERTED.{identityColumnName} INTO #OutputIds(Id)
        SELECT {columnList} FROM {tempTableName} ORDER BY TempOrder;
        SELECT Id FROM #OutputIds ORDER BY TempOrder;
        DROP TABLE #OutputIds;";

            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read()) identities.Add(reader.GetValue(0));
            }
            return identities;
        }

        private async Task<List<object>> InsertFromTempAndRetrieveIdentitiesWithOrderAsync<T>(string tempTableName, SqlConnection connection, CancellationToken cancellationToken) where T : class
        {
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>();
            var tableName = _queryCache.GetTableName<T>();
            var identityColumnName = _queryCache.GetColumnName(identityProp).Trim('[', ']');
            var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));

            var insertQuery = $@"
        CREATE TABLE #OutputIds (TempOrder INT, Id BIGINT);
        INSERT INTO {tableName} ({columnList}) OUTPUT INSERTED.{identityColumnName} INTO #OutputIds(Id)
        SELECT {columnList} FROM {tempTableName} ORDER BY TempOrder;
        SELECT Id FROM #OutputIds ORDER BY TempOrder;
        DROP TABLE #OutputIds;";

            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) identities.Add(reader.GetValue(0));
            }
            return identities;
        }

        private void DropTempTable(string tempTableName, SqlConnection connection)
        {
            try { _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, $"DROP TABLE {tempTableName}"); }
            catch { /* Ignore */ }
        }

        private async Task DropTempTableAsync(string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            try { await connection.ExecuteAsync(new CommandDefinition($"DROP TABLE {tempTableName}", transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken)).ConfigureAwait(false); }
            catch { /* Ignore */ }
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() => typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
    }

    internal class StoredProcedureExecutor
    {
        private readonly ConnectionManager _connectionManager;
        private readonly SqlBuilder _sqlBuilder;
        private const string VALID_NAME_REGEX = @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$";

        public StoredProcedureExecutor(ConnectionManager connection, SqlBuilder sqlBuilder)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            _connectionManager = connection;
            _sqlBuilder = sqlBuilder;
        }

        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class
        {
            if (string.IsNullOrWhiteSpace(procedureName)) throw new ArgumentException("Procedure name cannot be null or empty", "procedureName");
            if (!Regex.IsMatch(procedureName, VALID_NAME_REGEX)) throw new ArgumentException("Procedure name is not valid", "procedureName");
            var openConnection = _connectionManager.GetOpenConnection();
            return openConnection.Query<T>(procedureName, param: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, commandType: CommandType.StoredProcedure);
        }

        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrWhiteSpace(procedureName)) throw new ArgumentException("Procedure name cannot be null or empty", "procedureName");
            if (!Regex.IsMatch(procedureName, VALID_NAME_REGEX)) throw new ArgumentException("Procedure name is not valid", "procedureName");
            var openConnection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            return await openConnection.QueryAsync<T>(new CommandDefinition(procedureName, parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, commandType: CommandType.StoredProcedure, cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (string.IsNullOrWhiteSpace(procedureName)) throw new ArgumentException("Procedure name cannot be null or empty", "procedureName");
            if (!Regex.IsMatch(procedureName, VALID_NAME_REGEX)) throw new ArgumentException("Procedure name is not valid", "procedureName");
            var openConnection = _connectionManager.GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(procedureName, parameters, transaction ?? _connectionManager.CurrentTransaction, commandTimeout ?? _connectionManager.CommandTimeout, CommandType.StoredProcedure)) return mapper(multi);
        }

        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            if (string.IsNullOrWhiteSpace(procedureName)) throw new ArgumentException("Procedure name cannot be null or empty", "procedureName");
            if (!Regex.IsMatch(procedureName, VALID_NAME_REGEX)) throw new ArgumentException("Procedure name is not valid", "procedureName");
            var openConnection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            using (var multi = await openConnection.QueryMultipleAsync(new CommandDefinition(procedureName, parameters, transaction ?? _connectionManager.CurrentTransaction, commandTimeout ?? _connectionManager.CommandTimeout, CommandType.StoredProcedure, cancellationToken: cancellationToken)).ConfigureAwait(false)) return await asyncMapper(multi).ConfigureAwait(false);
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            if (string.IsNullOrWhiteSpace(functionName)) throw new ArgumentException("Function name cannot be null or empty", "functionName");
            if (!Regex.IsMatch(functionName, VALID_NAME_REGEX)) throw new ArgumentException("Function name is not valid", "functionName");
            var connection = _connectionManager.GetOpenConnection();
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout);
            return connection.ExecuteScalar<T>(commandDefinition);
        }

        public async Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(functionName)) throw new ArgumentException("Function name cannot be null or empty", "functionName");
            if (!Regex.IsMatch(functionName, VALID_NAME_REGEX)) throw new ArgumentException("Function name is not valid", "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteScalarAsync<T>(commandDefinition).ConfigureAwait(false);
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            if (string.IsNullOrWhiteSpace(functionName)) throw new ArgumentException("Function name cannot be null or empty", "functionName");
            if (!Regex.IsMatch(functionName, VALID_NAME_REGEX)) throw new ArgumentException("Function name is not valid", "functionName");
            var connection = _connectionManager.GetOpenConnection();
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout);
            return connection.Query<T>(commandDefinition);
        }

        public async Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(functionName)) throw new ArgumentException("Function name cannot be null or empty", "functionName");
            if (!Regex.IsMatch(functionName, VALID_NAME_REGEX)) throw new ArgumentException("Function name is not valid", "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.QueryAsync<T>(commandDefinition).ConfigureAwait(false);
        }
    }

    internal class EntityTracker : IDisposable
    {
        internal readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();
        private bool _disposed = false;

        public void Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = typeof(T).GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null).ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            if (!_attachedEntities.ContainsKey(key))
            {
                var clone = CloneEntity(entity);
                _attachedEntities.TryAdd(key, clone);
            }
        }

        public void Detach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = typeof(T).GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null).ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            _attachedEntities.TryRemove(key, out _);
        }

        public bool TryGetAttached(object key, out object value) => _attachedEntities.TryGetValue(key, out value);

        internal object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1) return primaryKeys[0].GetValue(entity);
            return string.Join("|", primaryKeys.Select(p => p.GetValue(entity)?.ToString() ?? "NULL"));
        }

        internal List<string> GetChangedProperties<T>(T original, T current)
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) == null && !object.Equals(p.GetValue(original), p.GetValue(current))).Select(p => p.Name).ToList();
        }

        private T CloneEntity<T>(T entity)
        {
            var clone = Activator.CreateInstance<T>();
            foreach (var prop in typeof(T).GetProperties().Where(p => p.CanWrite))
            {
                var value = prop.GetValue(entity);
                if (value != null && !prop.PropertyType.IsValueType && prop.PropertyType != typeof(string))
                {
                    var cloneMethod = prop.PropertyType.GetMethod("MemberwiseClone", BindingFlags.NonPublic | BindingFlags.Instance);
                    if (cloneMethod != null) value = cloneMethod.Invoke(value, null);
                }
                prop.SetValue(clone, value);
            }
            return clone;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing) _attachedEntities.Clear();
            _disposed = true;
        }
    }

    internal class SqlBuilder
    {
        private readonly QueryCache _queryCache;

        public SqlBuilder(QueryCache queryCache)
        {
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            _queryCache = queryCache;
        }

        public string BuildDynamicUpdateQuery<T>(List<string> changedProps, List<PropertyInfo> primaryKeys)
        {
            var tableName = _queryCache.GetTableName<T>();
            var changedProperties = changedProps.Select(p => typeof(T).GetProperty(p)).Where(p => p != null).ToList();
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

        public string BuildScalarFunctionQuery(string functionName, object parameters) => $"SELECT {functionName}({BuildParameters(parameters)})";
        public string BuildTableFunctionQuery(string functionName, object parameters) => $"SELECT * FROM {functionName}({BuildParameters(parameters)})";

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

        public DataTable ToDataTable<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> properties)
        {
            var dataTable = new DataTable();
            foreach (var property in properties)
            {
                var columnName = _queryCache.GetColumnName(property);
                var rawColumnName = columnName.Trim('[', ']');
                dataTable.Columns.Add(rawColumnName, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }
            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();
                foreach (var property in properties)
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