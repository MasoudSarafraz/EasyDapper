using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;
using System.Data.Common;
using System.Collections;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal sealed class DapperService : IDapperService, IDisposable
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly CrudOperations _crudOperations;
        private readonly BulkOperations _bulkOperations;
        private readonly StoredProcedureExecutor _storedProcedureExecutor;
        private readonly EntityTracker _entityTracker;
        private readonly SqlBuilder _sqlBuilder;

        public int TransactionCount() => _connectionManager.TransactionCount;

        public DapperService(string connectionString)
        {
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
            _connectionManager?.Dispose();
            _entityTracker?.Dispose();
        }
    }

    internal class ConnectionManager : IDisposable
    {
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private readonly ThreadLocal<IDbConnection> _threadLocalConnection;
        private readonly ThreadLocal<IDbTransaction> _threadLocalTransaction;
        private readonly ThreadLocal<int> _threadLocalTransactionCount;
        private readonly ThreadLocal<Stack<string>> _threadLocalSavePoints;
        private readonly int _timeOut;
        private const int DEFAULT_TIMEOUT = 30;

        public int TransactionCount => _threadLocalTransactionCount.Value;
        public IDbTransaction CurrentTransaction => _threadLocalTransaction.Value;
        public int CommandTimeout => _timeOut;

        public ConnectionManager(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            _connectionString = connectionString;
            _threadLocalConnection = new ThreadLocal<IDbConnection>(() =>
            {
                var conn = new SqlConnection(_connectionString);
                conn.Open();
                return conn;
            }, false);
            _threadLocalTransaction = new ThreadLocal<IDbTransaction>();
            _threadLocalTransactionCount = new ThreadLocal<int>(() => 0);
            _threadLocalSavePoints = new ThreadLocal<Stack<string>>(() => new Stack<string>());

            try
            {
                using (var tempConnection = new SqlConnection(_connectionString))
                {
                    tempConnection.Open();
                    _timeOut = tempConnection.ConnectionTimeout;
                }
            }
            catch
            {
                _timeOut = DEFAULT_TIMEOUT;
            }
        }

        public ConnectionManager(IDbConnection externalConnection)
        {
            _externalConnection = externalConnection ?? throw new ArgumentNullException(nameof(externalConnection));
            try
            {
                _timeOut = _externalConnection.ConnectionTimeout;
            }
            catch
            {
                _timeOut = DEFAULT_TIMEOUT;
            }
            _threadLocalTransaction = new ThreadLocal<IDbTransaction>();
            _threadLocalTransactionCount = new ThreadLocal<int>(() => 0);
            _threadLocalSavePoints = new ThreadLocal<Stack<string>>(() => new Stack<string>());
        }

        public IDbConnection GetOpenConnection()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                    _externalConnection.Open();
                return _externalConnection;
            }
            return _threadLocalConnection.Value;
        }

        public async Task<IDbConnection> GetOpenConnectionAsync()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                    _externalConnection.Open();
                return _externalConnection;
            }
            return _threadLocalConnection.Value;
        }

        public void BeginTransaction()
        {
            var connection = GetOpenConnection();
            if (_threadLocalTransaction.Value == null)
            {
                _threadLocalTransaction.Value = connection.BeginTransaction();
            }
            else
            {
                var savePointName = $"SavePoint_{Guid.NewGuid():N}";
                ExecuteTransactionCommand($"SAVE TRANSACTION {savePointName}");
                _threadLocalSavePoints.Value.Push(savePointName);
            }
            _threadLocalTransactionCount.Value++;
        }

        public void CommitTransaction()
        {
            if (_threadLocalTransaction.Value == null)
                throw new InvalidOperationException("No transaction is in progress");
            if (_threadLocalTransactionCount.Value <= 0)
                throw new InvalidOperationException("No active transactions to commit");

            _threadLocalTransactionCount.Value--;
            if (_threadLocalTransactionCount.Value == 0)
            {
                try
                {
                    _threadLocalTransaction.Value.Commit();
                }
                finally
                {
                    CleanupTransaction();
                }
            }
            else
            {
                _threadLocalSavePoints.Value.Pop();
            }
        }

        public void RollbackTransaction()
        {
            if (_threadLocalTransaction.Value == null)
                throw new InvalidOperationException("No transaction is in progress");
            try
            {
                if (_threadLocalTransactionCount.Value > 0)
                {
                    if (_threadLocalSavePoints.Value.Count > 0)
                    {
                        var savePointName = _threadLocalSavePoints.Value.Pop();
                        ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    }
                    else
                    {
                        _threadLocalTransaction.Value.Rollback();
                    }
                }
            }
            finally
            {
                _threadLocalTransactionCount.Value--;
                if (_threadLocalTransactionCount.Value == 0)
                {
                    CleanupTransaction();
                }
            }
        }

        private void ExecuteTransactionCommand(string commandText)
        {
            if (_threadLocalTransaction.Value == null || _threadLocalTransaction.Value.Connection == null)
                throw new InvalidOperationException("No active transaction");

            using (var command = _threadLocalTransaction.Value.Connection.CreateCommand())
            {
                command.Transaction = _threadLocalTransaction.Value;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void CleanupTransaction()
        {
            try
            {
                _threadLocalTransaction.Value?.Dispose();
            }
            catch { }
            finally
            {
                _threadLocalTransaction.Value = null;
                _threadLocalSavePoints.Value?.Clear();
            }
        }

        public void Dispose()
        {
            try
            {
                if (_threadLocalTransaction.Value != null)
                {
                    RollbackTransaction();
                }
            }
            catch { }

            try
            {
                if (_threadLocalConnection?.IsValueCreated == true)
                {
                    _threadLocalConnection.Value.Close();
                    _threadLocalConnection.Value.Dispose();
                }
            }
            catch { }

            _threadLocalConnection?.Dispose();
            _threadLocalTransaction?.Dispose();
            _threadLocalTransactionCount?.Dispose();
            _threadLocalSavePoints?.Dispose();
        }
    }

    internal class QueryCache
    {
        private static readonly ConcurrentDictionary<Type, string> InsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> UpdateQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> DeleteQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> GetByIdQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> PrimaryKeyCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo> IdentityPropertyCache = new ConcurrentDictionary<Type, PropertyInfo>();
        private static readonly ConcurrentDictionary<string, string> TableNameCache = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<string, string> ColumnNameCache = new ConcurrentDictionary<string, string>();
        private const string DEFAULT_SCHEMA = "dbo";

        public string GetTableName<T>()
        {
            var type = typeof(T);
            var cacheKey = $"{type.FullName}_{DEFAULT_SCHEMA}";
            return TableNameCache.GetOrAdd(cacheKey, key =>
            {
                var tableAttr = type.GetCustomAttribute<TableAttribute>(true);
                return tableAttr == null
                    ? $"[{DEFAULT_SCHEMA}].[{type.Name}]"
                    : $"[{tableAttr.Schema ?? DEFAULT_SCHEMA}].[{tableAttr.TableName}]";
            });
        }

        public string GetColumnName(PropertyInfo property)
        {
            var cacheKey = $"{property.DeclaringType?.FullName}_{property.Name}";
            return ColumnNameCache.GetOrAdd(cacheKey, key =>
            {
                var columnAttr = property.GetCustomAttribute<ColumnAttribute>(true);
                return columnAttr == null
                    ? $"[{property.Name}]"
                    : $"[{columnAttr.ColumnName}]";
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
                var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>(true) != null);
                if (identityPk > 1)
                    throw new InvalidOperationException("Multiple Identity primary keys are not supported");
                return properties;
            });
        }

        public PropertyInfo GetIdentityProperty<T>()
        {
            return IdentityPropertyCache.GetOrAdd(typeof(T), type =>
                type.GetProperties()
                    .FirstOrDefault(p =>
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
                return $@"INSERT INTO {tableName} ({columns}) 
                   VALUES ({values});
                   SELECT CAST(SCOPE_IDENTITY() AS INT);";
            }
            return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        }

        private string BuildUpdateQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var primaryKeys = GetPrimaryKeyProperties<T>();
            var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p) && !IsIdentity(p)).ToList();

            // اگر پراپرتی غیر کلیدی و غیر Identity وجود داشت، از روش معمول استفاده کن
            if (properties.Count > 0)
            {
                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            }

            // اگر تمام پراپرتی‌ها کلیدی هستند، فقط کلیدهای غیر Identity را در SET قرار بده
            var updatablePrimaryKeys = primaryKeys.Where(p => !IsIdentity(p)).ToList();
            if (updatablePrimaryKeys.Count == 0)
            {
                throw new InvalidOperationException($"Cannot update type {type.Name}. All properties are identity primary keys.");
            }

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
            var primaryKey = GetPrimaryKeyProperties<T>().Single();
            var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
            return $"SELECT {columns} FROM {tableName} WHERE {GetColumnName(primaryKey)} = @Id";
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
        }

        private bool IsPrimaryKey(PropertyInfo property) =>
            property.GetCustomAttribute<PrimaryKeyAttribute>(true) != null;

        private bool IsIdentity(PropertyInfo property) =>
            property.GetCustomAttribute<IdentityAttribute>(true) != null;
    }

    internal class CrudOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private readonly EntityTracker _entityTracker;

        public CrudOperations(ConnectionManager connectionManager, QueryCache queryCache, SqlBuilder sqlBuilder, EntityTracker entityTracker)
        {
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
            _entityTracker = entityTracker;
        }

        public int Insert<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
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
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetInsertQuery<T>();
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }
            return await connection.ExecuteAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public int Update<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker._attachedEntities.TryGetValue(key, out var original))
            {
                return BaseUpdate(entity);
            }
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;

            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = _connectionManager.GetOpenConnection();
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker._attachedEntities.TryGetValue(key, out var original))
            {
                return await BaseUpdateAsync(entity);
            }
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;

            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = await _connectionManager.GetOpenConnectionAsync();
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();

            // بررسی اینکه آیا کوئری نیاز به پارامترهای قدیمی دارد یا نه
            if (query.Contains("@old_"))
            {
                return UpdateListWithCompositeKeys(entities, query);
            }

            return connection.Execute(query, entities, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetUpdateQuery<T>();

            // بررسی اینکه آیا کوئری نیاز به پارامترهای قدیمی دارد یا نه
            if (query.Contains("@old_"))
            {
                return await UpdateListWithCompositeKeysAsync(entities, query, cancellationToken);
            }

            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: entities,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken
            );
            return await connection.ExecuteAsync(commandDefinition);
        }

        public int Delete<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            return connection.Execute(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken
            );
            return await connection.ExecuteAsync(commandDefinition);
        }

        public T GetById<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetGetByIdQuery<T>();
            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public T GetById<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private int BaseUpdate<T>(T entity) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();

            // بررسی اینکه آیا کوئری نیاز به پارامترهای قدیمی دارد یا نه
            if (query.Contains("@old_"))
            {
                return UpdateSingleWithCompositeKeys(entity, query);
            }

            return connection.Execute(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> BaseUpdateAsync<T>(T entity) where T : class
        {
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetUpdateQuery<T>();

            // بررسی اینکه آیا کوئری نیاز به پارامترهای قدیمی دارد یا نه
            if (query.Contains("@old_"))
            {
                return await UpdateSingleWithCompositeKeysAsync(entity, query);
            }

            return await connection.ExecuteAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private int UpdateListWithCompositeKeys<T>(IEnumerable<T> entities, string query) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var totalAffected = 0;

            foreach (var entity in entities)
            {
                totalAffected += UpdateSingleWithCompositeKeys(entity, query);
            }

            return totalAffected;
        }

        private async Task<int> UpdateListWithCompositeKeysAsync<T>(IEnumerable<T> entities, string query, CancellationToken cancellationToken) where T : class
        {
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var totalAffected = 0;

            foreach (var entity in entities)
            {
                totalAffected += await UpdateSingleWithCompositeKeysAsync(entity, query, cancellationToken);
            }

            return totalAffected;
        }

        private int UpdateSingleWithCompositeKeys<T>(T entity, string query) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();

            // ایجاد پارامترهای قدیمی
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys)
            {
                oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            }

            // ایجاد پارامترهای جدید
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys)
            {
                if (!IsIdentity(pk))
                {
                    newParams.Add(pk.Name, pk.GetValue(entity));
                }
            }

            // ادغام پارامترهای قدیمی و جدید
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);

            return connection.Execute(query, combinedParams, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> UpdateSingleWithCompositeKeysAsync<T>(T entity, string query, CancellationToken cancellationToken = default) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();

            // ایجاد پارامترهای قدیمی
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys)
            {
                oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            }

            // ایجاد پارامترهای جدید
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys)
            {
                if (!IsIdentity(pk))
                {
                    newParams.Add(pk.Name, pk.GetValue(entity));
                }
            }

            // ادغام پارامترهای قدیمی و جدید
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);

            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: combinedParams,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken
            );
            return await connection.ExecuteAsync(commandDefinition);
        }

        private void MergeDynamicParameters(DynamicParameters source, DynamicParameters destination)
        {
            if (source == null) return;

            var template = source as DynamicParameters;
            if (template != null)
            {
                foreach (var param in template.ParameterNames)
                {
                    destination.Add(param, template.Get<object>(param));
                }
            }
        }

        private bool IsIdentity(PropertyInfo property) =>
            property.GetCustomAttribute<IdentityAttribute>(true) != null;
    }

    internal class BulkOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private const int DEFAULT_BATCH_SIZE = 100;

        public BulkOperations(ConnectionManager connectionManager, QueryCache queryCache, SqlBuilder sqlBuilder)
        {
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
        }

        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var entityList = entities as IList<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities)
            {
                return InsertListWithIdentity(entityList);
            }
            InsertBulkCopy(entityList);
            return entityList.Count;
        }

        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));
            var entityList = entities as IList<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities)
            {
                return await InsertListWithIdentityAsync(entityList, cancellationToken);
            }
            await InsertBulkCopyAsync(entityList, cancellationToken);
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

                // اصلاح نگاشت ستون‌ها
                foreach (DataColumn column in dataTable.Columns)
                {
                    // پیدا کردن پراپرتی متناظر با ستون
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });

                    if (property != null)
                    {
                        var dbColumnName = _queryCache.GetColumnName(property);
                        bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                    }
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

                // اصلاح نگاشت ستون‌ها
                foreach (DataColumn column in dataTable.Columns)
                {
                    // پیدا کردن پراپرتی متناظر با ستون
                    var property = properties.FirstOrDefault(p => {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });

                    if (property != null)
                    {
                        var dbColumnName = _queryCache.GetColumnName(property);
                        bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                    }
                }

                await bulkCopy.WriteToServerAsync(reader, cancellationToken);
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
                        // تبدیل مقدار Identity به نوع مناسب
                        object convertedValue = Convert.ChangeType(identities[i], identityProp.PropertyType);
                        identityProp.SetValue(entities[i], convertedValue);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to set identity value for entity at index {i}. Identity value: {identities[i]}, Target type: {identityProp.PropertyType}", ex);
                    }
                }
            }
            return entities.Count;
        }

        private async Task<int> InsertListWithIdentityAsync<T>(IList<T> entities, CancellationToken cancellationToken) where T : class
        {
            var identities = await InsertBulkCopyWithIdentityAsync(entities, cancellationToken);
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null && identityProp.CanWrite)
            {
                for (int i = 0; i < entities.Count && i < identities.Count; i++)
                {
                    try
                    {
                        // تبدیل مقدار Identity به نوع مناسب
                        object convertedValue = Convert.ChangeType(identities[i], identityProp.PropertyType);
                        identityProp.SetValue(entities[i], convertedValue);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to set identity value for entity at index {i}. Identity value: {identities[i]}, Target type: {identityProp.PropertyType}", ex);
                    }
                }
            }
            return entities.Count;
        }

        private List<object> InsertBulkCopyWithIdentity<T>(IEnumerable<T> entities) where T : class
        {
            var random = new Random().Next(10, 100000000);
            var tableName = _queryCache.GetTableName<T>();
            var tempTableName = $"##Temp_{random}";
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>();
            var connection = (SqlConnection)_connectionManager.GetOpenConnection();
            var identities = new List<object>();
            try
            {
                // اصلاح کوئری ایجاد جدول موقت
                var createTempTableQuery = $@"SELECT TOP 0 {string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)))} INTO {tempTableName} FROM {tableName};";
                _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, createTempTableQuery);
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                    bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                    var dataTable = _sqlBuilder.ToDataTable(entities, properties);
                    using (var reader = dataTable.CreateDataReader())
                    {
                        // اصلاح نگاشت ستون‌ها
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            // پیدا کردن پراپرتی متناظر با ستون
                            var property = properties.FirstOrDefault(p => {
                                var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                                return dbColumnName == column.ColumnName;
                            });

                            if (property != null)
                            {
                                var dbColumnName = _queryCache.GetColumnName(property);
                                bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                            }
                        }

                        bulkCopy.WriteToServer(reader);
                    }
                }

                // اصلاح کوئری درج و بازیابی Identity
                var identityColumnName = _queryCache.GetColumnName(identityProp).Trim('[', ']');
                var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));

                // استفاده از متغیر جدول برای ذخیره مقادیر Identity
                var insertAndRetrieveQuery = $@"
                DECLARE @TempIds TABLE (Id BIGINT);
                
                INSERT INTO {tableName} ({columnList})
                OUTPUT INSERTED.{identityColumnName} INTO @TempIds
                SELECT {columnList} FROM {tempTableName};
                
                SELECT Id FROM @TempIds;";

                // استفاده از SqlCommand با تراکنش در سازنده
                using (var command = new SqlCommand(insertAndRetrieveQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var identityValue = reader.GetValue(0);
                        identities.Add(identityValue);
                    }
                }

                _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, $"DROP TABLE {tempTableName}");
            }
            catch (Exception ex)
            {
                try { _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, $"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}"); } catch { }
                throw new InvalidOperationException("Bulk copy with identity retrieval failed", ex);
            }
            return identities;
        }

        private async Task<List<object>> InsertBulkCopyWithIdentityAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            var random = new Random().Next(10, 100000000);
            var tableName = _queryCache.GetTableName<T>();
            var tempTableName = $"##Temp_{random}";
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>();
            var connection = (SqlConnection)await _connectionManager.GetOpenConnectionAsync();
            var identities = new List<object>();
            try
            {
                // اصلاح کوئری ایجاد جدول موقت
                var createTempTableQuery = $@"SELECT TOP 0 {string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)))} INTO {tempTableName} FROM {tableName};";
                await connection.ExecuteAsync(new CommandDefinition(createTempTableQuery, transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken));
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                    bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                    var dataTable = _sqlBuilder.ToDataTable(entities, properties);
                    using (var reader = dataTable.CreateDataReader())
                    {
                        // اصلاح نگاشت ستون‌ها
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            // پیدا کردن پراپرتی متناظر با ستون
                            var property = properties.FirstOrDefault(p => {
                                var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                                return dbColumnName == column.ColumnName;
                            });

                            if (property != null)
                            {
                                var dbColumnName = _queryCache.GetColumnName(property);
                                bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                            }
                        }

                        await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                    }
                }

                // اصلاح کوئری درج و بازیابی Identity
                var identityColumnName = _queryCache.GetColumnName(identityProp).Trim('[', ']');
                var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));

                // استفاده از متغیر جدول برای ذخیره مقادیر Identity
                var insertAndRetrieveQuery = $@"
                DECLARE @TempIds TABLE (Id BIGINT);
                
                INSERT INTO {tableName} ({columnList})
                OUTPUT INSERTED.{identityColumnName} INTO @TempIds
                SELECT {columnList} FROM {tempTableName};
                
                SELECT Id FROM @TempIds;";

                // استفاده از SqlCommand با تراکنش در سازنده
                using (var command = new SqlCommand(insertAndRetrieveQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        var identityValue = reader.GetValue(0);
                        identities.Add(identityValue);
                    }
                }

                await connection.ExecuteAsync(new CommandDefinition($"DROP TABLE {tempTableName}", transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken));
            }
            catch (Exception ex)
            {
                try { await connection.ExecuteAsync(new CommandDefinition($"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}", transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken)); } catch { }
                throw new InvalidOperationException("Bulk copy with identity retrieval failed", ex);
            }
            return identities;
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
        }
    }
    internal class StoredProcedureExecutor
    {
        private readonly ConnectionManager _connectionManager;
        private readonly SqlBuilder _sqlBuilder;

        public StoredProcedureExecutor(ConnectionManager connectionManager, SqlBuilder sqlBuilder)
        {
            _connectionManager = connectionManager;
            _sqlBuilder = sqlBuilder;
        }

        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class
        {
            _sqlBuilder.IsValidProcedureName(procedureName);
            var openConnection = _connectionManager.GetOpenConnection();
            return openConnection.Query<T>(
                procedureName,
                param: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                commandType: CommandType.StoredProcedure
            );
        }

        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            _sqlBuilder.IsValidProcedureName(procedureName);
            var openConnection = await _connectionManager.GetOpenConnectionAsync();
            return await openConnection.QueryAsync<T>(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction: _connectionManager.CurrentTransaction,
                    commandTimeout: _connectionManager.CommandTimeout,
                    commandType: CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false);
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            _sqlBuilder.IsValidProcedureName(procedureName);
            var openConnection = _connectionManager.GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(
                procedureName,
                parameters,
                transaction ?? _connectionManager.CurrentTransaction,
                commandTimeout ?? _connectionManager.CommandTimeout,
                CommandType.StoredProcedure))
            {
                return mapper(multi);
            }
        }

        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            _sqlBuilder.IsValidProcedureName(procedureName);
            var openConnection = await _connectionManager.GetOpenConnectionAsync();
            using (var multi = await openConnection.QueryMultipleAsync(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction ?? _connectionManager.CurrentTransaction,
                    commandTimeout ?? _connectionManager.CommandTimeout,
                    CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false))
            {
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            _sqlBuilder.IsValidFunctionName(functionName);
            var connection = _connectionManager.GetOpenConnection();
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout
            );
            return connection.ExecuteScalar<T>(commandDefinition);
        }

        public async Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            _sqlBuilder.IsValidFunctionName(functionName);
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken
            );
            return await connection.ExecuteScalarAsync<T>(commandDefinition);
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            _sqlBuilder.IsValidFunctionName(functionName);
            var connection = _connectionManager.GetOpenConnection();
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout
            );
            return connection.Query<T>(commandDefinition);
        }

        public async Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            _sqlBuilder.IsValidFunctionName(functionName);
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken
            );
            return await connection.QueryAsync<T>(commandDefinition);
        }
    }

    internal class EntityTracker : IDisposable
    {
        internal readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();

        public void Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var primaryKeys = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                .ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            if (!_attachedEntities.ContainsKey(key))
            {
                var clone = CloneEntity(entity);
                _attachedEntities.TryAdd(key, clone);
            }
        }

        public void Detach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            var primaryKeys = typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                .ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            _attachedEntities.TryRemove(key, out _);
        }

        internal object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1)
            {
                return primaryKeys[0].GetValue(entity);
            }
            return string.Join("|", primaryKeys.Select(p => p.GetValue(entity)?.ToString() ?? "NULL"));
        }

        internal List<string> GetChangedProperties<T>(T original, T current)
        {
            return typeof(T).GetProperties()
                .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) == null &&
                           !object.Equals(p.GetValue(original), p.GetValue(current)))
                .Select(p => p.Name)
                .ToList();
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
                    if (cloneMethod != null)
                    {
                        value = cloneMethod.Invoke(value, null);
                    }
                }
                prop.SetValue(clone, value);
            }
            return clone;
        }

        public void Dispose()
        {
            _attachedEntities.Clear();
        }
    }

    internal class SqlBuilder
    {
        private readonly QueryCache _queryCache;

        public SqlBuilder(QueryCache queryCache)
        {
            _queryCache = queryCache;
        }

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
            foreach (var pk in primaryKeys)
            {
                parameters.Add($"pk_{pk.Name}", pk.GetValue(entity));
            }
            foreach (var propName in changedProps)
            {
                var prop = typeof(T).GetProperty(propName);
                if (prop != null)
                {
                    parameters.Add(prop.Name, prop.GetValue(entity));
                }
            }
            return parameters;
        }

        public DynamicParameters CreatePrimaryKeyParameters<T>(T entity)
        {
            var parameters = new DynamicParameters();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            foreach (var pk in primaryKeys)
            {
                parameters.Add(pk.Name, pk.GetValue(entity));
            }
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

        public void IsValidProcedureName(string procedureName)
        {
            if (string.IsNullOrWhiteSpace(procedureName))
                throw new ArgumentException("Procedure name cannot be null or empty", nameof(procedureName));
            if (!Regex.IsMatch(procedureName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Procedure name is not valid", nameof(procedureName));
            }
        }

        public void IsValidFunctionName(string functionName)
        {
            if (string.IsNullOrWhiteSpace(functionName))
                throw new ArgumentException("Function name cannot be null or empty", nameof(functionName));
            if (!Regex.IsMatch(functionName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Function name is not valid", nameof(functionName));
            }
        }

        public string BuildScalarFunctionQuery(string functionName, object parameters)
        {
            return $"SELECT {functionName}({BuildParameters(parameters)})";
        }

        public string BuildTableFunctionQuery(string functionName, object parameters)
        {
            return $"SELECT * FROM {functionName}({BuildParameters(parameters)})";
        }

        private string BuildParameters(object parameters)
        {
            if (parameters == null) return "";
            return string.Join(", ", parameters.GetType().GetProperties().Select(p => $"@{p.Name}"));
        }

        public void ExecuteRawCommand(IDbConnection connection, IDbTransaction transaction, string commandText)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

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
                // حذف براکت‌ها برای نام ستون DataTable
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