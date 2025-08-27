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
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private readonly ThreadLocal<IDbConnection> _threadLocalConnection;
        private readonly ThreadLocal<IDbTransaction> _threadLocalTransaction;
        private readonly ThreadLocal<int> _threadLocalTransactionCount;
        private readonly ThreadLocal<Stack<string>> _threadLocalSavePoints;

        // کش‌های استاتیک برای thread-safety
        private static readonly ConcurrentDictionary<Type, string> InsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> UpdateQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> DeleteQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> GetByIdQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> PrimaryKeyCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();
        private static readonly ConcurrentDictionary<Type, PropertyInfo> IdentityPropertyCache = new ConcurrentDictionary<Type, PropertyInfo>();
        private static readonly ConcurrentDictionary<string, string> TableNameCache = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<string, string> ColumnNameCache = new ConcurrentDictionary<string, string>();

        private readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();

        private const int DEFAULT_BATCH_SIZE = 100;
        private const int DEFAULT_TIMEOUT = 30;
        private const string DEFAULT_SCHEMA = "dbo";
        private readonly int _timeOut;

        public int TransactionCount()
        {
            return _threadLocalTransactionCount.Value;
        }

        public DapperService(string connectionString)
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

        public DapperService(IDbConnection externalConnection)
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
                throw new InvalidOperationException("No transaction is in progress.");

            if (_threadLocalTransactionCount.Value <= 0)
                throw new InvalidOperationException("No active transactions to commit.");

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
                throw new InvalidOperationException("No transaction is in progress.");

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

        public int Insert<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var connection = GetOpenConnection();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp != null)
            {
                var newId = connection.ExecuteScalar(query, entity, _threadLocalTransaction.Value, _timeOut);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }

            return connection.Execute(query, entity, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var connection = await GetOpenConnectionAsync();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync(query, entity, _threadLocalTransaction.Value, _timeOut);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }

            return await connection.ExecuteAsync(query, entity, _threadLocalTransaction.Value, _timeOut);
        }

        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));

            var entityList = entities as IList<T> ?? entities.ToList();
            if (entityList.Count == 0) return 0;

            var identityProp = GetIdentityProperty<T>();
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

            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities)
            {
                return await InsertListWithIdentityAsync(entityList, cancellationToken);
            }

            await InsertBulkCopyAsync(entityList, cancellationToken);
            return entityList.Count;
        }

        public int Update<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
            var key = CreateCompositeKey(entity, primaryKeys);

            if (!_attachedEntities.TryGetValue(key, out var original))
            {
                return BaseUpdate(entity);
            }

            var changedProps = GetChangedProperties((T)original, entity);
            if (!changedProps.Any())
            {
                return 0;
            }

            var query = BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = BuildParameters(entity, primaryKeys, changedProps);
            var connection = GetOpenConnection();

            return connection.Execute(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
            var key = CreateCompositeKey(entity, primaryKeys);

            if (!_attachedEntities.TryGetValue(key, out var original))
            {
                return await BaseUpdateAsync(entity);
            }

            var changedProps = GetChangedProperties((T)original, entity);
            if (!changedProps.Any())
            {
                return 0;
            }

            var query = BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = BuildParameters(entity, primaryKeys, changedProps);
            var connection = await GetOpenConnectionAsync();

            return await connection.ExecuteAsync(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));

            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);

            return connection.Execute(query, entities, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));

            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: entities,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut,
                cancellationToken: cancellationToken
            );

            return await connection.ExecuteAsync(commandDefinition);
        }

        public int Delete<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);

            return connection.Execute(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);

            return await connection.ExecuteAsync(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));

            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = entities.Select(CreatePrimaryKeyParameters);

            return connection.Execute(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            if (entities == null) throw new ArgumentNullException(nameof(entities));

            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = entities.Select(CreatePrimaryKeyParameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut,
                cancellationToken: cancellationToken
            );

            return await connection.ExecuteAsync(commandDefinition);
        }

        public T GetById<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);

            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);

            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _threadLocalTransaction.Value, _timeOut);
        }

        public T GetById<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = GetSelectColumns<T>();
                var whereClause = BuildWhereClause<T>();
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

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
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public IQueryBuilder<T> Query<T>()
        {
            if (_externalConnection != null)
            {
                return new QueryBuilder<T>(_externalConnection);
            }

            return new QueryBuilder<T>(_threadLocalConnection.Value);
        }

        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();

            return openConnection.Query<T>(
                procedureName,
                param: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut,
                commandType: CommandType.StoredProcedure
            );
        }

        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            IsValidProcedureName(procedureName);
            var openConnection = await GetOpenConnectionAsync();

            return await openConnection.QueryAsync<T>(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction: _threadLocalTransaction.Value,
                    commandTimeout: _timeOut,
                    commandType: CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false);
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();

            using (var multi = openConnection.QueryMultiple(
                procedureName,
                parameters,
                transaction ?? _threadLocalTransaction.Value,
                commandTimeout ?? _timeOut,
                CommandType.StoredProcedure))
            {
                return mapper(multi);
            }
        }

        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            IsValidProcedureName(procedureName);
            var openConnection = await GetOpenConnectionAsync();

            using (var multi = await openConnection.QueryMultipleAsync(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction ?? _threadLocalTransaction.Value,
                    commandTimeout ?? _timeOut,
                    CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false))
            {
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }

        public void Attach<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
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

            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
            var key = CreateCompositeKey(entity, primaryKeys);

            _attachedEntities.TryRemove(key, out _);
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            IsValidFunctionName(functionName);
            var connection = GetOpenConnection();
            var query = BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut
            );

            return connection.ExecuteScalar<T>(commandDefinition);
        }

        public async Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            IsValidFunctionName(functionName);
            var connection = await GetOpenConnectionAsync();
            var query = BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut,
                cancellationToken: cancellationToken
            );

            return await connection.ExecuteScalarAsync<T>(commandDefinition);
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            IsValidFunctionName(functionName);
            var connection = GetOpenConnection();
            var query = BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut
            );

            return connection.Query<T>(commandDefinition);
        }

        public async Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            IsValidFunctionName(functionName);
            var connection = await GetOpenConnectionAsync();
            var query = BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(
                commandText: query,
                parameters: parameters,
                transaction: _threadLocalTransaction.Value,
                commandTimeout: _timeOut,
                cancellationToken: cancellationToken
            );

            return await connection.QueryAsync<T>(commandDefinition);
        }

        #region Helper Methods

        private object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1)
            {
                return primaryKeys[0].GetValue(entity);
            }

            return string.Join("|", primaryKeys.Select(p => p.GetValue(entity)?.ToString() ?? "NULL"));
        }

        private string GetTableName<T>()
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

        private string GetColumnName(PropertyInfo property)
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

        private List<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            return PrimaryKeyCache.GetOrAdd(typeof(T), type =>
            {
                var properties = type.GetProperties()
                    .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null)
                    .ToList();

                if (properties.Count == 0)
                {
                    throw new InvalidOperationException($"No primary key defined for {type.Name}");
                }

                var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>(true) != null);
                if (identityPk > 1)
                {
                    throw new InvalidOperationException("Multiple Identity primary keys are not supported");
                }

                return properties;
            });
        }

        private PropertyInfo GetIdentityProperty<T>()
        {
            return IdentityPropertyCache.GetOrAdd(typeof(T), type =>
            {
                return type.GetProperties()
                    .FirstOrDefault(p =>
                        p.GetCustomAttribute<IdentityAttribute>(true) != null &&
                        p.GetCustomAttribute<PrimaryKeyAttribute>(true) != null);
            });
        }

        private List<string> GetChangedProperties<T>(T original, T current)
        {
            return typeof(T).GetProperties()
                .Where(p => !IsPrimaryKey(p) && !object.Equals(p.GetValue(original), p.GetValue(current)))
                .Select(p => p.Name)
                .ToList();
        }

        private string BuildDynamicUpdateQuery<T>(List<string> changedProps, List<PropertyInfo> primaryKeys)
        {
            var tableName = GetTableName<T>();
            var changedProperties = changedProps
                .Select(p => typeof(T).GetProperty(p))
                .Where(p => p != null)
                .ToList();

            var setClause = string.Join(", ", changedProperties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @pk_{p.Name}"));

            return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
        }

        private DynamicParameters BuildParameters<T>(T entity, List<PropertyInfo> primaryKeys, List<string> changedProps)
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

        private int BaseUpdate<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);

            return connection.Execute(query, entity, _threadLocalTransaction.Value, _timeOut);
        }

        private async Task<int> BaseUpdateAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);

            return await connection.ExecuteAsync(query, entity, _threadLocalTransaction.Value, _timeOut);
        }

        private bool IsPrimaryKey(PropertyInfo property) =>
            property.GetCustomAttribute<PrimaryKeyAttribute>(true) != null;

        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
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

            return _threadLocalConnection.Value;
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

            return _threadLocalConnection.Value;
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

        private void IsValidProcedureName(string procedureName)
        {
            if (string.IsNullOrWhiteSpace(procedureName))
                throw new ArgumentException("Procedure name cannot be null or empty", nameof(procedureName));

            if (!Regex.IsMatch(procedureName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Procedure name is not valid", nameof(procedureName));
            }
        }

        private void IsValidFunctionName(string functionName)
        {
            if (string.IsNullOrWhiteSpace(functionName))
                throw new ArgumentException("Function name cannot be null or empty", nameof(functionName));

            if (!Regex.IsMatch(functionName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Function name is not valid", nameof(functionName));
            }
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

        private void ExecuteTransactionCommand(string commandText)
        {
            using (var command = _threadLocalTransaction.Value.Connection.CreateCommand())
            {
                command.Transaction = _threadLocalTransaction.Value;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void ExecuteRawCommand(string commandText)
        {
            using (var command = GetOpenConnection().CreateCommand())
            {
                command.Transaction = _threadLocalTransaction.Value;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void InsertBulkCopy<T>(IEnumerable<T> entities) where T : class
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>().ToList();
            var dataTable = ToDataTable(entities, properties);

            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy((SqlConnection)GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _timeOut;

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task InsertBulkCopyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>().ToList();
            var dataTable = ToDataTable(entities, properties);

            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy((SqlConnection)GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _timeOut;

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            }
        }

        private int InsertListWithIdentity<T>(IList<T> entities) where T : class
        {
            var identities = InsertBulkCopyWithIdentity(entities);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp.PropertyType == typeof(int))
            {
                for (int i = 0; i < entities.Count; i++)
                {
                    identityProp.SetValue(entities[i], identities[i]);
                }
            }

            return entities.Count;
        }

        private async Task<int> InsertListWithIdentityAsync<T>(IList<T> entities, CancellationToken cancellationToken) where T : class
        {
            var identities = await InsertBulkCopyWithIdentityAsync(entities, cancellationToken);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp.PropertyType == typeof(int))
            {
                for (int i = 0; i < entities.Count; i++)
                {
                    identityProp.SetValue(entities[i], identities[i]);
                }
            }

            return entities.Count;
        }

        private List<int> InsertBulkCopyWithIdentity<T>(IEnumerable<T> entities) where T : class
        {
            var random = new Random().Next(10, 100000000);
            var tableName = GetTableName<T>();
            var tempTableName = $"##Temp_{random}";
            var identityProp = GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>().ToList();
            var connection = (SqlConnection)GetOpenConnection();
            var identities = new List<int>();

            try
            {
                var createTempTableQuery = $@"SELECT TOP 0 {string.Join(", ", properties.Select(GetColumnName))} INTO {tempTableName} FROM {tableName};";
                ExecuteRawCommand(createTempTableQuery);

                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                    bulkCopy.BulkCopyTimeout = _timeOut;

                    var dataTable = ToDataTable(entities, properties);
                    using (var reader = dataTable.CreateDataReader())
                    {
                        bulkCopy.WriteToServer(reader);
                    }
                }

                var insertAndRetrieveQuery = $@"
                    INSERT INTO {tableName} ({string.Join(", ", properties.Select(p => GetColumnName(p)))})
                    OUTPUT INSERTED.{GetColumnName(identityProp)}
                    SELECT {string.Join(", ", properties.Select(GetColumnName))}
                    FROM {tempTableName}";

                identities = connection.Query<int>(insertAndRetrieveQuery, transaction: _threadLocalTransaction.Value).ToList();
                ExecuteRawCommand($"DROP TABLE {tempTableName}");
            }
            catch (Exception ex)
            {
                try { ExecuteRawCommand($"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}"); } catch { }
                throw new InvalidOperationException("Bulk copy with identity retrieval failed.", ex);
            }

            identities.Sort();
            return identities;
        }

        private async Task<List<int>> InsertBulkCopyWithIdentityAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            var random = new Random().Next(10, 100000000);
            var tableName = GetTableName<T>();
            var tempTableName = $"##Temp_{random}";
            var identityProp = GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>().ToList();
            var connection = (SqlConnection)await GetOpenConnectionAsync();
            var identities = new List<int>();

            try
            {
                var createTempTableQuery = $@"SELECT TOP 0 {string.Join(", ", properties.Select(GetColumnName))} INTO {tempTableName} FROM {tableName};";
                await connection.ExecuteAsync(new CommandDefinition(createTempTableQuery, transaction: _threadLocalTransaction.Value, cancellationToken: cancellationToken));

                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                    bulkCopy.BulkCopyTimeout = _timeOut;

                    var dataTable = ToDataTable(entities, properties);
                    using (var reader = dataTable.CreateDataReader())
                    {
                        await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                    }
                }

                var insertAndRetrieveQuery = $@"
                    INSERT INTO {tableName} ({string.Join(", ", properties.Select(p => GetColumnName(p)))})
                    OUTPUT INSERTED.{GetColumnName(identityProp)}
                    SELECT {string.Join(", ", properties.Select(GetColumnName))}
                    FROM {tempTableName}";

                identities = (await connection.QueryAsync<int>(new CommandDefinition(insertAndRetrieveQuery, transaction: _threadLocalTransaction.Value, cancellationToken: cancellationToken))).ToList();
                await connection.ExecuteAsync(new CommandDefinition($"DROP TABLE {tempTableName}", transaction: _threadLocalTransaction.Value, cancellationToken: cancellationToken));
            }
            catch (Exception ex)
            {
                try { await connection.ExecuteAsync(new CommandDefinition($"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}", transaction: _threadLocalTransaction.Value, cancellationToken: cancellationToken)); } catch { }
                throw new InvalidOperationException("Bulk copy with identity retrieval failed.", ex);
            }

            identities.Sort();
            return identities;
        }

        private DataTable ToDataTable<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> properties) where T : class
        {
            var dataTable = new DataTable();

            foreach (var property in properties)
            {
                var columnName = GetColumnName(property);
                dataTable.Columns.Add(columnName, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }

            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();

                foreach (var property in properties)
                {
                    var columnName = GetColumnName(property);
                    row[columnName] = property.GetValue(entity) ?? DBNull.Value;
                }

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        private string BuildTableFunctionQuery(string functionName, object parameters)
        {
            return $"SELECT * FROM {functionName}({BuildParameters(parameters)})";
        }

        private string BuildScalarFunctionQuery(string functionName, object parameters)
        {
            return $"SELECT {functionName}({BuildParameters(parameters)})";
        }

        private string BuildParameters(object parameters)
        {
            if (parameters == null) return "";

            return string.Join(", ", parameters.GetType().GetProperties().Select(p => $"@{p.Name}"));
        }

        private void CleanupTransaction()
        {
            _threadLocalTransaction.Value?.Dispose();
            _threadLocalTransaction.Value = null;
            _threadLocalSavePoints.Value?.Clear();
        }

        public void Dispose()
        {
            if (_threadLocalTransaction.Value != null)
            {
                try
                {
                    RollbackTransaction();
                }
                catch
                {
                    // Ignore errors during rollback on dispose
                }
            }

            if (_threadLocalConnection?.IsValueCreated == true)
            {
                _threadLocalConnection.Value.Close();
                _threadLocalConnection.Value.Dispose();
            }

            _threadLocalConnection?.Dispose();
            _threadLocalTransaction?.Dispose();
            _threadLocalTransactionCount?.Dispose();
            _threadLocalSavePoints?.Dispose();
        }

        #endregion
    }
}