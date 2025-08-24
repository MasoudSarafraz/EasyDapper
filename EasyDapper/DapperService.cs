using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;
using System.Data.Common;
using System.Collections;

namespace EasyDapper
{
    internal sealed class DapperService : IDapperService, IDisposable
    {
        // تغییر: اتصال Lazy را مستقیماً نگه می‌داریم تا از اتصال بیرونی جدا باشد.
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private readonly ThreadLocal<IDbConnection> _threadLocalConnection;
        private readonly ThreadLocal<IDbTransaction> _threadLocalTransaction;
        private readonly ThreadLocal<int> _threadLocalTransactionCount;
        private readonly ThreadLocal<Stack<string>> _threadLocalSavePoints;

        // حفظ کش‌های استاتیک برای thread-safety
        private static readonly ConcurrentDictionary<Type, string> InsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> UpdateQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> DeleteQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> GetByIdQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> BulkInsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> PrimaryKeyCache = new ConcurrentDictionary<Type, List<PropertyInfo>>();

        // تغییر: استفاده از ConcurrentDictionary برای ذخیره‌سازی موجودیت‌های Attach شده به صورت thread-safe
        private readonly ConcurrentDictionary<object, object> _attachedEntities = new ConcurrentDictionary<object, object>();

        private int BATCH_SIZE = 100;
        private int _timeOut;
        private string defualtSchema = "dbo";

        public int TransactionCount()
        {
            // تغییر: بازگرداندن تعداد تراکنش‌ها برای نخ جاری
            return _threadLocalTransactionCount.Value;
        }

        public DapperService(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            // تغییر: استفاده از ThreadLocal برای متغیرهای مرتبط با تراکنش و اتصال
            _threadLocalConnection = new ThreadLocal<IDbConnection>(() => new SqlConnection(_connectionString));
            _threadLocalTransaction = new ThreadLocal<IDbTransaction>();
            _threadLocalTransactionCount = new ThreadLocal<int>(() => 0);
            _threadLocalSavePoints = new ThreadLocal<Stack<string>>(() => new Stack<string>());
            // تغییر: دریافت Timeout از اتصال جدید ایجاد شده
            try
            {
                var tempConnection = new SqlConnection(_connectionString);
                _timeOut = tempConnection.ConnectionTimeout;
                tempConnection.Dispose();
            }
            catch
            {
                _timeOut = 30; // مقدار پیش‌فرض در صورت خطا
            }
        }

        public DapperService(IDbConnection externalConnection)
        {
            _externalConnection = externalConnection ?? throw new ArgumentNullException(nameof(externalConnection));
            // تغییر: دریافت Timeout از اتصال بیرونی
            try
            {
                _timeOut = _externalConnection.ConnectionTimeout;
            }
            catch
            {
                _timeOut = 30; // مقدار پیش‌فرض در صورت خطا
            }
            // تغییر: استفاده از ThreadLocal برای متغیرهای مرتبط با تراکنش
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
                var savePointName = $"SavePoint{_threadLocalTransactionCount.Value}";
                ExecuteTransactionCommand($"SAVE TRANSACTION {savePointName}");
                _threadLocalSavePoints.Value.Push(savePointName);
            }
            _threadLocalTransactionCount.Value++;
        }

        public void CommitTransaction()
        {
            if (_threadLocalTransaction.Value == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }
            if (_threadLocalTransactionCount.Value <= 0)
            {
                throw new InvalidOperationException("No active transactions to commit.");
            }
            _threadLocalTransactionCount.Value--;
            if (_threadLocalTransactionCount.Value == 0)
            {
                _threadLocalTransaction.Value.Commit();
                CleanupTransaction();
            }
            else
            {
                _threadLocalSavePoints.Value.Pop();
            }
        }

        public void RollbackTransaction()
        {
            if (_threadLocalTransaction.Value == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }
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
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;
            var connection = GetOpenConnection() as SqlConnection;
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities)
            {
                for (int i = 0; i < entityList.Count; i += BATCH_SIZE)
                {
                    var batch = entityList.GetRange(i, Math.Min(BATCH_SIZE, entityList.Count - i));
                    var query = BuildOptimizedBatchInsertQuery<T>(batch.Count);
                    var parameters = CreateOptimizedParameters(batch);
                    if (identityProp.PropertyType == typeof(int))
                    {
                        var generatedIds = connection.Query<int>(query, parameters, _threadLocalTransaction.Value, commandTimeout: _timeOut).ToList();
                        // تغییر: استفاده از حلقه ساده به جای Parallel.For برای سادگی و جلوگیری از مشکلات احتمالی در محیط‌های خاص
                        for (int j = 0; j < batch.Count; j++)
                        {
                            identityProp.SetValue(batch[j], generatedIds[j]);
                        }
                    }
                    if (identityProp.PropertyType == typeof(long))
                    {
                        var generatedIds = connection.Query<long>(query, parameters, _threadLocalTransaction.Value, commandTimeout: _timeOut).ToList();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            identityProp.SetValue(batch[j], generatedIds[j]);
                        }
                    }
                }
            }
            else
            {
                InsertBulkCopy(entities);
            }
            return entityList.Count;
        }

        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            var entityList = entities.ToList();
            if (entityList.Count == 0) return 0;
            var connection = await GetOpenConnectionAsync();
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null && generateIdentities)
            {
                for (int i = 0; i < entityList.Count; i += BATCH_SIZE)
                {
                    var batch = entityList.GetRange(i, Math.Min(BATCH_SIZE, entityList.Count - i));
                    var query = BuildOptimizedBatchInsertQuery<T>(batch.Count);
                    var parameters = await CreateParametersAsync(batch);
                    var commandDefinition = new CommandDefinition(
                        commandText: query,
                        parameters: parameters,
                        transaction: _threadLocalTransaction.Value,
                        commandTimeout: _timeOut,
                        cancellationToken: cancellationToken
                    );
                    if (identityProp.PropertyType == typeof(int))
                    {
                        var generatedIds = (await connection.QueryAsync<int>(commandDefinition)).ToList();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            identityProp.SetValue(batch[j], generatedIds[j]);
                        }
                    }
                    if (identityProp.PropertyType == typeof(long))
                    {
                        var generatedIds = (await connection.QueryAsync<long>(commandDefinition)).ToList();
                        for (int j = 0; j < batch.Count; j++)
                        {
                            identityProp.SetValue(batch[j], generatedIds[j]);
                        }
                    }
                }
            }
            else
            {
                await InsertBulkCopyAsync(entities, cancellationToken);
            }
            return entityList.Count;
        }

        public int Update<T>(T entity) where T : class
        {
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
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), BuildUpdateQuery<T>);
            return connection.Execute(query, entities, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
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
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);
            return connection.Execute(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), BuildDeleteQuery<T>);
            var parameters = entities.Select(CreatePrimaryKeyParameters);
            return connection.Execute(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
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
            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);
            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _threadLocalTransaction.Value, _timeOut);
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), BuildGetByIdQuery<T>);
            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _threadLocalTransaction.Value, _timeOut);
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
            return connection.QueryFirstOrDefault<T>(query, parameters, _threadLocalTransaction.Value, _timeOut);
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
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _threadLocalTransaction.Value, _timeOut);
        }

        public IQueryBuilder<T> Query<T>()
        {
            if (_externalConnection != null)
            {
                return new QueryBuilder<T>(_externalConnection);
            }
            // تغییر: استفاده از اتصال ThreadLocal
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
                transaction ?? _threadLocalTransaction.Value, // تغییر: استفاده از تراکنش ThreadLocal به عنوان پیش‌فرض
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
                    transaction ?? _threadLocalTransaction.Value, // تغییر: استفاده از تراکنش ThreadLocal به عنوان پیش‌فرض
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
            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
            var key = CreateCompositeKey(entity, primaryKeys);
            _attachedEntities.TryRemove(key, out _);
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
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

        private object CreateCompositeKey<T>(T entity, List<PropertyInfo> primaryKeys)
        {
            if (primaryKeys.Count == 1)
            {
                return primaryKeys[0].GetValue(entity);
            }
            return primaryKeys.Select(p => p.GetValue(entity)).ToArray();
        }

        private string GetTableName<T>()
        {
            var tableAttr = typeof(T).GetCustomAttribute<TableAttribute>();
            return tableAttr == null
                ? $"[{this.defualtSchema}].[{typeof(T).Name}]"
                : $"[{tableAttr.Schema ?? this.defualtSchema}].[{tableAttr.TableName}]";
        }

        private string GetColumnName(PropertyInfo property)
        {
            var columnAttr = property.GetCustomAttribute<ColumnAttribute>();
            return columnAttr == null
                ? $"[{property.Name}]"
                : $"[{columnAttr.ColumnName}]";
        }

        private List<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            return PrimaryKeyCache.GetOrAdd(typeof(T), type =>
            {
                var properties = typeof(T).GetProperties()
                    .Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>() != null)
                    .ToList();
                if (properties.Count == 0)
                {
                    throw new InvalidOperationException($"No primary key defined for {typeof(T).Name}");
                }
                var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>() != null);
                if (identityPk > 1)
                {
                    throw new InvalidOperationException("Multiple Identity primary keys are not supported");
                }
                return properties;
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
            foreach (var prop in changedProps.Select(p => typeof(T).GetProperty(p)))
            {
                parameters.Add(prop.Name, prop.GetValue(entity));
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

        private bool IsPrimaryKey(PropertyInfo property) => property.GetCustomAttribute<PrimaryKeyAttribute>() != null;

        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>() == null);
        }

        internal IDbConnection GetOpenConnection()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                {
                    _externalConnection.Open();
                }
                return _externalConnection;
            }
            // تغییر: استفاده از اتصال ThreadLocal
            var connection = _threadLocalConnection.Value;
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
            // تغییر: استفاده از اتصال ThreadLocal
            var connection = _threadLocalConnection.Value;
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
            if (!Regex.IsMatch(procedureName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Procedure name is not valid", nameof(procedureName));
            }
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

        private string BuildSimpleInsertQuery<T>(Type type)
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

        private string BuildBatchInsertWithOutputQuery<T>()
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
                ORDER BY Id ASC;";
        }

        private DynamicParameters CreateOptimizedParameters<T>(List<T> entities)
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

        private async Task<DynamicParameters> CreateParametersAsync<T>(List<T> entities)
        {
            return await Task.Run(() =>
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
            });
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
            using (var reader = ConvertToDbDataReader(dataTable))
            {
                using (var bulkCopy = new SqlBulkCopy((SqlConnection)GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
                {
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.WriteToServer(reader);
                }
            }
        }

        private async Task InsertBulkCopyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>().ToList();
            var dataTable = ToDataTable(entities, properties);
            using (var reader = ConvertToDbDataReader(dataTable))
            {
                using (var bulkCopy = new SqlBulkCopy((SqlConnection)GetOpenConnection(), SqlBulkCopyOptions.Default, (SqlTransaction)_threadLocalTransaction.Value))
                {
                    bulkCopy.DestinationTableName = tableName;
                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                }
            }
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
                    var dataTable = ToDataTable(entities, properties);
                    var DbDataReader = ConvertToDbDataReader(dataTable);
                    bulkCopy.WriteToServer(DbDataReader);
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

        private DbDataReader ConvertToDbDataReader(DataTable dataTable)
        {
            return dataTable.CreateDataReader();
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

        private int GetSqlServerVersion()
        {
            if (_connectionString == null)
            {
                return 0;
            }
            try
            {
                var tempConnection = new SqlConnection(_connectionString);
                if (tempConnection.State != ConnectionState.Open)
                {
                    tempConnection.Open();
                }
                string versionQuery = "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS NVARCHAR(128))";
                var majorVersion = tempConnection.QueryFirstOrDefault<string>(versionQuery);
                tempConnection.Close();
                tempConnection.Dispose();
                if (string.IsNullOrEmpty(majorVersion))
                {
                    return 0;
                }
                switch (majorVersion)
                {
                    case "16": return 2022;
                    case "15": return 2019;
                    case "14": return 2017;
                    case "13": return 2016;
                    case "12": return 2014;
                    case "11": return 2012;
                    case "10": return 2008;
                    case "9": return 2005;
                    case "8": return 2000;
                    default: return int.TryParse(majorVersion, out int version) ? version : 0;
                }
            }
            catch (Exception)
            {
                return 0;
            }
        }

        private void CleanupTransaction()
        {
            _threadLocalTransaction.Value?.Dispose();
            _threadLocalTransaction.Value = null;
            _threadLocalSavePoints.Value.Clear();
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
                    //Do Nothing
                }
            }
            if (_threadLocalConnection?.IsValueCreated == true)
            {
                _threadLocalConnection.Value.Dispose();
            }
            _threadLocalConnection?.Dispose();
            _threadLocalTransaction?.Dispose();
            _threadLocalTransactionCount?.Dispose();
            _threadLocalSavePoints?.Dispose();
        }
    }
}