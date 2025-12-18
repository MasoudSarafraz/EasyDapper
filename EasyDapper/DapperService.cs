using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal sealed class LockFreeLruCacheDapperService<TKey, TValue> : IDisposable
    {
        private readonly int _capacity;
        private readonly ConcurrentDictionary<TKey, LruNode> _nodes;
        private readonly ConcurrentDictionary<TKey, long> _accessCounts;
        private readonly ConcurrentQueue<AccessRecord> _accessRecords;
        private readonly int _evictionBatchSize;
        private readonly double _evictionThresholdFactor;
        private long _totalAccessCount;
        private long _lastEvictionSize;
        private int _evictionInProgress;

        public LockFreeLruCacheDapperService(int capacity)
        {
            if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));

            _capacity = capacity;
            _nodes = new ConcurrentDictionary<TKey, LruNode>();
            _accessCounts = new ConcurrentDictionary<TKey, long>();
            _accessRecords = new ConcurrentQueue<AccessRecord>();
            _evictionBatchSize = Math.Max(10, capacity / 20);
            _evictionThresholdFactor = 1.2;
            _totalAccessCount = 0;
            _lastEvictionSize = 0;
            _evictionInProgress = 0;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (valueFactory == null) throw new ArgumentNullException(nameof(valueFactory));

            var spinWait = new SpinWait();
            while (true)
            {
                if (_nodes.TryGetValue(key, out var existingNode))
                {
                    RecordAccess(key, existingNode);
                    return existingNode.Value;
                }

                var value = valueFactory(key);
                var newNode = new LruNode
                {
                    Value = value,
                    AccessCount = 1,
                    LastAccess = DateTime.UtcNow.Ticks
                };

                if (_nodes.TryAdd(key, newNode))
                {
                    RecordAccess(key, newNode);

                    var currentSize = _nodes.Count;
                    if (currentSize > _capacity * _evictionThresholdFactor)
                    {
                        TryTriggerEviction();
                    }

                    return value;
                }

                spinWait.SpinOnce();
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            if (_nodes.TryGetValue(key, out var node))
            {
                RecordAccess(key, node);
                value = node.Value;
                return true;
            }

            value = default(TValue);
            return false;
        }

        public bool TryRemove(TKey key)
        {
            if (_nodes.TryRemove(key, out var removedNode))
            {
                _accessCounts.TryRemove(key, out _);
                return true;
            }
            return false;
        }

        public void Clear()
        {
            _nodes.Clear();
            _accessCounts.Clear();
            Interlocked.Exchange(ref _totalAccessCount, 0);

            while (_accessRecords.TryDequeue(out _)) { }
        }

        public int Count => _nodes.Count;

        private void RecordAccess(TKey key, LruNode node)
        {
            Interlocked.Increment(ref node.AccessCount);
            node.LastAccess = DateTime.UtcNow.Ticks;
            Interlocked.Increment(ref _totalAccessCount);

            var record = new AccessRecord { Key = key, Timestamp = DateTime.UtcNow.Ticks };
            _accessRecords.Enqueue(record);

            if (_totalAccessCount % 1000 == 0)
            {
                CleanupAccessRecords();
            }
        }

        private void TryTriggerEviction()
        {
            if (Interlocked.CompareExchange(ref _evictionInProgress, 1, 0) == 0)
            {
                try
                {
                    PerformLockFreeEviction();
                }
                finally
                {
                    Interlocked.Exchange(ref _evictionInProgress, 0);
                }
            }
        }

        private void PerformLockFreeEviction()
        {
            var currentSize = _nodes.Count;
            if (currentSize <= _capacity) return;

            var excess = currentSize - _capacity;
            var targetEvictionCount = Math.Min(excess + _evictionBatchSize, currentSize - _capacity);

            if (targetEvictionCount <= 0) return;

            var recentAccesses = CollectRecentAccessPatterns();
            var candidates = _nodes
                .Select(pair => new Candidate
                {
                    Key = pair.Key,
                    Node = pair.Value,
                    Score = CalculateEvictionScore(pair.Value, recentAccesses)
                })
                .OrderBy(c => c.Score)
                .Take(targetEvictionCount)
                .ToList();

            foreach (var candidate in candidates)
            {
                if (_nodes.TryGetValue(candidate.Key, out var currentnode))
                {
                    var currentAccessCount = currentnode.AccessCount;
                    var currentLastAccess = currentnode.LastAccess;

                    if (currentAccessCount == candidate.Node.AccessCount &&
                        currentLastAccess == candidate.Node.LastAccess)
                    {
                        _nodes.TryRemove(candidate.Key, out _);
                        _accessCounts.TryRemove(candidate.Key, out _);
                    }
                }
            }

            _lastEvictionSize = _nodes.Count;
            CleanupAccessRecords();
        }

        private Dictionary<TKey, long> CollectRecentAccessPatterns()
        {
            var recentAccesses = new Dictionary<TKey, long>();
            var now = DateTime.UtcNow.Ticks;
            var timeWindow = TimeSpan.FromMinutes(5).Ticks;

            var records = _accessRecords
                .Where(r => now - r.Timestamp < timeWindow)
                .Take(1000)
                .GroupBy(r => r.Key)
                .ToDictionary(g => g.Key, g => (long)g.Count());

            return records;
        }

        private double CalculateEvictionScore(LruNode node, Dictionary<TKey, long> recentAccesses)
        {
            var timeSinceLastAccess = (DateTime.UtcNow.Ticks - node.LastAccess) / TimeSpan.TicksPerMinute;

            var frequencyScore = 1.0 / (1.0 + node.AccessCount);
            var timeScore = timeSinceLastAccess / (60 * 24);

            double recentPenalty = 1.0;
            if (recentAccesses.TryGetValue(node.Key, out var recentCount))
            {
                recentPenalty = 1.0 / (1.0 + recentCount);
            }

            return 0.6 * frequencyScore + 0.3 * timeScore + 0.1 * recentPenalty;
        }

        private void CleanupAccessRecords()
        {
            var now = DateTime.UtcNow.Ticks;
            var retentionTime = TimeSpan.FromMinutes(10).Ticks;

            var recentRecords = _accessRecords
                .Where(r => now - r.Timestamp < retentionTime)
                .Take(5000)
                .ToList();

            while (_accessRecords.TryDequeue(out _)) { }
            foreach (var record in recentRecords)
            {
                _accessRecords.Enqueue(record);
            }
        }

        public CacheInfo GetCacheInfo()
        {
            var nodes = _nodes.ToArray();
            var accessCounts = nodes.Select(n => n.Value.AccessCount).ToArray();
            var lastAccessTicks = nodes.Select(n => n.Value.LastAccess).ToArray();

            return new CacheInfo
            {
                Capacity = _capacity,
                CurrentSize = nodes.Length,
                HitRate = accessCounts.Length > 0 ? accessCounts.Average() : 0,
                OldestItem = lastAccessTicks.Length > 0 ? new DateTime(lastAccessTicks.Min()) : DateTime.UtcNow,
                NewestItem = lastAccessTicks.Length > 0 ? new DateTime(lastAccessTicks.Max()) : DateTime.UtcNow,
                TotalAccesses = Interlocked.Read(ref _totalAccessCount),
                EvictionInProgress = _evictionInProgress == 1
            };
        }

        private class LruNode
        {
            public TValue Value;
            public long AccessCount;
            public long LastAccess;
            public TKey Key;
        }

        private struct AccessRecord
        {
            public TKey Key;
            public long Timestamp;
        }

        private class Candidate
        {
            public TKey Key;
            public LruNode Node;
            public double Score;
        }

        public class CacheInfo
        {
            public int Capacity { get; set; }
            public int CurrentSize { get; set; }
            public double HitRate { get; set; }
            public DateTime OldestItem { get; set; }
            public DateTime NewestItem { get; set; }
            public long TotalAccesses { get; set; }
            public bool EvictionInProgress { get; set; }
        }

        public void Dispose()
        {
            Clear();
        }
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
            if (disposing)
            {
                _connectionManager?.Dispose();
                _entityTracker?.Dispose();
                _queryCache?.Dispose();
            }
            _disposed = true;
        }
    }

    internal class ConnectionManager : IDisposable
    {
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private readonly ConcurrentDictionary<int, IDbConnection> _connectionPool;
        private readonly ConcurrentDictionary<int, IDbTransaction> _transactionPool;
        private readonly ConcurrentDictionary<int, ConcurrentStack<string>> _savePointPool;
        private readonly int _timeOut;
        private const int DEFAULT_TIMEOUT = 30;
        private bool _disposed = false;

        public int TransactionCount => _transactionPool.Count;

        public IDbTransaction CurrentTransaction => _transactionPool.Values.FirstOrDefault();

        public int CommandTimeout => _timeOut;

        public ConnectionManager(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException("connectionString");
            _connectionString = connectionString;
            _connectionPool = new ConcurrentDictionary<int, IDbConnection>();
            _transactionPool = new ConcurrentDictionary<int, IDbTransaction>();
            _savePointPool = new ConcurrentDictionary<int, ConcurrentStack<string>>();
            _timeOut = GetConnectionTimeout();
        }

        public ConnectionManager(IDbConnection externalConnection)
        {
            if (externalConnection == null) throw new ArgumentNullException("externalConnection");
            _externalConnection = externalConnection;
            _timeOut = GetExternalConnectionTimeout();
            _transactionPool = new ConcurrentDictionary<int, IDbTransaction>();
            _savePointPool = new ConcurrentDictionary<int, ConcurrentStack<string>>();
        }

        private int GetConnectionTimeout()
        {
            try
            {
                using (var tempConnection = new SqlConnection(_connectionString))
                {
                    tempConnection.Open();
                    return tempConnection.ConnectionTimeout;
                }
            }
            catch { return DEFAULT_TIMEOUT; }
        }

        private int GetExternalConnectionTimeout()
        {
            try { return _externalConnection.ConnectionTimeout; }
            catch { return DEFAULT_TIMEOUT; }
        }

        public IDbConnection GetOpenConnection()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open) _externalConnection.Open();
                return _externalConnection;
            }

            if (!_connectionPool.TryGetValue(threadId, out var connection))
            {
                connection = new SqlConnection(_connectionString);
                connection.Open();
                _connectionPool.TryAdd(threadId, connection);
            }

            return connection;
        }

        public async Task<IDbConnection> GetOpenConnectionAsync()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open) _externalConnection.Open();
                return _externalConnection;
            }

            if (!_connectionPool.TryGetValue(threadId, out var connection))
            {
                connection = new SqlConnection(_connectionString);
                await Task.Run(() => connection.Open());
                _connectionPool.TryAdd(threadId, connection);
            }

            return connection;
        }

        public void BeginTransaction()
        {
            var connection = GetOpenConnection();
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (connection.State != ConnectionState.Open) connection.Open();

            if (!_transactionPool.TryGetValue(threadId, out var transaction))
            {
                transaction = connection.BeginTransaction();
                _transactionPool.TryAdd(threadId, transaction);
            }
            else
            {
                var savePointName = $"SavePoint_{Guid.NewGuid():N}";
                ExecuteTransactionCommand($"SAVE TRANSACTION {savePointName}");
                if (!_savePointPool.TryGetValue(threadId, out var savePoints))
                {
                    savePoints = new ConcurrentStack<string>();
                    _savePointPool.TryAdd(threadId, savePoints);
                }
                savePoints.Push(savePointName);
            }
        }

        public void CommitTransaction()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (!_transactionPool.TryGetValue(threadId, out var transaction))
                throw new InvalidOperationException("No transaction is in progress");

            if (_transactionPool.Count <= 0) throw new InvalidOperationException("No active transactions to commit");

            _transactionPool.TryRemove(threadId, out _);

            if (_savePointPool.TryGetValue(threadId, out var savePoints))
            {
                if (savePoints.TryPop(out var _))
                {
                    return;
                }
            }

            try { transaction.Commit(); }
            finally { CleanupTransaction(threadId); }
        }

        public void RollbackTransaction()
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (!_transactionPool.TryGetValue(threadId, out var transaction))
                throw new InvalidOperationException("No transaction is in progress");

            try
            {
                if (_savePointPool.TryGetValue(threadId, out var savePoints))
                {
                    if (savePoints.TryPop(out var savePointName))
                    {
                        ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    }
                    else
                    {
                        transaction.Rollback();
                    }
                }
            }
            finally
            {
                _transactionPool.TryRemove(threadId, out _);
                CleanupTransaction(threadId);
            }
        }

        private void ExecuteTransactionCommand(string commandText)
        {
            var threadId = Thread.CurrentThread.ManagedThreadId;

            if (!_transactionPool.TryGetValue(threadId, out var transaction) || transaction.Connection == null)
                throw new InvalidOperationException("No active transaction");

            using (var command = transaction.Connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void CleanupTransaction(int threadId)
        {
            try
            {
                if (_transactionPool.TryGetValue(threadId, out var transaction))
                {
                    transaction?.Dispose();
                }

                if (_savePointPool.TryGetValue(threadId, out var savePoints))
                {
                    savePoints?.Clear();
                }
            }
            catch { }
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
                try
                {
                    foreach (var transaction in _transactionPool.Values)
                    {
                        try { if (transaction != null) transaction.Rollback(); }
                        catch { }
                    }
                }
                catch { }

                try
                {
                    foreach (var connection in _connectionPool.Values)
                    {
                        try { if (connection != null && connection.State == ConnectionState.Open) { connection.Close(); connection.Dispose(); } }
                        catch { }
                    }
                }
                catch { }

                _connectionPool.Clear();
                _transactionPool.Clear();
                _savePointPool.Clear();
                _externalConnection?.Dispose();
            }
            _disposed = true;
        }
    }

    internal class QueryCache : IDisposable
    {
        private readonly LockFreeLruCacheDapperService<Type, string> InsertQueryCache = new LockFreeLruCacheDapperService<Type, string>(500);
        private readonly LockFreeLruCacheDapperService<Type, string> UpdateQueryCache = new LockFreeLruCacheDapperService<Type, string>(500);
        private readonly LockFreeLruCacheDapperService<Type, string> DeleteQueryCache = new LockFreeLruCacheDapperService<Type, string>(500);
        private readonly LockFreeLruCacheDapperService<Type, string> GetByIdQueryCache = new LockFreeLruCacheDapperService<Type, string>(500);
        private readonly LockFreeLruCacheDapperService<Type, List<PropertyInfo>> PrimaryKeyCache = new LockFreeLruCacheDapperService<Type, List<PropertyInfo>>(200);
        private readonly LockFreeLruCacheDapperService<Type, PropertyInfo> IdentityPropertyCache = new LockFreeLruCacheDapperService<Type, PropertyInfo>(200);
        private readonly LockFreeLruCacheDapperService<string, string> TableNameCache = new LockFreeLruCacheDapperService<string, string>(100);
        private readonly LockFreeLruCacheDapperService<string, string> ColumnNameCache = new LockFreeLruCacheDapperService<string, string>(500);
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
                var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>(true) != null);
                if (identityPk > 1) throw new InvalidOperationException("Multiple Identity primary keys are not supported");
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
            var primaryKey = GetPrimaryKeyProperties<T>().Single();
            var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
            return $"SELECT {columns} FROM {tableName} WHERE {GetColumnName(primaryKey)} = @Id";
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() => typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
        private bool IsPrimaryKey(PropertyInfo property) => property.GetCustomAttribute<PrimaryKeyAttribute>(true) != null;
        private bool IsIdentity(PropertyInfo property) => property.GetCustomAttribute<IdentityAttribute>(true) != null;

        public void Dispose()
        {
            InsertQueryCache?.Dispose();
            UpdateQueryCache?.Dispose();
            DeleteQueryCache?.Dispose();
            GetByIdQueryCache?.Dispose();
            PrimaryKeyCache?.Dispose();
            IdentityPropertyCache?.Dispose();
            TableNameCache?.Dispose();
            ColumnNameCache?.Dispose();
        }
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
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker._attachedEntities.TryGetValue(key, out var original)) return BaseUpdate(entity);
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
            if (!_entityTracker._attachedEntities.TryGetValue(key, out var original)) return await BaseUpdateAsync(entity);
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;
            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = await _connectionManager.GetOpenConnectionAsync();
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateListWithCompositeKeysAsync(entities, query, cancellationToken);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: entities, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetGetByIdQuery<T>();
            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateSingleWithCompositeKeysAsync(entity, query);
            return await connection.ExecuteAsync(query, entity, _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
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
            foreach (var entity in entities) totalAffected += await UpdateSingleWithCompositeKeysAsync(entity, query, cancellationToken);
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
            return await connection.ExecuteAsync(commandDefinition);
        }

        private void MergeDynamicParameters(DynamicParameters source, DynamicParameters destination)
        {
            if (source == null) return;
            foreach (var paramName in source.ParameterNames)
            {
                destination.Add(paramName, source.Get<object>(paramName));
            }
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
            if (identityProp != null && generateIdentities) return await InsertListWithIdentityAsync(entityList, cancellationToken);
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
                foreach (DataColumn column in dataTable.Columns)
                {
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
                foreach (DataColumn column in dataTable.Columns)
                {
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
            var identities = await InsertBulkCopyWithIdentityAsync(entities, cancellationToken);
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
            var tempTableName = $"##Temp_{Guid.NewGuid():N}";
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
            var tempTableName = $"##Temp_{Guid.NewGuid():N}";
            var identities = new List<object>();
            var connection = (SqlConnection)await _connectionManager.GetOpenConnectionAsync();
            try
            {
                await CreateTempTableWithOrderColumnAsync<T>(tempTableName, connection, cancellationToken);
                await BulkCopyToTempTableWithOrderAsync<T>(entities, tempTableName, connection, cancellationToken);
                identities = await InsertFromTempAndRetrieveIdentitiesWithOrderAsync<T>(tempTableName, connection, cancellationToken);
            }
            finally { await DropTempTableAsync(tempTableName, connection, cancellationToken); }
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
            await connection.ExecuteAsync(new CommandDefinition(createTempTableQuery, transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken));
        }

        private void BulkCopyToTempTableWithOrder<T>(IEnumerable<T> entities, string tempTableName, SqlConnection connection)
        {
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);

            dataTable.Columns.Add("TempOrder", typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                dataTable.Rows[i]["TempOrder"] = i;
            }

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
                    if (property != null)
                    {
                        var dbColumnName = _queryCache.GetColumnName(property);
                        bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                    }
                    else if (column.ColumnName == "TempOrder")
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, "TempOrder");
                    }
                }
                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task BulkCopyToTempTableWithOrderAsync<T>(IEnumerable<T> entities, string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            var properties = GetInsertProperties<T>();
            var dataTable = _sqlBuilder.ToDataTable(entities, properties);

            dataTable.Columns.Add("TempOrder", typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                dataTable.Rows[i]["TempOrder"] = i;
            }

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
                    if (property != null)
                    {
                        var dbColumnName = _queryCache.GetColumnName(property);
                        bulkCopy.ColumnMappings.Add(column.ColumnName, dbColumnName.Trim('[', ']'));
                    }
                    else if (column.ColumnName == "TempOrder")
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, "TempOrder");
                    }
                }
                await bulkCopy.WriteToServerAsync(reader, cancellationToken);
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
        
        INSERT INTO {tableName} ({columnList})
        OUTPUT INSERTED.{identityColumnName} INTO #OutputIds(Id)
        SELECT {columnList}
        FROM {tempTableName}
        ORDER BY TempOrder;
        
        ;WITH OrderedIds AS (
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as RowNum, Id
            FROM #OutputIds
        ),
        OrderedTemp AS (
            SELECT ROW_NUMBER() OVER (ORDER BY TempOrder) as RowNum, TempOrder
            FROM {tempTableName}
        )
        UPDATE #OutputIds
        SET TempOrder = ot.TempOrder
        FROM #OutputIds oi
        INNER JOIN OrderedIds oid ON oi.Id = oid.Id
        INNER JOIN OrderedTemp ot ON oid.RowNum = ot.RowNum;
        
        SELECT Id FROM #OutputIds ORDER BY TempOrder;
        
        DROP TABLE #OutputIds;";

            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        identities.Add(reader.GetValue(0));
                    }
                }
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
        
        INSERT INTO {tableName} ({columnList})
        OUTPUT INSERTED.{identityColumnName} INTO #OutputIds(Id)
        SELECT {columnList}
        FROM {tempTableName}
        ORDER BY TempOrder;
        
        ;WITH OrderedIds AS (
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as RowNum, Id
            FROM #OutputIds
        ),
        OrderedTemp AS (
            SELECT ROW_NUMBER() OVER (ORDER BY TempOrder) as RowNum, TempOrder
            FROM {tempTableName}
        )
        UPDATE #OutputIds
        SET TempOrder = ot.TempOrder
        FROM #OutputIds oi
        INNER JOIN OrderedIds oid ON oi.Id = oid.Id
        INNER JOIN OrderedTemp ot ON oid.RowNum = ot.RowNum;
        
        SELECT Id FROM #OutputIds ORDER BY TempOrder;
        
        DROP TABLE #OutputIds;";

            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        identities.Add(reader.GetValue(0));
                    }
                }
            }

            return identities;
        }

        private void DropTempTable(string tempTableName, SqlConnection connection)
        {
            try { _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, $"DROP TABLE {tempTableName}"); }
            catch { }
        }

        private async Task DropTempTableAsync(string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            try { await connection.ExecuteAsync(new CommandDefinition($"DROP TABLE {tempTableName}", transaction: _connectionManager.CurrentTransaction, cancellationToken: cancellationToken)); }
            catch { }
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() => typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
    }

    internal class StoredProcedureExecutor
    {
        private readonly ConnectionManager _connectionManager;
        private readonly SqlBuilder _sqlBuilder;
        private const string VALID_NAME_REGEX = @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$";

        public StoredProcedureExecutor(ConnectionManager connectionManager, SqlBuilder sqlBuilder)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            _connectionManager = connectionManager;
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
            var openConnection = await _connectionManager.GetOpenConnectionAsync();
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
            var openConnection = await _connectionManager.GetOpenConnectionAsync();
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.ExecuteScalarAsync<T>(commandDefinition);
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
            var connection = await _connectionManager.GetOpenConnectionAsync();
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters, transaction: _connectionManager.CurrentTransaction, commandTimeout: _connectionManager.CommandTimeout, cancellationToken: cancellationToken);
            return await connection.QueryAsync<T>(commandDefinition);
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