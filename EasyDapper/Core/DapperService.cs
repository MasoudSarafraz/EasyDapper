using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

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
        private volatile bool _disposed = false;

        public int TransactionCount()
        {
            if (_disposed) return 0;
            return _connectionManager.TransactionCount;
        }

        public DapperService(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException("connectionString");
            _connectionManager = new ConnectionManager(connectionString);
            _queryCache = new QueryCache();
            _sqlBuilder = new SqlBuilder(_queryCache);
            _entityTracker = new EntityTracker();
            _connectionManager.TrackingInvalidated += _entityTracker.Clear;
            _connectionManager.TransactionStarted += _entityTracker.BeginTransactionCheckpoint;
            _connectionManager.TransactionCommitted += _entityTracker.CommitTransactionCheckpoint;
            _connectionManager.TransactionRolledBack += _entityTracker.RollbackTransactionCheckpoint;
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
            _connectionManager.TrackingInvalidated += _entityTracker.Clear;
            _connectionManager.TransactionStarted += _entityTracker.BeginTransactionCheckpoint;
            _connectionManager.TransactionCommitted += _entityTracker.CommitTransactionCheckpoint;
            _connectionManager.TransactionRolledBack += _entityTracker.RollbackTransactionCheckpoint;
            _crudOperations = new CrudOperations(_connectionManager, _queryCache, _sqlBuilder, _entityTracker);
            _bulkOperations = new BulkOperations(_connectionManager, _queryCache, _sqlBuilder);
            _storedProcedureExecutor = new StoredProcedureExecutor(_connectionManager, _sqlBuilder);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DapperService));
        }

        private TResult ExecuteOperation<TResult>(Func<TResult> operation)
        {
            using (_connectionManager.AcquireOperation())
            {
                ThrowIfDisposed();
                return operation();
            }
        }

        private void ExecuteOperation(Action operation)
        {
            if (_connectionManager.HasCurrentOperation)
            {
                ThrowIfDisposed();
                operation();
                return;
            }
            using (_connectionManager.AcquireOperation())
            {
                ThrowIfDisposed();
                operation();
            }
        }

        private async Task<TResult> ExecuteOperationAsync<TResult>(Func<Task<TResult>> operation,
            CancellationToken cancellationToken = default)
        {
            using (await _connectionManager.AcquireOperationAsync(cancellationToken).ConfigureAwait(false))
            {
                ThrowIfDisposed();
                return await operation().ConfigureAwait(false);
            }
        }

        public void BeginTransaction()
        {
            ThrowIfDisposed();
            _connectionManager.BeginTransaction();
        }

        public void CommitTransaction()
        {
            ThrowIfDisposed();
            _connectionManager.CommitTransaction();
        }

        public void RollbackTransaction()
        {
            ThrowIfDisposed();
            _connectionManager.RollbackTransaction();
        }

        public int Insert<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.Insert(entity);
        }

        public Task<int> InsertAsync<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.InsertAsync(entity);
        }

        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class
        {
            ThrowIfDisposed();
            using (var prepared = _bulkOperations.PrepareInsert(entities, generateIdentities))
            {
                var affected = ExecuteOperation(() => _bulkOperations.ExecutePrepared(prepared));
                _bulkOperations.CompleteInsert(prepared);
                return affected;
            }
        }

        public Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return InsertPreparedListAsync(entities, generateIdentities, cancellationToken);
        }

        private async Task<int> InsertPreparedListAsync<T>(IEnumerable<T> entities, bool generateIdentities,
            CancellationToken cancellationToken) where T : class
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (var prepared = _bulkOperations.PrepareInsert(entities, generateIdentities))
            {
                var affected = await _bulkOperations.ExecutePreparedAsync(prepared, cancellationToken).ConfigureAwait(false);
                _bulkOperations.CompleteInsert(prepared);
                return affected;
            }
        }

        public int Update<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.Update(entity);
        }

        public Task<int> UpdateAsync<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.UpdateAsync(entity);
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.UpdateList(entities);
        }

        public Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.UpdateListAsync(entities, cancellationToken);
        }

        public int Delete<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.Delete(entity);
        }

        public Task<int> DeleteAsync<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.DeleteAsync(entity);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.DeleteList(entities);
        }

        public Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.DeleteListAsync(entities, cancellationToken);
        }

        public void Attach<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            _entityTracker.Attach(entity);
        }

        public void Detach<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            _entityTracker.Detach(entity);
        }

        public T GetById<T>(object id) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.GetById<T>(id);
        }

        public Task<T> GetByIdAsync<T>(object id) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.GetByIdAsync<T>(id);
        }

        public T GetById<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.GetById(entity);
        }

        public Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            ThrowIfDisposed();
            return _crudOperations.GetByIdAsync(entity);
        }

        public IQueryBuilder<T> Query<T>()
        {
            ThrowIfDisposed();
            return new QueryBuilder<T>(_connectionManager, _queryCache);
        }

        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class
        {
            ThrowIfDisposed();
            StoredProcedureExecutor.ValidateProcedureName(procedureName);
            return ExecutePreparedOperation((connection, transaction) =>
                _storedProcedureExecutor.PrepareStoredProcedure(connection, transaction, procedureName, parameters, resultType: typeof(T)),
                command => _storedProcedureExecutor.ExecutePreparedQuery<T>(command));
        }

        public Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return ExecutePreparedOperationAsync((connection, transaction) =>
                _storedProcedureExecutor.PrepareStoredProcedure(connection, transaction, procedureName, parameters, resultType: typeof(T)),
                command => _storedProcedureExecutor.ExecutePreparedQueryAsync<T>(command, cancellationToken), cancellationToken,
                () => StoredProcedureExecutor.ValidateProcedureName(procedureName));
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            ThrowIfDisposed();
            StoredProcedureExecutor.ValidateProcedureName(procedureName);
            if (mapper == null) throw new ArgumentNullException(nameof(mapper));
            return ExecutePreparedOperation((connection, currentTransaction) =>
                _storedProcedureExecutor.PrepareStoredProcedure(connection, currentTransaction, procedureName, parameters,
                    true, transaction, commandTimeout),
                command => _storedProcedureExecutor.ExecutePreparedMultiResult(command, mapper));
        }

        public Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return ExecutePreparedOperationAsync((connection, currentTransaction) =>
                _storedProcedureExecutor.PrepareStoredProcedure(connection, currentTransaction, procedureName, parameters,
                    true, transaction, commandTimeout),
                command => _storedProcedureExecutor.ExecutePreparedMultiResultAsync(command, asyncMapper, cancellationToken), cancellationToken,
                () =>
                {
                    StoredProcedureExecutor.ValidateProcedureName(procedureName);
                    if (asyncMapper == null) throw new ArgumentNullException(nameof(asyncMapper));
                });
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            ThrowIfDisposed();
            StoredProcedureExecutor.ValidateFunctionName(functionName);
            return ExecutePreparedOperation((connection, transaction) =>
                _storedProcedureExecutor.PrepareFunction(connection, transaction, functionName, parameters, false),
                command => _storedProcedureExecutor.ExecutePreparedScalar<T>(command));
        }

        public Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return ExecutePreparedOperationAsync((connection, transaction) =>
                _storedProcedureExecutor.PrepareFunction(connection, transaction, functionName, parameters, false),
                command => _storedProcedureExecutor.ExecutePreparedScalarAsync<T>(command, cancellationToken), cancellationToken,
                () => StoredProcedureExecutor.ValidateFunctionName(functionName));
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            ThrowIfDisposed();
            StoredProcedureExecutor.ValidateFunctionName(functionName);
            return ExecutePreparedOperation((connection, transaction) =>
                _storedProcedureExecutor.PrepareFunction(connection, transaction, functionName, parameters, true, typeof(T)),
                command => _storedProcedureExecutor.ExecutePreparedQuery<T>(command));
        }

        public Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return ExecutePreparedOperationAsync((connection, transaction) =>
                _storedProcedureExecutor.PrepareFunction(connection, transaction, functionName, parameters, true, typeof(T)),
                command => _storedProcedureExecutor.ExecutePreparedQueryAsync<T>(command, cancellationToken), cancellationToken,
                () => StoredProcedureExecutor.ValidateFunctionName(functionName));
        }

        private TResult ExecutePreparedOperation<TResult>(
            Func<IDbConnection, IDbTransaction, StoredProcedureExecutor.PreparedCommand> prepare,
            Func<StoredProcedureExecutor.PreparedCommand, TResult> execute)
        {
            IDbConnection connection;
            IDbTransaction transaction;
            using (_connectionManager.AcquireOperation())
            {
                ThrowIfDisposed();
                connection = _connectionManager.GetOpenConnection();
                transaction = _connectionManager.CurrentTransaction;
            }
            // Parameter getters and custom binders are consumer code. They may wait
            // for another service call even when ExecutionContext does not flow.
            using (var command = prepare(connection, transaction))
            {
                try
                {
                    using (_connectionManager.AcquirePreparedOperation()) return execute(command);
                }
                finally { command.CompleteCallbacks(); }
            }
        }

        private async Task<TResult> ExecutePreparedOperationAsync<TResult>(
            Func<IDbConnection, IDbTransaction, StoredProcedureExecutor.PreparedCommand> prepare,
            Func<StoredProcedureExecutor.PreparedCommand, Task<TResult>> execute, CancellationToken cancellationToken,
            Action validate)
        {
            validate();
            var connection = await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            IDbTransaction transaction;
            using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
            {
                transaction = _connectionManager.CurrentTransaction;
            }
            using (var command = prepare(connection, transaction))
            {
                try
                {
                    using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
                        return await execute(command).ConfigureAwait(false);
                }
                finally { command.CompleteCallbacks(); }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (!disposing) return;

            _connectionManager.Dispose();
            _disposed = true;
            SafeDispose(_entityTracker);
            SafeDispose(_queryCache);
        }

        private static void SafeDispose(IDisposable disposable)
        {
            if (disposable == null) return;
            try { disposable.Dispose(); }
            catch
            {
            }
        }
    }
}
