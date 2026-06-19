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
        private bool _disposed = false;

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

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(DapperService));
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
            return _bulkOperations.InsertList(entities, generateIdentities);
        }

        public Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return _bulkOperations.InsertListAsync(entities, generateIdentities, cancellationToken);
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
            return _storedProcedureExecutor.ExecuteStoredProcedure<T>(procedureName, parameters);
        }

        public Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteStoredProcedureAsync<T>(procedureName, parameters, cancellationToken);
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteMultiResultStoredProcedure(procedureName, mapper, parameters, transaction, commandTimeout);
        }

        public Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteMultiResultStoredProcedureAsync(procedureName, asyncMapper, parameters, transaction, commandTimeout, cancellationToken);
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteScalarFunction<T>(functionName, parameters);
        }

        public Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteScalarFunctionAsync<T>(functionName, parameters, cancellationToken);
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteTableFunction<T>(functionName, parameters);
        }

        public Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return _storedProcedureExecutor.ExecuteTableFunctionAsync<T>(functionName, parameters, cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;
            if (!disposing) return;

            SafeDispose(_connectionManager);
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
