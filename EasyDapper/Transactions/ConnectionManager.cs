using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
#if NET45
using System.Runtime.Remoting.Messaging;
#endif

namespace EasyDapper
{
    internal class ConnectionManager : IDisposable
    {
        private readonly string _connectionString;
        private readonly IDbConnection _externalConnection;
        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private readonly Stack<string> _savePointStack = new Stack<string>();
        private readonly int _timeOut;
        private const int DEFAULT_TIMEOUT = 30;
        private bool _disposed;
        private readonly object _lock = new object();
        private readonly SemaphoreSlim _connectionOpenGate = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _operationGate = new SemaphoreSlim(1, 1);
#if NET45
        private readonly string _operationContextKey = "EasyDapper.Operation." + Guid.NewGuid().ToString("N");
#else
        private readonly AsyncLocal<OperationLease> _operationContext = new AsyncLocal<OperationLease>();
#endif

        internal event Action TrackingInvalidated;
        internal event Action TransactionStarted;
        internal event Action TransactionCommitted;
        internal event Action TransactionRolledBack;

        internal bool HasCurrentOperation
        {
            get
            {
                var operation = GetOperationContext();
                return operation != null && !operation.IsReleased;
            }
        }

        public int TransactionCount
        {
            get
            {
                lock (_lock)
                {
                    return _transaction == null ? 0 : 1 + _savePointStack.Count;
                }
            }
        }

        public IDbTransaction CurrentTransaction
        {
            get { lock (_lock) { return _transaction; } }
        }

        public int CommandTimeout => _timeOut;

        public bool IsDisposed
        {
            get { lock (_lock) { return _disposed; } }
        }

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
            try
            {
                var builder = new SqlConnectionStringBuilder(_connectionString);
                return builder.ConnectTimeout > 0 ? builder.ConnectTimeout : DEFAULT_TIMEOUT;
            }
            catch { return DEFAULT_TIMEOUT; }
        }

        private int GetExternalConnectionTimeout()
        {
            try { return _externalConnection.ConnectionTimeout; }
            catch { return DEFAULT_TIMEOUT; }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ConnectionManager));
        }

        // Normal operations hold a lease until command execution and buffering finish.
        // Multi-result callbacks take responsibility for their reader's lifetime.
        public IDisposable AcquireOperation()
        {
            return AcquireOperation(false);
        }

        private IDisposable AcquireOperation(bool allowDisposed)
        {
            var lease = CreateOperationLease(allowDisposed);
            try
            {
                lease.Wait();
                lease.MarkEntered();
                if (!allowDisposed) lock (_lock) { ThrowIfDisposed(); }
                return lease;
            }
            catch
            {
                lease.Dispose();
                throw;
            }
        }

        // This entry point is synchronous so ownership belongs to its caller
        // and follows that caller across the await of the returned task.
        public Task<IDisposable> AcquireOperationAsync(CancellationToken cancellationToken = default)
        {
            var lease = CreateOperationLease(false);
            return WaitForOperationAsync(lease, cancellationToken);
        }

        internal IDisposable AcquirePreparedOperation() => AcquireOperation(true);

        internal Task<IDisposable> AcquirePreparedOperationAsync(CancellationToken cancellationToken)
        {
            var lease = CreateOperationLease(true);
            return WaitForOperationAsync(lease, cancellationToken, true);
        }

        internal void ReleaseOperationForCallback()
        {
            GetOperationContext()?.Dispose();
        }

        private async Task<IDisposable> WaitForOperationAsync(OperationLease lease, CancellationToken cancellationToken,
            bool allowDisposed = false)
        {
            try
            {
                await lease.WaitAsync(cancellationToken).ConfigureAwait(false);
                lease.MarkEntered();
                if (!allowDisposed) lock (_lock) { ThrowIfDisposed(); }
                return lease;
            }
            catch
            {
                lease.Dispose();
                throw;
            }
        }

        private OperationLease CreateOperationLease(bool allowDisposed)
        {
            if (!allowDisposed) lock (_lock) { ThrowIfDisposed(); }
            var parent = GetOperationContext();
            if (parent == null || !parent.TryReserveChild()) parent = null;
            var lease = new OperationLease(this, parent);
            SetOperationContext(lease);
            return lease;
        }

        private OperationLease GetOperationContext()
        {
#if NET45
            return CallContext.LogicalGetData(_operationContextKey) as OperationLease;
#else
            return _operationContext.Value;
#endif
        }

        private void SetOperationContext(OperationLease lease)
        {
#if NET45
            if (lease == null) CallContext.FreeNamedDataSlot(_operationContextKey);
            else CallContext.LogicalSetData(_operationContextKey, lease);
#else
            _operationContext.Value = lease;
#endif
        }

        private sealed class OperationLease : MarshalByRefObject, IDisposable
        {
            private readonly ConnectionManager _owner;
            private readonly OperationLease _parent;
            private readonly object _stateLock = new object();
            private readonly SemaphoreSlim _childGate = new SemaphoreSlim(1, 1);
            private int _references = 1;
            private int _entered;
            private int _released;

            public OperationLease(ConnectionManager owner, OperationLease parent)
            {
                _owner = owner;
                _parent = parent;
            }

            public bool IsReleased => Volatile.Read(ref _released) != 0;
            public bool HasParent => _parent != null;
            private SemaphoreSlim Gate => _parent == null ? _owner._operationGate : _parent._childGate;
            public void Wait() { Gate.Wait(); }
            public Task WaitAsync(CancellationToken cancellationToken) => Gate.WaitAsync(cancellationToken);
            public void MarkEntered() { Volatile.Write(ref _entered, 1); }

            public bool TryReserveChild()
            {
                lock (_stateLock)
                {
                    if (_released != 0 || Volatile.Read(ref _entered) == 0) return false;
                    Interlocked.Increment(ref _references);
                    return true;
                }
            }

            public void Dispose()
            {
                if (ReferenceEquals(_owner.GetOperationContext(), this))
                    _owner.SetOperationContext(_parent != null && !_parent.IsReleased ? _parent : null);
                lock (_stateLock)
                {
                    if (_released != 0) return;
                    Volatile.Write(ref _released, 1);
                }
                ReleaseReference();
            }

            private void ReleaseReference()
            {
                if (Interlocked.Decrement(ref _references) != 0) return;
                if (Volatile.Read(ref _entered) != 0) Gate.Release();
                _parent?.ReleaseReference();
            }
        }

        private IDbConnection GetConnection()
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                if (_externalConnection != null) return _externalConnection;
                return _connection ?? (_connection = new SqlConnection(_connectionString));
            }
        }

        public IDbConnection GetOpenConnection()
        {
            var existing = GetConnection();
            if (existing.State == ConnectionState.Open) return existing;
            _connectionOpenGate.Wait();
            try
            {
                var connection = GetConnection();
                PrepareConnectionForOpen(connection);
                if (connection.State != ConnectionState.Open) connection.Open();
                EnsureConnectionIsOpen(connection);
                return connection;
            }
            finally { _connectionOpenGate.Release(); }
        }

        public async Task<IDbConnection> GetOpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var existing = GetConnection();
            if (existing.State == ConnectionState.Open) return existing;
            await _connectionOpenGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var connection = GetConnection();
                PrepareConnectionForOpen(connection);
                if (connection.State != ConnectionState.Open)
                {
                    if (connection is DbConnection asyncConnection)
                        await asyncConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    else
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        connection.Open();
                    }
                }
                EnsureConnectionIsOpen(connection);
                return connection;
            }
            finally { _connectionOpenGate.Release(); }
        }

        private static void PrepareConnectionForOpen(IDbConnection connection)
        {
            if (connection.State == ConnectionState.Broken) connection.Close();
            if (connection.State == ConnectionState.Connecting)
                throw new InvalidOperationException("The connection is being opened outside this EasyDapper service.");
        }

        private static void EnsureConnectionIsOpen(IDbConnection connection)
        {
            if (connection.State != ConnectionState.Open)
                throw new InvalidOperationException("The connection did not reach the Open state.");
        }

        public void BeginTransaction()
        {
            using (AcquireOperation())
            {
                var connection = GetOpenConnection();
                lock (_lock)
                {
                    if (_transaction != null)
                    {
                        // A SQL Server savepoint identifier is limited to 32 characters.
                        var savePointName = "SP_" + Guid.NewGuid().ToString("N").Substring(0, 29);
                        ExecuteTransactionCommand($"SAVE TRANSACTION {savePointName}");
                        _savePointStack.Push(savePointName);
                    }
                    else { _transaction = connection.BeginTransaction(); }
                    TransactionStarted?.Invoke();
                }
            }
        }

        public void CommitTransaction()
        {
            using (AcquireOperation())
            lock (_lock)
            {
                if (_transaction == null) throw new InvalidOperationException("No transaction is in progress");
                if (_savePointStack.Count != 0)
                {
                    _savePointStack.Pop();
                    TransactionCommitted?.Invoke();
                    return;
                }
                var committed = false;
                try
                {
                    _transaction.Commit();
                    committed = true;
                }
                finally
                {
                    CleanupTransaction();
                    if (!committed) TrackingInvalidated?.Invoke();
                }
                TransactionCommitted?.Invoke();
            }
        }

        public void RollbackTransaction()
        {
            using (AcquireOperation())
            lock (_lock)
            {
                if (_transaction == null) throw new InvalidOperationException("No transaction is in progress");
                if (_savePointStack.Count != 0)
                {
                    var savePointName = _savePointStack.Peek();
                    ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    _savePointStack.Pop();
                    TransactionRolledBack?.Invoke();
                    return;
                }
                var rolledBack = false;
                try
                {
                    _transaction.Rollback();
                    rolledBack = true;
                }
                finally
                {
                    CleanupTransaction();
                    if (!rolledBack) TrackingInvalidated?.Invoke();
                }
                TransactionRolledBack?.Invoke();
            }
        }

        private void ExecuteTransactionCommand(string commandText)
        {
            if (_transaction == null || _transaction.Connection == null)
                throw new InvalidOperationException("No active transaction");
            using (var command = _transaction.Connection.CreateCommand())
            {
                command.Transaction = _transaction;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        private void CleanupTransaction()
        {
            try { _transaction?.Dispose(); }
            catch { }
            _transaction = null;
            _savePointStack.Clear();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            lock (_lock) { if (_disposed) return; }
            using (AcquireOperation(true))
            {
                var currentOperation = GetOperationContext();
                var openingCallback = _externalConnection != null ||
                    currentOperation != null && currentOperation.HasParent && _connection?.State == ConnectionState.Open;
                var enteredOpenGate = !openingCallback;
                if (enteredOpenGate) _connectionOpenGate.Wait();
                try
                {
                    lock (_lock)
                    {
                        if (_disposed) return;
                        _disposed = true;
                        if (!disposing) return;
                        if (_transaction != null)
                        {
                            try { _transaction.Rollback(); }
                            catch { }
                            CleanupTransaction();
                            TrackingInvalidated?.Invoke();
                        }
                        if (_connection != null)
                        {
                            try { _connection.Close(); }
                            catch { }
                            try { _connection.Dispose(); }
                            catch { }
                            _connection = null;
                        }
                    }
                }
                finally { if (enteredOpenGate) _connectionOpenGate.Release(); }
            }
        }
    }
}
