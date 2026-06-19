using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper
{
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
        private readonly object _connectionOpenLock = new object();

        public int TransactionCount
        {
            get
            {
                lock (_lock)
                {
                    if (_transaction == null) return 0;
                    return 1 + _savePointStack.Count;
                }
            }
        }

        public IDbTransaction CurrentTransaction
        {
            get
            {
                lock (_lock) { return _transaction; }
            }
        }

        public int CommandTimeout => _timeOut;

        public bool IsDisposed
        {
            get
            {
                lock (_lock) { return _disposed; }
            }
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
            catch
            {
                return DEFAULT_TIMEOUT;
            }
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

        public IDbConnection GetOpenConnection()
        {
            lock (_lock)
            {
                ThrowIfDisposed();
                if (_externalConnection != null)
                {
                    EnsureExternalConnectionOpen(_externalConnection);
                    return _externalConnection;
                }
                if (_connection == null) _connection = new SqlConnection(_connectionString);
                EnsureConnectionOpen();
                return _connection;
            }
        }

        public async Task<IDbConnection> GetOpenConnectionAsync()
        {
            IDbConnection conn;
            lock (_lock)
            {
                ThrowIfDisposed();
                if (_externalConnection != null)
                {
                    conn = _externalConnection;
                }
                else
                {
                    if (_connection == null) _connection = new SqlConnection(_connectionString);
                    conn = _connection;
                }
            }

            if (conn.State != ConnectionState.Open && conn.State != ConnectionState.Connecting)
            {
                var externalAsync = conn as DbConnection;
                if (externalAsync != null)
                {
                    await externalAsync.OpenAsync().ConfigureAwait(false);
                }
                else
                {
                    lock (_connectionOpenLock)
                    {
                        if (conn.State != ConnectionState.Open) conn.Open();
                    }
                }
            }
            return conn;
        }

        private void EnsureExternalConnectionOpen(IDbConnection connection)
        {
            if (connection.State == ConnectionState.Broken)
            {
                connection.Close();
                connection.Open();
            }
            else if (connection.State != ConnectionState.Open && connection.State != ConnectionState.Connecting)
            {
                connection.Open();
            }
        }

        private void EnsureConnectionOpen()
        {
            if (_connection.State == ConnectionState.Broken)
            {
                _connection.Close();
                _connection.Open();
            }
            else if (_connection.State != ConnectionState.Open && _connection.State != ConnectionState.Connecting)
            {
                _connection.Open();
            }
        }

        public void BeginTransaction()
        {
            lock (_lock)
            {
                ThrowIfDisposed();
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
                ThrowIfDisposed();
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
                ThrowIfDisposed();
                if (_transaction == null) throw new InvalidOperationException("No transaction is in progress");
                if (_savePointStack.TryPop(out var savePointName))
                {
                    ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    return;
                }
                try { _transaction.Rollback(); }
                finally { CleanupTransaction(); }
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
            try
            {
                _transaction?.Dispose();
            }
            catch
            {
            }
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
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;

                if (!disposing) return;

                if (_transaction != null)
                {
                    try { _transaction.Rollback(); }
                    catch
                    {
                    }
                    try { _transaction.Dispose(); }
                    catch
                    {
                    }
                    _transaction = null;
                    _savePointStack.Clear();
                }

                if (_connection != null && _connection != _externalConnection)
                {
                    try
                    {
                        if (_connection.State == ConnectionState.Open) _connection.Close();
                    }
                    catch
                    {
                    }
                    try { _connection.Dispose(); }
                    catch
                    {
                    }
                    _connection = null;
                }
            }
        }
    }
}
