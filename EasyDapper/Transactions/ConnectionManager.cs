using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper
{
    /// <summary>
    /// Manages the lifetime of a SQL Server connection and the active transaction.
    /// Supports nested transactions via SQL Server SAVE TRANSACTION savepoints.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The manager owns a single physical <see cref="IDbConnection"/> that is created lazily on
    /// first use. When a transaction is started, all subsequent operations on the connection
    /// participate in that transaction until <see cref="CommitTransaction"/> or
    /// <see cref="RollbackTransaction"/> is called.
    /// </para>
    /// <para>
    /// Nested calls to <see cref="BeginTransaction"/> do not start a new SQL Server transaction
    /// (which is not supported on a single connection). Instead they create a named savepoint
    /// using <c>SAVE TRANSACTION</c>. <see cref="CommitTransaction"/> simply pops the savepoint
    /// (no SQL is executed), while <see cref="RollbackTransaction"/> issues
    /// <c>ROLLBACK TRANSACTION &lt;savepoint-name&gt;</c> to undo only the work done since the
    /// savepoint was created.
    /// </para>
    /// </remarks>
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

        /// <summary>
        /// Returns the current depth of nested transactions (0 when no transaction is active,
        /// 1 for the outermost transaction, 2 for one level of savepoint, etc.).
        /// </summary>
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

        /// <summary>
        /// Reads the configured Connect Timeout from the connection string WITHOUT opening
        /// a physical connection. Falls back to <see cref="DEFAULT_TIMEOUT"/> if the value
        /// cannot be determined.
        /// </summary>
        private int GetConnectionTimeout()
        {
            try
            {
                // Use SqlConnectionStringBuilder to read Connect Timeout without
                // opening a real connection (which is expensive and can block).
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
            // External connection: no lock needed (we never close external connections).
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
                // If we have savepoints, popping one means we commit only the work since that savepoint.
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
                    if (_savePointStack.TryPop(out var savePointName))
                        ExecuteTransactionCommand($"ROLLBACK TRANSACTION {savePointName}");
                    else
                        _transaction.Rollback();
                }
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
            lock (_lock)
            {
                if (_disposed) return;
                if (disposing)
                {
                    try { if (_transaction != null) _transaction.Rollback(); } catch { }
                    try
                    {
                        if (_connection != null && _connection != _externalConnection)
                        {
                            if (_connection.State == ConnectionState.Open) _connection.Close();
                            _connection.Dispose();
                        }
                    }
                    catch { }
                    // Never dispose external connection.
                    _transaction = null;
                    _connection = null;
                }
                _disposed = true;
            }
        }
    }
}
