using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace EasyDapper
{
    internal class StoredProcedureExecutor
    {
        private readonly ConnectionManager _connectionManager;
        private readonly SqlBuilder _sqlBuilder;

        private static readonly Regex ValidNameRegex =
            new Regex(@"\A[\w\d_]+(?:\.[\w\d_]+)?\z", RegexOptions.Compiled);
        private static readonly FieldInfo GridReaderNativeReader = typeof(SqlMapper.GridReader).GetField(
            "reader", BindingFlags.Instance | BindingFlags.NonPublic);
        private static readonly bool AsyncQueryReturnsEmptyWithoutColumns = typeof(SqlMapper).Assembly.GetName().Version.Major >= 2;

        public StoredProcedureExecutor(ConnectionManager connection, SqlBuilder sqlBuilder)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            _connectionManager = connection;
            _sqlBuilder = sqlBuilder;
        }

        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class
        {
            ValidateName(procedureName, "procedureName");
            var openConnection = _connectionManager.GetOpenConnection();
            return openConnection.Query<T>(procedureName, param: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                commandType: CommandType.StoredProcedure);
        }

        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class
        {
            ValidateName(procedureName, "procedureName");
            var openConnection = await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return await openConnection.QueryAsync<T>(new CommandDefinition(procedureName, parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper,
            object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            ValidateName(procedureName, "procedureName");
            if (mapper == null) throw new ArgumentNullException(nameof(mapper));
            var openConnection = _connectionManager.GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(procedureName, parameters,
                transaction ?? _connectionManager.CurrentTransaction,
                commandTimeout ?? _connectionManager.CommandTimeout,
                CommandType.StoredProcedure))
            {
                _connectionManager.ReleaseOperationForCallback();
                return mapper(multi);
            }
        }

        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper,
            object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            ValidateName(procedureName, "procedureName");
            if (asyncMapper == null) throw new ArgumentNullException(nameof(asyncMapper));
            var openConnection = await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            using (var multi = await openConnection.QueryMultipleAsync(new CommandDefinition(procedureName,
                parameters, transaction ?? _connectionManager.CurrentTransaction,
                commandTimeout ?? _connectionManager.CommandTimeout,
                CommandType.StoredProcedure, cancellationToken: cancellationToken)).ConfigureAwait(false))
            {
                _connectionManager.ReleaseOperationForCallback();
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            ValidateName(functionName, "functionName");
            var connection = _connectionManager.GetOpenConnection();
            parameters = _sqlBuilder.NormalizeFunctionParameters(parameters, connection, out var orderedNames);
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters, orderedNames);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout);
            return connection.ExecuteScalar<T>(commandDefinition);
        }

        public async Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            ValidateName(functionName, "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            parameters = _sqlBuilder.NormalizeFunctionParameters(parameters, connection, out var orderedNames);
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters, orderedNames);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.ExecuteScalarAsync<T>(commandDefinition).ConfigureAwait(false);
        }

        public IEnumerable<T> ExecuteTableFunction<T>(string functionName, object parameters)
        {
            ValidateName(functionName, "functionName");
            var connection = _connectionManager.GetOpenConnection();
            parameters = _sqlBuilder.NormalizeFunctionParameters(parameters, connection, out var orderedNames);
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters, orderedNames);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout);
            return connection.Query<T>(commandDefinition);
        }

        public async Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            ValidateName(functionName, "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            parameters = _sqlBuilder.NormalizeFunctionParameters(parameters, connection, out var orderedNames);
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters, orderedNames);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.QueryAsync<T>(commandDefinition).ConfigureAwait(false);
        }

        internal static void ValidateProcedureName(string name) => ValidateName(name, "procedureName");
        internal static void ValidateFunctionName(string name) => ValidateName(name, "functionName");

        internal PreparedCommand PrepareStoredProcedure(IDbConnection connection, IDbTransaction expectedTransaction,
            string procedureName, object parameters, bool mapperCallbacks = false,
            IDbTransaction transactionOverride = null, int? commandTimeout = null, Type resultType = null)
        {
            return PrepareCommand(connection, expectedTransaction, procedureName, parameters, CommandType.StoredProcedure,
                false, null, mapperCallbacks, transactionOverride, commandTimeout,
                mapperCallbacks ? typeof(SqlMapper.GridReader) : resultType);
        }

        internal PreparedCommand PrepareFunction(IDbConnection connection, IDbTransaction expectedTransaction,
            string functionName, object parameters, bool tableFunction, Type resultType = null)
        {
            parameters = _sqlBuilder.NormalizeFunctionParameters(parameters);
            var query = tableFunction ? _sqlBuilder.BuildTableFunctionQuery(functionName, parameters)
                : _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            return PrepareCommand(connection, expectedTransaction, query, parameters, null,
                tableFunction, functionName, false, null, null, tableFunction ? resultType : null);
        }

        internal PreparedCommand PrepareText(IDbConnection connection, IDbTransaction expectedTransaction,
            string commandText, object parameters, Type resultType = null, int? commandTimeout = null)
            => PrepareCommand(connection, expectedTransaction, commandText, parameters, null,
                false, null, false, null, commandTimeout, resultType);

        private PreparedCommand PrepareCommand(IDbConnection connection, IDbTransaction expectedTransaction,
            string commandText, object parameters, CommandType? commandType, bool tableFunction, string functionName,
            bool mapperCallbacks, IDbTransaction transactionOverride, int? commandTimeout, Type resultIdentityType)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = commandText;
                command.CommandType = commandType ?? CommandType.Text;
                command.CommandTimeout = commandTimeout ?? _connectionManager.CommandTimeout;
                command.Transaction = transactionOverride ?? expectedTransaction;
                // Function arguments include every dynamic-template member. Stored-procedure
                // binding discovers those members without filtering against a provisional query.
                var bindingType = functionName != null && parameters is DynamicParameters
                    ? CommandType.StoredProcedure : commandType;
                var bound = _sqlBuilder.BindPreparedParameters(connection, command, parameters, bindingType,
                    _connectionManager, mapperCallbacks, resultIdentityType);
                if (functionName != null && parameters is DynamicParameters)
                {
                    command.CommandText = tableFunction
                        ? _sqlBuilder.BuildTableFunctionQuery(functionName, parameters, bound.ParameterNames)
                        : _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters, bound.ParameterNames);
                }
                bound.CaptureCommandState();
                return new PreparedCommand(connection, command, bound, expectedTransaction, transactionOverride, commandType);
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        internal IEnumerable<T> ExecutePreparedQuery<T>(PreparedCommand command)
        {
            if (!SupportsConcurrentReaders(command)) return command.Connection.Query<T>(CreateDefinition(command));
            using (var reader = command.Connection.QueryMultiple(CreateDefinition(command)))
            {
                _connectionManager.ReleaseOperationForCallback();
                try
                {
                    if (HasNoColumns(reader))
                    {
                        command.Parameters.AbortCallbacks();
                        return new T[0];
                    }
                    var result = reader.Read<T>(buffered: true);
                    FinishBufferedResults(reader, command);
                    return result;
                }
                catch { command.Parameters.AbortCallbacks(); throw; }
            }
        }

        internal async Task<IEnumerable<T>> ExecutePreparedQueryAsync<T>(PreparedCommand command, CancellationToken cancellationToken)
        {
            if (!SupportsConcurrentReaders(command))
                return await command.Connection.QueryAsync<T>(CreateDefinition(command, cancellationToken)).ConfigureAwait(false);
            using (var reader = await command.Connection.QueryMultipleAsync(CreateDefinition(command, cancellationToken)).ConfigureAwait(false))
            {
                _connectionManager.ReleaseOperationForCallback();
                try
                {
                    if (AsyncQueryReturnsEmptyWithoutColumns && HasNoColumns(reader))
                    {
                        command.Parameters.AbortCallbacks();
                        return new T[0];
                    }
                    var result = await reader.ReadAsync<T>(buffered: true).ConfigureAwait(false);
                    await FinishBufferedResultsAsync(reader, command).ConfigureAwait(false);
                    return result;
                }
                catch { command.Parameters.AbortCallbacks(); throw; }
            }
        }

        internal T ExecutePreparedScalar<T>(PreparedCommand command)
            => command.Connection.ExecuteScalar<T>(CreateDefinition(command));

        internal Task<T> ExecutePreparedScalarAsync<T>(PreparedCommand command, CancellationToken cancellationToken)
            => command.Connection.ExecuteScalarAsync<T>(CreateDefinition(command, cancellationToken));

        internal int ExecutePreparedNonQuery(PreparedCommand command)
            => command.Connection.Execute(CreateDefinition(command));

        internal Task<int> ExecutePreparedNonQueryAsync(PreparedCommand command, CancellationToken cancellationToken)
            => command.Connection.ExecuteAsync(CreateDefinition(command, cancellationToken));

        internal T ExecutePreparedFirstOrDefault<T>(PreparedCommand command)
        {
            if (!SupportsConcurrentReaders(command)) return command.Connection.QueryFirstOrDefault<T>(CreateDefinition(command));
            using (var reader = command.Connection.QueryMultiple(CreateDefinition(command)))
            {
                _connectionManager.ReleaseOperationForCallback();
                try
                {
                    var result = reader.ReadFirstOrDefault<T>();
                    FinishBufferedResults(reader, command);
                    return result;
                }
                catch { command.Parameters.AbortCallbacks(); throw; }
            }
        }

        internal async Task<T> ExecutePreparedFirstOrDefaultAsync<T>(PreparedCommand command, CancellationToken cancellationToken)
        {
            if (!SupportsConcurrentReaders(command))
            {
                var result = await command.Connection.QueryFirstOrDefaultAsync(typeof(T), CreateDefinition(command, cancellationToken)).ConfigureAwait(false);
                return result == null ? default(T) : (T)result;
            }
            using (var reader = await command.Connection.QueryMultipleAsync(CreateDefinition(command, cancellationToken)).ConfigureAwait(false))
            {
                _connectionManager.ReleaseOperationForCallback();
                try
                {
                    var result = await reader.ReadFirstOrDefaultAsync<T>().ConfigureAwait(false);
                    await FinishBufferedResultsAsync(reader, command).ConfigureAwait(false);
                    return result;
                }
                catch { command.Parameters.AbortCallbacks(); throw; }
            }
        }

        private static bool SupportsConcurrentReaders(PreparedCommand command)
        {
            try { return new SqlConnectionStringBuilder(command.Connection.ConnectionString).MultipleActiveResultSets; }
            catch (ArgumentException) { return false; }
        }

        private static bool HasNoColumns(SqlMapper.GridReader reader)
        {
            if (GridReaderNativeReader == null || !typeof(IDataReader).IsAssignableFrom(GridReaderNativeReader.FieldType))
                throw new NotSupportedException("The installed Dapper version does not expose the expected GridReader reader field.");
            return ((IDataReader)GridReaderNativeReader.GetValue(reader)).FieldCount == 0;
        }

        private static void FinishBufferedResults(SqlMapper.GridReader reader, PreparedCommand command)
        {
            while (!reader.IsConsumed) reader.ReadFirstOrDefault<object>();
            command.Parameters.OnCompleted();
        }

        private static async Task FinishBufferedResultsAsync(SqlMapper.GridReader reader, PreparedCommand command)
        {
            while (!reader.IsConsumed) await reader.ReadFirstOrDefaultAsync<object>().ConfigureAwait(false);
            command.Parameters.OnCompleted();
        }

        internal T ExecutePreparedMultiResult<T>(PreparedCommand command, Func<SqlMapper.GridReader, T> mapper)
        {
            using (var reader = command.Connection.QueryMultiple(CreateDefinition(command)))
            {
                _connectionManager.ReleaseOperationForCallback();
                return mapper(reader);
            }
        }

        internal async Task<T> ExecutePreparedMultiResultAsync<T>(PreparedCommand command,
            Func<SqlMapper.GridReader, Task<T>> mapper, CancellationToken cancellationToken)
        {
            using (var reader = await command.Connection.QueryMultipleAsync(CreateDefinition(command, cancellationToken)).ConfigureAwait(false))
            {
                _connectionManager.ReleaseOperationForCallback();
                return await mapper(reader).ConfigureAwait(false);
            }
        }

        private CommandDefinition CreateDefinition(PreparedCommand prepared, CancellationToken cancellationToken = default)
        {
            var transaction = _connectionManager.CurrentTransaction;
            if (prepared.TransactionOverride == null && !ReferenceEquals(transaction, prepared.ExpectedTransaction))
                throw new InvalidOperationException("The transaction changed while command parameters were being prepared.");
            var command = prepared.Command;
            return new CommandDefinition(command.CommandText, prepared.Parameters,
                prepared.TransactionOverride ?? transaction, command.CommandTimeout, prepared.CommandTypeHint,
                cancellationToken: cancellationToken);
        }

        internal sealed class PreparedCommand : IDisposable
        {
            private readonly PreparedConnection _connection;
            internal IDbCommand Command { get; }
            internal SqlBuilder.BoundParameters Parameters { get; }
            internal IDbTransaction ExpectedTransaction { get; }
            internal IDbTransaction TransactionOverride { get; }
            internal CommandType? CommandTypeHint { get; }
            internal IDbConnection Connection => _connection;

            internal PreparedCommand(IDbConnection source, IDbCommand command, SqlBuilder.BoundParameters parameters,
                IDbTransaction expectedTransaction, IDbTransaction transactionOverride, CommandType? commandTypeHint)
            {
                _connection = new PreparedConnection(source, command);
                Command = command;
                Parameters = parameters;
                ExpectedTransaction = expectedTransaction;
                TransactionOverride = transactionOverride;
                CommandTypeHint = commandTypeHint;
            }

            internal void CompleteCallbacks() => Parameters.CompleteCallbacks();
            public void Dispose() { if (!_connection.CommandTaken) Command.Dispose(); }
        }

        private sealed class PreparedConnection : DbConnection, IDbConnection
        {
            private readonly IDbConnection _source;
            private readonly IDbCommand _command;
            private int _taken;
            internal PreparedConnection(IDbConnection source, IDbCommand command) { _source = source; _command = command; }
            internal bool CommandTaken => Volatile.Read(ref _taken) != 0;
            public override string ConnectionString { get => _source.ConnectionString; set => _source.ConnectionString = value; }
            public override int ConnectionTimeout => _source.ConnectionTimeout;
            public override string Database => _source.Database;
            public override string DataSource => (_source as DbConnection)?.DataSource ?? "";
            public override string ServerVersion => (_source as DbConnection)?.ServerVersion ?? "";
            public override ConnectionState State => _source.State;
            public override void ChangeDatabase(string name) => _source.ChangeDatabase(name);
            public override void Open() => _source.Open();
            public override Task OpenAsync(CancellationToken cancellationToken)
            {
                if (_source is DbConnection connection) return connection.OpenAsync(cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                _source.Open();
                return Task.FromResult(0);
            }
            public override void Close() => _source.Close();
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
                => (DbTransaction)_source.BeginTransaction(isolationLevel);
            protected override DbCommand CreateDbCommand() => (DbCommand)TakeCommand();
            IDbCommand IDbConnection.CreateCommand() => TakeCommand();
            private IDbCommand TakeCommand()
            {
                if (Interlocked.Exchange(ref _taken, 1) != 0)
                    throw new InvalidOperationException("A prepared command can only execute once.");
                return _command;
            }
            protected override void Dispose(bool disposing) { }
        }

        private static void ValidateName(string name, string paramName)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Name cannot be null or empty", paramName);
            if (!ValidNameRegex.IsMatch(name))
                throw new ArgumentException("Name is not valid", paramName);
        }
    }
}
