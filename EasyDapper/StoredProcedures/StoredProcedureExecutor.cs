using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace EasyDapper
{
    /// <summary>
    /// Executes stored procedures and table-valued/scalar functions on behalf of
    /// <see cref="DapperService"/>. All object names are validated against a strict allow-list
    /// regular expression before being interpolated into SQL to prevent injection through
    /// crafted procedure or function names.
    /// </summary>
    internal class StoredProcedureExecutor
    {
        private readonly ConnectionManager _connectionManager;
        private readonly SqlBuilder _sqlBuilder;

        // Compiled once and reused: matching a bare identifier or schema.identifier.
        // `[\w\d_]+` covers letters, digits and underscore (a superset of valid SQL identifiers).
        private static readonly Regex ValidNameRegex =
            new Regex(@"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$", RegexOptions.Compiled);

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
            var openConnection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
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
            var openConnection = _connectionManager.GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(procedureName, parameters,
                transaction ?? _connectionManager.CurrentTransaction,
                commandTimeout ?? _connectionManager.CommandTimeout,
                CommandType.StoredProcedure))
            {
                return mapper(multi);
            }
        }

        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper,
            object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            ValidateName(procedureName, "procedureName");
            var openConnection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            using (var multi = await openConnection.QueryMultipleAsync(new CommandDefinition(procedureName,
                parameters, transaction ?? _connectionManager.CurrentTransaction,
                commandTimeout ?? _connectionManager.CommandTimeout,
                CommandType.StoredProcedure, cancellationToken: cancellationToken)).ConfigureAwait(false))
            {
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }

        public T ExecuteScalarFunction<T>(string functionName, object parameters = null)
        {
            ValidateName(functionName, "functionName");
            var connection = _connectionManager.GetOpenConnection();
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout);
            return connection.ExecuteScalar<T>(commandDefinition);
        }

        public async Task<T> ExecuteScalarFunctionAsync<T>(string functionName, object parameters = null, CancellationToken cancellationToken = default)
        {
            ValidateName(functionName, "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _sqlBuilder.BuildScalarFunctionQuery(functionName, parameters);
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
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout);
            return connection.Query<T>(commandDefinition);
        }

        public async Task<IEnumerable<T>> ExecuteTableFunctionAsync<T>(string functionName, object parameters, CancellationToken cancellationToken = default)
        {
            ValidateName(functionName, "functionName");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _sqlBuilder.BuildTableFunctionQuery(functionName, parameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.QueryAsync<T>(commandDefinition).ConfigureAwait(false);
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
