using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Interfaces;

namespace ProfessionalDapperLibrary.Implementations
{
    internal class StoredProcedureExecutor<T> : IStoredProcedureExecutor<T>
    {
        private readonly Lazy<IDbConnection> _lazyConnection;

        public StoredProcedureExecutor(string connectionString)
        {
            _lazyConnection = new Lazy<IDbConnection>(() => new SqlConnection(connectionString));
        }

        private IDbConnection GetOpenConnection()
        {
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }

        public IEnumerable<T> Execute(string procedureName, object parameters = null)
        {
            using (var connection = GetOpenConnection())
            {
                return connection.Query<T>(
                    procedureName,
                    param: parameters,
                    commandType: CommandType.StoredProcedure
                );
            }
        }

        public async Task<IEnumerable<T>> ExecuteAsync(string procedureName, object parameters = null)
        {
            using (var connection = GetOpenConnection())
            {
                return await connection.QueryAsync<T>(
                    procedureName,
                    param: parameters,
                    commandType: CommandType.StoredProcedure
                );
            }
        }

        public void Dispose()
        {
            if (_lazyConnection.IsValueCreated)
            {
                _lazyConnection.Value.Dispose();
            }
        }
    }
}