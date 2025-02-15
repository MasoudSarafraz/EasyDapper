using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Interfaces;
using EasyDapper.Attributes;
using EasyDapper.Factories;

namespace EasyDapper.Implementations
{
    internal class DapperService : IDapperService
    {
        private readonly IDbConnection _externalConnection;
        private readonly Lazy<IDbConnection> _lazyConnection;
        private IDbTransaction _transaction;

        public DapperService(string connectionString)
        {
            _lazyConnection = new Lazy<IDbConnection>(() => new SqlConnection(connectionString));
        }

        public DapperService(IDbConnection externalConnection)
        {
            _externalConnection = externalConnection;
        }

        private IDbConnection GetOpenConnection()
        {
            if (_externalConnection != null)
            {
                return _externalConnection;
            }

            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }

        public void BeginTransaction()
        {
            if (_transaction != null)
            {
                throw new InvalidOperationException("A transaction is already in progress.");
            }

            var connection = GetOpenConnection();
            _transaction = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if (_transaction == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }

            try
            {
                _transaction.Commit();
            }
            catch
            {
                _transaction.Rollback();
                throw;
            }
            finally
            {
                _transaction.Dispose();
                _transaction = null;
            }
        }

        public void RollbackTransaction()
        {
            if (_transaction == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }

            _transaction.Rollback();
            _transaction.Dispose();
            _transaction = null;
        }

        public int Insert<T>(T entity)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var properties = typeof(T).GetProperties();
            var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
            var values = string.Join(", ", properties.Select(p => "@" + p.Name));
            var query = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";

            return connection.Execute(query, entity, _transaction);
        }

        public async Task<int> InsertAsync<T>(T entity)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var properties = typeof(T).GetProperties();
            var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
            var values = string.Join(", ", properties.Select(p => "@" + p.Name));
            var query = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";

            return await connection.ExecuteAsync(query, entity, _transaction);
        }

        public int Update<T>(T entity)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var properties = typeof(T).GetProperties();
            var setClause = string.Join(", ", properties
                .Where(p => !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                .Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            var primaryKey = properties.FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))?.Name;

            if (primaryKey == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var query = $"UPDATE {tableName} SET {setClause} WHERE {GetColumnName(properties.First(p => p.Name == primaryKey))} = @{primaryKey}";

            return connection.Execute(query, entity, _transaction);
        }

        public async Task<int> UpdateAsync<T>(T entity)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var properties = typeof(T).GetProperties();
            var setClause = string.Join(", ", properties
                .Where(p => !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                .Select(p => $"{GetColumnName(p)} = @{p.Name}"));
            var primaryKey = properties.FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))?.Name;

            if (primaryKey == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var query = $"UPDATE {tableName} SET {setClause} WHERE {GetColumnName(properties.First(p => p.Name == primaryKey))} = @{primaryKey}";

            return await connection.ExecuteAsync(query, entity, _transaction);
        }

        public int Delete<T>(object id)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var primaryKeyProperty = typeof(T).GetProperties().FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));

            if (primaryKeyProperty == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var primaryKey = GetColumnName(primaryKeyProperty);
            var query = $"DELETE FROM {tableName} WHERE {primaryKey} = @Id";

            return connection.Execute(query, new { Id = id }, _transaction);
        }

        public async Task<int> DeleteAsync<T>(object id)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var primaryKeyProperty = typeof(T).GetProperties().FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));

            if (primaryKeyProperty == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var primaryKey = GetColumnName(primaryKeyProperty);
            var query = $"DELETE FROM {tableName} WHERE {primaryKey} = @Id";

            return await connection.ExecuteAsync(query, new { Id = id }, _transaction);
        }

        public T GetById<T>(object id)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var primaryKeyProperty = typeof(T).GetProperties().FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));

            if (primaryKeyProperty == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var primaryKey = GetColumnName(primaryKeyProperty);
            var query = $"SELECT * FROM {tableName} WHERE {primaryKey} = @Id";

            return connection.QueryFirstOrDefault<T>(query, new { Id = id }, _transaction);
        }

        public async Task<T> GetByIdAsync<T>(object id)
        {
            var connection = GetOpenConnection();
            var tableName = GetTableName<T>();
            var primaryKeyProperty = typeof(T).GetProperties().FirstOrDefault(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));

            if (primaryKeyProperty == null)
            {
                throw new InvalidOperationException("Entity must have a property named 'Id' as the primary key.");
            }

            var primaryKey = GetColumnName(primaryKeyProperty);
            var query = $"SELECT * FROM {tableName} WHERE {primaryKey} = @Id";

            return await connection.QueryFirstOrDefaultAsync<T>(query, new { Id = id }, _transaction);
        }

        public IQueryBuilder<T> CreateQueryBuilder<T>()
        {
            return new QueryBuilder<T>(_lazyConnection.Value.ConnectionString);
        }

        public IStoredProcedureExecutor<T> CreateStoredProcedureExecutor<T>()
        {
            return StoredProcedureExecutorFactory.Create<T>(_lazyConnection.Value.ConnectionString);
        }

        public void Dispose()
        {
            _transaction?.Dispose();
            if (_lazyConnection.IsValueCreated)
            {
                _lazyConnection.Value.Dispose();
            }
        }

        private string GetTableName<T>()
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            return tableAttribute?.TableName ?? typeof(T).Name;
        }

        private string GetColumnName(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            return columnAttribute?.ColumnName ?? property.Name;
        }
    }
}