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
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.SqlTypes;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;
using System.Xml.Linq;


namespace EasyDapper.Implementations
{
    internal sealed class DapperService : IDapperService, IDisposable
    {
        private readonly IDbConnection _externalConnection;
        private readonly Lazy<IDbConnection> _lazyConnection;
        private IDbTransaction _transaction;
        private static readonly ConcurrentDictionary<Type, string> InsertQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> UpdateQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> DeleteQueryCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> GetByIdQueryCache = new ConcurrentDictionary<Type, string>();

        public DapperService(string connectionString)
        {
            _lazyConnection = new Lazy<IDbConnection>(() => new SqlConnection(connectionString));
        }

        public DapperService(IDbConnection externalConnection)
        {
            _externalConnection = externalConnection ?? throw new ArgumentNullException(nameof(externalConnection));
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
            try
            {
                _transaction.Rollback();
            }
            finally
            {
                _transaction.Dispose();
                _transaction = null;
            }
        }

        //public int Insert<T>(T entity) where T : class
        //{
        //    var connection = GetOpenConnection();
        //    var query = InsertQueryCache.GetOrAdd(typeof(T), type =>
        //    {
        //        var tableName = GetTableName<T>();
        //        var properties = GetInsertProperties<T>();
        //        var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
        //        var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
        //        return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        //    });
        //    return connection.Execute(query, entity, _transaction);
        //}
        public int Insert<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = InsertQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var properties = GetInsertProperties<T>();
                var identityProp1 = GetIdentityProperty<T>();
                var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
                var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
                if (identityProp1 != null)
                {
                    var outputClause = $"OUTPUT INSERTED.{GetColumnName(identityProp1)}";
                    return $"INSERT INTO {tableName} ({columns}) {outputClause} VALUES ({values})";
                }
                return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
            });
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = connection.ExecuteScalar<int>(query, entity, _transaction);
                identityProp.SetValue(entity, newId);
                return 1;
            }
            return connection.Execute(query, entity, _transaction);
        }

        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            //var connection = await GetOpenConnectionAsync();
            //var query = InsertQueryCache.GetOrAdd(typeof(T), type =>
            //{
            //    var tableName = GetTableName<T>();
            //    var properties = GetInsertProperties<T>();
            //    var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
            //    var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            //    return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
            //});
            //return await connection.ExecuteAsync(query, entity, _transaction);
            var connection = await GetOpenConnectionAsync();
            var query = InsertQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var properties = GetInsertProperties<T>();
                var identityProp1 = GetIdentityProperty<T>();
                var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
                var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
                if (identityProp1 != null)
                {
                    var outputClause = $"OUTPUT INSERTED.{GetColumnName(identityProp1)}";
                    return $"INSERT INTO {tableName} ({columns}) {outputClause} VALUES ({values})";
                }
                return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
            });
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync<int>(query, entity, _transaction);
                identityProp.SetValue(entity, newId);
                return 1;
            }
            return await connection.ExecuteAsync(query, entity, _transaction);
        }
        public int InsertList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }
            var entityList = entities.ToList();
            var connection = GetOpenConnection();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();

            if (identityProp != null)
            {
                var generatedIds = connection.Query<int>(query, entityList, _transaction).ToList();
                for (int i = 0; i < entityList.Count; i++)
                {
                    identityProp.SetValue(entityList[i], generatedIds[i]);
                }

                return generatedIds.Count;
            }

            return connection.Execute(query, entityList, _transaction);
        }

        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }

            var entityList = entities.ToList();
            var connection = await GetOpenConnectionAsync();
            var query = InsertQueryCache.GetOrAdd(typeof(T), BuildInsertQuery<T>);
            var identityProp = GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var generatedIds = (await connection.QueryAsync<int>(query, entityList, _transaction)).ToList();
                for (int i = 0; i < entityList.Count; i++)
                {
                    identityProp.SetValue(entityList[i], generatedIds[i]);
                }

                return generatedIds.Count;
            }

            return await connection.ExecuteAsync(query, entityList, _transaction);
        }

        public int Update<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p));

                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));

                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            });
            return connection.Execute(query, entity, _transaction);
        }
        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }
            var connection = GetOpenConnection();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p));

                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));

                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            });

            return connection.Execute(query, entities, _transaction);
        }

        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p));

                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));

                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            });
            return await connection.ExecuteAsync(query, entity, _transaction);
        }
        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }
            var connection = await GetOpenConnectionAsync();
            var query = UpdateQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var properties = typeof(T).GetProperties().Where(p => !IsPrimaryKey(p));

                var setClause = string.Join(", ", properties.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));

                return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
            });

            return await connection.ExecuteAsync(query, entities, _transaction);
        }

        public int Delete<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"DELETE FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyProperties<T>()
                .ToDictionary(p => p.Name, p => p.GetValue(entity));

            return connection.Execute(query, parameters, _transaction);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"DELETE FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyProperties<T>()
                .ToDictionary(p => p.Name, p => p.GetValue(entity));

            return await connection.ExecuteAsync(query, parameters, _transaction);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }
            var connection = GetOpenConnection();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"DELETE FROM {tableName} WHERE {whereClause}";
            });

            var primaryKeyProperties = GetPrimaryKeyProperties<T>();
            var parameters = entities.Select(entity =>
                primaryKeyProperties.ToDictionary(pk => pk.Name, pk => pk.GetValue(entity)));

            return connection.Execute(query, parameters, _transaction);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null || !entities.Any())
            {
                throw new ArgumentException("Entities list cannot be null or empty", nameof(entities));
            }
            var connection = await GetOpenConnectionAsync();
            var query = DeleteQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"DELETE FROM {tableName} WHERE {whereClause}";
            });

            var primaryKeyProperties = GetPrimaryKeyProperties<T>();
            var parameters = entities.Select(entity =>
                primaryKeyProperties.ToDictionary(pk => pk.Name, pk => pk.GetValue(entity)));

            return await connection.ExecuteAsync(query, parameters, _transaction);
        }

        //public T GetById<T>(string Id)
        //{
        //    object key = Id;
        //    var connection = GetOpenConnection();
        //    var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
        //    {
        //        var tableName = GetTableName<T>();
        //        var columns = string.Join(", ", typeof(T).GetProperties()
        //            .Select(p => $"{GetColumnName(p)} AS {p.Name}"));
        //        var primaryKeys = GetPrimaryKeyProperties<T>();
        //        var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
        //        return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
        //    });

        //    var parameters = GetPrimaryKeyProperties<T>()
        //        .ToDictionary(p => p.Name, p => key);

        //    return connection.QueryFirstOrDefault<T>(query, parameters, _transaction);
        //}
        public T GetById<T>(string Id)
        {
            // این متد فقط برای کلیدهای اصلی تکمقداری قابل استفاده است
            var primaryKeys = GetPrimaryKeyProperties<T>().ToList();
            if (primaryKeys.Count != 1)
            {
                throw new InvalidOperationException("This method is only supported for single primary key entities");
            }
            object key = Convert.ChangeType(Id, primaryKeys[0].PropertyType);
            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = GetSelectColumns<T>();
                var whereClause = $"{GetColumnName(primaryKeys[0])} = @Id";
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });
            return connection.QueryFirstOrDefault<T>(query, new { Id = key }, _transaction);
        }

        public async Task<T> GetByIdAsync<T>(string Id)
        {
            object key = Id;
            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyProperties<T>()
                .ToDictionary(p => p.Name, p => key);

            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _transaction);
        }
        //public T GetById<T>(T entity) where T : class
        //{
        //    var connection = GetOpenConnection();
        //    var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
        //    {
        //        var tableName = GetTableName<T>();
        //        var columns = string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
        //        var primaryKeys = GetPrimaryKeyProperties<T>();
        //        var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
        //        return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
        //    });

        //    var parameters = GetPrimaryKeyProperties<T>()
        //        .ToDictionary(p => p.Name, p => p.GetValue(entity));

        //    return connection.QueryFirstOrDefault<T>(query, parameters, _transaction);
        //}
        public T GetById<T>(T entity) where T : class
        {
            var connection = GetOpenConnection();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = GetSelectColumns<T>();
                var whereClause = BuildWhereClause<T>();
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });
            var parameters = GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters, _transaction);
        }
        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            var connection = await GetOpenConnectionAsync();
            var query = GetByIdQueryCache.GetOrAdd(typeof(T), type =>
            {
                var tableName = GetTableName<T>();
                var columns = string.Join(", ", typeof(T).GetProperties()
                    .Select(p => $"{GetColumnName(p)} AS {p.Name}"));
                var primaryKeys = GetPrimaryKeyProperties<T>();
                var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
                return $"SELECT {columns} FROM {tableName} WHERE {whereClause}";
            });

            var parameters = GetPrimaryKeyProperties<T>().ToDictionary(p => p.Name, p => p.GetValue(entity));

            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters, _transaction);
        }
        public IQueryBuilder<T> QueryBuilder<T>()
        {
            if (_externalConnection != null)
            {
                throw new InvalidOperationException("QueryBuilder is not supported when using an external connection.");
            }
            return new QueryBuilder<T>(_lazyConnection.Value.ConnectionString);
        }
        //public IStoredProcedureExecutor<T> CreateStoredProcedureExecutor<T>()
        //{
        //    if (_externalConnection != null)
        //        throw new InvalidOperationException("Stored procedures are not supported when using an external connection.");

        //    return StoredProcedureExecutorFactory.Create<T>(_lazyConnection.Value.ConnectionString);
        //}
        public void Dispose()
        {
            _transaction?.Dispose();
            if (_lazyConnection?.IsValueCreated == true)
            {
                _lazyConnection.Value.Dispose();
            }
        }
        public IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            //openConnection.Open();
            return openConnection.Query<T>(
            procedureName,
            param: parameters,
            commandType: CommandType.StoredProcedure
            );
        }
        public async Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            //await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return await openConnection.QueryAsync<T>(
            new CommandDefinition(
                procedureName,
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken
            )
            ).ConfigureAwait(false);
        }        
        public T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            using (var multi = openConnection.QueryMultiple(
                procedureName,
                parameters,
                transaction,
                commandTimeout ?? openConnection.ConnectionTimeout,
                CommandType.StoredProcedure))
            {
                return mapper(multi);
            }
        }
        public async Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            IsValidProcedureName(procedureName);
            var openConnection = GetOpenConnection();
            using (var multi = await openConnection.QueryMultipleAsync(
                new CommandDefinition(
                    procedureName,
                    parameters,
                    transaction,
                    commandTimeout ?? openConnection.ConnectionTimeout,
                    CommandType.StoredProcedure,
                    cancellationToken: cancellationToken
                )
            ).ConfigureAwait(false))
            {
                return await asyncMapper(multi).ConfigureAwait(false);
            }
        }
        private string GetTableName<T>()
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            if (tableAttribute == null)
            {
                return $"[{typeof(T).Name}]";
            }
            var schema = string.IsNullOrWhiteSpace(tableAttribute.Schema)
                ? null
                : $"[{tableAttribute.Schema}]";

            return schema == null
                ? $"[{tableAttribute.TableName}]"
                : $"{schema}.[{tableAttribute.TableName}]";
        }
        private string GetColumnName(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            return columnAttribute == null ? $"[{property.Name}]" : $"[{columnAttribute.ColumnName}]";
        }
        private IEnumerable<PropertyInfo> GetPrimaryKeyProperties<T>()
        {
            var properties = typeof(T).GetProperties().Where(p => p.GetCustomAttribute<PrimaryKeyAttribute>() != null).ToList();
            if (properties.Count == 0)
            {
                throw new InvalidOperationException($"No primary key defined for type {typeof(T).Name}");
            }
            var identityPk = properties.Count(p => p.GetCustomAttribute<IdentityAttribute>() != null);
            if (identityPk > 1)
            {
                throw new InvalidOperationException("Multiple Identity primary keys are not supported");
            }
            return properties;
        }
        private bool IsPrimaryKey(PropertyInfo property) => property.GetCustomAttribute<PrimaryKeyAttribute>() != null;
        private IEnumerable<PropertyInfo> GetInsertProperties<T>()
        {
            return typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>() == null);
        }
        private IDbConnection GetOpenConnection()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                {
                    _externalConnection.Open();
                }
                return _externalConnection;
            }
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }
        private async Task<IDbConnection> GetOpenConnectionAsync()
        {
            if (_externalConnection != null)
            {
                if (_externalConnection.State != ConnectionState.Open)
                {
                    _externalConnection.Open();
                }                    
                return _externalConnection;
            }
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                if (connection is SqlConnection sqlConnection)
                {
                    await sqlConnection.OpenAsync().ConfigureAwait(false);
                }                    
                else
                {
                    connection.Open();
                }                    
            }
            return connection;
        }
        private string BuildWhereClause<T>()
        {
            var primaryKeys = GetPrimaryKeyProperties<T>();
            return string.Join(" AND ", primaryKeys.Select(p => $"{GetColumnName(p)} = @{p.Name}"));
        }
        private DynamicParameters GetPrimaryKeyValues<T>(T entity)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in GetPrimaryKeyProperties<T>())
            {
                var value = pk.GetValue(entity);
                parameters.Add(pk.Name, value);
            }
            return parameters;
        }
        private string GetSelectColumns<T>()
        {
            return string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetColumnName(p)} AS {p.Name}"));
        }
        private PropertyInfo GetIdentityProperty<T>()
        {
            return typeof(T).GetProperties()
                .FirstOrDefault(p =>
                    p.GetCustomAttribute<IdentityAttribute>() != null &&
                    p.GetCustomAttribute<PrimaryKeyAttribute>() != null);
        }
        private string BuildInsertQuery<T>(Type type)
        {
            var tableName = GetTableName<T>();
            var properties = GetInsertProperties<T>();
            var identityProp = GetIdentityProperty<T>();
            var columns = string.Join(", ", properties.Select(p => GetColumnName(p)));
            var values = string.Join(", ", properties.Select(p => $"@{p.Name}"));
            if (identityProp != null)
            {
                var outputClause = $"OUTPUT INSERTED.{GetColumnName(identityProp)}";
                return $"INSERT INTO {tableName} ({columns}) {outputClause} VALUES ({values})";
            }
            return $"INSERT INTO {tableName} ({columns}) VALUES ({values})";
        }
        private void IsValidProcedureName(string procedureName)
        {
            //if (string.IsNullOrWhiteSpace(procedureName))
            //{
            //    throw new ArgumentException(
            //        "Procedure name cannot be null or whitespace.",
            //        nameof(procedureName)
            //    );
            //}
            if (!Regex.IsMatch(procedureName, @"^[\w\d_]+\.[\w\d_]+$|^[\w\d_]+$"))
            {
                throw new ArgumentException("Procedure name is not valid", nameof(procedureName));
            }
        }
    }
}