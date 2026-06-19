using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal class CrudOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private readonly EntityTracker _entityTracker;

        public CrudOperations(ConnectionManager connectionManager, QueryCache queryCache,
            SqlBuilder sqlBuilder, EntityTracker entityTracker)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            if (entityTracker == null) throw new ArgumentNullException("entityTracker");
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
            _entityTracker = entityTracker;
        }

        public int Insert<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetInsertQuery<T>();
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = connection.ExecuteScalar(query, entity,
                    _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }
            return connection.Execute(query, entity,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetInsertQuery<T>();
            var identityProp = _queryCache.GetIdentityProperty<T>();
            if (identityProp != null)
            {
                var newId = await connection.ExecuteScalarAsync(query, entity,
                    _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
                identityProp.SetValue(entity, Convert.ChangeType(newId, identityProp.PropertyType));
                return 1;
            }
            return await connection.ExecuteAsync(query, entity,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int Update<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker.TryGetAttached(key, out var original)) return BaseUpdate(entity);
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;
            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = _connectionManager.GetOpenConnection();
            return connection.Execute(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>().ToList();
            var key = _entityTracker.CreateCompositeKey(entity, primaryKeys);
            if (!_entityTracker.TryGetAttached(key, out var original)) return await BaseUpdateAsync(entity).ConfigureAwait(false);
            var changedProps = _entityTracker.GetChangedProperties((T)original, entity);
            if (!changedProps.Any()) return 0;
            var query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changedProps, primaryKeys);
            var parameters = _sqlBuilder.BuildParameters(entity, primaryKeys, changedProps);
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            return await connection.ExecuteAsync(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return UpdateListWithCompositeKeys(entities, query);
            return connection.Execute(query, entities,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateListWithCompositeKeysAsync(entities, query, cancellationToken).ConfigureAwait(false);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: entities,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        public int Delete<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return connection.Execute(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            return await connection.ExecuteAsync(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            return connection.Execute(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetDeleteQuery<T>();
            var parameters = entities.Select(_sqlBuilder.CreatePrimaryKeyParameters);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: parameters,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        public T GetById<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            object parameters;
            if (primaryKeys.Count == 1)
            {
                parameters = new { Id = id };
            }
            else
            {
                parameters = BuildCompositeKeyParameters<T>(id, primaryKeys);
            }
            return connection.QueryFirstOrDefault<T>(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetGetByIdQuery<T>();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            object parameters;
            if (primaryKeys.Count == 1)
            {
                parameters = new { Id = id };
            }
            else
            {
                parameters = BuildCompositeKeyParameters<T>(id, primaryKeys);
            }
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        public T GetById<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return connection.QueryFirstOrDefault<T>(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetGetByIdQuery<T>();
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            return await connection.QueryFirstOrDefaultAsync<T>(query, parameters,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        private static object BuildCompositeKeyParameters<T>(object id, List<PropertyInfo> primaryKeys) where T : class
        {
            var idType = id.GetType();
            var dynamicParams = new DynamicParameters();
            foreach (var pk in primaryKeys)
            {
                var idProp = idType.GetProperty(pk.Name);
                if (idProp != null)
                {
                    dynamicParams.Add(pk.Name, idProp.GetValue(id));
                    continue;
                }

                if (id is System.Collections.Generic.IDictionary<string, object> dict && dict.TryGetValue(pk.Name, out var dictValue))
                {
                    dynamicParams.Add(pk.Name, dictValue);
                    continue;
                }
                throw new ArgumentException(
                    $"Cannot resolve composite primary key value for '{pk.Name}'. " +
                    $"Supply an object with a property named '{pk.Name}' or use the GetById<T>(T entity) overload.",
                    "id");
            }
            return dynamicParams;
        }

        private int BaseUpdate<T>(T entity) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return UpdateSingleWithCompositeKeys(entity, query);
            return connection.Execute(query, entity,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> BaseUpdateAsync<T>(T entity) where T : class
        {
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var query = _queryCache.GetUpdateQuery<T>();
            if (query.Contains("@old_")) return await UpdateSingleWithCompositeKeysAsync(entity, query).ConfigureAwait(false);
            return await connection.ExecuteAsync(query, entity,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout).ConfigureAwait(false);
        }

        private int UpdateListWithCompositeKeys<T>(IEnumerable<T> entities, string query) where T : class
        {
            var totalAffected = 0;
            foreach (var entity in entities)
                totalAffected += UpdateSingleWithCompositeKeys(entity, query);
            return totalAffected;
        }

        private async Task<int> UpdateListWithCompositeKeysAsync<T>(IEnumerable<T> entities, string query, CancellationToken cancellationToken) where T : class
        {
            var totalAffected = 0;
            foreach (var entity in entities)
                totalAffected += await UpdateSingleWithCompositeKeysAsync(entity, query, cancellationToken).ConfigureAwait(false);
            return totalAffected;
        }

        private int UpdateSingleWithCompositeKeys<T>(T entity, string query) where T : class
        {
            var connection = _connectionManager.GetOpenConnection();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys) oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys) if (!IsIdentity(pk)) newParams.Add(pk.Name, pk.GetValue(entity));
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);
            return connection.Execute(query, combinedParams,
                _connectionManager.CurrentTransaction, _connectionManager.CommandTimeout);
        }

        private async Task<int> UpdateSingleWithCompositeKeysAsync<T>(T entity, string query, CancellationToken cancellationToken = default) where T : class
        {
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            var oldParams = new DynamicParameters();
            foreach (var pk in primaryKeys) oldParams.Add($"old_{pk.Name}", pk.GetValue(entity));
            var newParams = new DynamicParameters();
            foreach (var pk in primaryKeys) if (!IsIdentity(pk)) newParams.Add(pk.Name, pk.GetValue(entity));
            var combinedParams = new DynamicParameters();
            MergeDynamicParameters(oldParams, combinedParams);
            MergeDynamicParameters(newParams, combinedParams);
            var commandDefinition = new CommandDefinition(commandText: query, parameters: combinedParams,
                transaction: _connectionManager.CurrentTransaction,
                commandTimeout: _connectionManager.CommandTimeout,
                cancellationToken: cancellationToken);
            return await connection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
        }

        private void MergeDynamicParameters(DynamicParameters source, DynamicParameters destination)
        {
            if (source == null) return;
            foreach (var paramName in source.ParameterNames)
                destination.Add(paramName, source.Get<object>(paramName));
        }

        private bool IsIdentity(PropertyInfo property) =>
            property.GetCustomAttribute<IdentityAttribute>(true) != null;
    }
}
