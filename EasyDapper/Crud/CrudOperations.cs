using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
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
        private readonly StoredProcedureExecutor _executor;

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
            _executor = new StoredProcedureExecutor(connectionManager, sqlBuilder);
        }

        public int Insert<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var identity = _queryCache.GetIdentityProperty<T>();
            using (var command = Prepare(_queryCache.GetInsertQuery<T>(), entity))
            {
                if (identity == null) return ExecuteNonQuery(command);
                var value = Execute(command, () => _executor.ExecutePreparedScalar<object>(command));
                identity.SetValue(entity, Convert.ChangeType(value,
                    Nullable.GetUnderlyingType(identity.PropertyType) ?? identity.PropertyType));
                return 1;
            }
        }

        public async Task<int> InsertAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var identity = _queryCache.GetIdentityProperty<T>();
            using (var command = await PrepareAsync(_queryCache.GetInsertQuery<T>(), entity).ConfigureAwait(false))
            {
                if (identity == null) return await ExecuteNonQueryAsync(command).ConfigureAwait(false);
                var value = await ExecuteAsync(command, () => _executor.ExecutePreparedScalarAsync<object>(command,
                    CancellationToken.None)).ConfigureAwait(false);
                identity.SetValue(entity, Convert.ChangeType(value,
                    Nullable.GetUnderlyingType(identity.PropertyType) ?? identity.PropertyType));
                return 1;
            }
        }

        public int Update<T>(T entity) where T : class => UpdateOne(entity, false);
        public Task<int> UpdateAsync<T>(T entity) where T : class => UpdateOneAsync(entity, false, CancellationToken.None);

        private int UpdateOne<T>(T entity, bool fullUpdate) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            while (true)
            {
                var update = PrepareUpdate(entity, fullUpdate);
                if (update.Query == null)
                {
                    using (_connectionManager.AcquireOperation())
                        if (_entityTracker.IsCurrentAttachment(update.Key, update.Attachment)) return 0;
                    continue;
                }
                using (var command = Prepare(update.Query, update.Parameters))
                {
                    var counter = update.Attachment == null || !CanCountGeneratedUpdate(command.Command, update.Query)
                        ? null : AddTargetRowCounter(command.Command);
                    command.Parameters.CaptureCommandState();
                    try
                    {
                        using (_connectionManager.AcquirePreparedOperation())
                        {
                            if (!_connectionManager.IsDisposed && !update.SkipAttachmentValidation &&
                                !_entityTracker.IsCurrentAttachment(update.Key, update.Attachment)) continue;
                            var affected = _executor.ExecutePreparedNonQuery(command);
                            if (counter != null && UpdatedTarget(counter, affected))
                                _entityTracker.AcceptPreparedSnapshot(update.Attachment, update.Snapshot);
                            return affected;
                        }
                    }
                    finally { command.CompleteCallbacks(); }
                }
            }
        }

        private async Task<int> UpdateOneAsync<T>(T entity, bool fullUpdate, CancellationToken cancellationToken) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var update = PrepareUpdate(entity, fullUpdate);
                if (update.Query == null)
                {
                    using (await _connectionManager.AcquireOperationAsync(cancellationToken).ConfigureAwait(false))
                        if (_entityTracker.IsCurrentAttachment(update.Key, update.Attachment)) return 0;
                    continue;
                }
                using (var command = await PrepareAsync(update.Query, update.Parameters, cancellationToken).ConfigureAwait(false))
                {
                    var counter = update.Attachment == null || !CanCountGeneratedUpdate(command.Command, update.Query)
                        ? null : AddTargetRowCounter(command.Command);
                    command.Parameters.CaptureCommandState();
                    try
                    {
                        using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
                        {
                            if (!_connectionManager.IsDisposed && !update.SkipAttachmentValidation &&
                                !_entityTracker.IsCurrentAttachment(update.Key, update.Attachment)) continue;
                            var affected = await _executor.ExecutePreparedNonQueryAsync(command, cancellationToken).ConfigureAwait(false);
                            if (counter != null && UpdatedTarget(counter, affected))
                                _entityTracker.AcceptPreparedSnapshot(update.Attachment, update.Snapshot);
                            return affected;
                        }
                    }
                    finally { command.CompleteCallbacks(); }
                }
            }
        }

        private PreparedUpdate PrepareUpdate<T>(T entity, bool fullUpdate) where T : class
        {
            if (fullUpdate && !_entityTracker.HasAttachments<T>())
            {
                var query = _queryCache.GetUpdateQuery<T>();
                if (!query.Contains("@old_"))
                    return new PreparedUpdate { Query = query, Parameters = entity, SkipAttachmentValidation = true };
            }
            var keys = _queryCache.GetPrimaryKeyProperties<T>();
            var values = EntityTracker.CaptureValues(entity, keys);
            var key = _entityTracker.CreateCompositeKey<T>(values, keys);
            _entityTracker.TryGetAttachment(key, out var attachment);
            var update = new PreparedUpdate { Key = key, Attachment = attachment };
            if (attachment == null)
            {
                update.Query = _queryCache.GetUpdateQuery<T>();
                update.Parameters = update.Query.Contains("@old_") ? BuildKeyOnlyParameters(keys, values) : (object)entity;
                return update;
            }

            EntityTracker.CaptureValues(entity, typeof(T).GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0), values);
            if (fullUpdate)
            {
                update.Query = _queryCache.GetUpdateQuery<T>();
                update.Parameters = update.Query.Contains("@old_") ? BuildKeyOnlyParameters(keys, values)
                    : entity is SqlMapper.IDynamicParameters ? (object)entity : BuildFrozenFullParameters<T>(values);
            }
            else
            {
                var changed = _entityTracker.GetChangedProperties<T>(attachment, values);
                if (changed.Count == 0) return update;
                update.Query = _sqlBuilder.BuildDynamicUpdateQuery<T>(changed, keys);
                var parameters = new DynamicParameters();
                foreach (var property in keys) parameters.Add("pk_" + property.Name, values[property.Name]);
                foreach (var name in changed) parameters.Add(name, values[name]);
                update.Parameters = parameters;
            }
            // Constructors and setters belong to consumer code, so build the
            // compatibility clone before acquiring the SQL execution lease.
            update.Snapshot = _entityTracker.PrepareSnapshot<T>(values);
            return update;
        }

        private sealed class PreparedUpdate
        {
            internal object Key;
            internal EntityTracker.Attachment Attachment;
            internal EntityTracker.Snapshot Snapshot;
            internal string Query;
            internal object Parameters;
            internal bool SkipAttachmentValidation;
        }

        private static DynamicParameters BuildKeyOnlyParameters(List<PropertyInfo> keys, Dictionary<string, object> values)
        {
            var parameters = new DynamicParameters();
            foreach (var property in keys) parameters.Add("old_" + property.Name, values[property.Name]);
            foreach (var property in keys.Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null))
                parameters.Add(property.Name, values[property.Name]);
            return parameters;
        }

        private static DynamicParameters BuildFrozenFullParameters<T>(Dictionary<string, object> values)
        {
            var parameters = new DynamicParameters();
            foreach (var property in typeof(T).GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0).OrderBy(p => p.Name))
            {
                // Identity columns are still needed when they form the WHERE key.
                if (property.GetCustomAttribute<IdentityAttribute>(true) != null &&
                    property.GetCustomAttribute<PrimaryKeyAttribute>(true) == null) continue;
                parameters.Add(property.Name, new FrozenPropertyParameter(property.PropertyType, values[property.Name]));
            }
            return parameters;
        }

        // Binding full updates of attached rows must retain the declared type
        // metadata that Dapper normally derives from the original entity type.
        private sealed class FrozenPropertyParameter : SqlMapper.ICustomQueryParameter
        {
            private readonly Type _type;
            private readonly object _value;
            internal FrozenPropertyParameter(Type type, object value) { _type = type; _value = value; }
            public void AddParameter(IDbCommand command, string name)
            {
                if (typeof(SqlMapper.ICustomQueryParameter).IsAssignableFrom(_type))
                {
                    ((SqlMapper.ICustomQueryParameter)_value).AddParameter(command, name);
                    return;
                }
                SqlMapper.ITypeHandler handler;
#pragma warning disable 618
                var dbType = LookupParameterType(_type, name, true, out handler);
                if (dbType == (DbType)(-1))
                {
                    SqlMapper.PackListParameters(command, name, _value);
                    return;
                }
                var parameter = command.CreateParameter();
                parameter.ParameterName = name;
                parameter.Direction = ParameterDirection.Input;
                if (handler != null) handler.SetValue(parameter, _value ?? DBNull.Value);
                else
                {
                    if (_type == typeof(object))
                    {
                        SqlMapper.ITypeHandler ignored;
                        dbType = _value == null || _value is DBNull
                            ? ParameterTypeLookup.ReturnType == typeof(DbType) ? DbType.Object : (DbType?)null
                            : LookupParameterType(_value.GetType(), name, false, out ignored);
                    }
                    if (dbType.HasValue && dbType.Value != DbType.Time) parameter.DbType = dbType.Value;
                    var value = _value;
                    if (value != null && _type.FullName == "System.Data.Linq.Binary")
                        value = _type.GetMethod("ToArray").Invoke(value, null);
                    parameter.Value = SqlMapper.SanitizeParameterValue(value);
                    if (_type == typeof(string))
                        parameter.Size = value == null ? 0 : ((string)value).Length > DbString.DefaultLength ? -1 : DbString.DefaultLength;
                }
#pragma warning restore 618
                command.Parameters.Add(parameter);
            }
        }

        private static readonly MethodInfo ParameterTypeLookup = FindParameterTypeLookup();

        private static MethodInfo FindParameterTypeLookup()
        {
            // Dapper 1.x returns DbType; newer consumers return DbType?. Keep
            // the installed dependency's ABI instead of emitting either call.
            var method = typeof(SqlMapper).GetMethod("LookupDbType", BindingFlags.Public | BindingFlags.Static,
                null, new[] { typeof(Type), typeof(string), typeof(bool), typeof(SqlMapper.ITypeHandler).MakeByRefType() }, null);
            if (method == null || method.ReturnType != typeof(DbType) && method.ReturnType != typeof(DbType?))
                throw new MissingMethodException(typeof(SqlMapper).FullName, "LookupDbType");
            return method;
        }

        private static DbType? LookupParameterType(Type type, string name, bool demand, out SqlMapper.ITypeHandler handler)
        {
            var arguments = new object[] { type, name, demand, null };
            object result;
            try { result = ParameterTypeLookup.Invoke(null, arguments); }
            catch (TargetInvocationException error)
            {
                if (error.InnerException != null) ExceptionDispatchInfo.Capture(error.InnerException).Throw();
                throw;
            }
            handler = (SqlMapper.ITypeHandler)arguments[3];
            return result == null ? (DbType?)null : (DbType)result;
        }

        private static IDbDataParameter AddTargetRowCounter(IDbCommand command)
        {
            var name = "__EasyDapperTargetRows";
            while (command.Parameters.Cast<IDataParameter>().Any(p => string.Equals(
                p.ParameterName?.TrimStart('@', ':', '?'), name, StringComparison.OrdinalIgnoreCase)) ||
                command.CommandText.IndexOf("@" + name, StringComparison.OrdinalIgnoreCase) >= 0)
                name += "_";
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = DbType.Int32;
            parameter.Direction = ParameterDirection.Output;
            command.Parameters.Add(parameter);
            command.CommandText += "; SET @" + name + " = @@ROWCOUNT;";
            return parameter;
        }

        private static bool CanCountGeneratedUpdate(IDbCommand command, string query)
            => command.CommandType == CommandType.Text && string.Equals(command.CommandText, query, StringComparison.Ordinal);

        private static bool UpdatedTarget(IDbDataParameter counter, int affected)
        {
            // Providers implementing the command abstraction without populating
            // output parameters retain their existing affected-count behavior.
            return counter.Value == null || counter.Value == DBNull.Value
                ? affected > 0 : Convert.ToInt32(counter.Value) > 0;
        }

        public int UpdateList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var affected = 0;
            foreach (var entity in entities)
            {
                if (entity == null) throw new ArgumentException("Entities cannot contain null values.", "entities");
                affected += UpdateOne(entity, true);
            }
            return affected;
        }

        public async Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var affected = 0;
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (entity == null) throw new ArgumentException("Entities cannot contain null values.", "entities");
                affected += await UpdateOneAsync(entity, true, cancellationToken).ConfigureAwait(false);
            }
            return affected;
        }

        public int Delete<T>(T entity) where T : class
            => DeleteOne(entity);

        private int DeleteOne<T>(T entity, IDbConnection readyConnection = null) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            using (var command = Prepare(_queryCache.GetDeleteQuery<T>(), parameters, readyConnection: readyConnection))
                return ExecuteNonQuery(command);
        }

        public Task<int> DeleteAsync<T>(T entity) where T : class => DeleteOneAsync(entity, CancellationToken.None);

        private async Task<int> DeleteOneAsync<T>(T entity, CancellationToken cancellationToken,
            IDbConnection readyConnection = null) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var parameters = _sqlBuilder.CreatePrimaryKeyParameters(entity);
            using (var command = await PrepareAsync(_queryCache.GetDeleteQuery<T>(), parameters, cancellationToken,
                readyConnection: readyConnection).ConfigureAwait(false))
                return await ExecuteNonQueryAsync(command, cancellationToken).ConfigureAwait(false);
        }

        public int DeleteList<T>(IEnumerable<T> entities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            // Preserve the existing empty-list connection opening and model
            // validation, while leaving enumeration outside the SQL lease.
            IDbConnection connection;
            using (_connectionManager.AcquireOperation()) connection = _connectionManager.GetOpenConnection();
            _queryCache.GetDeleteQuery<T>();
            var affected = 0;
            foreach (var entity in entities) affected += DeleteOne(entity, connection);
            return affected;
        }

        public async Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var connection = await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            _queryCache.GetDeleteQuery<T>();
            var affected = 0;
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();
                affected += await DeleteOneAsync(entity, cancellationToken, connection).ConfigureAwait(false);
            }
            return affected;
        }

        public T GetById<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var parameters = BuildIdParameters<T>(id);
            using (var command = Prepare(_queryCache.GetGetByIdQuery<T>(), parameters, typeof(T)))
                return Execute(command, () => _executor.ExecutePreparedFirstOrDefault<T>(command));
        }

        public async Task<T> GetByIdAsync<T>(object id) where T : class
        {
            if (id == null) throw new ArgumentNullException("id");
            var parameters = BuildIdParameters<T>(id);
            using (var command = await PrepareAsync(_queryCache.GetGetByIdQuery<T>(), parameters, resultType: typeof(T)).ConfigureAwait(false))
                return await ExecuteAsync(command, () => _executor.ExecutePreparedFirstOrDefaultAsync<T>(command,
                    CancellationToken.None)).ConfigureAwait(false);
        }

        public T GetById<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            using (var command = Prepare(_queryCache.GetGetByIdQuery<T>(), parameters, typeof(T)))
                return Execute(command, () => _executor.ExecutePreparedFirstOrDefault<T>(command));
        }

        public async Task<T> GetByIdAsync<T>(T entity) where T : class
        {
            if (entity == null) throw new ArgumentNullException("entity");
            var parameters = _sqlBuilder.GetPrimaryKeyValues(entity);
            using (var command = await PrepareAsync(_queryCache.GetGetByIdQuery<T>(), parameters, resultType: typeof(T)).ConfigureAwait(false))
                return await ExecuteAsync(command, () => _executor.ExecutePreparedFirstOrDefaultAsync<T>(command,
                    CancellationToken.None)).ConfigureAwait(false);
        }

        private DynamicParameters BuildIdParameters<T>(object id) where T : class
        {
            var keys = _queryCache.GetPrimaryKeyProperties<T>();
            var parameters = new DynamicParameters();
            if (keys.Count == 1) { parameters.Add(keys[0].Name, id); return parameters; }
            var idType = id.GetType();
            foreach (var key in keys)
            {
                var property = idType.GetProperty(key.Name);
                if (property != null) parameters.Add(key.Name, property.GetValue(id));
                else if (id is IDictionary<string, object> dictionary && dictionary.TryGetValue(key.Name, out var value))
                    parameters.Add(key.Name, value);
                else throw new ArgumentException("Cannot resolve composite primary key value for '" + key.Name +
                    "'. Supply an object with a property named '" + key.Name + "' or use the GetById<T>(T entity) overload.", "id");
            }
            return parameters;
        }

        private StoredProcedureExecutor.PreparedCommand Prepare(string query, object parameters, Type resultType = null,
            IDbConnection readyConnection = null)
        {
            IDbConnection connection;
            IDbTransaction transaction;
            using (readyConnection == null ? _connectionManager.AcquireOperation() : _connectionManager.AcquirePreparedOperation())
            {
                connection = readyConnection ?? _connectionManager.GetOpenConnection();
                transaction = _connectionManager.CurrentTransaction;
            }
            return _executor.PrepareText(connection, transaction, query, parameters, resultType);
        }

        private async Task<StoredProcedureExecutor.PreparedCommand> PrepareAsync(string query, object parameters,
            CancellationToken cancellationToken = default, Type resultType = null, IDbConnection readyConnection = null)
        {
            var connection = readyConnection ?? await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            IDbTransaction transaction;
            using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
            {
                transaction = _connectionManager.CurrentTransaction;
            }
            return _executor.PrepareText(connection, transaction, query, parameters, resultType);
        }

        private TResult Execute<TResult>(StoredProcedureExecutor.PreparedCommand command, Func<TResult> execute)
        {
            try { using (_connectionManager.AcquirePreparedOperation()) return execute(); }
            finally { command.CompleteCallbacks(); }
        }

        private async Task<TResult> ExecuteAsync<TResult>(StoredProcedureExecutor.PreparedCommand command,
            Func<Task<TResult>> execute, CancellationToken cancellationToken = default)
        {
            try
            {
                using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
                    return await execute().ConfigureAwait(false);
            }
            finally { command.CompleteCallbacks(); }
        }

        private int ExecuteNonQuery(StoredProcedureExecutor.PreparedCommand command)
            => Execute(command, () => _executor.ExecutePreparedNonQuery(command));

        private Task<int> ExecuteNonQueryAsync(StoredProcedureExecutor.PreparedCommand command, CancellationToken cancellationToken = default)
            => ExecuteAsync(command, () => _executor.ExecutePreparedNonQueryAsync(command, cancellationToken), cancellationToken);
    }
}
