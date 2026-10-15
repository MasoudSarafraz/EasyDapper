using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Dapper;

namespace EasyDapper
{
    internal class SqlBuilder
    {
        private readonly QueryCache _queryCache;
        private static readonly Regex ParameterNameRegex =
            new Regex(@"\A[\p{L}_][\p{L}\p{Nd}_]*\z", RegexOptions.Compiled);
        private static readonly ConstructorInfo BindingIdentityConstructor = typeof(SqlMapper.Identity).GetConstructor(
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
            new[] { typeof(string), typeof(CommandType?), typeof(string), typeof(Type), typeof(Type), typeof(Type[]), typeof(int) }, null)
            ?? typeof(SqlMapper.Identity).GetConstructor(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
                new[] { typeof(string), typeof(CommandType?), typeof(string), typeof(Type), typeof(Type), typeof(int), typeof(int) }, null);
        private static readonly bool BindingIdentityUsesTypesHash =
            BindingIdentityConstructor != null && BindingIdentityConstructor.GetParameters()[5].ParameterType == typeof(int);

        public SqlBuilder(QueryCache queryCache)
        {
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            _queryCache = queryCache;
        }

        public string BuildDynamicUpdateQuery<T>(List<string> changedProps, List<PropertyInfo> primaryKeys)
        {
            var tableName = _queryCache.GetTableName<T>();
            var changedProperties = changedProps
                .Select(p => typeof(T).GetProperty(p))
                .Where(p => p != null)
                .ToList();
            var setClause = string.Join(", ", changedProperties.Select(p => $"{_queryCache.GetColumnName(p)} = @{p.Name}"));
            var whereClause = string.Join(" AND ", primaryKeys.Select(p => $"{_queryCache.GetColumnName(p)} = @pk_{p.Name}"));
            return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
        }

        public DynamicParameters BuildParameters<T>(T entity, List<PropertyInfo> primaryKeys, List<string> changedProps)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in primaryKeys) parameters.Add($"pk_{pk.Name}", pk.GetValue(entity));
            foreach (var propName in changedProps)
            {
                var prop = typeof(T).GetProperty(propName);
                if (prop != null) parameters.Add(prop.Name, prop.GetValue(entity));
            }
            return parameters;
        }

        public DynamicParameters CreatePrimaryKeyParameters<T>(T entity)
        {
            var parameters = new DynamicParameters();
            var primaryKeys = _queryCache.GetPrimaryKeyProperties<T>();
            foreach (var pk in primaryKeys) parameters.Add(pk.Name, pk.GetValue(entity));
            return parameters;
        }

        public DynamicParameters GetPrimaryKeyValues<T>(T entity)
        {
            var parameters = new DynamicParameters();
            foreach (var pk in _queryCache.GetPrimaryKeyProperties<T>())
            {
                var value = pk.GetValue(entity);
                parameters.Add(pk.Name, value);
            }
            return parameters;
        }

        public string BuildScalarFunctionQuery(string functionName, object parameters, IEnumerable<string> orderedNames = null)
            => $"SELECT {functionName}({BuildFunctionArguments(parameters, orderedNames)})";

        public string BuildTableFunctionQuery(string functionName, object parameters, IEnumerable<string> orderedNames = null)
            => $"SELECT * FROM {functionName}({BuildFunctionArguments(parameters, orderedNames)})";

        private string BuildFunctionArguments(object parameters, IEnumerable<string> orderedNames)
            => orderedNames == null ? BuildParameters(parameters)
                : string.Join(", ", orderedNames.Select(name => "@" + ValidateParameterName(name)));

        private string BuildParameters(object parameters)
        {
            if (parameters == null) return "";
            IEnumerable<string> names;
            if (parameters is DynamicParameters dynamicParameters)
                names = dynamicParameters.ParameterNames;
            else if (parameters is IDictionary dictionary)
                names = dictionary.Keys.Cast<object>().Select(GetDictionaryParameterName);
            else if (parameters is IEnumerable<KeyValuePair<string, object>> namedParameters)
                names = namedParameters.Select(p => p.Key);
            else
                names = parameters.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(p => p.CanRead && p.GetIndexParameters().Length == 0).Select(p => p.Name);
            return string.Join(", ", names.Select(name => "@" + ValidateParameterName(name)));
        }

        internal object NormalizeFunctionParameters(object parameters, IDbConnection connection = null)
            => NormalizeFunctionParameters(parameters, connection, out _);

        internal object NormalizeFunctionParameters(object parameters, IDbConnection connection, out string[] orderedNames)
        {
            orderedNames = null;
            if (parameters is DynamicParameters dynamicParameters && connection != null)
            {
                // Dapper expands anonymous templates only while binding a command.
                // This adapter binds through Dapper's public API without sending a command,
                // retaining the original parameter bag's types and output metadata.
                var bindingConnection = new ParameterBindingConnection(connection);
                bindingConnection.Execute("EasyDapper_BindFunctionParameters", new BindingParameters(dynamicParameters),
                    commandType: CommandType.StoredProcedure);
                orderedNames = bindingConnection.ParameterNames;
                return dynamicParameters;
            }
            if (parameters is IDictionary dictionary)
            {
                var result = new DynamicParameters();
                foreach (DictionaryEntry entry in dictionary)
                    result.Add(ValidateParameterName(GetDictionaryParameterName(entry.Key)), entry.Value);
                return result;
            }
            if (parameters is IEnumerable<KeyValuePair<string, object>> namedParameters)
            {
                var result = new DynamicParameters();
                foreach (var entry in namedParameters)
                    result.Add(ValidateParameterName(entry.Key), entry.Value);
                return result;
            }
            return parameters;
        }

        private static string GetDictionaryParameterName(object key)
        {
            if (!(key is string name)) throw new ArgumentException("Function parameter names must be strings.", "parameters");
            return name;
        }

        private static string ValidateParameterName(string name)
        {
            if (!string.IsNullOrEmpty(name) && (name[0] == '@' || name[0] == ':' || name[0] == '?'))
                name = name.Substring(1);
            if (string.IsNullOrEmpty(name) || !ParameterNameRegex.IsMatch(name))
                throw new ArgumentException("Function parameter name is not valid.", "parameters");
            return name;
        }

        internal BoundParameters BindPreparedParameters(IDbConnection connection, IDbCommand command,
            object parameters, CommandType? bindingCommandType, ConnectionManager manager, bool mapperCallbacks,
            Type resultIdentityType = null)
        {
            var binder = parameters as SqlMapper.IDynamicParameters;
            if (binder == null && parameters != null) binder = new DynamicParameters(parameters);
            if (binder != null)
            {
                var bindingConnection = new ParameterBindingConnection(connection, command);
                bindingConnection.Execute(command.CommandText, new BindingParameters(binder, parameters.GetType(), resultIdentityType),
                    commandType: bindingCommandType);
            }
            return new BoundParameters(command, parameters as SqlMapper.IParameterCallbacks, manager, mapperCallbacks);
        }

        internal sealed class BoundParameters : DynamicParameters, SqlMapper.IDynamicParameters, SqlMapper.IParameterCallbacks
        {
            private readonly IDbCommand _command;
            private readonly SqlMapper.IParameterCallbacks _callbacks;
            private readonly ConnectionManager _manager;
            private readonly bool _mapperCallbacks;
            private int _completionRequested;
            private int _callbacksCompleted;
            private string _commandText;
            private int _commandTimeout;
            private CommandType _commandType;
            private IDbConnection _connection;
            private IDbTransaction _transaction;
            private UpdateRowSource _updatedRowSource;
            private IDataParameter[] _boundParameterObjects;

            public BoundParameters(IDbCommand command, SqlMapper.IParameterCallbacks callbacks,
                ConnectionManager manager, bool mapperCallbacks)
            {
                _command = command;
                _callbacks = callbacks;
                _manager = manager;
                _mapperCallbacks = mapperCallbacks;
                ParameterNames = command.Parameters.Cast<IDataParameter>().Select(p => p.ParameterName).ToArray();
            }

            public new string[] ParameterNames { get; }

            internal void CaptureCommandState()
            {
                _commandText = _command.CommandText;
                _commandTimeout = _command.CommandTimeout;
                _commandType = _command.CommandType;
                _connection = _command.Connection;
                _transaction = _command.Transaction;
                _updatedRowSource = _command.UpdatedRowSource;
                _boundParameterObjects = _command.Parameters.Cast<IDataParameter>().ToArray();
            }

            public new void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                if (!ReferenceEquals(command, _command))
                    throw new InvalidOperationException("Prepared parameters must execute on their original command.");
                // Dapper configures the command before invoking its parameter binder.
                // Restore the native command state that the original binder supplied.
                command.Connection = _connection;
                command.Transaction = _transaction;
                command.CommandText = _commandText;
                command.CommandTimeout = _commandTimeout;
                command.CommandType = _commandType;
                command.UpdatedRowSource = _updatedRowSource;
            }

            public void OnCompleted()
            {
                System.Threading.Volatile.Write(ref _completionRequested, 1);
                if (_mapperCallbacks && !_manager.HasCurrentOperation) CompleteCallbacks();
            }

            public void CompleteCallbacks()
            {
                if (System.Threading.Volatile.Read(ref _completionRequested) == 0 ||
                    System.Threading.Interlocked.Exchange(ref _callbacksCompleted, 1) != 0) return;
                if (_callbacks == null) return;
                var parameters = _command.Parameters;
                var restore = parameters.Count == 0 && _boundParameterObjects != null && _boundParameterObjects.Length != 0;
                try
                {
                    // Dapper 2 clears sync-command parameters before this deferred callback.
                    // Supply the same native output objects that the callback originally saw.
                    if (restore) foreach (var parameter in _boundParameterObjects) parameters.Add(parameter);
                    _callbacks.OnCompleted();
                }
                finally
                {
                    if (restore) parameters.Clear();
                }
            }

            internal void AbortCallbacks()
                => System.Threading.Interlocked.Exchange(ref _callbacksCompleted, 1);
        }

        private sealed class BindingParameters : SqlMapper.IDynamicParameters
        {
            private readonly SqlMapper.IDynamicParameters _parameters;
            private readonly Type _parameterType;
            private readonly Type _resultType;
            public BindingParameters(SqlMapper.IDynamicParameters parameters, Type parameterType = null, Type resultType = null)
            {
                _parameters = parameters;
                _parameterType = parameterType ?? parameters.GetType();
                _resultType = resultType;
            }
            public void AddParameters(IDbCommand command, SqlMapper.Identity identity)
            {
                if (BindingIdentityConstructor == null)
                    throw new NotSupportedException("The installed Dapper version does not expose the expected immutable Identity constructor.");
                // ForDynamicParameters changes gridIndex to -1. Construct an immutable
                // identity with the consumer's original parameter and result types instead.
                // Dapper 1 uses a type array; Dapper 2 uses its precomputed hash.
                // These commands have no additional mapped types in either version.
                var originalIdentity = (SqlMapper.Identity)BindingIdentityConstructor.Invoke(new object[]
                {
                    identity.sql, identity.commandType, identity.connectionString, _resultType,
                    _parameterType, BindingIdentityUsesTypesHash ? (object)0 : null, identity.gridIndex
                });
                _parameters.AddParameters(((ParameterBindingCommand)command).ProviderCommand, originalIdentity);
            }
        }

        private sealed class ParameterBindingConnection : IDbConnection
        {
            private readonly IDbConnection _source;
            private readonly IDbCommand _preparedCommand;
            public ParameterBindingConnection(IDbConnection source, IDbCommand preparedCommand = null)
            {
                _source = source;
                _preparedCommand = preparedCommand;
            }
            public string[] ParameterNames { get; private set; }
            public string ConnectionString { get => _source.ConnectionString; set => throw new NotSupportedException(); }
            public int ConnectionTimeout => _source.ConnectionTimeout;
            public string Database => _source.Database;
            public ConnectionState State => ConnectionState.Open;
            public IDbCommand CreateCommand() => new ParameterBindingCommand(this,
                _preparedCommand ?? _source.CreateCommand(), _preparedCommand == null);
            public IDbTransaction BeginTransaction() => throw new NotSupportedException();
            public IDbTransaction BeginTransaction(IsolationLevel level) => throw new NotSupportedException();
            public void ChangeDatabase(string name) => throw new NotSupportedException();
            public void Open() => throw new NotSupportedException();
            public void Close() => throw new NotSupportedException();
            public void Dispose() { }
            public void CaptureParameterNames(IDataParameterCollection parameters)
                => ParameterNames = parameters.Cast<IDataParameter>().Select(p => p.ParameterName).ToArray();
        }

        private sealed class ParameterBindingCommand : IDbCommand
        {
            private readonly IDbCommand _command;
            private readonly ParameterBindingConnection _bindingConnection;
            private readonly bool _disposeProviderCommand;
            private readonly IDataParameterCollection _parameters;
            private bool _bindingCompleted;
            public ParameterBindingCommand(ParameterBindingConnection connection, IDbCommand command, bool disposeProviderCommand)
            {
                Connection = connection;
                _bindingConnection = connection;
                _command = command;
                _disposeProviderCommand = disposeProviderCommand;
                _parameters = new BindingParameterCollection(command.Parameters, this);
            }
            public IDbCommand ProviderCommand => _command;
            public string CommandText { get; set; }
            public int CommandTimeout { get; set; }
            public CommandType CommandType { get; set; }
            public IDbConnection Connection { get; set; }
            public IDbTransaction Transaction { get; set; }
            public UpdateRowSource UpdatedRowSource { get => _command.UpdatedRowSource; set => _command.UpdatedRowSource = value; }
            public IDataParameterCollection Parameters => _parameters;
            public IDbDataParameter CreateParameter() => _command.CreateParameter();
            // This is deliberately a no-op. The provider command is never executed.
            public int ExecuteNonQuery()
            {
                _bindingConnection.CaptureParameterNames(_command.Parameters);
                _bindingCompleted = true;
                return 0;
            }
            public object ExecuteScalar() => throw new NotSupportedException();
            public IDataReader ExecuteReader() => throw new NotSupportedException();
            public IDataReader ExecuteReader(CommandBehavior behavior) => throw new NotSupportedException();
            public void Cancel() => throw new NotSupportedException();
            public void Prepare() => throw new NotSupportedException();
            public void Dispose() { if (_disposeProviderCommand) _command.Dispose(); }

            private sealed class BindingParameterCollection : IDataParameterCollection
            {
                private readonly IDataParameterCollection _source;
                private readonly ParameterBindingCommand _owner;
                internal BindingParameterCollection(IDataParameterCollection source, ParameterBindingCommand owner)
                { _source = source; _owner = owner; }
                public object this[string name] { get => _source[name]; set => _source[name] = value; }
                public object this[int index] { get => _source[index]; set => _source[index] = value; }
                public int Count => _source.Count;
                public bool IsFixedSize => _source.IsFixedSize;
                public bool IsReadOnly => _source.IsReadOnly;
                public bool IsSynchronized => _source.IsSynchronized;
                public object SyncRoot => _source.SyncRoot;
                public int Add(object value) => _source.Add(value);
                public void Clear()
                {
                    // Newer Dapper versions clear parameters after Execute. This command
                    // only binds; its native parameters must survive for actual execution.
                    if (!_owner._bindingCompleted) _source.Clear();
                }
                public bool Contains(string name) => _source.Contains(name);
                public bool Contains(object value) => _source.Contains(value);
                public int IndexOf(string name) => _source.IndexOf(name);
                public int IndexOf(object value) => _source.IndexOf(value);
                public void Insert(int index, object value) => _source.Insert(index, value);
                public void Remove(object value) => _source.Remove(value);
                public void RemoveAt(string name) => _source.RemoveAt(name);
                public void RemoveAt(int index) => _source.RemoveAt(index);
                public void CopyTo(Array array, int index) => _source.CopyTo(array, index);
                public IEnumerator GetEnumerator() => _source.GetEnumerator();
            }
        }

        public void ExecuteRawCommand(IDbConnection connection, IDbTransaction transaction, string commandText)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (string.IsNullOrWhiteSpace(commandText)) throw new ArgumentNullException("commandText");
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = commandText;
                command.ExecuteNonQuery();
            }
        }

        public DataTable ToDataTable<T>(IEnumerable<T> entities, IEnumerable<PropertyInfo> properties)
        {
            var dataTable = new DataTable();
            var propertyList = properties as IList<PropertyInfo> ?? properties.ToList();
            foreach (var property in propertyList)
            {
                var columnName = _queryCache.GetColumnName(property);
                var rawColumnName = columnName.Trim('[', ']');
                dataTable.Columns.Add(rawColumnName,
                    Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }
            foreach (var entity in entities)
            {
                var row = dataTable.NewRow();
                foreach (var property in propertyList)
                {
                    var columnName = _queryCache.GetColumnName(property);
                    var rawColumnName = columnName.Trim('[', ']');
                    row[rawColumnName] = property.GetValue(entity) ?? DBNull.Value;
                }
                dataTable.Rows.Add(row);
            }
            return dataTable;
        }
    }
}
