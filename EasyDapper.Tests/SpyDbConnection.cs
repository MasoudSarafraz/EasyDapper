using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace EasyDapper.Tests
{
    public sealed class SpyDbConnection : IDbConnection
    {
        public sealed class SpyDbCommand : IDbCommand
        {
            private readonly SpyDbConnection _owner;
            public SpyDbCommand(SpyDbConnection owner) { _owner = owner; }
            public string CommandText { get; set; }
            public int CommandTimeout { get; set; }
            public CommandType CommandType { get; set; }
            public IDbConnection Connection { get; set; }
            public IDbTransaction Transaction { get; set; }
            public UpdateRowSource UpdatedRowSource { get; set; }
            private readonly List<IDbDataParameter> _parameters = new List<IDbDataParameter>();
            public IDataParameterCollection Parameters => new SpyParameterCollection(_parameters);

            public void Cancel() { }
            public IDbDataParameter CreateParameter() => new SpyDbDataParameter();
            public int ExecuteNonQuery()
            {
                _owner.ExecutedCommands.Add(this);
                return _owner.NextExecuteNonQueryResult;
            }
            public IDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);
            public IDataReader ExecuteReader(CommandBehavior behavior)
            {
                _owner.ExecutedCommands.Add(this);
                return _owner.NextDataReader ?? new EmptyDataReader();
            }
            public object ExecuteScalar()
            {
                _owner.ExecutedCommands.Add(this);
                return _owner.NextExecuteScalarResult;
            }
            public void Prepare() { }
            public void Dispose() { }

            public IDictionary<string, object> GetParameterSnapshot()
            {
                var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                foreach (var p in _parameters)
                {
                    if (!string.IsNullOrEmpty(p.ParameterName)) dict[p.ParameterName] = p.Value;
                }
                return dict;
            }
        }

        private sealed class SpyDbDataParameter : IDbDataParameter
        {
            public DbType DbType { get; set; }
            public ParameterDirection Direction { get; set; }
            public bool IsNullable { get; set; }
            public string ParameterName { get; set; }
            public int Size { get; set; }
            public string SourceColumn { get; set; }
            public DataRowVersion SourceVersion { get; set; }
            public object Value { get; set; }
            public byte Precision { get; set; }
            public byte Scale { get; set; }
        }

        private sealed class SpyParameterCollection : IDataParameterCollection
        {
            private readonly List<IDbDataParameter> _parameters;
            public SpyParameterCollection(List<IDbDataParameter> parameters) { _parameters = parameters; }
            public int Add(object value) { _parameters.Add((IDbDataParameter)value); return _parameters.Count - 1; }
            public void Clear() { _parameters.Clear(); }
            public bool Contains(object value) => _parameters.Contains(value);
            public bool Contains(string parameterName) => _parameters.Any(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
            public int IndexOf(object value) => _parameters.IndexOf((IDbDataParameter)value);
            public int IndexOf(string parameterName) => _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
            public void Insert(int index, object value) { _parameters.Insert(index, (IDbDataParameter)value); }
            public void Remove(object value) { _parameters.Remove((IDbDataParameter)value); }
            public void RemoveAt(int index) { _parameters.RemoveAt(index); }
            public void RemoveAt(string parameterName) { _parameters.RemoveAll(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase)); }
            public object this[int index]
            {
                get { return _parameters[index]; }
                set { _parameters[index] = (IDbDataParameter)value; }
            }
            public object this[string parameterName]
            {
                get
                {
                    var idx = IndexOf(parameterName);
                    return idx >= 0 ? _parameters[idx] : null;
                }
                set
                {
                    _parameters.RemoveAll(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
                    var typed = value as IDbDataParameter;
                    if (typed != null) _parameters.Add(typed);
                }
            }
            public bool IsReadOnly => false;
            public bool IsFixedSize => false;
            public bool IsSynchronized => false;
            public object SyncRoot => this;
            public int Count => _parameters.Count;
            public void CopyTo(Array array, int index) { ((ICollection)_parameters).CopyTo(array, index); }
            IEnumerator IEnumerable.GetEnumerator() => _parameters.GetEnumerator();
        }

        public sealed class EmptyDataReader : IDataReader
        {
            public int Depth => 0;
            public bool IsClosed => true;
            public int RecordsAffected => 0;
            public int FieldCount => 0;
            public object this[int i] => null;
            public object this[string name] => null;
            public void Close() { }
            public void Dispose() { }
            public bool GetBoolean(int i) => false;
            public byte GetByte(int i) => 0;
            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
            public char GetChar(int i) => '\0';
            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => 0;
            public IDataReader GetData(int i) => null;
            public string GetDataTypeName(int i) => null;
            public DateTime GetDateTime(int i) => default(DateTime);
            public decimal GetDecimal(int i) => 0;
            public double GetDouble(int i) => 0;
            public Type GetFieldType(int i) => typeof(object);
            public float GetFloat(int i) => 0;
            public Guid GetGuid(int i) => Guid.Empty;
            public short GetInt16(int i) => 0;
            public int GetInt32(int i) => 0;
            public long GetInt64(int i) => 0;
            public string GetName(int i) => null;
            public int GetOrdinal(string name) => -1;
            public string GetString(int i) => null;
            public object GetValue(int i) => null;
            public int GetValues(object[] values) => 0;
            public bool IsDBNull(int i) => true;
            public bool NextResult() => false;
            public bool Read() => false;
            public DataTable GetSchemaTable() => null;
        }

        public string ConnectionString { get; set; }
        public int ConnectionTimeout => 30;
        public string Database => "Spy";
        public ConnectionState State { get; set; } = ConnectionState.Closed;
        public bool WasOpened { get; private set; }

        public List<SpyDbCommand> ExecutedCommands { get; } = new List<SpyDbCommand>();
        public int NextExecuteNonQueryResult { get; set; } = 1;
        public object NextExecuteScalarResult { get; set; } = 1;
        public IDataReader NextDataReader { get; set; }

        public IDbTransaction BeginTransaction() => new SpyDbTransaction(this);
        public IDbTransaction BeginTransaction(IsolationLevel il) => new SpyDbTransaction(this);
        public void ChangeDatabase(string databaseName) { }
        public void Close() => State = ConnectionState.Closed;
        public IDbCommand CreateCommand() => new SpyDbCommand(this);
        public void Open()
        {
            WasOpened = true;
            State = ConnectionState.Open;
        }
        public void Dispose() => State = ConnectionState.Closed;

        public string LastCommandText => ExecutedCommands.Count > 0 ? ExecutedCommands[ExecutedCommands.Count - 1].CommandText : null;

        public SpyDbCommand LastCommand => ExecutedCommands.Count > 0 ? ExecutedCommands[ExecutedCommands.Count - 1] : null;

        public void Reset()
        {
            ExecutedCommands.Clear();
            NextExecuteNonQueryResult = 1;
            NextExecuteScalarResult = 1;
            NextDataReader = null;
        }
    }

    internal sealed class SpyDbTransaction : IDbTransaction
    {
        private readonly SpyDbConnection _connection;
        public SpyDbTransaction(SpyDbConnection connection) { _connection = connection; }
        public IDbConnection Connection => _connection;
        public IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        public void Commit() { }
        public void Rollback() { }
        public void Dispose() { }
    }
}
