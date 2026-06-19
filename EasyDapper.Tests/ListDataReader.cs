using System;
using System.Collections.Generic;
using System.Data;

namespace EasyDapper.Tests
{
    public sealed class ListDataReader<T> : IDataReader
    {
        private readonly IList<T> _rows;
        private readonly Func<T, object[]> _rowToArray;
        private readonly string[] _columnNames;
        private int _position = -1;

        public ListDataReader(IList<T> rows, string[] columnNames, Func<T, object[]> rowToArray)
        {
            _rows = rows;
            _columnNames = columnNames;
            _rowToArray = rowToArray;
        }

        public int FieldCount => _columnNames.Length;
        public object this[int i] => _rowToArray(_rows[_position])[i];
        public object this[string name] => _rowToArray(_rows[_position])[Array.IndexOf(_columnNames, name)];

        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => _rows.Count;

        public bool Read()
        {
            if (_position < _rows.Count - 1) { _position++; return true; }
            return false;
        }

        public bool NextResult() => false;
        public void Close() { }
        public void Dispose() { }
        public DataTable GetSchemaTable() => null;

        public string GetName(int i) => _columnNames[i];
        public int GetOrdinal(string name) => Array.IndexOf(_columnNames, name);
        public Type GetFieldType(int i) => typeof(object);
        public string GetDataTypeName(int i) => "object";
        public object GetValue(int i) => this[i];
        public int GetValues(object[] values)
        {
            var arr = _rowToArray(_rows[_position]);
            for (int i = 0; i < arr.Length && i < values.Length; i++) values[i] = arr[i];
            return arr.Length;
        }
        public bool IsDBNull(int i) => this[i] == null || this[i] == DBNull.Value;

        public bool GetBoolean(int i) => Convert.ToBoolean(this[i]);
        public byte GetByte(int i) => Convert.ToByte(this[i]);
        public char GetChar(int i) => Convert.ToChar(this[i]);
        public DateTime GetDateTime(int i) => Convert.ToDateTime(this[i]);
        public decimal GetDecimal(int i) => Convert.ToDecimal(this[i]);
        public double GetDouble(int i) => Convert.ToDouble(this[i]);
        public float GetFloat(int i) => Convert.ToSingle(this[i]);
        public Guid GetGuid(int i) => (Guid)this[i];
        public short GetInt16(int i) => Convert.ToInt16(this[i]);
        public int GetInt32(int i) => Convert.ToInt32(this[i]);
        public long GetInt64(int i) => Convert.ToInt64(this[i]);
        public string GetString(int i) => (string)this[i];

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => 0;
        public IDataReader GetData(int i) => null;
    }
}
