using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace EasyDapper.Tests
{
    public sealed class StubConnection : IDbConnection
    {
        public string ConnectionString { get; set; }
        public int ConnectionTimeout => 30;
        public string Database => "Stub";
        public ConnectionState State { get; set; } = ConnectionState.Closed;
        public bool WasOpened { get; private set; }

        public IDbTransaction BeginTransaction() => throw new NotSupportedException();
        public IDbTransaction BeginTransaction(IsolationLevel il) => throw new NotSupportedException();
        public void ChangeDatabase(string databaseName) => throw new NotSupportedException();
        public void Close() => State = ConnectionState.Closed;
        public IDbCommand CreateCommand() => throw new NotSupportedException();
        public void Open()
        {
            WasOpened = true;
            State = ConnectionState.Open;
        }
        public void Dispose() => State = ConnectionState.Closed;
    }
}
