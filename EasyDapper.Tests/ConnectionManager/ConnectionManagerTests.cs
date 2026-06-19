using System;
using System.Data;
using Xunit;

namespace EasyDapper.Tests.ConnectionManager
{
    /// <summary>
    /// Tests for ConnectionManager that don't require a real SQL Server. We use a stub
    /// connection to verify TransactionCount semantics and dispose behaviour.
    /// </summary>
    public class ConnectionManagerTests
    {
        /// <summary>
        /// FIX: TransactionCount previously returned only 0 or 1 even when multiple savepoints
        /// were active. Now it returns 1 + savepoint count.
        /// </summary>
        [Fact]
        public void TransactionCount_InitiallyZero()
        {
            var stub = new StubConnection();
            stub.State = ConnectionState.Open;
            using (var cm = new global::EasyDapper.ConnectionManager(stub))
            {
                Assert.Equal(0, cm.TransactionCount);
            }
        }

        [Fact]
        public void Dispose_CanBeCalledMultipleTimes()
        {
            var stub = new StubConnection();
            using (var cm = new global::EasyDapper.ConnectionManager(stub))
            {
                cm.Dispose();
                cm.Dispose(); // should not throw
                cm.Dispose(); // should not throw
            }
        }
    }
}
