using System;
using System.Data;
using Xunit;

namespace EasyDapper.Tests.ConnectionManager
{
    public class ConnectionManagerTests
    {
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
                cm.Dispose();
                cm.Dispose();
            }
        }
    }
}
