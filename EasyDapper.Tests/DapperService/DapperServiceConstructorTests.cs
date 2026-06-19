using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceConstructorTests
    {
        [Fact]
        public void Constructor_NullConnectionString_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new global::EasyDapper.DapperService((string)null));
        }

        [Fact]
        public void Constructor_EmptyConnectionString_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new global::EasyDapper.DapperService(""));
        }

        [Fact]
        public void Constructor_WhitespaceConnectionString_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new global::EasyDapper.DapperService("   "));
        }

        [Fact]
        public void Constructor_NullExternalConnection_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new global::EasyDapper.DapperService((System.Data.IDbConnection)null));
        }

        [Fact]
        public void Constructor_WithConnectionString_InitializesService()
        {
            using (var svc = new global::EasyDapper.DapperService("Server=localhost;Database=Test;Integrated Security=true"))
            {
                Assert.NotNull(svc);
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void Constructor_WithExternalConnection_DoesNotOpenConnection()
        {
            var spy = new SpyDbConnection();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                Assert.False(spy.WasOpened, "Constructor must not open external connection");
                Assert.Equal(ConnectionState.Closed, spy.State);
            }
        }

        [Fact]
        public void Constructor_ConnectionString_DoesNotOpenConnection()
        {
            using (var svc = new global::EasyDapper.DapperService("Server=localhost;Database=Test;Integrated Security=true;Connect Timeout=1"))
            {
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void Dispose_CanBeCalledMultipleTimes()
        {
            var spy = new SpyDbConnection();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.Dispose();
            svc.Dispose();
            svc.Dispose();
        }

        [Fact]
        public void Dispose_DoesNotCloseExternalConnection()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.Dispose();
            Assert.Equal(ConnectionState.Open, spy.State);
        }
    }
}
