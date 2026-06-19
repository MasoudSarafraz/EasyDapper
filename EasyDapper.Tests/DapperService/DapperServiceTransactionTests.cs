using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceTransactionTests
    {
        private global::EasyDapper.DapperService CreateService()
            => new global::EasyDapper.DapperService(new SpyDbConnection());

        [Fact]
        public void TransactionCount_InitiallyZero()
        {
            using (var svc = CreateService())
            {
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void BeginTransaction_IncrementsCountToOne()
        {
            using (var svc = CreateService())
            {
                svc.BeginTransaction();
                Assert.Equal(1, svc.TransactionCount());
            }
        }

        [Fact]
        public void MultipleBeginTransactions_IncrementCountToN()
        {
            using (var svc = CreateService())
            {
                svc.BeginTransaction();
                svc.BeginTransaction();
                svc.BeginTransaction();
                Assert.Equal(3, svc.TransactionCount());
            }
        }

        [Fact]
        public void CommitTransaction_DecrementsCount()
        {
            using (var svc = CreateService())
            {
                svc.BeginTransaction();
                svc.BeginTransaction();
                svc.BeginTransaction();
                Assert.Equal(3, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(2, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(1, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void RollbackTransaction_DecrementsCount()
        {
            using (var svc = CreateService())
            {
                svc.BeginTransaction();
                svc.BeginTransaction();
                svc.BeginTransaction();

                svc.RollbackTransaction();
                Assert.Equal(2, svc.TransactionCount());
            }
        }

        [Fact]
        public void CommitTransaction_WithoutBegin_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<InvalidOperationException>(() => svc.CommitTransaction());
            }
        }

        [Fact]
        public void RollbackTransaction_WithoutBegin_Throws()
        {
            using (var svc = CreateService())
            {
                Assert.Throws<InvalidOperationException>(() => svc.RollbackTransaction());
            }
        }

        [Fact]
        public void BeginTransaction_OpensConnection_WhenConnectionIsClosed()
        {
            var spy = new SpyDbConnection();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                Assert.True(spy.WasOpened);
                Assert.Equal(ConnectionState.Open, spy.State);
            }
        }

        [Fact]
        public void BeginTransaction_WithPreOpenedExternalConnection_DoesNotReopen()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                Assert.True(spy.WasOpened);
                Assert.Equal(ConnectionState.Open, spy.State);
            }
        }

        [Fact]
        public void Dispose_WithActiveTransaction_RollsBack()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
            }
        }
    }
}
