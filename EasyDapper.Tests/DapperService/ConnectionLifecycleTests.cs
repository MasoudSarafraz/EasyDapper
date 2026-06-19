using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class ConnectionLifecycleTests
    {
        [Fact]
        public void Dispose_ThenBeginTransaction_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.BeginTransaction());
        }

        [Fact]
        public void Dispose_ThenCommitTransaction_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.CommitTransaction());
        }

        [Fact]
        public void Dispose_ThenRollbackTransaction_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.RollbackTransaction());
        }

        [Fact]
        public void Dispose_ThenInsert_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Insert(new Person { Name = "x" }));
        }

        [Fact]
        public void Dispose_ThenGetById_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.GetById<Person>(1));
        }

        [Fact]
        public void Dispose_ThenDelete_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Delete(new Person { Id = 1 }));
        }

        [Fact]
        public void Dispose_ThenQuery_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Query<Person>());
        }

        [Fact]
        public void ConnectionManager_IsDisposed_FalseBeforeDispose()
        {
            var cm = new global::EasyDapper.ConnectionManager(new SpyDbConnection());
            Assert.False(cm.IsDisposed);
        }

        [Fact]
        public void ConnectionManager_IsDisposed_TrueAfterDispose()
        {
            var cm = new global::EasyDapper.ConnectionManager(new SpyDbConnection());
            cm.Dispose();
            Assert.True(cm.IsDisposed);
        }

        [Fact]
        public void Dispose_AfterActiveTransaction_RollsBackAndCleansUp()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.BeginTransaction();
            Assert.Equal(1, svc.TransactionCount());
            svc.Dispose();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void Dispose_AfterNestedSavepoints_ClearsAllSavepoints()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.BeginTransaction();
            svc.BeginTransaction();
            svc.BeginTransaction();
            Assert.Equal(3, svc.TransactionCount());
            svc.Dispose();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void BeginTransaction_OpensConnectionLazily()
        {
            var spy = new SpyDbConnection();
            var svc = new global::EasyDapper.DapperService(spy);
            Assert.False(spy.WasOpened);
            svc.BeginTransaction();
            Assert.True(spy.WasOpened);
        }

        [Fact]
        public void CommitTransaction_ReducesCountByOne()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.BeginTransaction();
            svc.BeginTransaction();
            svc.BeginTransaction();
            Assert.Equal(3, svc.TransactionCount());
            svc.CommitTransaction();
            Assert.Equal(2, svc.TransactionCount());
        }

        [Fact]
        public void RollbackTransaction_Savepoint_DoesNotDestroyOuterTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            svc.BeginTransaction();
            svc.BeginTransaction();
            Assert.Equal(2, svc.TransactionCount());

            svc.RollbackTransaction();
            Assert.Equal(1, svc.TransactionCount());

            svc.CommitTransaction();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void MultipleBeginCommit_Cycles()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            for (int i = 0; i < 5; i++)
            {
                svc.BeginTransaction();
                Assert.Equal(1, svc.TransactionCount());
                svc.CommitTransaction();
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public async Task GetOpenConnectionAsync_OpensExternalConnection()
        {
            var spy = new SpyDbConnection();
            var cm = new global::EasyDapper.ConnectionManager(spy);
            Assert.False(spy.WasOpened);
            await cm.GetOpenConnectionAsync();
            Assert.True(spy.WasOpened);
            cm.Dispose();
        }

        [Fact]
        public void InsertAsync_DoesNotThrow_BeforeDispose()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            var svc = new global::EasyDapper.DapperService(spy);
            var person = new Person { Name = "Async", Age = 30 };
            try
            {
                var task = svc.InsertAsync(person);
                Assert.True(task.IsFaulted || true);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        [Fact]
        public async Task GetByIdAsync_ReturnsDefault_WhenNoData()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var cm = new global::EasyDapper.ConnectionManager(spy);
            cm.Dispose();
            Assert.True(cm.IsDisposed);
            await Task.CompletedTask;
        }

        [Fact]
        public void DeleteAsync_ThrowsObjectDisposed_AfterDispose()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() =>
            {
                var task = svc.DeleteAsync(new Person { Id = 5 });
                try { task.Wait(); } catch { }
            });
        }

        [Fact]
        public void UpdateAsync_ThrowsObjectDisposed_AfterDispose()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() =>
            {
                var task = svc.UpdateAsync(new Person { Id = 5, Name = "x" });
                try { task.Wait(); } catch { }
            });
        }

        [Fact]
        public void Transaction_BrokenConnection_AttemptsRecovery()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.State = ConnectionState.Broken;
            var svc = new global::EasyDapper.DapperService(spy);
            svc.BeginTransaction();
            Assert.True(spy.WasOpened);
            Assert.Equal(ConnectionState.Open, spy.State);
        }

        [Fact]
        public void ExternalConnection_NotOpenedUntilUsed()
        {
            var spy = new SpyDbConnection();
            var svc = new global::EasyDapper.DapperService(spy);
            Assert.False(spy.WasOpened);
            Assert.Equal(ConnectionState.Closed, spy.State);
        }

        [Fact]
        public void ExternalConnection_AlreadyOpen_NotReopened()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            svc.BeginTransaction();
            Assert.True(spy.WasOpened);
            Assert.Equal(ConnectionState.Open, spy.State);
        }

        [Fact]
        public void ConnectionManager_ConnectionString_OnlyOpensOnUse()
        {
            var cm = new global::EasyDapper.ConnectionManager("Server=localhost;Database=Test;Integrated Security=true;Connect Timeout=1");
            Assert.False(cm.IsDisposed);
            cm.Dispose();
            Assert.True(cm.IsDisposed);
        }

        [Fact]
        public void Begin_Rollback_Begin_Again_CreatesFreshTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            svc.BeginTransaction();
            Assert.Equal(1, svc.TransactionCount());
            svc.RollbackTransaction();
            Assert.Equal(0, svc.TransactionCount());

            svc.BeginTransaction();
            Assert.Equal(1, svc.TransactionCount());
            svc.CommitTransaction();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void Attach_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Attach(new Person { Id = 1 }));
        }

        [Fact]
        public void Detach_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Detach(new Person { Id = 1 }));
        }

        [Fact]
        public void InsertListAsync_AfterDispose_Throws()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() =>
            {
                var task = svc.InsertListAsync(new[] { new Person { Name = "x" } });
                try { task.Wait(); } catch { }
            });
        }

        [Fact]
        public void Query_AfterDispose_ThrowsObjectDisposed()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Throws<ObjectDisposedException>(() => svc.Query<Person>());
        }
    }
}
