using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.ConnectionManager
{
    public class ConnectionManagerRegressionTests
    {
        [Fact]
        public void NestedTransaction_UsesAValidSqlServerSavepointAndRollsBackToIt()
        {
            var connection = new TransactionConnection();
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                manager.BeginTransaction();
                manager.BeginTransaction();
                var savepoint = connection.Commands[0].Substring("SAVE TRANSACTION ".Length);
                Assert.InRange(savepoint.Length, 1, 32);

                manager.RollbackTransaction();

                Assert.Equal("ROLLBACK TRANSACTION " + savepoint, connection.Commands[1]);
                Assert.Equal(1, manager.TransactionCount);
            }
        }

        [Fact]
        public void FailedSavepointRollback_PreservesDepthAndCanBeRetried()
        {
            var connection = new TransactionConnection { FailSavepointRollback = true };
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                var invalidations = 0;
                var rollbacks = 0;
                manager.TrackingInvalidated += () => invalidations++;
                manager.TransactionRolledBack += () => rollbacks++;
                manager.BeginTransaction();
                manager.BeginTransaction();

                Assert.Throws<InvalidOperationException>(() => manager.RollbackTransaction());
                Assert.Equal(2, manager.TransactionCount);
                Assert.Equal(0, invalidations);
                Assert.Equal(0, rollbacks);

                connection.FailSavepointRollback = false;
                manager.RollbackTransaction();
                Assert.Equal(1, manager.TransactionCount);
                Assert.Equal(0, invalidations);
                Assert.Equal(1, rollbacks);
                manager.CommitTransaction();
                Assert.Equal(1, connection.Transaction.CommitCalls);
            }
        }

        [Fact]
        public void FailedCommit_CleansUpAndInvalidatesTrackedSnapshotsBeforeReleasingGate()
        {
            var connection = new TransactionConnection { FailCommit = true };
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                var invalidations = 0;
                manager.TrackingInvalidated += () =>
                {
                    invalidations++;
                    Assert.True(manager.HasCurrentOperation);
                    using (manager.AcquireOperation()) { Assert.True(manager.HasCurrentOperation); }
                };
                manager.BeginTransaction();

                Assert.Throws<InvalidOperationException>(() => manager.CommitTransaction());

                Assert.Equal(0, manager.TransactionCount);
                Assert.Equal(1, invalidations);
            }
        }

        [Fact]
        public async Task ConcurrentAsyncOpening_BothCallersWaitForOneOpenToComplete()
        {
            var connection = new DelayedConnection();
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                var first = manager.GetOpenConnectionAsync();
                var second = manager.GetOpenConnectionAsync();
                try
                {
                    Assert.False(first.IsCompleted);
                    Assert.False(second.IsCompleted);
                    Assert.Equal(1, connection.OpenCalls);
                }
                finally { connection.CompleteOpen(); }

                var connections = await Task.WhenAll(first, second);
                Assert.All(connections, item => Assert.Equal(ConnectionState.Open, item.State));
                Assert.Equal(1, connection.OpenCalls);
            }
        }

        [Fact]
        public async Task SyncOpening_WaitsForAnAlreadyPendingAsyncOpen()
        {
            var connection = new DelayedConnection();
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                var first = manager.GetOpenConnectionAsync();
                var started = NewSignal();
                var second = Task.Run(() =>
                {
                    started.SetResult(true);
                    return manager.GetOpenConnection();
                });
                try
                {
                    await started.Task;
                    Assert.False(second.IsCompleted);
                    Assert.Equal(0, connection.SyncOpenCalls);
                }
                finally { connection.CompleteOpen(); }

                await Task.WhenAll(first, second);
                Assert.Equal(0, connection.SyncOpenCalls);
            }
        }

        [Fact]
        public async Task CanceledQueuedOpen_DoesNotStartAnotherOpenOrLeakGate()
        {
            var connection = new DelayedConnection();
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            using (var cancellation = new CancellationTokenSource())
            {
                var first = manager.GetOpenConnectionAsync();
                try
                {
                    var second = manager.GetOpenConnectionAsync(cancellation.Token);
                    cancellation.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
                    Assert.False(first.IsCompleted);
                }
                finally { connection.CompleteOpen(); }
                await first;
                Assert.Same(connection, await manager.GetOpenConnectionAsync());
                Assert.Equal(1, connection.OpenCalls);
            }
        }

        [Fact]
        public async Task AsyncOpening_ClosesBrokenConnectionBeforeReopening()
        {
            var connection = new DelayedConnection(ConnectionState.Broken);
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                var opening = manager.GetOpenConnectionAsync();
                try { Assert.Equal(1, connection.CloseCalls); }
                finally { connection.CompleteOpen(); }
                Assert.Equal(ConnectionState.Open, (await opening).State);
            }
        }

        [Fact]
        public async Task OperationLease_FlowsAcrossAwaitAndRestoresParentAfterNestedOperations()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            using (await manager.AcquireOperationAsync())
            {
                await Task.Yield();
                using (manager.AcquireOperation()) { Assert.True(manager.HasCurrentOperation); }
                Assert.True(manager.HasCurrentOperation);
                using (await manager.AcquireOperationAsync()) { Assert.True(manager.HasCurrentOperation); }
                Assert.True(manager.HasCurrentOperation);
            }
        }

        [Fact]
        public async Task IndependentAsyncOperations_WaitAndReleaseInOrder()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            {
                var firstEntered = NewSignal();
                var secondEntered = NewSignal();
                var releaseFirst = NewSignal();
                var releaseSecond = NewSignal();
                var first = HoldOperationAsync(manager, firstEntered, releaseFirst);
                await firstEntered.Task;
                var second = HoldOperationAsync(manager, secondEntered, releaseSecond);
                try { Assert.False(secondEntered.Task.IsCompleted); }
                finally { releaseFirst.TrySetResult(true); releaseSecond.TrySetResult(true); }

                await Task.WhenAll(first, second);
                Assert.True(secondEntered.Task.IsCompleted);
            }
        }

        [Fact]
        public async Task TransactionCommit_WaitsForTheActiveCommand()
        {
            var connection = new TransactionConnection();
            using (var manager = new global::EasyDapper.ConnectionManager(connection))
            {
                manager.BeginTransaction();
                var entered = NewSignal();
                var release = NewSignal();
                var activeOperation = HoldOperationAsync(manager, entered, release);
                await entered.Task;
                var commitStarted = NewSignal();
                var commit = Task.Run(() =>
                {
                    commitStarted.SetResult(true);
                    manager.CommitTransaction();
                });
                try
                {
                    await commitStarted.Task;
                    Assert.False(commit.IsCompleted);
                    Assert.Equal(0, connection.Transaction.CommitCalls);
                }
                finally { release.TrySetResult(true); }
                await Task.WhenAll(activeOperation, commit);
                Assert.Equal(1, connection.Transaction.CommitCalls);
            }
        }

        [Fact]
        public async Task Dispose_WaitsForTheActiveCommandAndRejectsSubsequentAcquisition()
        {
            var manager = new global::EasyDapper.ConnectionManager(new StubConnection());
            var entered = NewSignal();
            var release = NewSignal();
            var activeOperation = HoldOperationAsync(manager, entered, release);
            await entered.Task;
            var disposalStarted = NewSignal();
            var disposal = Task.Run(() =>
            {
                disposalStarted.SetResult(true);
                manager.Dispose();
            });
            try
            {
                await disposalStarted.Task;
                Assert.False(disposal.IsCompleted);
                Assert.False(manager.IsDisposed);
            }
            finally { release.TrySetResult(true); }
            await Task.WhenAll(activeOperation, disposal);
            Assert.True(manager.IsDisposed);
            Assert.Throws<ObjectDisposedException>(() => manager.AcquireOperation());
            manager.Dispose();
        }

        [Fact]
        public async Task CancellationWhileWaitingForOperation_DoesNotReleaseAnotherLease()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            using (var cancellation = new CancellationTokenSource())
            {
                var entered = NewSignal();
                var release = NewSignal();
                var activeOperation = HoldOperationAsync(manager, entered, release);
                await entered.Task;
                var canceledOperation = AcquireAndReleaseAsync(manager, cancellation.Token);
                try
                {
                    cancellation.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledOperation);
                    Assert.False(activeOperation.IsCompleted);
                }
                finally { release.TrySetResult(true); }
                await activeOperation;
                await AcquireAndReleaseAsync(manager, CancellationToken.None);
            }
        }

        [Fact]
        public void CallbackRelease_IsIdempotentAndDoesNotReleaseTheNextOperation()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            {
                var outer = manager.AcquireOperation();
                manager.ReleaseOperationForCallback();
                manager.ReleaseOperationForCallback();
                Assert.False(manager.HasCurrentOperation);
                using (manager.AcquireOperation())
                {
                    outer.Dispose();
                    Assert.True(manager.HasCurrentOperation);
                    using (manager.AcquireOperation()) { Assert.True(manager.HasCurrentOperation); }
                }
                using (manager.AcquireOperation()) { }
            }
        }

        [Fact]
        public async Task CallbackRelease_FromAsyncChildContext_AllowsNextCallerAndFinalScopeCleanup()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            {
                var outer = await manager.AcquireOperationAsync();
                await ReleaseForCallbackAsync(manager);
                Assert.False(manager.HasCurrentOperation);
                outer.Dispose();
                await AcquireAndReleaseAsync(manager, CancellationToken.None);
            }
        }

        [Fact]
        public async Task SiblingNestedOperations_AreSerializedAndKeepRootGateUntilBothFinish()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            {
                var outer = await manager.AcquireOperationAsync();
                var firstEntered = NewSignal();
                var secondEntered = NewSignal();
                var independentEntered = NewSignal();
                var releaseFirst = NewSignal();
                var releaseSecond = NewSignal();
                var releaseIndependent = NewSignal();
                var first = HoldOperationAsync(manager, firstEntered, releaseFirst);
                await firstEntered.Task;
                var second = HoldOperationAsync(manager, secondEntered, releaseSecond);
                Task independent = null;
                try
                {
                    Assert.False(secondEntered.Task.IsCompleted);
                    outer.Dispose();
                    independent = HoldOperationAsync(manager, independentEntered, releaseIndependent);
                    Assert.False(independentEntered.Task.IsCompleted);
                    releaseFirst.SetResult(true);
                    await first;
                    await secondEntered.Task;
                    Assert.False(independentEntered.Task.IsCompleted);
                }
                finally
                {
                    outer.Dispose();
                    releaseFirst.TrySetResult(true);
                    releaseSecond.TrySetResult(true);
                    releaseIndependent.TrySetResult(true);
                }
                await Task.WhenAll(first, second, independent);
                Assert.True(independentEntered.Task.IsCompleted);
            }
        }

        [Fact]
        public async Task CanceledNestedWait_DoesNotReleaseActiveSiblingOrLeakParentReservation()
        {
            using (var manager = new global::EasyDapper.ConnectionManager(new StubConnection()))
            using (var cancellation = new CancellationTokenSource())
            {
                var outer = await manager.AcquireOperationAsync();
                var entered = NewSignal();
                var release = NewSignal();
                var first = HoldOperationAsync(manager, entered, release);
                await entered.Task;
                var queued = AcquireAndReleaseAsync(manager, cancellation.Token);
                try
                {
                    cancellation.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
                    Assert.False(first.IsCompleted);
                }
                finally { outer.Dispose(); release.TrySetResult(true); }
                await first;
                await AcquireAndReleaseAsync(manager, CancellationToken.None);
            }
        }

        private static async Task ReleaseForCallbackAsync(global::EasyDapper.ConnectionManager manager)
        {
            await Task.Yield();
            manager.ReleaseOperationForCallback();
        }

        private static TaskCompletionSource<bool> NewSignal()
            => new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static async Task HoldOperationAsync(global::EasyDapper.ConnectionManager manager,
            TaskCompletionSource<bool> entered, TaskCompletionSource<bool> release)
        {
            using (await manager.AcquireOperationAsync())
            {
                entered.SetResult(true);
                await release.Task;
            }
        }

        private static async Task AcquireAndReleaseAsync(global::EasyDapper.ConnectionManager manager, CancellationToken token)
        {
            using (await manager.AcquireOperationAsync(token)) { }
        }

        private sealed class DelayedConnection : DbConnection
        {
            private readonly TaskCompletionSource<bool> _opened = NewSignal();
            private ConnectionState _state;
            public DelayedConnection(ConnectionState state = ConnectionState.Closed) { _state = state; }
            public int OpenCalls { get; private set; }
            public int SyncOpenCalls { get; private set; }
            public int CloseCalls { get; private set; }
            public override string ConnectionString { get; set; }
            public override string Database => "Delayed";
            public override string DataSource => "Delayed";
            public override string ServerVersion => "1";
            public override ConnectionState State => _state;
            public override int ConnectionTimeout => 30;
            public override void ChangeDatabase(string databaseName) { }
            public override void Close() { CloseCalls++; _state = ConnectionState.Closed; }
            public override void Open() { SyncOpenCalls++; _state = ConnectionState.Open; }
            public override Task OpenAsync(CancellationToken cancellationToken)
            {
                OpenCalls++;
                _state = ConnectionState.Connecting;
                return _opened.Task;
            }
            public void CompleteOpen() { _state = ConnectionState.Open; _opened.TrySetResult(true); }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
            protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
        }

        private sealed class TransactionConnection : IDbConnection
        {
            public string ConnectionString { get; set; }
            public int ConnectionTimeout => 30;
            public string Database => "Transaction";
            public ConnectionState State { get; private set; } = ConnectionState.Open;
            public List<string> Commands { get; } = new List<string>();
            public bool FailSavepointRollback { get; set; }
            public bool FailCommit { get; set; }
            public AuditTransaction Transaction { get; private set; }
            public IDbTransaction BeginTransaction() => Transaction = new AuditTransaction(this);
            public IDbTransaction BeginTransaction(IsolationLevel level) => BeginTransaction();
            public void ChangeDatabase(string databaseName) { }
            public void Close() { State = ConnectionState.Closed; }
            public void Open() { State = ConnectionState.Open; }
            public void Dispose() { }
            public IDbCommand CreateCommand() => new AuditCommand(this);

            public sealed class AuditTransaction : IDbTransaction
            {
                private readonly TransactionConnection _owner;
                public AuditTransaction(TransactionConnection owner) { _owner = owner; }
                public IDbConnection Connection => _owner;
                public IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
                public int CommitCalls { get; private set; }
                public void Commit()
                {
                    CommitCalls++;
                    if (_owner.FailCommit) throw new InvalidOperationException("Commit failed.");
                }
                public void Rollback() { }
                public void Dispose() { }
            }

            private sealed class AuditCommand : IDbCommand
            {
                private readonly TransactionConnection _owner;
                public AuditCommand(TransactionConnection owner) { _owner = owner; }
                public string CommandText { get; set; }
                public int CommandTimeout { get; set; }
                public CommandType CommandType { get; set; }
                public IDbConnection Connection { get; set; }
                public IDbTransaction Transaction { get; set; }
                public UpdateRowSource UpdatedRowSource { get; set; }
                public IDataParameterCollection Parameters => throw new NotSupportedException();
                public IDbDataParameter CreateParameter() => throw new NotSupportedException();
                public void Cancel() { }
                public int ExecuteNonQuery()
                {
                    _owner.Commands.Add(CommandText);
                    if (_owner.FailSavepointRollback && CommandText.StartsWith("ROLLBACK TRANSACTION ", StringComparison.Ordinal))
                        throw new InvalidOperationException("Savepoint rollback failed.");
                    return 0;
                }
                public IDataReader ExecuteReader() => throw new NotSupportedException();
                public IDataReader ExecuteReader(CommandBehavior behavior) => throw new NotSupportedException();
                public object ExecuteScalar() => throw new NotSupportedException();
                public void Prepare() { }
                public void Dispose() { }
            }
        }
    }
}
