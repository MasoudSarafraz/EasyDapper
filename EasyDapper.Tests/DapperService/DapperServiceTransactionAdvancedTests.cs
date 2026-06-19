using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class DapperServiceTransactionAdvancedTests
    {
        [Fact]
        public void NestedTransactions_FiveLevelsDeep_CountMatches()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                Assert.Equal(1, svc.TransactionCount());

                svc.BeginTransaction();
                Assert.Equal(2, svc.TransactionCount());

                svc.BeginTransaction();
                Assert.Equal(3, svc.TransactionCount());

                svc.BeginTransaction();
                Assert.Equal(4, svc.TransactionCount());

                svc.BeginTransaction();
                Assert.Equal(5, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(4, svc.TransactionCount());

                svc.RollbackTransaction();
                Assert.Equal(3, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(2, svc.TransactionCount());

                svc.RollbackTransaction();
                Assert.Equal(1, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void Savepoint_Rollback_PreservesOuterTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.BeginTransaction();

                svc.RollbackTransaction();
                Assert.Equal(1, svc.TransactionCount());
            }
        }

        [Fact]
        public void Savepoint_Commit_DoesNotExecuteSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.BeginTransaction();

                int countBefore = spy.ExecutedCommands.Count;
                svc.CommitTransaction();

                Assert.Equal(countBefore, spy.ExecutedCommands.Count);
            }
        }

        [Fact]
        public void BeginTransaction_CreatesSavepoint_ExecutesSaveTransactionSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                int countBefore = spy.ExecutedCommands.Count;

                svc.BeginTransaction();

                Assert.True(spy.ExecutedCommands.Count > countBefore);
                Assert.True(spy.ExecutedCommands.Any(c => c.CommandText.Contains("SAVE TRANSACTION")));
            }
        }

        [Fact]
        public void RollbackTransaction_Savepoint_ExecutesRollbackTransactionSql()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.BeginTransaction();

                svc.RollbackTransaction();

                Assert.True(spy.ExecutedCommands.Any(c => c.CommandText.Contains("ROLLBACK TRANSACTION")));
            }
        }

        [Fact]
        public void Transaction_InsertOperation_ParticipatesInTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.Insert(new Person { Name = "Test", Age = 25 });

                var insertCmd = spy.ExecutedCommands.LastOrDefault(c => c.CommandText.Contains("INSERT"));
                Assert.NotNull(insertCmd);
                Assert.NotNull(insertCmd.Transaction);
            }
        }

        [Fact]
        public void Transaction_CommitTransaction_RemovesActiveTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                Assert.Equal(1, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(0, svc.TransactionCount());

                svc.Insert(new Person { Name = "Test", Age = 25 });
                var insertCmd = spy.ExecutedCommands.Last(c => c.CommandText.Contains("INSERT"));
                Assert.Null(insertCmd.Transaction);
            }
        }

        [Fact]
        public void Transaction_RollbackTransaction_RemovesActiveTransaction()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.RollbackTransaction();

                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void MultipleBeginRollbackCycles_StateRemainsConsistent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                for (int i = 0; i < 10; i++)
                {
                    svc.BeginTransaction();
                    Assert.Equal(1, svc.TransactionCount());
                    svc.RollbackTransaction();
                    Assert.Equal(0, svc.TransactionCount());
                }
            }
        }

        [Fact]
        public void MultipleBeginCommitCycles_StateRemainsConsistent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                for (int i = 0; i < 10; i++)
                {
                    svc.BeginTransaction();
                    svc.CommitTransaction();
                }
                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void Begin_Rollback_Begin_Commit_StateConsistent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.RollbackTransaction();

                svc.BeginTransaction();
                svc.CommitTransaction();

                Assert.Equal(0, svc.TransactionCount());
            }
        }

        [Fact]
        public void Commit_AfterRollback_Throws()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.RollbackTransaction();

                Assert.Throws<InvalidOperationException>(() => svc.CommitTransaction());
            }
        }

        [Fact]
        public void Rollback_AfterCommit_Throws()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.CommitTransaction();

                Assert.Throws<InvalidOperationException>(() => svc.RollbackTransaction());
            }
        }

        [Fact]
        public void Dispose_WithActiveTransaction_CountBecomesZero()
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
        public void TransactionCount_AfterDispose_ReturnsZero()
        {
            var svc = new global::EasyDapper.DapperService(new SpyDbConnection());
            svc.Dispose();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void BeginTransaction_ThenInsert_ThenRollback_TransactionParticipates()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteScalarResult = 1;
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                var person = new Person { Name = "Test", Age = 25 };
                svc.Insert(person);
                svc.RollbackTransaction();

                var insertCmd = spy.ExecutedCommands.FirstOrDefault(c => c.CommandText.Contains("INSERT"));
                Assert.NotNull(insertCmd);
                Assert.NotNull(insertCmd.Transaction);
            }
        }

        [Fact]
        public void NestedSavepoint_RollbackOnlyAffectsThatSavepoint()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            using (var svc = new global::EasyDapper.DapperService(spy))
            {
                svc.BeginTransaction();
                svc.BeginTransaction();
                svc.BeginTransaction();

                Assert.Equal(3, svc.TransactionCount());

                svc.RollbackTransaction();
                Assert.Equal(2, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(1, svc.TransactionCount());

                svc.CommitTransaction();
                Assert.Equal(0, svc.TransactionCount());
            }
        }
    }
}
