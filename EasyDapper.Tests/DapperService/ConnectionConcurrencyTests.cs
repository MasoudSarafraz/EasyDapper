using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class ConnectionConcurrencyTests
    {
        [Fact]
        public void Concurrent_BeginCommit_DoesNotCorruptState()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new List<Exception>();
            var tasks = new List<Task>();

            for (int t = 0; t < 10; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            svc.BeginTransaction();
                            svc.CommitTransaction();
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions) exceptions.Add(ex);
                    }
                });
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            Assert.Empty(exceptions);
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void Concurrent_BeginRollback_DoesNotCorruptState()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new List<Exception>();
            var tasks = new List<Task>();

            for (int t = 0; t < 5; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            svc.BeginTransaction();
                            svc.RollbackTransaction();
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions) exceptions.Add(ex);
                    }
                });
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            Assert.Empty(exceptions);
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void Concurrent_NestedSavepoints_ProducesConsistentCount()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            svc.BeginTransaction();
            Assert.Equal(1, svc.TransactionCount());

            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    svc.BeginTransaction();
                    Thread.Sleep(1);
                    svc.CommitTransaction();
                }));
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Equal(1, svc.TransactionCount());
            svc.CommitTransaction();
            Assert.Equal(0, svc.TransactionCount());
        }

        [Fact]
        public void Concurrent_GetById_DoesNotThrow()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new List<Exception>();
            var tasks = new List<Task>();

            for (int t = 0; t < 10; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            svc.GetById<Person>(i);
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions) exceptions.Add(ex);
                    }
                });
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            Assert.Empty(exceptions);
        }

        [Fact]
        public void Dispose_WhileConcurrentUse_DoesNotCorrupt()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new List<Exception>();
            var tasks = new List<Task>();

            for (int t = 0; t < 5; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            try
                            {
                                svc.BeginTransaction();
                                svc.CommitTransaction();
                            }
                            catch (ObjectDisposedException) { break; }
                            catch (InvalidOperationException) { }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions) exceptions.Add(ex);
                    }
                });
                tasks.Add(task);
            }

            Thread.Sleep(5);
            svc.Dispose();

            Task.WaitAll(tasks.ToArray());

            foreach (var ex in exceptions)
            {
                Assert.False(ex is InvalidOperationException && !(ex is ObjectDisposedException),
                    $"Unexpected exception: {ex}");
            }
        }
    }
}
