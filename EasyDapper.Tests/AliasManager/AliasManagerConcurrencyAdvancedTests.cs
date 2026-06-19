using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.AliasManager
{
    public class AliasManagerConcurrencyAdvancedTests
    {
        private global::EasyDapper.AliasManager CreateManager()
            => new global::EasyDapper.AliasManager();

        [Fact]
        public void Concurrent_GetUniqueAliasForType_AllDistinct()
        {
            var manager = CreateManager();
            var aliases = new ConcurrentQueue<string>();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 20; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            var alias = manager.GetUniqueAliasForType(typeof(Employee));
                            aliases.Enqueue(alias);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var list = aliases.ToList();
            Assert.Equal(2000, list.Count);
            Assert.Equal(2000, list.Distinct().Count());
        }

        [Fact]
        public void Concurrent_GenerateAliasAndSubQueryAlias_AllDistinct()
        {
            var manager = CreateManager();
            var aliases = new ConcurrentQueue<string>();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 10; t++)
            {
                var makeSubQuery = (t % 2 == 0);
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            string alias;
                            if (makeSubQuery)
                                alias = manager.GenerateSubQueryAlias("[dbo].[Person]");
                            else
                                alias = manager.GenerateAlias("[dbo].[Person]");
                            aliases.Enqueue(alias);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var list = aliases.ToList();
            Assert.Equal(1000, list.Count);
            Assert.Equal(1000, list.Distinct().Count());
        }

        [Fact]
        public void Concurrent_SetTableAlias_NoLostUpdates()
        {
            var manager = CreateManager();
            var exceptions = new ConcurrentQueue<Exception>();
            var successCount = 0;
            var lockObj = new object();

            var tasks = new List<Task>();
            var tableNames = new[] { "[dbo].[T1]", "[dbo].[T2]", "[dbo].[T3]", "[dbo].[T4]", "[dbo].[T5]" };

            for (int t = 0; t < 20; t++)
            {
                var tn = tableNames[t % tableNames.Length];
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            try
                            {
                                var alias = manager.GenerateAlias(tn);
                                manager.SetTableAlias(tn, alias);
                                lock (lockObj) { successCount++; }
                            }
                            catch (InvalidOperationException) { }
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            Assert.True(successCount > 0);

            Assert.Equal(tableNames.Length, manager.GetAllTableAliases().Count());
        }

        [Fact]
        public void Concurrent_ReadDuringWrite_NoExceptions()
        {
            var manager = CreateManager();
            var exceptions = new ConcurrentQueue<Exception>();

            var writerTask = Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        var alias = manager.GenerateAlias("[dbo].[Person]");
                        try { manager.SetTableAlias("[dbo].[Person]", alias); }
                        catch (InvalidOperationException) { }
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            });

            var readerTasks = new List<Task>();
            for (int r = 0; r < 5; r++)
            {
                readerTasks.Add(Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 1000; i++)
                        {
                            manager.GetAliasForTable("[dbo].[Person]");
                            manager.GetAliasForType(typeof(Person));
                            manager.IsSubqueryAlias("Person_A1");
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                }));
            }

            Task.WaitAll(new[] { writerTask }.Concat(readerTasks).ToArray());

            Assert.Empty(exceptions);
        }

        [Fact]
        public void Concurrent_GetAliasForType_FirstCallWins()
        {
            var manager = CreateManager();
            var results = new ConcurrentQueue<string>();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 50; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        var alias = manager.GetAliasForType(typeof(Person));
                        results.Enqueue(alias);
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var list = results.ToList();
            Assert.Equal(50, list.Count);
            Assert.Single(list.Distinct());
        }

        [Fact]
        public void Concurrent_SetSubQueryAlias_LastWriteWins()
        {
            var manager = CreateManager();
            var exceptions = new ConcurrentQueue<Exception>();
            var aliases = new ConcurrentQueue<string>();

            var tasks = new List<Task>();
            for (int t = 0; t < 20; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var alias = manager.GenerateSubQueryAlias("[dbo].[Person]");
                            manager.SetSubQueryAlias(typeof(Person), alias);
                            aliases.Enqueue(alias);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            Assert.True(manager.TryGetSubQueryAlias(typeof(Person), out var finalAlias));
            Assert.Contains(aliases, a => a == finalAlias);
        }

        [Fact]
        public void Concurrent_ClearAliases_DuringUse_DoesNotCorrupt()
        {
            var manager = CreateManager();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 100; i++)
                    {
                        try { manager.ClearAliases(); }
                        catch { }
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        try { manager.GetAliasForTable("[dbo].[Person]"); }
                        catch (InvalidOperationException) { }
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        manager.GenerateAlias("[dbo].[Person]");
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
        }

        [Fact]
        public void Concurrent_MixedOperations_LongRunning_Stable()
        {
            var manager = CreateManager();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 500; i++)
                        manager.GetAliasForTable("[dbo].[Person]");
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 500; i++)
                        manager.GetAliasForType(typeof(Person));
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 200; i++)
                        manager.GetUniqueAliasForType(typeof(Person));
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var alias = manager.GenerateSubQueryAlias("[dbo].[Person]");
                        manager.SetSubQueryAlias(typeof(Person), alias);
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 200; i++)
                        manager.IsSubqueryAlias("Person_A1");
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 100; i++)
                        manager.GetAllTableAliases();
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            Assert.True(manager.TryGetTypeAlias(typeof(Person), out var primaryAlias));
            Assert.StartsWith("Person_A", primaryAlias);
        }

        [Fact]
        public void Concurrent_HighVolume_10000Aliases_AllUnique()
        {
            var manager = CreateManager();
            var aliases = new ConcurrentQueue<string>();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 50; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 200; i++)
                            aliases.Enqueue(manager.GenerateAlias("[dbo].[X]"));
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var list = aliases.ToList();
            Assert.Equal(10000, list.Count);
            Assert.Equal(10000, list.Distinct().Count());
        }

        [Fact]
        public void Concurrent_ManyTableNames_AllGetDistinctPrimaryAliases()
        {
            var manager = CreateManager();
            var tableAliases = new ConcurrentDictionary<string, string>();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 10; t++)
            {
                var tn = $"[dbo].[Table{t}]";
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            var alias = manager.GetAliasForTable(tn);
                            tableAliases[tn] = alias;
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            Assert.Equal(10, tableAliases.Count);
            var distinctAliases = tableAliases.Values.Distinct().ToList();
            Assert.Equal(10, distinctAliases.Count);
        }
    }
}
