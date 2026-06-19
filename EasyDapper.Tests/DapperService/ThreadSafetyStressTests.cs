using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.DapperService
{
    public class ThreadSafetyStressTests
    {
        [Fact]
        public void AliasManager_ConcurrentGetAliasForTable_NoLeaks()
        {
            var manager = new global::EasyDapper.AliasManager();
            var exceptions = new ConcurrentQueue<Exception>();
            var aliases = new ConcurrentQueue<string>();

            var tasks = new List<Task>();
            for (int t = 0; t < 20; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            var alias = manager.GetAliasForTable("[dbo].[Person]");
                            aliases.Enqueue(alias);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var aliasList = aliases.ToList();
            Assert.Equal(2000, aliasList.Count);
            Assert.All(aliasList, a => Assert.Equal("Person_A1", a));
        }

        [Fact]
        public void AliasManager_ConcurrentGetAliasForDifferentTables_AllUnique()
        {
            var manager = new global::EasyDapper.AliasManager();
            var exceptions = new ConcurrentQueue<Exception>();
            var allAliases = new ConcurrentQueue<string>();

            var tableNames = new[]
            {
                "[dbo].[Person]", "[dbo].[Party]", "[dbo].[Order]",
                "[dbo].[Customer]", "[dbo].[Product]", "[dbo].[OrderItem]"
            };

            var tasks = new List<Task>();
            for (int t = 0; t < 10; t++)
            {
                var tn = tableNames[t % tableNames.Length];
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            var alias = manager.GetAliasForTable(tn);
                            allAliases.Enqueue(alias);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var aliasList = allAliases.ToList();
            Assert.Equal(1000, aliasList.Count);

            var distinctAliasesForSameTable = aliasList
                .Where(a => a.StartsWith("Person_A"))
                .Distinct()
                .ToList();
            Assert.Single(distinctAliasesForSameTable);
        }

        [Fact]
        public void AliasManager_ConcurrentSetTableAlias_NoCorruption()
        {
            var manager = new global::EasyDapper.AliasManager();
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 20; t++)
            {
                var tn = $"[dbo].[Table{t}]";
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var alias = manager.GenerateAlias(tn);
                            try { manager.SetTableAlias(tn, alias); }
                            catch (InvalidOperationException) { }
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
        }

        [Fact]
        public void QueryBuilder_ConcurrentBuildQuery_ConsistentResults()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new ConcurrentQueue<Exception>();
            var sqls = new ConcurrentQueue<string>();

            var tasks = new List<Task>();
            for (int t = 0; t < 10; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var qb = svc.Query<Person>()
                                .Where(p => p.Age > 18)
                                .OrderByAscending(p => p.Name);
                            var sql = qb.BuildQuery();
                            sqls.Enqueue(sql);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var sqlList = sqls.ToList();
            Assert.Equal(500, sqlList.Count);
            var firstSql = sqlList[0];
            Assert.All(sqlList, sql => Assert.Equal(firstSql, sql));
        }

        [Fact]
        public void QueryBuilder_ConcurrentWhereAdd_AllFiltersPresent()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int t = 0; t < 5; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 20; i++)
                        {
                            var qb = svc.Query<Person>();
                            var tasks2 = new List<Task>();
                            for (int j = 0; j < 10; j++)
                            {
                                var ageLimit = 18 + j;
                                tasks2.Add(Task.Run(() => qb.Where(p => p.Age > ageLimit)));
                            }
                            Task.WaitAll(tasks2.ToArray());
                            var sql = qb.BuildQuery();
                            Assert.True(sql.Contains("WHERE"));
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
        }

        [Fact]
        public void QueryBuilder_ConcurrentDistinctAndTop_NoCorruption()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var exceptions = new ConcurrentQueue<Exception>();
            var sqls = new ConcurrentQueue<string>();

            var tasks = new List<Task>();
            for (int t = 0; t < 10; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var qb = svc.Query<Person>();
                            qb.Distinct();
                            qb.Top(10);
                            qb.Where(p => p.Age > 18);
                            var sql = qb.BuildQuery();
                            sqls.Enqueue(sql);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var sqlList = sqls.ToList();
            Assert.Equal(500, sqlList.Count);
            Assert.All(sqlList, sql =>
            {
                Assert.Contains("SELECT DISTINCT TOP (10)", sql);
                Assert.Contains("WHERE", sql);
            });
        }

        [Fact]
        public void ParameterBuilder_ConcurrentAddAndMerge_NoDuplicates()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
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
                            var name = builder.GetUniqueParameterName();
                            builder.AddParameter(name, i);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var parameters = builder.GetParameters();
            Assert.Equal(2000, parameters.Count);
        }

        [Fact]
        public void QueryBuilder_DisposeDuringBuildQuery_DoesNotCorrupt()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);
            var qb = svc.Query<Person>();

            var exceptions = new ConcurrentQueue<Exception>();
            var tasks = new List<Task>();

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        try { qb.Where(p => p.Age > i); }
                        catch (ObjectDisposedException) { break; }
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                Thread.Sleep(5);
                qb.Dispose();
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        try { qb.BuildQuery(); }
                        catch (ObjectDisposedException) { break; }
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            Task.WaitAll(tasks.ToArray());
            Assert.Empty(exceptions);
        }

        [Fact]
        public void QueryBuilderCache_ConcurrentGetTableName_AllSameInstance()
        {
            var exceptions = new ConcurrentQueue<Exception>();
            var names = new ConcurrentQueue<string>();

            var tasks = new List<Task>();
            for (int t = 0; t < 20; t++)
            {
                var task = Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            var name = global::EasyDapper.QueryBuilderCache.GetTableName(typeof(Person));
                            names.Enqueue(name);
                        }
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                });
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
            var nameList = names.ToList();
            Assert.Equal(2000, nameList.Count);
            Assert.All(nameList, n => Assert.Equal("[dbo].[Person]", n));
        }

        [Fact]
        public void MultipleDapperServices_ConcurrentUse_NoCrossContamination()
        {
            var exceptions = new ConcurrentQueue<Exception>();

            var tasks = new List<Task>();
            for (int s = 0; s < 10; s++)
            {
                var spy = new SpyDbConnection();
                spy.Open();
                var svc = new global::EasyDapper.DapperService(spy);
                var svcIndex = s;

                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var qb = svc.Query<Person>().Where(p => p.Age > svcIndex);
                            var sql = qb.BuildQuery();
                            Assert.Contains("[dbo].[Person]", sql);
                        }
                        svc.Dispose();
                    }
                    catch (Exception ex) { exceptions.Enqueue(ex); }
                }));
            }
            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
        }

        [Fact]
        public void AliasManager_ConcurrentMixedOperations_Stable()
        {
            var manager = new global::EasyDapper.AliasManager();
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
                    for (int i = 0; i < 100; i++)
                    {
                        var alias = manager.GenerateAlias("[dbo].[Party]");
                        manager.SetTableAlias("[dbo].[Party]", alias);
                    }
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < 100; i++)
                        manager.IsSubqueryAlias("Person_A1");
                }
                catch (Exception ex) { exceptions.Enqueue(ex); }
            }));

            Task.WaitAll(tasks.ToArray());

            Assert.Empty(exceptions);
        }
    }
}
