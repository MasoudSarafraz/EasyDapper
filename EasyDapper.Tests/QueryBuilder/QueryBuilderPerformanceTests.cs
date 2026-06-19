using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using EasyDapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class QueryBuilderPerformanceTests
    {
        private IQueryBuilder<T> CreateBuilder<T>() where T : class
        {
            var stub = new StubConnection();
            return new global::EasyDapper.QueryBuilder<T>(stub);
        }

        [Fact]
        public void Benchmark_SimpleWhere_10000Iterations_CompletesQuickly()
        {
            var stub = new StubConnection();
            var svc = new global::EasyDapper.DapperService(stub);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 10000; i++)
            {
                var qb = svc.Query<Person>().Where(p => p.Age > i);
                qb.BuildQuery();
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"10000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }

        [Fact]
        public void Benchmark_JoinQuery_1000Iterations_CompletesQuickly()
        {
            var stub = new StubConnection();
            var svc = new global::EasyDapper.DapperService(stub);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var qb = svc.Query<Order>()
                    .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                    .InnerJoin<Order, Product>((o, p) => o.ProductId == p.Id)
                    .Where(o => o.Id > i);
                qb.BuildQuery();
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"1000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }

        [Fact]
        public void Benchmark_SelfJoin_1000Iterations_CompletesQuickly()
        {
            var stub = new StubConnection();
            var svc = new global::EasyDapper.DapperService(stub);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var qb = svc.Query<Employee>()
                    .InnerJoin<Employee, Employee>((e, mgr) => e.ManagerId == mgr.Id);
                qb.BuildQuery();
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"1000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }

        [Fact]
        public void Benchmark_ComplexQuery_1000Iterations_CompletesQuickly()
        {
            var stub = new StubConnection();
            var svc = new global::EasyDapper.DapperService(stub);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var qb = svc.Query<Order>()
                    .InnerJoin<Order, Customer>((o, c) => o.CustomerId == c.Id)
                    .Where(o => o.Id > 0)
                    .Sum(o => o.Id, "TotalOrders")
                    .GroupBy(o => o.CustomerId)
                    .Having(o => o.CustomerId > 0)
                    .OrderBy("Customer DESC")
                    .Paging(10, 1);
                qb.BuildQuery();
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 10000,
                $"1000 iterations took {sw.ElapsedMilliseconds}ms (expected < 10000ms)");
        }

        [Fact]
        public void Benchmark_AttachUpdate_1000Iterations_CompletesQuickly()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            spy.NextExecuteNonQueryResult = 1;
            var svc = new global::EasyDapper.DapperService(spy);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var person = new Person { Id = i, Name = "Name" + i, Age = 20 + i };
                svc.Attach(person);
                person.Age = 21 + i;
                svc.Update(person);
                svc.Detach(person);
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"1000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }

        [Fact]
        public void Benchmark_GetById_10000Iterations_CompletesQuickly()
        {
            var spy = new SpyDbConnection();
            spy.Open();
            var svc = new global::EasyDapper.DapperService(spy);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 10000; i++)
            {
                svc.GetById<Person>(i);
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"10000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }

        [Fact]
        public void Benchmark_RegexCache_ParameterRename_1000Iterations()
        {
            var q1 = CreateBuilder<Person>().Where(p => p.Age > 30);
            var q2 = CreateBuilder<Person>().Where(p => p.Age < 20);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var freshQ1 = CreateBuilder<Person>().Where(p => p.Age > 30);
                var freshQ2 = CreateBuilder<Person>().Where(p => p.Age < 20);
                freshQ1.Union(freshQ2).BuildQuery();
            }
            sw.Stop();

            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"1000 iterations took {sw.ElapsedMilliseconds}ms (expected < 5000ms)");
        }
    }
}
