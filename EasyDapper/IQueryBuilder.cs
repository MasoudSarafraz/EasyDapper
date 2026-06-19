using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper
{
    /// <summary>
    /// A fluent, LINQ-style query builder for SQL Server. Instances are obtained via
    /// <see cref="IDapperService.Query{T}"/> and are bound to the originating service's
    /// connection and transaction.
    /// </summary>
    /// <typeparam name="T">The entity type that the FROM clause targets.</typeparam>
    public interface IQueryBuilder<T> : IDisposable
    {
        IQueryBuilder<T> Where(Expression<Func<T, bool>> filter);
        IEnumerable<T> Execute();
        IEnumerable<TResult> Execute<TResult>();
        Task<IEnumerable<T>> ExecuteAsync();
        Task<IEnumerable<TResult>> ExecuteAsync<TResult>();
        IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns);
        IQueryBuilder<T> Select<TSource>(params Expression<Func<TSource, object>>[] columns);
        IQueryBuilder<T> Select(Expression<Func<T, object>> columns);
        IQueryBuilder<T> Select<TSource>(Expression<Func<TSource, object>> columns);
        IQueryBuilder<T> Count();
        IQueryBuilder<T> OrderBy(string orderByClause);
        IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector);
        IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector);
        IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1);
        IQueryBuilder<T> CustomJoin<TLeft, TRight>(string stringJoin, Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber");
        IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null);
        IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null);
        IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null);
        IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null);
        IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null);
        IQueryBuilder<T> GroupBy(params Expression<Func<T, object>>[] groupByColumns);
        IQueryBuilder<T> Having(Expression<Func<T, bool>> havingCondition);
        IQueryBuilder<T> Top(int count);
        IQueryBuilder<T> Union(IQueryBuilder<T> queryBuilder);
        IQueryBuilder<T> UnionAll(IQueryBuilder<T> queryBuilder);
        IQueryBuilder<T> Intersect(IQueryBuilder<T> queryBuilder);
        IQueryBuilder<T> Except(IQueryBuilder<T> queryBuilder);

        /// <summary>
        /// Adds the SQL <c>DISTINCT</c> keyword to the SELECT clause. Has no effect on already-
        /// rendered clauses added before this call.
        /// </summary>
        IQueryBuilder<T> Distinct();

        /// <summary>
        /// Renders the accumulated clauses to a SQL string. Useful for diagnostics and for
        /// executing the query through other database helpers.
        /// </summary>
        string BuildQuery();

        /// <summary>
        /// Equivalent to <see cref="BuildQuery"/>. Kept for backwards compatibility with callers
        /// that learned the older name from earlier documentation.
        /// </summary>
        string GetRawSql();
    }
}
