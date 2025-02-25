using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper
{
    public interface IQueryBuilder<T>
    {
        IQueryBuilder<T> Where(Expression<Func<T, bool>> filter);
        IEnumerable<T> Execute();
        IEnumerable<TResult> Execute<TResult>();
        Task<IEnumerable<T>> ExecuteAsync();
        Task<IEnumerable<TResult>> ExecuteAsync<TResult>();
        IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns);
        IQueryBuilder<T> Select<TSource>(params Expression<Func<TSource, object>>[] columns);
        IQueryBuilder<T> Count();
        IQueryBuilder<T> OrderBy(string orderByClause);
        IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1);
        IQueryBuilder<T> CustomJoin<TLeft, TRight>(string stringJoin, Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition);
        IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy);
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
        // Explicit Method for BuildQuery
        // به دلیل اینکه کلاس اینترنال هستش متد های OuterApply و CrossApply به مشکل میخوردند . بنابراین
        string BuildQuery();
    }

}