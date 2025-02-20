using EasyDapper.Implementations;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper.Interfaces
{
    public interface IQueryBuilder<T>
    {
        IQueryBuilder<T> Where(Expression<Func<T, bool>> filter);
        IEnumerable<T> Execute();
        Task<IEnumerable<T>> ExecuteAsync();
        IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns);
        IQueryBuilder<T> Count();
        IQueryBuilder<T> OrderBy(string orderByClause);
        IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1);
        IQueryBuilder<T> InnerJoin<TJoin>(Expression<Func<T, TJoin, bool>> onCondition);
        IQueryBuilder<T> LeftJoin<TJoin>(Expression<Func<T, TJoin, bool>> onCondition);
        IQueryBuilder<T> RightJoin<TJoin>(Expression<Func<T, TJoin, bool>> onCondition);
        IQueryBuilder<T> FullJoin<TJoin>(Expression<Func<T, TJoin, bool>> onCondition);
        IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder);
        IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy);
        // Explicit Method for BuildQuery
        // به دلیل اینکه کلاس اینترنال هستش متد های outerapply و crossapply به مشکل میخوردند . بنابراین
        string BuildQuery();
    }

}