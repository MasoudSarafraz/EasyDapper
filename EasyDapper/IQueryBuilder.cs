using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace EasyDapper.Interfaces
{
    public interface IQueryBuilder<T>
    {
        IQueryBuilder<T> Where(Expression<Func<T, bool>> filter);
        IEnumerable<T> Execute();
        Task<IEnumerable<T>> ExecuteAsync();
    }
}