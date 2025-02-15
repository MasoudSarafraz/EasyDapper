using EasyDapper.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EasyDapper.Interfaces
{
    public interface IDapperService : IDisposable
    {
        IQueryBuilder<T> CreateQueryBuilder<T>();
        IStoredProcedureExecutor<T> CreateStoredProcedureExecutor<T>();

        int Insert<T>(T entity);
        Task<int> InsertAsync<T>(T entity);

        int Update<T>(T entity);
        Task<int> UpdateAsync<T>(T entity);

        int Delete<T>(object id);
        Task<int> DeleteAsync<T>(object id);

        T GetById<T>(object id);
        Task<T> GetByIdAsync<T>(object id);

        void BeginTransaction();
        void CommitTransaction();
        void RollbackTransaction();
    }
}