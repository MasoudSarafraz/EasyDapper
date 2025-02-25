using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace EasyDapper
{
    public interface IDapperService : IDisposable
    {
        IQueryBuilder<T> Query<T>();
        //IStoredProcedureExecutor<T> CreateStoredProcedureExecutor<T>();
        int Insert<T>(T entity) where T : class;
        Task<int> InsertAsync<T>(T entity) where T : class;
        int InsertList<T>(IEnumerable<T> entities) where T : class;
        Task<int> InsertListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class;
        int Update<T>(T entity) where T : class;
        Task<int> UpdateAsync<T>(T entity) where T : class;
        int UpdateList<T>(IEnumerable<T> entities) where T : class;
        Task<int> UpdateListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class;
        int Delete<T>(T entity) where T : class;
        Task<int> DeleteAsync<T>(T entity) where T : class;
        int DeleteList<T>(IEnumerable<T> entities) where T : class;
        Task<int> DeleteListAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class;
        T GetById<T>(object Id) where T : class;
        Task<T> GetByIdAsync<T>(object Id) where T : class;
        T GetById<T>(T entity) where T : class;
        Task<T> GetByIdAsync<T>(T entity) where T : class;
        void BeginTransaction();
        void CommitTransaction();
        void RollbackTransaction();
        int TransactionCount();
        IEnumerable<T> ExecuteStoredProcedure<T>(string procedureName, object parameters = null) where T : class;
        Task<IEnumerable<T>> ExecuteStoredProcedureAsync<T>(string procedureName, object parameters = null, CancellationToken cancellationToken = default) where T : class;
        T ExecuteMultiResultStoredProcedure<T>(string procedureName, Func<SqlMapper.GridReader, T> mapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null) where T : class;
        Task<T> ExecuteMultiResultStoredProcedureAsync<T>(string procedureName, Func<SqlMapper.GridReader, Task<T>> asyncMapper, object parameters = null, IDbTransaction transaction = null, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class;
    }
}