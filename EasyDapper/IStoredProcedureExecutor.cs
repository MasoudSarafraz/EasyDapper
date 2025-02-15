using System.Collections.Generic;
using System.Threading.Tasks;

namespace EasyDapper.Interfaces
{
    public interface IStoredProcedureExecutor<T>
    {
        IEnumerable<T> Execute(string procedureName, object parameters = null);
        Task<IEnumerable<T>> ExecuteAsync(string procedureName, object parameters = null);
    }
}