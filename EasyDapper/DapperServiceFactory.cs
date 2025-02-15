using System.Data;
using EasyDapper.Implementations;
using EasyDapper.Interfaces;

namespace EasyDapper.Factories
{
    public static class DapperServiceFactory
    {
        public static IDapperService Create(string connectionString)
        {
            return new DapperService(connectionString);
        }
        public static IDapperService Create(IDbConnection externalConnection)
        {
            return new DapperService(externalConnection);
        }
    }
}