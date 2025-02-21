using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Dapper;


namespace ProfessionalDapperLibrary.Implementations
{
    //internal sealed class StoredProcedureExecutor<T> : IStoredProcedureExecutor<T>
    //{
    //    private readonly string _connectionString;

    //    public StoredProcedureExecutor(string connectionString)
    //    {
    //        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    //    }

    //    public IEnumerable<T> Execute(string procedureName, object parameters = null)
    //    {
    //        ValidateProcedureName(procedureName);

    //        using (var connection = new SqlConnection(_connectionString))
    //        {
    //            connection.Open();
    //            return connection.Query<T>(
    //                procedureName,
    //                param: parameters,
    //                commandType: CommandType.StoredProcedure
    //            );
    //        }
    //    }

    //    public async Task<IEnumerable<T>> ExecuteAsync(
    //        string procedureName,
    //        object parameters = null,
    //        CancellationToken cancellationToken = default)
    //    {
    //        ValidateProcedureName(procedureName);

    //        using (var connection = new SqlConnection(_connectionString))
    //        {
    //            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    //            return await connection.QueryAsync<T>(
    //                new CommandDefinition(
    //                    procedureName,
    //                    parameters,
    //                    commandType: CommandType.StoredProcedure,
    //                    cancellationToken: cancellationToken
    //                )
    //            ).ConfigureAwait(false);
    //        }
    //    }

    //    private static void ValidateProcedureName(string procedureName)
    //    {
    //        if (string.IsNullOrWhiteSpace(procedureName))
    //        {
    //            throw new ArgumentException(
    //                "Procedure name cannot be null or whitespace.",
    //                nameof(procedureName)
    //            );
    //        }
    //    }

    //}
}