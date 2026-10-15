using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal class BulkOperations
    {
        private readonly ConnectionManager _connectionManager;
        private readonly QueryCache _queryCache;
        private readonly SqlBuilder _sqlBuilder;
        private const int DEFAULT_BATCH_SIZE = 100;

        public BulkOperations(ConnectionManager connectionManager, QueryCache queryCache, SqlBuilder sqlBuilder)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (queryCache == null) throw new ArgumentNullException("queryCache");
            if (sqlBuilder == null) throw new ArgumentNullException("sqlBuilder");
            _connectionManager = connectionManager;
            _queryCache = queryCache;
            _sqlBuilder = sqlBuilder;
        }

        public int InsertList<T>(IEnumerable<T> entities, bool generateIdentities = false) where T : class
        {
            using (var prepared = PrepareInsert(entities, generateIdentities))
            {
                var affected = ExecutePrepared(prepared);
                CompleteInsert(prepared);
                return affected;
            }
        }

        public async Task<int> InsertListAsync<T>(IEnumerable<T> entities, bool generateIdentities = false, CancellationToken cancellationToken = default) where T : class
        {
            cancellationToken.ThrowIfCancellationRequested();
            using (var prepared = PrepareInsert(entities, generateIdentities))
            {
                var affected = await ExecutePreparedAsync(prepared, cancellationToken).ConfigureAwait(false);
                CompleteInsert(prepared);
                return affected;
            }
        }

        internal PreparedInsert<T> PrepareInsert<T>(IEnumerable<T> entities, bool generateIdentities) where T : class
        {
            if (entities == null) throw new ArgumentNullException("entities");
            var entityList = entities.ToList();
            if (entityList.Count == 0) return new PreparedInsert<T>(entityList, null, null, null, false);
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>().ToList();
            var dataTable = _sqlBuilder.ToDataTable(entityList, properties);
            return new PreparedInsert<T>(entityList, properties, dataTable, identityProp,
                generateIdentities && identityProp != null);
        }

        internal sealed class PreparedInsert<T> : IDisposable where T : class
        {
            internal readonly IList<T> Entities;
            internal readonly IList<PropertyInfo> Properties;
            internal readonly DataTable Table;
            internal readonly PropertyInfo IdentityProperty;
            internal readonly bool GenerateIdentities;
            internal List<object> Identities;

            internal PreparedInsert(IList<T> entities, IList<PropertyInfo> properties, DataTable table,
                PropertyInfo identityProperty, bool generateIdentities)
            {
                Entities = entities;
                Properties = properties;
                Table = table;
                IdentityProperty = identityProperty;
                GenerateIdentities = generateIdentities;
            }

            public void Dispose() => Table?.Dispose();
        }

        internal int ExecutePrepared<T>(PreparedInsert<T> prepared) where T : class
        {
            if (prepared.Entities.Count == 0) return 0;
            if (prepared.GenerateIdentities) prepared.Identities = InsertBulkCopyWithIdentity(prepared);
            else InsertBulkCopy(prepared);
            return prepared.Entities.Count;
        }

        internal async Task<int> ExecutePreparedAsync<T>(PreparedInsert<T> prepared, CancellationToken cancellationToken) where T : class
        {
            if (prepared.Entities.Count == 0) return 0;
            var connection = (SqlConnection)await _connectionManager.GetOpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            using (await _connectionManager.AcquirePreparedOperationAsync(cancellationToken).ConfigureAwait(false))
            {
                if (prepared.GenerateIdentities)
                    prepared.Identities = await InsertBulkCopyWithIdentityAsync(prepared, connection, cancellationToken).ConfigureAwait(false);
                else await InsertBulkCopyAsync(prepared, connection, cancellationToken).ConfigureAwait(false);
                return prepared.Entities.Count;
            }
        }

        internal void CompleteInsert<T>(PreparedInsert<T> prepared) where T : class
        {
            if (prepared.Identities != null && prepared.IdentityProperty.CanWrite)
                SetIdentityValues(prepared.Entities, prepared.Identities, prepared.IdentityProperty);
        }

        private void InsertBulkCopy<T>(PreparedInsert<T> prepared) where T : class
        {
            var properties = prepared.Properties;
            var dataTable = prepared.Table;
            var connection = (SqlConnection)_connectionManager.GetOpenConnection();
            var transaction = (SqlTransaction)_connectionManager.CurrentTransaction;
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                ConfigureBulkCopy<T>(bulkCopy, dataTable, properties);
                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task InsertBulkCopyAsync<T>(PreparedInsert<T> prepared, SqlConnection connection, CancellationToken cancellationToken) where T : class
        {
            var properties = prepared.Properties;
            var dataTable = prepared.Table;
            var transaction = (SqlTransaction)_connectionManager.CurrentTransaction;
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                ConfigureBulkCopy<T>(bulkCopy, dataTable, properties);
                await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
            }
        }

        private void ConfigureBulkCopy<T>(SqlBulkCopy bulkCopy, DataTable dataTable, IEnumerable<PropertyInfo> properties) where T : class
        {
            var propertyList = properties as IList<PropertyInfo> ?? properties.ToList();
            bulkCopy.DestinationTableName = _queryCache.GetTableName<T>();
            bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
            bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
            foreach (DataColumn column in dataTable.Columns)
            {
                var property = propertyList.FirstOrDefault(p =>
                {
                    var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                    return dbColumnName == column.ColumnName;
                });
                if (property != null)
                    bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
            }
        }

        private static void SetIdentityValues<T>(IList<T> entities, List<object> identities, PropertyInfo identityProp) where T : class
        {
            if (entities.Count != identities.Count)
                throw new InvalidOperationException("The number of generated identities does not match the number of inserted entities.");
            var identityType = Nullable.GetUnderlyingType(identityProp.PropertyType) ?? identityProp.PropertyType;
            for (int i = 0; i < entities.Count; i++)
            {
                try
                {
                    object convertedValue = Convert.ChangeType(identities[i], identityType);
                    identityProp.SetValue(entities[i], convertedValue);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Failed to set identity value for entity at index {i}. " +
                        $"Identity value: {identities[i]}, Target type: {identityProp.PropertyType}", ex);
                }
            }
        }

        private List<object> InsertBulkCopyWithIdentity<T>(PreparedInsert<T> prepared) where T : class
        {
            var tempTableName = $"#Temp_{Guid.NewGuid():N}";
            var orderColumnName = $"Order_{Guid.NewGuid():N}";
            var identities = new List<object>();
            var connection = (SqlConnection)_connectionManager.GetOpenConnection();
            try
            {
                CreateTempTableWithOrderColumn<T>(tempTableName, orderColumnName, connection);
                BulkCopyToTempTableWithOrder(prepared, tempTableName, orderColumnName, connection);
                identities = InsertFromTempAndRetrieveIdentitiesWithOrder<T>(tempTableName, orderColumnName, connection, prepared.Entities.Count);
            }
            finally { DropTempTable(tempTableName, connection); }
            return identities;
        }

        private async Task<List<object>> InsertBulkCopyWithIdentityAsync<T>(PreparedInsert<T> prepared, SqlConnection connection, CancellationToken cancellationToken) where T : class
        {
            var tempTableName = $"#Temp_{Guid.NewGuid():N}";
            var orderColumnName = $"Order_{Guid.NewGuid():N}";
            var identities = new List<object>();
            try
            {
                await CreateTempTableWithOrderColumnAsync<T>(tempTableName, orderColumnName, connection, cancellationToken).ConfigureAwait(false);
                await BulkCopyToTempTableWithOrderAsync(prepared, tempTableName, orderColumnName, connection, cancellationToken).ConfigureAwait(false);
                identities = await InsertFromTempAndRetrieveIdentitiesWithOrderAsync<T>(tempTableName, orderColumnName, connection, prepared.Entities.Count, cancellationToken).ConfigureAwait(false);
            }
            finally { await DropTempTableAsync(tempTableName, connection, CancellationToken.None).ConfigureAwait(false); }
            return identities;
        }

        private void CreateTempTableWithOrderColumn<T>(string tempTableName, string orderColumnName, SqlConnection connection)
        {
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var projection = string.IsNullOrEmpty(columns) ? "" : columns + ", ";
            var createTempTableQuery = $"SELECT TOP 0 {projection}0 AS [{orderColumnName}] INTO {tempTableName} FROM {_queryCache.GetTableName<T>()};";
            _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, createTempTableQuery);
        }

        private async Task CreateTempTableWithOrderColumnAsync<T>(string tempTableName, string orderColumnName, SqlConnection connection, CancellationToken cancellationToken)
        {
            var properties = GetInsertProperties<T>();
            var columns = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var projection = string.IsNullOrEmpty(columns) ? "" : columns + ", ";
            var createTempTableQuery = $"SELECT TOP 0 {projection}0 AS [{orderColumnName}] INTO {tempTableName} FROM {_queryCache.GetTableName<T>()};";
            await connection.ExecuteAsync(new CommandDefinition(createTempTableQuery,
                transaction: _connectionManager.CurrentTransaction,
                cancellationToken: cancellationToken)).ConfigureAwait(false);
        }

        private void BulkCopyToTempTableWithOrder<T>(PreparedInsert<T> prepared, string tempTableName, string orderColumnName, SqlConnection connection) where T : class
        {
            var propertyList = prepared.Properties;
            var dataTable = prepared.Table;
            dataTable.Columns.Add(orderColumnName, typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++) dataTable.Rows[i][orderColumnName] = i;
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = propertyList.FirstOrDefault(p =>
                    {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null)
                        bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                    else if (column.ColumnName == orderColumnName)
                        bulkCopy.ColumnMappings.Add(column.ColumnName, orderColumnName);
                }
                bulkCopy.WriteToServer(reader);
            }
        }

        private async Task BulkCopyToTempTableWithOrderAsync<T>(PreparedInsert<T> prepared, string tempTableName, string orderColumnName, SqlConnection connection, CancellationToken cancellationToken) where T : class
        {
            var propertyList = prepared.Properties;
            var dataTable = prepared.Table;
            dataTable.Columns.Add(orderColumnName, typeof(int));
            for (int i = 0; i < dataTable.Rows.Count; i++) dataTable.Rows[i][orderColumnName] = i;
            using (var reader = dataTable.CreateDataReader())
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = DEFAULT_BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = _connectionManager.CommandTimeout;
                foreach (DataColumn column in dataTable.Columns)
                {
                    var property = propertyList.FirstOrDefault(p =>
                    {
                        var dbColumnName = _queryCache.GetColumnName(p).Trim('[', ']');
                        return dbColumnName == column.ColumnName;
                    });
                    if (property != null)
                        bulkCopy.ColumnMappings.Add(column.ColumnName, _queryCache.GetColumnName(property).Trim('[', ']'));
                    else if (column.ColumnName == orderColumnName)
                        bulkCopy.ColumnMappings.Add(column.ColumnName, orderColumnName);
                }
                await bulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
            }
        }

        internal string BuildIdentityInsertQuery<T>(string tempTableName, string orderColumnName, string targetAlias = "Target") where T : class
        {
            var identityProp = _queryCache.GetIdentityProperty<T>();
            var properties = GetInsertProperties<T>();
            var tableName = _queryCache.GetTableName<T>();
            var identityColumnName = _queryCache.GetColumnName(identityProp);
            var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var valueList = string.Join(", ", properties.Select(p => $"[Source].{_queryCache.GetColumnName(p)}"));
            var insertClause = string.IsNullOrEmpty(columnList)
                ? "INSERT DEFAULT VALUES"
                : $"INSERT ({columnList}) VALUES ({valueList})";
            return $@"
        DECLARE @EasyDapperOutput TABLE ([InputOrder] INT NOT NULL PRIMARY KEY, [Id] DECIMAL(38, 0));
        MERGE {tableName} AS [{targetAlias}]
        USING {tempTableName} AS [Source] ON 1 = 0
        WHEN NOT MATCHED THEN {insertClause}
        OUTPUT [Source].[{orderColumnName}], INSERTED.{identityColumnName}
            INTO @EasyDapperOutput ([InputOrder], [Id]);
        SELECT [Id] FROM @EasyDapperOutput ORDER BY [InputOrder];";
        }

        private List<object> InsertFromTempAndRetrieveIdentitiesWithOrder<T>(string tempTableName, string orderColumnName, SqlConnection connection, int expectedCount) where T : class
        {
            if (SupportsMerge(connection))
            {
                var targetAlias = $"Target_{Guid.NewGuid():N}";
                try { return ExecuteIdentityInsertQuery(BuildIdentityInsertQuery<T>(tempTableName, orderColumnName, targetAlias), connection); }
                catch (SqlException error) when (IsUnsupportedMergeTarget<T>(error, connection, targetAlias)) { }
            }
            return ExecuteOrderedIdentityInsert<T>(tempTableName, orderColumnName, connection, expectedCount);
        }

        private List<object> ExecuteIdentityInsertQuery(string insertQuery, SqlConnection connection, SqlTransaction transaction = null)
        {
            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, transaction ?? (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read()) identities.Add(reader.GetValue(0));
                }
            }
            return identities;
        }

        private async Task<List<object>> InsertFromTempAndRetrieveIdentitiesWithOrderAsync<T>(string tempTableName, string orderColumnName, SqlConnection connection, int expectedCount, CancellationToken cancellationToken) where T : class
        {
            if (SupportsMerge(connection))
            {
                var targetAlias = $"Target_{Guid.NewGuid():N}";
                try
                {
                    return await ExecuteIdentityInsertQueryAsync(BuildIdentityInsertQuery<T>(tempTableName, orderColumnName, targetAlias), connection, cancellationToken).ConfigureAwait(false);
                }
                catch (SqlException error) when (IsUnsupportedMergeTarget<T>(error, connection, targetAlias)) { }
            }
            return await ExecuteOrderedIdentityInsertAsync<T>(tempTableName, orderColumnName, connection, expectedCount, cancellationToken).ConfigureAwait(false);
        }

        private async Task<List<object>> ExecuteIdentityInsertQueryAsync(string insertQuery, SqlConnection connection, CancellationToken cancellationToken, SqlTransaction transaction = null)
        {
            var identities = new List<object>();
            using (var command = new SqlCommand(insertQuery, connection, transaction ?? (SqlTransaction)_connectionManager.CurrentTransaction))
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        identities.Add(reader.GetValue(0));
                }
            }
            return identities;
        }

        private List<object> ExecuteOrderedIdentityInsert<T>(string tempTableName, string orderColumnName,
            SqlConnection connection, int expectedCount) where T : class
        {
            var transaction = (SqlTransaction)_connectionManager.CurrentTransaction;
            var ownsTransaction = transaction == null;
            var savepoint = "Bulk_" + Guid.NewGuid().ToString("N").Substring(0, 27);
            if (ownsTransaction) transaction = connection.BeginTransaction();
            else transaction.Save(savepoint);
            try
            {
                var identities = ExecuteIdentityInsertQuery(BuildOrderedIdentityInsertQuery<T>(tempTableName, orderColumnName), connection, transaction);
                ValidateIdentityCount(identities, expectedCount);
                if (ownsTransaction) transaction.Commit();
                return identities;
            }
            catch
            {
                RollbackOrderedInsert(transaction, ownsTransaction, savepoint);
                throw;
            }
            finally { if (ownsTransaction) transaction.Dispose(); }
        }

        private async Task<List<object>> ExecuteOrderedIdentityInsertAsync<T>(string tempTableName, string orderColumnName,
            SqlConnection connection, int expectedCount, CancellationToken cancellationToken) where T : class
        {
            var transaction = (SqlTransaction)_connectionManager.CurrentTransaction;
            var ownsTransaction = transaction == null;
            var savepoint = "Bulk_" + Guid.NewGuid().ToString("N").Substring(0, 27);
            if (ownsTransaction) transaction = connection.BeginTransaction();
            else transaction.Save(savepoint);
            try
            {
                var identities = await ExecuteIdentityInsertQueryAsync(BuildOrderedIdentityInsertQuery<T>(tempTableName, orderColumnName), connection, cancellationToken, transaction).ConfigureAwait(false);
                ValidateIdentityCount(identities, expectedCount);
                cancellationToken.ThrowIfCancellationRequested();
                if (ownsTransaction) transaction.Commit();
                return identities;
            }
            catch
            {
                RollbackOrderedInsert(transaction, ownsTransaction, savepoint);
                throw;
            }
            finally { if (ownsTransaction) transaction.Dispose(); }
        }

        private static void ValidateIdentityCount(List<object> identities, int expectedCount)
        {
            if (identities.Count != expectedCount)
                throw new InvalidOperationException("The number of generated identities does not match the number of inserted entities.");
        }

        private static void RollbackOrderedInsert(SqlTransaction transaction, bool ownsTransaction, string savepoint)
        {
            try
            {
                if (ownsTransaction) transaction.Rollback();
                else transaction.Rollback(savepoint);
            }
            catch { }
        }

        private static bool SupportsMerge(SqlConnection connection)
        {
            int majorVersion;
            return !int.TryParse(connection.ServerVersion.Split('.')[0], out majorVersion) || majorVersion >= 10;
        }

        private bool IsUnsupportedMergeTarget<T>(SqlException error, SqlConnection connection, string targetAlias) where T : class
        {
            if ((error.Number != 358 && error.Number != 10794) || !string.IsNullOrEmpty(error.Procedure)) return false;
            // A direct rule-target compilation failure identifies our unique alias.
            // A trigger's dynamic MERGE has its own alias, so it must not be retried.
            // This also works when VIEW DEFINITION is denied on the destination.
            if (error.Number == 358)
                return error.Message.IndexOf("'" + targetAlias + "'", StringComparison.Ordinal) >= 0;
            try
            {
                var tableName = _queryCache.GetTableName<T>();
                var identityColumn = _queryCache.GetColumnName(_queryCache.GetIdentityProperty<T>());
                // SNAPSHOT is a table hint reserved for memory-optimized targets.
                // A zero-row SELECT proves the outer target's kind without DML or
                // VIEW DEFINITION permissions; a disk table rejects it with 367.
                using (var probe = new SqlCommand($"SELECT TOP (0) {identityColumn} FROM {tableName} WITH (SNAPSHOT);",
                    connection, (SqlTransaction)_connectionManager.CurrentTransaction))
                    probe.ExecuteNonQuery();
                return true;
            }
            catch
            {
                // A trigger can have completed/rolled back the transaction. Preserve
                // the original failure instead of masking it with a metadata error.
                return false;
            }
        }

        private string BuildOrderedIdentityInsertQuery<T>(string tempTableName, string orderColumnName) where T : class
        {
            var properties = GetInsertProperties<T>().ToList();
            var columnList = string.Join(", ", properties.Select(p => _queryCache.GetColumnName(p)));
            var identityColumn = _queryCache.GetColumnName(_queryCache.GetIdentityProperty<T>());
            var tableName = _queryCache.GetTableName<T>();
            var output = $"OUTPUT @InputOrder, INSERTED.{identityColumn} INTO @EasyDapperOutput ([InputOrder], [Id])";
            var insert = properties.Count == 0
                ? $"INSERT INTO {tableName} {output} DEFAULT VALUES;"
                : $"INSERT INTO {tableName} ({columnList}) {output} SELECT {columnList} FROM {tempTableName} WHERE [{orderColumnName}] = @InputOrder;";
            return $@"
        DECLARE @EasyDapperOutput TABLE ([InputOrder] INT NOT NULL PRIMARY KEY, [Id] DECIMAL(38, 0));
        DECLARE @InputOrder INT;
        CREATE UNIQUE CLUSTERED INDEX [EasyDapperInputOrder] ON {tempTableName} ([{orderColumnName}]);
        DECLARE EasyDapperRows CURSOR LOCAL FAST_FORWARD FOR
            SELECT [{orderColumnName}] FROM {tempTableName} ORDER BY [{orderColumnName}];
        OPEN EasyDapperRows;
        FETCH NEXT FROM EasyDapperRows INTO @InputOrder;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            {insert}
            FETCH NEXT FROM EasyDapperRows INTO @InputOrder;
        END;
        CLOSE EasyDapperRows;
        DEALLOCATE EasyDapperRows;
        SELECT [Id] FROM @EasyDapperOutput ORDER BY [InputOrder];";
        }

        private void DropTempTable(string tempTableName, SqlConnection connection)
        {
            try { _sqlBuilder.ExecuteRawCommand(connection, _connectionManager.CurrentTransaction, $"DROP TABLE {tempTableName}"); }
            catch {  }
        }

        private async Task DropTempTableAsync(string tempTableName, SqlConnection connection, CancellationToken cancellationToken)
        {
            try
            {
                await connection.ExecuteAsync(new CommandDefinition($"DROP TABLE {tempTableName}",
                    transaction: _connectionManager.CurrentTransaction,
                    cancellationToken: cancellationToken)).ConfigureAwait(false);
            }
            catch {  }
        }

        private IEnumerable<PropertyInfo> GetInsertProperties<T>() =>
            typeof(T).GetProperties().Where(p => p.GetCustomAttribute<IdentityAttribute>(true) == null);
    }
}
