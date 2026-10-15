using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace EasyDapper.LoadTests;

internal sealed class Fixture : IAsyncDisposable
{
    public const string TableName = "__EasyDapperLoad_d8cd825aaad34d4fa388efb25d761a11";
    public const string QualifiedTable = "[dbo].[" + TableName + "]";
    public const int RowCount = 4096;
    private const string OwnerProperty = "EasyDapperLoadRunId";
    private readonly SqlConnection _owner;
    private bool _created;
    public Guid RunId { get; } = Guid.NewGuid();
    public string ConnectionString { get; }
    public ServerDetails? Server { get; private set; }
    public object ConnectionSettings { get; }
    public string? CleanupError { get; private set; }
    public bool Dropped { get; private set; }

    public Fixture(string suppliedConnectionString, Options options)
    {
        var builder = new SqlConnectionStringBuilder(suppliedConnectionString)
        {
            InitialCatalog = "tempdb",
            ApplicationName = "EasyDapper.LoadTests",
            ConnectTimeout = Options.CommandTimeoutSeconds,
            MaxPoolSize = options.MaxPoolSize,
            MultipleActiveResultSets = options.Mars
        };
        ConnectionString = builder.ConnectionString;
        ConnectionSettings = new
        {
            server = builder.DataSource, database = builder.InitialCatalog,
            pooling = builder.Pooling, minPoolSize = builder.MinPoolSize, maxPoolSize = builder.MaxPoolSize,
            mars = builder.MultipleActiveResultSets, connectTimeoutSeconds = builder.ConnectTimeout,
            commandTimeoutSeconds = Options.CommandTimeoutSeconds, encrypt = builder.Encrypt,
            trustServerCertificate = builder.TrustServerCertificate,
            integratedSecurity = builder.IntegratedSecurity
        };
        _owner = new SqlConnection(ConnectionString);
    }

    public async Task CreateAsync(CancellationToken cancellationToken)
    {
        await _owner.OpenAsync(cancellationToken).ConfigureAwait(false);
        var lockResult = await _owner.ExecuteScalarAsync<int>(new CommandDefinition("""
            DECLARE @result int;
            EXEC @result = sys.sp_getapplock @Resource=@Resource, @LockMode='Exclusive',
                @LockOwner='Session', @LockTimeout=0;
            SELECT @result;
            """, new { Resource = "EasyDapper.LoadTests:" + TableName },
            commandTimeout: Options.CommandTimeoutSeconds, cancellationToken: cancellationToken)).ConfigureAwait(false);
        if (lockResult < 0) throw new InvalidOperationException("Another process owns this compiled fixture; run cases sequentially.");
        var exists = await _owner.ExecuteScalarAsync<int>(new CommandDefinition(
            "SELECT CASE WHEN OBJECT_ID(@Name, 'U') IS NULL THEN 0 ELSE 1 END;",
            new { Name = "dbo." + TableName }, commandTimeout: Options.CommandTimeoutSeconds,
            cancellationToken: cancellationToken)).ConfigureAwait(false);
        if (exists != 0)
            throw new InvalidOperationException("Fixture table already exists. Refusing to use or drop an existing table: tempdb.dbo." + TableName);

        using (var transaction = _owner.BeginTransaction())
        {
            try
            {
                await _owner.ExecuteAsync(new CommandDefinition($"""
                    CREATE TABLE {QualifiedTable} (
                        [Id] int NOT NULL PRIMARY KEY,
                        [Value] bigint NOT NULL,
                        [Payload] nvarchar(64) NOT NULL
                    );
                    EXEC sys.sp_addextendedproperty @name=@OwnerProperty, @value=@RunId,
                        @level0type=N'SCHEMA', @level0name=N'dbo',
                        @level1type=N'TABLE', @level1name=@TableName;
                    ;WITH N AS (
                        SELECT TOP (4096) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Id
                        FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS a(n)
                        CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS b(n)
                        CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS c(n)
                        CROSS JOIN (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS d(n)
                    )
                    INSERT INTO {QualifiedTable} ([Id],[Value],[Payload])
                        SELECT CAST(Id AS int), 0, N'row:' + CONVERT(nvarchar(10),Id) FROM N;
                    """, new { OwnerProperty, RunId = RunId.ToString("D"), TableName },
                    transaction, Options.CommandTimeoutSeconds, cancellationToken: cancellationToken)).ConfigureAwait(false);
                transaction.Commit();
                _created = true;
            }
            catch
            {
                try { transaction.Rollback(); } catch { }
                throw;
            }
        }
        Server = (await _owner.QueryAsync<ServerDetails>(new CommandDefinition("""
            SELECT CAST(@@VERSION AS nvarchar(max)) AS FullVersion,
                CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(100)) AS ProductVersion,
                CAST(SERVERPROPERTY('Edition') AS nvarchar(100)) AS Edition,
                CAST(SERVERPROPERTY('MachineName') AS nvarchar(128)) AS MachineName,
                CAST(SERVERPROPERTY('InstanceName') AS nvarchar(128)) AS InstanceName,
                CAST(SERVERPROPERTY('ProcessID') AS int) AS ProcessId,
                CAST(SERVERPROPERTY('EngineEdition') AS int) AS EngineEdition;
            """, commandTimeout: Options.CommandTimeoutSeconds,
            cancellationToken: cancellationToken)).ConfigureAwait(false)).Single();
    }

    public Task ResetAsync() => _owner.ExecuteAsync(new CommandDefinition(
        $"UPDATE {QualifiedTable} SET [Value]=0;", commandTimeout: Options.CommandTimeoutSeconds));

    public async Task<CorrectnessReport> ValidateAsync(long[] successfulUpdates)
    {
        var rows = (await _owner.QueryAsync<LoadRow>(new CommandDefinition(
            $"SELECT [Id],[Value],[Payload] FROM {QualifiedTable} ORDER BY [Id];",
            commandTimeout: Options.CommandTimeoutSeconds)).ConfigureAwait(false)).ToList();
        var bad = new List<string>();
        if (rows.Count != RowCount) bad.Add($"Expected {RowCount} rows, found {rows.Count}.");
        var seen = new HashSet<int>();
        foreach (var row in rows)
        {
            if (!seen.Add(row.Id)) bad.Add("Duplicate row Id " + row.Id);
            if (row.Id < 1 || row.Id > RowCount) { bad.Add("Unexpected row Id " + row.Id); continue; }
            var expected = row.Id <= successfulUpdates.Length ? successfulUpdates[row.Id - 1] : 0;
            if (row.Value != expected) bad.Add($"Id {row.Id}: expected Value={expected}, got {row.Value}.");
            if (row.Payload != "row:" + row.Id) bad.Add("Payload mismatch for Id " + row.Id);
        }
        var missing = Enumerable.Range(1, RowCount).Count(id => !seen.Contains(id));
        return new CorrectnessReport(rows.Count, missing, bad.Count == 0, successfulUpdates.Sum(),
            bad.Take(20).ToArray(), bad.Count);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_created)
            {
                if (_owner.State != ConnectionState.Open) await _owner.OpenAsync().ConfigureAwait(false);
                await _owner.ExecuteAsync(new CommandDefinition($"""
                    DECLARE @actual nvarchar(36);
                    SELECT @actual=CONVERT(nvarchar(36),[value]) FROM sys.extended_properties
                      WHERE [class]=1 AND [major_id]=OBJECT_ID(@Name) AND [minor_id]=0 AND [name]=@OwnerProperty;
                    IF @actual IS NULL OR @actual <> @RunId
                        THROW 50000, 'Fixture ownership changed; refusing DROP.', 1;
                    DROP TABLE {QualifiedTable};
                    """, new { Name = "dbo." + TableName, OwnerProperty, RunId = RunId.ToString("D") },
                    commandTimeout: Options.CommandTimeoutSeconds)).ConfigureAwait(false);
                Dropped = true;
            }
        }
        catch (Exception error) { CleanupError = error.GetType().FullName + ": " + error.Message; }
        finally { _owner.Dispose(); }
    }
}

internal sealed class ServerDetails
{
    public string FullVersion { get; set; } = "";
    public string ProductVersion { get; set; } = "";
    public string Edition { get; set; } = "";
    public string MachineName { get; set; } = "";
    public string? InstanceName { get; set; }
    public int ProcessId { get; set; }
    public int EngineEdition { get; set; }
}

internal sealed record CorrectnessReport(int ActualRows, int MissingRows, bool Passed,
    long ExpectedSuccessfulUpdates, string[] FailureExamples, int FailureCount);
