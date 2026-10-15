using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;

namespace EasyDapper.LoadTests;

internal sealed class ServerObserver : IAsyncDisposable
{
    private readonly SqlConnection _connection;
    private readonly int[] _spids;
    private Process? _sqlProcess;
    public string? DmvUnavailableReason { get; private set; }
    public string? SqlProcessUnavailableReason { get; private set; }
    public ServerObserver(string connectionString, int[] spids, ServerDetails? server)
    {
        _connection = new SqlConnection(connectionString);
        _spids = spids;
        if (server == null || !string.Equals(server.MachineName, Environment.MachineName, StringComparison.OrdinalIgnoreCase))
            SqlProcessUnavailableReason = "Server is remote or machine identity unavailable; no local SQL process sampled.";
        else
        {
            try
            {
                var process = Process.GetProcessById(server.ProcessId);
                if (!process.ProcessName.StartsWith("sqlservr", StringComparison.OrdinalIgnoreCase))
                { process.Dispose(); SqlProcessUnavailableReason = "Server process ID does not identify local sqlservr."; }
                else _sqlProcess = process;
            }
            catch (Exception error) { SqlProcessUnavailableReason = error.GetType().Name + ": " + error.Message; }
        }
    }

    public async Task InitializeAsync()
    {
        try { await _connection.OpenAsync().ConfigureAwait(false); await SnapshotAsync().ConfigureAwait(false); }
        catch (Exception error) { DmvUnavailableReason = error.GetType().Name + ": " + error.Message; }
    }

    public double? SqlCpuMilliseconds()
    {
        if (_sqlProcess == null) return null;
        try { return _sqlProcess.TotalProcessorTime.TotalMilliseconds; }
        catch (Exception error) { SqlProcessUnavailableReason = error.GetType().Name + ": " + error.Message; return null; }
    }

    public async Task<SessionSnapshot?> SnapshotAsync()
    {
        if (DmvUnavailableReason != null) return null;
        try
        {
            var rows = (await _connection.QueryAsync<SessionCounters>(new CommandDefinition("""
                SELECT s.session_id AS SessionId, s.cpu_time AS CpuMilliseconds,
                    s.reads AS Reads, s.writes AS Writes, s.logical_reads AS LogicalReads,
                    (SELECT COUNT(*) FROM sys.dm_exec_requests r WHERE r.session_id=s.session_id) AS ActiveRequests
                FROM sys.dm_exec_sessions s WHERE s.session_id IN @Spids;
                """, new { Spids = _spids }, commandTimeout: 2)).ConfigureAwait(false)).ToArray();
            return new SessionSnapshot(rows.Length, rows.Sum(x => x.CpuMilliseconds), rows.Sum(x => x.Reads),
                rows.Sum(x => x.Writes), rows.Sum(x => x.LogicalReads), rows.Sum(x => x.ActiveRequests));
        }
        catch (Exception error) { DmvUnavailableReason = error.GetType().Name + ": " + error.Message; return null; }
    }

    public async ValueTask DisposeAsync()
    {
        _sqlProcess?.Dispose();
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class SessionCounters
    {
        public int SessionId { get; set; }
        public long CpuMilliseconds { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long LogicalReads { get; set; }
        public int ActiveRequests { get; set; }
    }
}

internal sealed record SessionSnapshot(int VisibleSessions, long CpuMilliseconds, long PhysicalReads,
    long Writes, long LogicalReads, int ActiveRequests);

internal sealed class PhaseSampler
{
    private readonly ServerObserver _server;
    public long MaxWorkingSetBytes { get; private set; }
    public int MaxThreadPoolThreadCount { get; private set; }
    public long MaxPendingWorkItems { get; private set; }
    public int MinAvailableWorkerThreads { get; private set; } = int.MaxValue;
    public int MaxActiveSqlRequests { get; private set; }
    public int Samples { get; private set; }
    public PhaseSampler(ServerObserver server) => _server = server;
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using var process = Process.GetCurrentProcess();
                process.Refresh();
                MaxWorkingSetBytes = Math.Max(MaxWorkingSetBytes, process.WorkingSet64);
                var pool = ThreadPoolSnapshot.Capture();
                MaxThreadPoolThreadCount = Math.Max(MaxThreadPoolThreadCount, pool.ThreadCount);
                MaxPendingWorkItems = Math.Max(MaxPendingWorkItems, pool.PendingWorkItems);
                MinAvailableWorkerThreads = Math.Min(MinAvailableWorkerThreads, pool.AvailableWorkerThreads);
                var sessions = await _server.SnapshotAsync().ConfigureAwait(false);
                if (sessions != null) MaxActiveSqlRequests = Math.Max(MaxActiveSqlRequests, sessions.ActiveRequests);
                Samples++;
                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }
}
