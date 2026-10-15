using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Dapper;

namespace EasyDapper.LoadTests;

internal sealed class SqlBenchmark
{
    private readonly Options _options;
    private readonly Fixture _fixture;
    private readonly List<WorkerConnection> _connections = [];
    private readonly ConcurrentQueue<object> _errors = new();
    private int _errorExamples;
    private readonly SemaphoreSlim _directSharedGate = new(1, 1);
    private readonly ProviderTimings? _providerTimings;
    private string? _querySql;
    private object? _queryShape;
    private bool _workersDrained = true;
    public SqlBenchmark(Options options, Fixture fixture)
    { _options = options; _fixture = fixture; _providerTimings = options.ProviderTimings ? new ProviderTimings() : null; }

    public async Task<object> RunAsync()
    {
        try
        {
            await OpenWorkersAsync().ConfigureAwait(false);
            if (_options.Query) CaptureQueryShape();
            var spids = _connections.Select(x => x.Spid).ToArray();
            await using var server = new ServerObserver(_fixture.ConnectionString, spids, _fixture.Server);
            await server.InitializeAsync().ConfigureAwait(false);
            PhaseResult? warmup = null;
            CorrectnessReport? warmupCorrectness = null;
            if (_options.WarmupSeconds > 0)
            {
                warmup = await RunPhaseAsync("warmup", _options.WarmupSeconds, server).ConfigureAwait(false);
                if (_workersDrained)
                {
                    warmupCorrectness = await _fixture.ValidateAsync(warmup.ExpectedUpdates).ConfigureAwait(false);
                    await _fixture.ResetAsync().ConfigureAwait(false);
                }
            }
            PhaseResult? measured = null;
            CorrectnessReport? correctness = null;
            if (_workersDrained)
            {
                measured = await RunPhaseAsync("measured", _options.Seconds, server).ConfigureAwait(false);
                correctness = await _fixture.ValidateAsync(measured.ExpectedUpdates).ConfigureAwait(false);
            }
            var passed = _workersDrained && (warmup?.Errors ?? 0) == 0 &&
                (warmupCorrectness?.Passed ?? true) && measured is { Errors: 0 } && correctness?.Passed == true;
            return new
            {
                passed, connectionSpids = spids,
                resourceScope = _options.Shared ? "one pre-opened physical connection shared by all workers" : "one pre-opened physical connection per worker",
                queryShape = _queryShape,
                directDapperSharedGate = _options.Backend == "dapper" && _options.Shared,
                sharedGateExplanation = _options.Backend == "dapper" && _options.Shared
                    ? "Harness SemaphoreSlim protects the shared SqlConnection; this is the valid single-command direct-Dapper reference."
                    : null,
                cancellation = "Stop dispatch at deadline. Native command timeout=30s. Public EasyDapper GetByIdAsync/UpdateAsync/QueryBuilder.ExecuteAsync have no cancellation-token overload; drain watchdog closes harness-owned connections on overrun.",
                sampling = "Process/threadpool/own-SPID DMV poll every 200ms; observer work is included in process CPU and allocation. No server timings are inferred from client latency.",
                warmup = warmup?.Report, warmupCorrectness,
                measured = measured?.Report, correctness,
                providerTimingsEnabled = _options.ProviderTimings,
                errorExamples = _errors.ToArray(), totalErrorExamplesRetained = _errors.Count
            };
        }
        finally
        {
            foreach (var connection in _connections)
            {
                if (_workersDrained) connection.Dispose();
                else connection.Abort(); // Never block on the service gate after a drain timeout.
            }
            _providerTimings?.Dispose();
            _directSharedGate.Dispose();
        }
    }

    private async Task OpenWorkersAsync()
    {
        using var openingDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        using var openingGate = new SemaphoreSlim(16, 16);
        var count = _options.Shared ? 1 : _options.Workers;
        // Fill slots in deterministic worker order, despite concurrent connection opening.
        var opened = new WorkerConnection[count];
        try
        {
            await Task.WhenAll(Enumerable.Range(0, count).Select(async index =>
            {
                await openingGate.WaitAsync(openingDeadline.Token).ConfigureAwait(false);
                try
                {
                    var connection = new SqlConnection(_fixture.ConnectionString);
                    try
                    {
                        await connection.OpenAsync(openingDeadline.Token).ConfigureAwait(false);
                        var spid = await connection.ExecuteScalarAsync<int>(new CommandDefinition("SELECT @@SPID;",
                            commandTimeout: Options.CommandTimeoutSeconds, cancellationToken: openingDeadline.Token)).ConfigureAwait(false);
                        opened[index] = new WorkerConnection(connection, spid,
                            _options.Backend == "easy" ? DapperServiceFactory.Create(connection) : null);
                    }
                    catch { connection.Dispose(); throw; }
                }
                finally { openingGate.Release(); }
            })).ConfigureAwait(false);
        }
        finally { _connections.AddRange(opened.Where(x => x != null)); }
    }

    private async Task<PhaseResult> RunPhaseAsync(string phase, double seconds, ServerObserver server)
    {
        var metrics = Enumerable.Range(0, _options.Workers).Select(_ => new WorkerMetrics()).ToArray();
        var timings = _options.ProviderTimings ? new TimingMetrics() : null;
        using var abort = new CancellationTokenSource();
        using var sampleStop = new CancellationTokenSource();
        var startSignal = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, _options.Workers)
            .Select(index => WorkerAsync(index, metrics[index], timings, startSignal.Task, seconds, phase, abort.Token)).ToArray();
        var sampler = new PhaseSampler(server);
        var sessionsBefore = await server.SnapshotAsync().ConfigureAwait(false);
        var sqlCpuBefore = server.SqlCpuMilliseconds();
        var before = ProcessSnapshot.Capture();
        var start = Stopwatch.GetTimestamp();
        var samplerTask = sampler.RunAsync(sampleStop.Token);
        startSignal.SetResult(start);
        var allWorkers = Task.WhenAll(workers);
        // Dispatch closes by the shared monotonic timestamp, even if this timer wakes late.
        await Task.Delay(TimeSpan.FromSeconds(seconds)).ConfigureAwait(false);
        bool drainTimedOut = false;
        try { await allWorkers.WaitAsync(TimeSpan.FromSeconds(_options.DrainSeconds)).ConfigureAwait(false); }
        catch (TimeoutException)
        {
            drainTimedOut = true;
            abort.Cancel();
            AddError(phase, -1, "drain", new TimeoutException("Worker drain deadline exceeded; closing benchmark-owned connections."));
            foreach (var connection in _connections) connection.Abort();
            try { await allWorkers.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false); }
            catch (TimeoutException) { _workersDrained = false; }
        }
        var end = Stopwatch.GetTimestamp();
        var after = ProcessSnapshot.Capture();
        var sqlCpuAfter = server.SqlCpuMilliseconds();
        sampleStop.Cancel();
        await samplerTask.ConfigureAwait(false);
        var sessionsAfter = await server.SnapshotAsync().ConfigureAwait(false);
        if (_providerTimings != null) await _providerTimings.DrainEventsAsync().ConfigureAwait(false);
        var readLatency = new LatencyHistogram();
        var updateLatency = new LatencyHistogram();
        foreach (var metric in metrics)
        {
            readLatency.Merge(metric.ReadLatency);
            updateLatency.Merge(metric.UpdateLatency);
        }
        var allLatency = new LatencyHistogram();
        allLatency.Merge(readLatency); allLatency.Merge(updateLatency);
        var successfulReads = metrics.Sum(x => x.Reads);
        var successfulUpdates = metrics.Sum(x => x.Updates);
        var errors = metrics.Sum(x => x.Errors) + (drainTimedOut ? 1 : 0);
        var wallSeconds = (end - start) / (double)Stopwatch.Frequency;
        var cpuMilliseconds = after.CpuMilliseconds - before.CpuMilliseconds;
        return new PhaseResult(errors, metrics.Select(x => x.SuccessfulUpdates).ToArray(), new
        {
            name = phase, dispatchSeconds = seconds, wallSecondsIncludingDrain = wallSeconds,
            drainSeconds = Math.Max(0, wallSeconds - seconds), drainTimedOut, workersDrained = _workersDrained,
            operations = new
            {
                attempted = allLatency.Count, succeeded = successfulReads + successfulUpdates,
                successfulReads, successfulUpdates, errors,
                correctnessErrors = metrics.Sum(x => x.CorrectnessErrors),
                workersStoppedAfterError = metrics.Count(x => x.StoppedAfterError),
                successfulOperationsPerSecond = (successfulReads + successfulUpdates) / wallSeconds,
                attemptedOperationsPerSecond = allLatency.Count / wallSeconds
            },
            latency = new { all = allLatency.Summary(), read = readLatency.Summary(), update = updateLatency.Summary(), includesFailures = true },
            process = new
            {
                cpuMilliseconds, cpuPercentOfOneCore = cpuMilliseconds / (wallSeconds * 10),
                cpuPercentOfAvailableProcessors = cpuMilliseconds / (wallSeconds * 10 * Environment.ProcessorCount),
                allocatedBytes = after.AllocatedBytes - before.AllocatedBytes,
                allocatedBytesPerAttempt = allLatency.Count == 0 ? (double?)null : (after.AllocatedBytes - before.AllocatedBytes) / (double)allLatency.Count,
                gen0Collections = after.GcCounts[0] - before.GcCounts[0],
                gen1Collections = after.GcCounts[1] - before.GcCounts[1],
                gen2Collections = after.GcCounts[2] - before.GcCounts[2],
                workingSetBeforeBytes = before.WorkingSetBytes, workingSetAfterBytes = after.WorkingSetBytes,
                sampledPeakWorkingSetBytes = Math.Max(sampler.MaxWorkingSetBytes, Math.Max(before.WorkingSetBytes, after.WorkingSetBytes)),
                lifetimePeakWorkingSetBytes = after.LifetimePeakWorkingSetBytes,
                threadPoolBefore = before.ThreadPool, threadPoolAfter = after.ThreadPool,
                sampledPeakThreadPoolThreads = sampler.MaxThreadPoolThreadCount,
                sampledPeakPendingWorkItems = sampler.MaxPendingWorkItems,
                sampledMinAvailableWorkerThreads = sampler.MinAvailableWorkerThreads,
                samplerSamples = sampler.Samples
            },
            sqlServer = new
            {
                processCpuMilliseconds = sqlCpuBefore.HasValue && sqlCpuAfter.HasValue ? sqlCpuAfter - sqlCpuBefore : null,
                processCpuScope = "Entire local SQL Server process; includes unrelated workloads if present.",
                processCpuUnavailableReason = server.SqlProcessUnavailableReason,
                ownSessionsBefore = sessionsBefore, ownSessionsAfter = sessionsAfter,
                ownSessionCpuMilliseconds = Difference(sessionsBefore, sessionsAfter, s => s.CpuMilliseconds),
                ownSessionPhysicalReads = Difference(sessionsBefore, sessionsAfter, s => s.PhysicalReads),
                ownSessionWrites = Difference(sessionsBefore, sessionsAfter, s => s.Writes),
                ownSessionLogicalReads = Difference(sessionsBefore, sessionsAfter, s => s.LogicalReads),
                sampledPeakActiveOwnRequests = sampler.MaxActiveSqlRequests,
                dmvUnavailableReason = server.DmvUnavailableReason
            },
            providerTimings = timings?.Summary()
        });
    }

    private static long? Difference(SessionSnapshot? before, SessionSnapshot? after, Func<SessionSnapshot, long> select)
        => before == null || after == null || before.VisibleSessions != after.VisibleSessions ? null : select(after) - select(before);

    private async Task WorkerAsync(int worker, WorkerMetrics metrics, TimingMetrics? timings,
        Task<long> startSignal, double seconds, string phase, CancellationToken cancellationToken)
    {
        var start = await startSignal.ConfigureAwait(false);
        var deadline = start + (long)(seconds * Stopwatch.Frequency);
        var resource = _connections[_options.Shared ? 0 : worker];
        long sequence = 0;
        var entity = new LoadRow { Id = worker + 1, Value = 0, Payload = "row:" + (worker + 1) };
        while (Stopwatch.GetTimestamp() < deadline && !cancellationToken.IsCancellationRequested)
        {
            var id = _options.Mixed || _options.Query ? worker + 1 : (int)((worker * 997L + sequence * 17) % Fixture.RowCount) + 1;
            sequence++;
            var readStart = Stopwatch.GetTimestamp();
            bool readSucceeded = false;
            using (timings == null ? null : _providerTimings!.Begin(timings))
            {
                try
                {
                    var row = await ReadAsync(resource, id, cancellationToken).ConfigureAwait(false);
                    var expectedValue = _options.Mixed ? metrics.SuccessfulUpdates : 0;
                    if (row == null || row.Id != id || row.Value != expectedValue || row.Payload != "row:" + id)
                        throw new DataIntegrityException($"Id={id}, expected Value={expectedValue}; returned Id={row?.Id}, Value={row?.Value}, Payload={row?.Payload}.");
                    metrics.Reads++;
                    readSucceeded = true;
                }
                catch (Exception error)
                {
                    metrics.Errors++;
                    if (error is DataIntegrityException) metrics.CorrectnessErrors++;
                    AddError(phase, worker, "read", error);
                    // A failed worker stops: retrying can conceal corruption or amplify failure storms.
                    metrics.StoppedAfterError = true;
                }
                finally { metrics.ReadLatency.Record(Stopwatch.GetTimestamp() - readStart); }
            }
            if (!readSucceeded) break;
            if (!_options.Mixed || Stopwatch.GetTimestamp() >= deadline || cancellationToken.IsCancellationRequested) continue;
            entity.Value = metrics.SuccessfulUpdates + 1;
            var updateStart = Stopwatch.GetTimestamp();
            bool updateSucceeded = false;
            using (timings == null ? null : _providerTimings!.Begin(timings))
            {
                try
                {
                    var affected = await UpdateAsync(resource, entity, cancellationToken).ConfigureAwait(false);
                    if (affected != 1) throw new DataIntegrityException("Update for Id " + entity.Id + " affected " + affected + " rows instead of 1.");
                    metrics.Updates++;
                    metrics.SuccessfulUpdates++;
                    updateSucceeded = true;
                }
                catch (Exception error)
                {
                    metrics.Errors++;
                    if (error is DataIntegrityException) metrics.CorrectnessErrors++;
                    AddError(phase, worker, "update", error);
                    metrics.StoppedAfterError = true;
                }
                finally { metrics.UpdateLatency.Record(Stopwatch.GetTimestamp() - updateStart); }
            }
            if (!updateSucceeded) break;
        }
    }

    private async Task<LoadRow?> ReadAsync(WorkerConnection connection, int id, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_options.Query) return await ReadQueryAsync(connection, id).ConfigureAwait(false);
        if (connection.Service != null) return await connection.Service.GetByIdAsync<LoadRow>((object)id).ConfigureAwait(false);
        if (_options.Shared) await _directSharedGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await connection.Connection.QueryFirstOrDefaultAsync<LoadRow>(
                $"SELECT [Id] AS [Id], [Value] AS [Value], [Payload] AS [Payload] FROM {Fixture.QualifiedTable} WHERE [Id]=@Id",
                new { Id = id }, commandTimeout: Options.CommandTimeoutSeconds).ConfigureAwait(false);
        }
        finally { if (_options.Shared) _directSharedGate.Release(); }
    }

    private IQueryBuilder<LoadRow> NewQuery(IDapperService service, int id)
    {
        var query = service.Query<LoadRow>().Where(row => row.Id == id);
        // Each lambda captures a primitive closure field. All redundant predicates
        // remain true for this worker's unique point-read row.
        for (var index = 1; index < _options.Clauses; index++)
        {
            var cutoff = id - 1;
            query.Where(row => row.Id > cutoff);
        }
        return query;
    }

    private void CaptureQueryShape()
    {
        // This pre-window control also lets the direct backend use precisely the
        // public builder's SQL shape without reflection or internal APIs.
        var resource = _connections[0];
        var control = resource.Service ?? DapperServiceFactory.Create(resource.Connection);
        try
        {
            using var query = NewQuery(control, 1);
            _querySql = query.BuildQuery();
            var actualNames = Regex.Matches(_querySql, @"@p\d+\b")
                .Select(match => match.Value).Distinct(StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal);
            var expectedNames = Enumerable.Range(1, _options.Clauses).Select(index => "@p" + index).ToHashSet(StringComparer.Ordinal);
            if (!actualNames.SetEquals(expectedNames))
                throw new InvalidOperationException("Unexpected public QueryBuilder parameter inventory; refusing an unmatched direct comparison.");
            _queryShape = new
            {
                sql = _querySql, sha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(_querySql))),
                parameterCount = actualNames.Count, parameterNames = expectedNames.OrderBy(name => int.Parse(name[2..])).ToArray(),
                bindings = "@p1=worker Id; @p2..@pN=Id-1, all Int32. Shape captured outside the window from the same public model/lambda factory.",
                easyWork = "Fresh QueryBuilder and N captured Where lambdas per operation, followed by buffered ExecuteAsync.",
                directWork = "Prebuilt identical SQL plus fresh N-value DynamicParameters per operation, followed by buffered QueryAsync. Direct Dapper does not translate expressions."
            };
        }
        finally { if (resource.Service == null) control.Dispose(); }
    }

    private async Task<LoadRow?> ReadQueryAsync(WorkerConnection resource, int id)
    {
        if (resource.Service != null)
        {
            using var query = NewQuery(resource.Service, id);
            return (await query.ExecuteAsync().ConfigureAwait(false)).SingleOrDefault();
        }
        var parameters = new DynamicParameters();
        parameters.Add("p1", id);
        for (var index = 2; index <= _options.Clauses; index++) parameters.Add("p" + index, id - 1);
        return (await resource.Connection.QueryAsync<LoadRow>(_querySql!, parameters,
            commandTimeout: Options.CommandTimeoutSeconds).ConfigureAwait(false)).SingleOrDefault();
    }

    private static Task<int> UpdateAsync(WorkerConnection connection, LoadRow entity, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return connection.Service != null ? connection.Service.UpdateAsync(entity)
            : connection.Connection.ExecuteAsync($"UPDATE {Fixture.QualifiedTable} SET [Value]=@Value, [Payload]=@Payload WHERE [Id]=@Id",
                entity, commandTimeout: Options.CommandTimeoutSeconds);
    }

    private void AddError(string phase, int worker, string operation, Exception error)
    {
        if (Interlocked.Increment(ref _errorExamples) <= 20)
            _errors.Enqueue(new { phase, worker, operation, type = error.GetType().FullName,
                sqlErrorNumber = error is SqlException sql ? (int?)sql.Number : null, error.Message });
    }

    private sealed class WorkerConnection(SqlConnection connection, int spid, IDapperService? service) : IDisposable
    {
        public SqlConnection Connection { get; } = connection;
        public int Spid { get; } = spid;
        public IDapperService? Service { get; } = service;
        public void Abort() { try { Connection.Close(); } catch { } }
        public void Dispose() { Service?.Dispose(); Connection.Dispose(); }
    }
    private sealed record PhaseResult(long Errors, long[] ExpectedUpdates, object Report);
    private sealed class DataIntegrityException(string message) : Exception(message);
}
