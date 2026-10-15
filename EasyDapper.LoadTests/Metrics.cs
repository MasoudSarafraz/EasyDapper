using System.Diagnostics;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace EasyDapper.LoadTests;

internal sealed class LatencyHistogram
{
    // Fixed memory, worker-owned counters. Percentiles are upper bucket bounds:
    // 1 microsecond absolute resolution below 128us, then <= 0.8% relative width.
    private const int SubBuckets = 128;
    private readonly long[] _counts = new long[4096];
    public long Count { get; private set; }
    public long TotalTicks { get; private set; }
    public long MaxTicks { get; private set; }

    public void Record(long ticks)
    {
        var microseconds = Math.Max(0, ticks * 1_000_000.0 / Stopwatch.Frequency);
        int bucket;
        if (microseconds < SubBuckets) bucket = (int)microseconds;
        else
        {
            var exponent = (int)Math.Floor(Math.Log2(microseconds));
            var lower = Math.Pow(2, exponent);
            bucket = SubBuckets + (exponent - 7) * SubBuckets + (int)((microseconds - lower) * SubBuckets / lower);
        }
        _counts[Math.Min(bucket, _counts.Length - 1)]++;
        Count++;
        TotalTicks += ticks;
        MaxTicks = Math.Max(MaxTicks, ticks);
    }

    public void Merge(LatencyHistogram other)
    {
        for (var i = 0; i < _counts.Length; i++) _counts[i] += other._counts[i];
        Count += other.Count;
        TotalTicks += other.TotalTicks;
        MaxTicks = Math.Max(MaxTicks, other.MaxTicks);
    }

    private double? Percentile(double percentile)
    {
        if (Count == 0) return null;
        var rank = Math.Max(1, (long)Math.Ceiling(Count * percentile));
        long sum = 0;
        for (var i = 0; i < _counts.Length; i++)
        {
            sum += _counts[i];
            if (sum < rank) continue;
            if (i < SubBuckets) return (i + 1) / 1000.0;
            var index = i - SubBuckets;
            var lower = Math.Pow(2, index / SubBuckets + 7);
            return (lower + (index % SubBuckets + 1) * lower / SubBuckets) / 1000.0;
        }
        return MaxTicks * 1000.0 / Stopwatch.Frequency;
    }

    public object Summary() => new
    {
        count = Count, meanMilliseconds = Count == 0 ? (double?)null : TotalTicks * 1000.0 / Stopwatch.Frequency / Count,
        p50Milliseconds = Percentile(0.5), p95Milliseconds = Percentile(0.95), p99Milliseconds = Percentile(0.99),
        maxMilliseconds = Count == 0 ? (double?)null : MaxTicks * 1000.0 / Stopwatch.Frequency,
        percentileMethod = "fixed histogram upper bucket bound; 1us below 128us, <=0.8% relative width above"
    };
}

internal sealed class WorkerMetrics
{
    public long Reads;
    public long Updates;
    public long Errors;
    public long CorrectnessErrors;
    public bool StoppedAfterError;
    public long SuccessfulUpdates;
    public LatencyHistogram ReadLatency { get; } = new();
    public LatencyHistogram UpdateLatency { get; } = new();
}

internal sealed class ProcessSnapshot
{
    public double CpuMilliseconds { get; init; }
    public long AllocatedBytes { get; init; }
    public int[] GcCounts { get; init; } = [];
    public long WorkingSetBytes { get; init; }
    public long LifetimePeakWorkingSetBytes { get; init; }
    public ThreadPoolSnapshot ThreadPool { get; init; } = new();
    public static ProcessSnapshot Capture()
    {
        using var process = Process.GetCurrentProcess();
        process.Refresh();
        return new ProcessSnapshot
        {
            CpuMilliseconds = process.TotalProcessorTime.TotalMilliseconds,
            AllocatedBytes = GC.GetTotalAllocatedBytes(precise: true),
            GcCounts = [GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2)],
            WorkingSetBytes = process.WorkingSet64,
            LifetimePeakWorkingSetBytes = process.PeakWorkingSet64,
            ThreadPool = ThreadPoolSnapshot.Capture()
        };
    }
}

internal sealed class ThreadPoolSnapshot
{
    public int ThreadCount { get; init; }
    public long PendingWorkItems { get; init; }
    public long CompletedWorkItems { get; init; }
    public int AvailableWorkerThreads { get; init; }
    public int AvailableIoThreads { get; init; }
    public int MaxWorkerThreads { get; init; }
    public int MaxIoThreads { get; init; }
    public static ThreadPoolSnapshot Capture()
    {
        ThreadPool.GetAvailableThreads(out var availableWorkers, out var availableIo);
        ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
        return new ThreadPoolSnapshot
        {
            ThreadCount = ThreadPool.ThreadCount, PendingWorkItems = ThreadPool.PendingWorkItemCount,
            CompletedWorkItems = ThreadPool.CompletedWorkItemCount,
            AvailableWorkerThreads = availableWorkers, AvailableIoThreads = availableIo,
            MaxWorkerThreads = maxWorkers, MaxIoThreads = maxIo
        };
    }
}

internal static class EnvironmentReport
{
    public static object Create() => new
    {
        runtime = RuntimeInformation.FrameworkDescription, runtimeVersion = Environment.Version.ToString(),
        os = RuntimeInformation.OSDescription, machine = Environment.MachineName,
        processArchitecture = RuntimeInformation.ProcessArchitecture.ToString(),
        osArchitecture = RuntimeInformation.OSArchitecture.ToString(), logicalProcessorCount = Environment.ProcessorCount,
        serverGc = GCSettings.IsServerGC, gcLatencyMode = GCSettings.LatencyMode.ToString(),
        availableMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
        stopwatchFrequency = Stopwatch.Frequency, processId = Environment.ProcessId,
        harness = AssemblyDetails(typeof(EnvironmentReport).Assembly),
        easyDapper = AssemblyDetails(typeof(DapperServiceFactory).Assembly),
        dapper = AssemblyDetails(typeof(Dapper.SqlMapper).Assembly),
        sqlClient = AssemblyDetails(typeof(System.Data.SqlClient.SqlConnection).Assembly)
    };

    private static object AssemblyDetails(Assembly assembly) => new
    {
        name = assembly.GetName().Name, assemblyVersion = assembly.GetName().Version?.ToString(),
        fileVersion = FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion,
        informationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
        path = assembly.Location, sha256 = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(assembly.Location)))
    };
}
