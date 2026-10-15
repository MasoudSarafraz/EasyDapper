using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace EasyDapper.LoadTests;

internal static class QueryBuildBenchmark
{
    internal static int Run(string[] args)
    {
        int workers = ReadInt(args, "--workers", 1, 1, 256);
        int clauses = ReadInt(args, "--clauses", 64, 1, 2048);
        int seconds = ReadInt(args, "--seconds", 5, 1, 120);
        string label = ReadString(args, "--label", "unspecified")!;
        string? output = ReadString(args, "--output", null);
        using var service = DapperServiceFactory.Create("Server=.;Database=master;Integrated Security=true;TrustServerCertificate=true");
        string Build()
        {
            var query = service.Query<QueryBuildRow>();
            for (int index = 0; index < clauses; index++)
            {
                int cutoff = index;
                query.Where(row => row.Id > cutoff);
            }
            return query.BuildQuery();
        }

        // No database connection is opened: this isolates ORM translation and
        // checkpoint allocation from SQL/network/connection-pool effects.
        string expectedSql = Build();
        for (int index = 0; index < Math.Max(4, 128 / clauses); index++)
            if (Build() != expectedSql) throw new InvalidOperationException("Non-deterministic warmup SQL.");
        ThreadPool.GetMinThreads(out int originalMinWorkers, out int minIo);
        ThreadPool.SetMinThreads(Math.Max(originalMinWorkers, workers), minIo);
        using var ready = new CountdownEvent(workers);
        using var start = new ManualResetEventSlim();
        long deadline = 0;
        var tasks = Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            var latencies = new List<long>(1024);
            long failures = 0;
            string? error = null;
            ready.Signal();
            start.Wait();
            while (Stopwatch.GetTimestamp() < Volatile.Read(ref deadline))
            {
                long began = Stopwatch.GetTimestamp();
                try
                {
                    if (!string.Equals(Build(), expectedSql, StringComparison.Ordinal))
                        throw new InvalidOperationException("SQL differs from the single-thread control.");
                    latencies.Add(Stopwatch.GetTimestamp() - began);
                }
                catch (Exception exception)
                {
                    failures++;
                    error ??= exception.GetType().Name + ": " + exception.Message;
                }
            }
            return (latencies, failures, error);
        })).ToArray();
        if (!ready.Wait(TimeSpan.FromSeconds(30))) throw new TimeoutException("Workers failed to initialize.");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        using var process = Process.GetCurrentProcess();
        var gcBefore = Enumerable.Range(0, 3).Select(GC.CollectionCount).ToArray();
        long allocatedBefore = GC.GetTotalAllocatedBytes(true);
        TimeSpan cpuBefore = process.TotalProcessorTime;
        long started = Stopwatch.GetTimestamp();
        Volatile.Write(ref deadline, started + seconds * Stopwatch.Frequency);
        start.Set();
        if (!Task.WaitAll(tasks, TimeSpan.FromSeconds(seconds + 30))) throw new TimeoutException("Workers failed to drain.");
        long stopped = Stopwatch.GetTimestamp();
        TimeSpan cpu = process.TotalProcessorTime - cpuBefore;
        long allocated = GC.GetTotalAllocatedBytes(true) - allocatedBefore;
        var collections = Enumerable.Range(0, 3).Select(generation => GC.CollectionCount(generation) - gcBefore[generation]).ToArray();
        process.Refresh();
        var samples = tasks.SelectMany(task => task.Result.latencies).OrderBy(value => value).ToArray();
        long errors = tasks.Sum(task => task.Result.failures);
        double elapsed = (stopped - started) / (double)Stopwatch.Frequency;
        double Percentile(double fraction) => samples.Length == 0 ? 0
            : samples[Math.Clamp((int)Math.Ceiling(samples.Length * fraction) - 1, 0, samples.Length - 1)] * 1000d / Stopwatch.Frequency;
        var library = typeof(DapperServiceFactory).Assembly;
        var result = new
        {
            SchemaVersion = 1, TimestampUtc = DateTime.UtcNow, Label = label,
            Scenario = "query-build", Workers = workers, Clauses = clauses,
            RequestedSeconds = seconds, ElapsedSeconds = elapsed,
            Success = samples.LongLength, Errors = errors,
            ErrorSamples = tasks.Select(task => task.Result.error).Where(error => error != null).Distinct().ToArray(),
            OperationsPerSecond = samples.Length / elapsed,
            LatencyMilliseconds = new { P50 = Percentile(.50), P95 = Percentile(.95), P99 = Percentile(.99), Max = Percentile(1) },
            CpuMilliseconds = cpu.TotalMilliseconds,
            CpuPercentOfOneCore = cpu.TotalSeconds / elapsed * 100,
            CpuPercentOfAvailableProcessors = cpu.TotalSeconds / elapsed / Environment.ProcessorCount * 100,
            AllocatedBytes = allocated, AllocatedBytesPerSuccess = samples.Length == 0 ? 0 : allocated / (double)samples.Length,
            CollectionsByGeneration = collections,
            PeakWorkingSetBytes = process.PeakWorkingSet64,
            WorkingSetBytesAfter = process.WorkingSet64,
            ProcessorCount = Environment.ProcessorCount,
            Runtime = RuntimeInformation.FrameworkDescription, Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            OperatingSystem = RuntimeInformation.OSDescription,
            LibraryPath = library.Location, LibrarySha256 = HashFile(library.Location),
            ConsumerSha256 = HashFile(typeof(QueryBuildBenchmark).Assembly.Location),
            DapperVersion = typeof(Dapper.SqlMapper).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            SqlSha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(expectedSql))),
            Measurement = "Fresh independent builders on one service, no SQL I/O; closed-loop fixed workers. Allocation includes worker sample collection. p99 is nearest rank; interpret with Success sample count. CPU/GC exclude warmup. Peak working set includes warmup."
        };
        string json = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
        if (output != null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(output))!);
            File.WriteAllText(output, json);
        }
        Console.WriteLine(json);
        return errors == 0 ? 0 : 1;
    }

    private static string HashFile(string path) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)));
    private static string? ReadString(string[] args, string name, string? fallback)
    {
        int index = Array.IndexOf(args, name);
        if (index < 0) return fallback;
        if (index + 1 >= args.Length) throw new ArgumentException("Missing " + name);
        return args[index + 1];
    }
    private static int ReadInt(string[] args, string name, int fallback, int min, int max)
    {
        int value = int.Parse(ReadString(args, name, fallback.ToString())!);
        if (value < min || value > max) throw new ArgumentOutOfRangeException(name);
        return value;
    }
    private sealed class QueryBuildRow { public int Id { get; set; } }
}
