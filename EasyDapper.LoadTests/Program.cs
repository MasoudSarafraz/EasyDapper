using System.Text.Json;
using System.Text.Json.Nodes;

namespace EasyDapper.LoadTests;

internal static class Program
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, WriteIndented = true };

    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "query-build") return QueryBuildBenchmark.Run(args);
        if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
        { Console.WriteLine(Options.Usage); return 0; }
        if (args[0] == "sql") args = args[1..];

        var startedUtc = DateTimeOffset.UtcNow;
        Options? options = null;
        Fixture? fixture = null;
        JsonNode? benchmark = null;
        Exception? failure = null;
        var environment = EnvironmentReport.Create();
        try
        {
            options = Options.Parse(args);
            var connectionString = Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION");
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("EASYDAPPER_SQL_TEST_CONNECTION is required; no SQL server fallback is used.");
            fixture = new Fixture(connectionString, options);
            using var setupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await fixture.CreateAsync(setupDeadline.Token).ConfigureAwait(false);
            benchmark = JsonSerializer.SerializeToNode(await new SqlBenchmark(options, fixture).RunAsync().ConfigureAwait(false), JsonOptions);
        }
        catch (Exception error) { failure = error; }
        finally { if (fixture != null) await fixture.DisposeAsync().ConfigureAwait(false); }

        var passed = failure == null && benchmark?["passed"]?.GetValue<bool>() == true && fixture?.CleanupError == null;
        var report = new
        {
            schemaVersion = 1, kind = "sql-closed-loop", passed,
            classification = "Bounded synthetic closed-loop benchmark; this is not production capacity certification.",
            startedUtc, finishedUtc = DateTimeOffset.UtcNow, options, environment,
            effectiveConnection = fixture?.ConnectionSettings,
            sqlServer = fixture?.Server,
            fixture = fixture == null ? null : new
            {
                database = "tempdb", schema = "dbo", table = Fixture.TableName,
                runId = fixture.RunId, seedRows = Fixture.RowCount,
                ownership = "Exclusive session sp_getapplock; refuse pre-existing table; matching GUID extended property required to DROP.",
                dropped = fixture.Dropped, cleanupError = fixture.CleanupError
            },
            benchmark,
            failure = failure == null ? null : new { type = failure.GetType().FullName, failure.Message, failure.StackTrace }
        };
        var json = JsonSerializer.Serialize(report, JsonOptions);
        if (!string.IsNullOrWhiteSpace(options?.Output))
        {
            var path = Path.GetFullPath(options.Output);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await File.WriteAllTextAsync(path, json + Environment.NewLine).ConfigureAwait(false);
        }
        Console.WriteLine(json);
        return passed ? 0 : failure is ArgumentException ? 2 : 3;
    }
}
