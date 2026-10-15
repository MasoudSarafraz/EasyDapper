using System.Globalization;

namespace EasyDapper.LoadTests;

internal sealed record Options(
    string Backend,
    string Scenario,
    int Workers,
    double Seconds,
    double WarmupSeconds,
    bool Mars,
    int MaxPoolSize,
    int DrainSeconds,
    bool ProviderTimings,
    int Clauses,
    string Label,
    string? Output)
{
    public const int CommandTimeoutSeconds = 30;
    public static readonly int[] AllowedWorkers = [1, 16, 64, 128, 256];
    public bool Shared => Scenario == "shared-read";
    public bool Mixed => Scenario == "local-mixed";
    public bool Query => Scenario == "local-query";

    public static Options Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i += 2)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal) || i + 1 >= args.Length)
                throw new ArgumentException("Supply options as --name value pairs; use --help for usage.");
            if (!values.TryAdd(args[i][2..], args[i + 1]))
                throw new ArgumentException("Repeated option: " + args[i]);
        }
        var allowed = new HashSet<string>(StringComparer.Ordinal)
        { "backend", "scenario", "workers", "seconds", "warmup", "mars", "max-pool-size", "drain-seconds", "provider-timings", "clauses", "label", "output" };
        foreach (var name in values.Keys)
            if (!allowed.Contains(name)) throw new ArgumentException("Unknown option: --" + name);
        string Get(string name, string fallback) => values.TryGetValue(name, out var value) ? value : fallback;
        int Integer(string name, int fallback) => int.Parse(Get(name, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
        double Number(string name, double fallback) => double.Parse(Get(name, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
        var result = new Options(Get("backend", "easy"), Get("scenario", "local-read"),
            Integer("workers", 1), Number("seconds", 5), Number("warmup", 2),
            bool.Parse(Get("mars", "false")), Integer("max-pool-size", 512), Integer("drain-seconds", 35),
            bool.Parse(Get("provider-timings", "false")), Integer("clauses", 1),
            Get("label", "unspecified"), values.GetValueOrDefault("output"));
        if (result.Backend is not ("easy" or "dapper")) throw new ArgumentException("--backend must be easy or dapper.");
        if (result.Scenario is not ("shared-read" or "local-read" or "local-mixed" or "local-query"))
            throw new ArgumentException("--scenario must be shared-read, local-read, local-mixed, or local-query.");
        if (result.Clauses is not (1 or 64 or 128)) throw new ArgumentException("--clauses must be 1, 64, or 128 for SQL cases.");
        if (!AllowedWorkers.Contains(result.Workers)) throw new ArgumentException("--workers must be 1, 16, 64, 128, or 256.");
        if (!double.IsFinite(result.Seconds) || result.Seconds <= 0 || result.Seconds > 300)
            throw new ArgumentException("--seconds must be finite and in (0, 300].");
        if (!double.IsFinite(result.WarmupSeconds) || result.WarmupSeconds < 0 || result.WarmupSeconds > 120)
            throw new ArgumentException("--warmup must be finite and in [0, 120].");
        if (result.MaxPoolSize < result.Workers + 2 || result.MaxPoolSize > 4096)
            throw new ArgumentException("--max-pool-size must allow workers plus fixture/observer connections, and be at most 4096.");
        if (result.DrainSeconds < Options.CommandTimeoutSeconds + 1 || result.DrainSeconds > 120)
            throw new ArgumentException("--drain-seconds must be in [31, 120].");
        return result;
    }

    public static string Usage => """
        EasyDapper SQL closed-loop load harness (one fresh process per case)
        Required: EASYDAPPER_SQL_TEST_CONNECTION=<SQL connection string>
        --backend easy|dapper              default easy
        --scenario shared-read|local-read|local-mixed|local-query  default local-read
        --clauses 1|64|128                 local-query conditions; default 1
        --workers 1|16|64|128|256           default 1
        --seconds <0..300>                 measured dispatch window; default 5
        --warmup <0..120>                  default 2
        --mars true|false                  default false
        --max-pool-size <workers+2..4096>   default 512
        --drain-seconds <31..120>           deadline after dispatch closes; default 35
        --provider-timings true|false       diagnostic runs only; default false
        --label <version label>            actual DLL SHA256 is also recorded
        --output <JSON path>               JSON also goes to stdout
        query-build [CPU harness options]  does not require SQL configuration
        Connection database is always tempdb; connect/command timeout is 30 seconds.
        """;
}
