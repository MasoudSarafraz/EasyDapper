using System.Collections.Concurrent;
using System.Diagnostics;

namespace EasyDapper.LoadTests;

// Opt-in diagnostic observer. None of these intervals is pure gate wait or
// server CPU: Before follows client preparation/admission, and reader After can
// precede Dapper materialization. The main throughput matrix leaves this off.
internal sealed class ProviderTimings : IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>, IDisposable
{
    private readonly AsyncLocal<Operation?> _current = new();
    private readonly ConcurrentDictionary<Guid, CommandTiming> _commands = new();
    private readonly ConcurrentBag<IDisposable> _subscriptions = new();
    private readonly IDisposable _allListeners;
    public ProviderTimings() => _allListeners = DiagnosticListener.AllListeners.Subscribe(this);

    public Operation Begin(TimingMetrics metrics)
    {
        var operation = new Operation(this, _current.Value, metrics);
        _current.Value = operation;
        return operation;
    }

    public void OnNext(DiagnosticListener listener)
    {
        if (listener.Name.Contains("SqlClient", StringComparison.OrdinalIgnoreCase))
            _subscriptions.Add(listener.Subscribe(this, name =>
                name.EndsWith("WriteCommandBefore", StringComparison.Ordinal) ||
                name.EndsWith("WriteCommandAfter", StringComparison.Ordinal) ||
                name.EndsWith("WriteCommandError", StringComparison.Ordinal)));
    }

    public void OnNext(KeyValuePair<string, object?> item)
    {
        var timestamp = Stopwatch.GetTimestamp();
        var before = item.Key.EndsWith("WriteCommandBefore", StringComparison.Ordinal);
        var error = item.Key.EndsWith("WriteCommandError", StringComparison.Ordinal);
        var id = item.Value?.GetType().GetProperty("OperationId")?.GetValue(item.Value);
        if (before)
        {
            var operation = _current.Value;
            if (operation == null) return; // Exclude fixture and observer commands.
            if (id is not Guid guid)
            {
                Interlocked.Increment(ref operation.Metrics.MissingOperationIds);
                return;
            }
            Interlocked.Increment(ref operation.BeforeEvents);
            Interlocked.Increment(ref operation.Metrics.BeforeEvents);
            if (Interlocked.CompareExchange(ref operation.FirstBefore, timestamp, 0) == 0)
                operation.Metrics.RecordPreCommand(timestamp - operation.Start);
            if (!_commands.TryAdd(guid, new CommandTiming(operation, timestamp)))
                Interlocked.Increment(ref operation.Metrics.DuplicateOperationIds);
        }
        else if (id is Guid guid && _commands.TryRemove(guid, out var command))
        {
            var operation = command.Operation;
            Interlocked.Increment(ref operation.EndEvents);
            Interlocked.Increment(ref operation.Metrics.EndEvents);
            if (error) Interlocked.Increment(ref operation.Metrics.ErrorEvents);
            operation.Metrics.RecordExecute(timestamp - command.Start);
            Interlocked.Exchange(ref operation.LastEnd, timestamp);
            var publicEnd = Interlocked.Read(ref operation.PublicEnd);
            if (publicEnd != 0 && timestamp > publicEnd)
                Interlocked.Increment(ref operation.Metrics.EndAfterPublicReturn);
        }
        else if (_current.Value is { } current)
            Interlocked.Increment(ref current.Metrics.UnmatchedEndEvents);
    }

    public async Task DrainEventsAsync()
    {
        var end = Stopwatch.GetTimestamp() + Stopwatch.Frequency / 4;
        while (!_commands.IsEmpty && Stopwatch.GetTimestamp() < end)
            await Task.Delay(10).ConfigureAwait(false);
    }

    public void OnCompleted() { }
    public void OnError(Exception error) { }
    public void Dispose()
    {
        _allListeners.Dispose();
        foreach (var subscription in _subscriptions) subscription.Dispose();
    }

    private sealed record CommandTiming(Operation Operation, long Start);
    internal sealed class Operation : IDisposable
    {
        private readonly ProviderTimings _owner;
        private readonly Operation? _previous;
        public readonly TimingMetrics Metrics;
        public readonly long Start = Stopwatch.GetTimestamp();
        public long FirstBefore;
        public long BeforeEvents;
        public long EndEvents;
        public long LastEnd;
        public long PublicEnd;
        public Operation(ProviderTimings owner, Operation? previous, TimingMetrics metrics)
        { _owner = owner; _previous = previous; Metrics = metrics; Interlocked.Increment(ref metrics.PublicCalls); }
        public void Dispose()
        {
            var end = Stopwatch.GetTimestamp();
            Interlocked.Exchange(ref PublicEnd, end);
            if (Interlocked.Read(ref BeforeEvents) == 0) Interlocked.Increment(ref Metrics.CallsWithoutBefore);
            var lastEnd = Interlocked.Read(ref LastEnd);
            if (lastEnd != 0 && lastEnd <= end && Interlocked.Read(ref BeforeEvents) == Interlocked.Read(ref EndEvents))
                Metrics.RecordAfterProvider(end - lastEnd);
            _owner._current.Value = _previous;
        }
    }
}

internal sealed class TimingMetrics
{
    private readonly object _lock = new();
    private readonly LatencyHistogram _preCommand = new();
    private readonly LatencyHistogram _execute = new();
    private readonly LatencyHistogram _afterProvider = new();
    public long PublicCalls, BeforeEvents, EndEvents, ErrorEvents, EndAfterPublicReturn;
    public long CallsWithoutBefore, MissingOperationIds, DuplicateOperationIds, UnmatchedEndEvents;
    public void RecordPreCommand(long ticks) { lock (_lock) _preCommand.Record(ticks); }
    public void RecordExecute(long ticks) { lock (_lock) _execute.Record(ticks); }
    public void RecordAfterProvider(long ticks) { lock (_lock) _afterProvider.Record(ticks); }
    public object Summary()
    {
        lock (_lock) return new
        {
            publicCalls = PublicCalls, beforeEvents = BeforeEvents, endEvents = EndEvents,
            missingEndEvents = Math.Max(0, BeforeEvents - EndEvents), callsWithoutBefore = CallsWithoutBefore,
            missingOperationIds = MissingOperationIds, duplicateOperationIds = DuplicateOperationIds,
            unmatchedEndEvents = UnmatchedEndEvents, providerErrorEvents = ErrorEvents,
            endAfterPublicReturn = EndAfterPublicReturn,
            preCommand = _preCommand.Summary(), nativeExecute = _execute.Summary(),
            afterLastProviderEndToPublicReturn = _afterProvider.Summary(),
            definitions = new
            {
                preCommand = "Public call to first native Before: includes preparation, scheduling, open/admission/lease wait.",
                nativeExecute = "Matched native Before to After/Error: provider Execute interval; excludes later reader consumption/materialization.",
                afterLastProviderEndToPublicReturn = "Only calls with all end events observed before return; remaining reader/materialization/cleanup work.",
                exclusions = "No interval is pure gate wait, pure server elapsed time, or SQL server CPU; missing/late events are explicit."
            }
        };
    }
}
