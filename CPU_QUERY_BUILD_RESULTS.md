# CPU query-construction benchmark: journal + field-read optimization

These measurements belong to the recorded October 12 binaries. The later
correctness review is documented in [DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md);
the CPU matrix was not repeated for that revision.

All 54 recorded runs passed: 3 library versions × 3 clause counts × 2 worker counts × 3 repetitions. Every run exited with code 0, recorded zero errors, used the same consumer binary, and produced identical SQL within its clause-count group. The three frozen netstandard2.0 library files were independently rehashed and matched the reports and manifest.

This measures fresh independent query builders on one service, adding 1/64/512 captured-value WHERE clauses and constructing SQL. No database connection is opened and no SQL is executed. It is a closed-loop fixed-worker CPU workload, not database requests per second, connection-pool capacity, or application latency. Each run requested 3 measured seconds; setup/warmup were excluded. Source: [manifest](C:/Users/Masoud/AppData/Local/Temp/EasyDapperHighLoad-20261012/results/manifest-query.json), [full analysis JSON](C:/Users/Masoud/AppData/Local/Temp/EasyDapperHighLoad-20261012/CPU_BENCHMARK_ANALYSIS.json), [aggregate CSV](C:/Users/Masoud/AppData/Local/Temp/EasyDapperHighLoad-20261012/CPU_BENCHMARK_AGGREGATES.csv), and [QueryBuildBenchmark.cs](G:/Projects/EasyDapper/EasyDapper.LoadTests/QueryBuildBenchmark.cs:18) in the repository.

Host: Intel Core i7-6700HQ, 4 physical cores / 8 logical processors; Windows 11 Enterprise 10.0.22631; .NET 8.0.19 x64. The runtime OS-description string identifies the same kernel version as Windows 10.0.22631. Reported CPU is process CPU divided by wall time, normalized over 8 logical processors; 100% in that column would mean all available logical processors. CPU ms/query is also supplied in the JSON.

Versions: before = corrected working version before the two CPU optimizations; optimized = incremental checkpoint undo journals plus the guarded closure-field read path; original = original repository baseline. This experiment measures the combined changes and cannot attribute improvement to one of them individually.

SHA-256 consumer (identical in all 54 cases and rehashed from the current consumer DLL): 3E5A179B60052A6DB88E464730A27DD2E75757E0AB575A288698AEBEC6CC5566
- before library: 665FD1FE50EF51D45CBF5C09E316D95D776BCA34C0DD9A033FA96DE991F180A9
- optimized library: B64AEFA0B695369DF07494E8DF699C366812A2DC0BB8D72CE6011D5324807A08
- original library: BADAD0BDAF78F7E257457E5292DDF3833A6F27FF6B386F144F0FA501EAFF2A1A

- 1 clauses: all 18 SQL fingerprints equal 38A338A5F43B92E5B944FD049DD6BBF5350CE10FEF1EE56E450313549C523ED9
- 64 clauses: all 18 SQL fingerprints equal AF03FF11100ED3EEACA90859780332C5403985B43353D1FEA1486344AC0E4720
- 512 clauses: all 18 SQL fingerprints equal 75C28B7C46C64299F620CF20D24680722CF23EAA95838F37200C4BE1C2F450AA

Each cell below is the median of three runs [minimum–maximum]. Quantiles are medians of per-run nearest-rank quantiles, not a percentile pooled across repetitions. Bytes include the harness latency-sample collection. RPS includes construction and SQL equality checking; measurement stops after in-flight workers drain.

| Clauses | Workers | Version | Queries/s | p95 ms | p99 ms | Allocated bytes/query | CPU % of available | Successful samples/run |
|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 1 | before | 5,621.8 [5,427.9–5,640.6] | 0.261 [0.260–0.276] | 0.389 [0.374–0.426] | 21,539 [21,525–21,540] | 10.42 [10.41–10.74] | 16,867 [16,292–16,924] |
| 1 | 1 | optimized | 53,693.2 [52,657.3–54,823.5] | 0.032 [0.030–0.033] | 0.063 [0.063–0.066] | 17,509 [17,508–17,509] | 10.81 [10.42–11.06] | 161,103 [157,989–164,515] |
| 1 | 1 | original | 6,563.1 [6,239.0–6,669.1] | 0.235 [0.220–0.249] | 0.328 [0.314–0.353] | 12,195 [12,195–12,196] | 10.35 [10.28–10.68] | 19,692 [18,719–20,011] |
| 1 | 8 | before | 21,945.7 [19,552.9–22,438.0] | 1.631 [0.878–1.653] | 2.410 [2.358–3.075] | 22,283 [22,253–22,294] | 10.81 [9.89–18.99] | 65,848 [58,721–67,325] |
| 1 | 8 | optimized | 84,715.5 [82,166.5–85,258.5] | 0.117 [0.115–0.117] | 1.162 [1.160–1.180] | 18,445 [18,444–18,450] | 6.38 [6.12–7.03] | 254,189 [246,540–255,814] |
| 1 | 8 | original | 27,058.8 [25,881.8–28,235.0] | 0.323 [0.318–0.327] | 2.073 [1.918–2.304] | 12,202 [12,201–12,205] | 17.84 [14.19–19.79] | 81,189 [77,659–84,720] |
| 64 | 1 | before | 98.4 [98.3–98.5] | 12.171 [12.134–12.235] | 13.597 [13.330–13.629] | 537,724 [537,724–537,750] | 10.72 [10.52–11.19] | 296 [295–296] |
| 64 | 1 | optimized | 2,549.3 [2,483.6–2,584.7] | 0.864 [0.863–0.865] | 1.200 [1.180–1.222] | 195,226 [195,226–195,227] | 10.94 [10.74–11.07] | 7,649 [7,452–7,755] |
| 64 | 1 | original | 114.3 [114.3–116.4] | 10.877 [10.778–10.955] | 12.549 [12.491–12.871] | 370,574 [370,559–370,576] | 10.41 [10.22–10.85] | 343 [343–350] |
| 64 | 8 | before | 427.5 [423.2–436.5] | 22.517 [21.635–23.360] | 30.067 [25.467–31.747] | 552,458 [552,101–553,114] | 62.74 [56.81–62.82] | 1,288 [1,274–1,316] |
| 64 | 8 | optimized | 4,003.4 [3,802.6–4,231.0] | 3.397 [3.158–3.485] | 6.306 [5.078–6.420] | 223,739 [223,325–229,988] | 8.98 [8.14–11.91] | 12,013 [11,411–12,697] |
| 64 | 8 | original | 462.7 [448.2–491.9] | 25.285 [19.841–28.873] | 36.496 [23.770–46.484] | 370,797 [370,675–370,888] | 55.44 [50.36–56.57] | 1,392 [1,349–1,481] |
| 512 | 1 | before | 10.6 [9.7–10.6] | 105.944 [103.167–126.955] | 106.264 [105.602–143.740] | 7,708,320 [7,708,180–7,708,408] | 10.45 [10.44–10.71] | 32 [30–32] |
| 512 | 1 | optimized | 313.2 [291.3–317.1] | 6.391 [6.254–6.916] | 7.304 [7.049–8.593] | 1,443,751 [1,443,751–1,443,781] | 10.86 [10.28–11.13] | 940 [874–952] |
| 512 | 1 | original | 14.4 [14.1–14.5] | 76.573 [76.001–86.889] | 86.761 [76.595–87.892] | 2,895,969 [2,895,878–2,896,119] | 10.32 [10.29–10.48] | 44 [43–44] |
| 512 | 8 | before | 42.6 [41.7–42.7] | 201.061 [199.353–209.421] | 208.801 [208.337–224.010] | 7,795,197 [7,786,760–7,808,387] | 11.59 [11.27–11.82] | 132 [130–134] |
| 512 | 8 | optimized | 428.1 [423.8–454.2] | 24.584 [23.133–24.624] | 28.105 [27.984–32.258] | 1,690,679 [1,689,029–1,692,524] | 7.47 [6.63–7.53] | 1,288 [1,275–1,365] |
| 512 | 8 | original | 57.9 [55.0–58.7] | 147.107 [146.218–175.030] | 158.340 [149.896–190.659] | 2,897,402 [2,897,372–2,897,735] | 45.46 [41.97–48.94] | 180 [170–182] |

Comparison factors below divide optimized medians by each reference median. Allocation reduction is 100 × (1 − optimized/reference); a negative number means MORE allocation. The JSON also includes repeat-paired RPS ratios and p95/p99 reductions. Three repeats and their min/max ranges do not establish a statistical confidence interval.

| Clauses | Workers | RPS × before | RPS × original | Allocation reduction vs before | Allocation reduction vs original |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 9.55 | 8.18 | 18.7% | -43.6% |
| 1 | 8 | 3.86 | 3.13 | 17.2% | -51.2% |
| 64 | 1 | 25.91 | 22.31 | 63.7% | 47.3% |
| 64 | 8 | 9.36 | 8.65 | 59.5% | 39.7% |
| 512 | 1 | 29.64 | 21.69 | 81.3% | 50.1% |
| 512 | 8 | 10.06 | 7.39 | 78.3% | 41.6% |

The optimized version improves median construction throughput in every tested shape: 3.86–29.64× versus before and 3.13–22.31× versus original. Allocation falls 17.2–81.3% versus before. For 64/512 clauses it also falls 39.7–50.1% versus original, but the one-clause optimized builder still allocates 43.6% (1 worker) / 51.2% (8 workers) MORE than original. Therefore this is not an across-the-board memory reduction against the original release.

Eight workers do not yield eightfold throughput. Optimized eight/one-worker median RPS ratios are 1.58× (1 clause), 1.57× (64), and 1.37× (512). This matrix by itself does not identify the cause of scaling limits or measure concurrent mutation of the same builder.

Tail-latency limitation: before, 512 clauses / 1 worker has only 30–32 successful samples per run; its p99 is the observed maximum. Original at that shape has 43–44 samples, also making p99 the maximum. Before at 512 / 8 has only 130–134 samples per run, so p99 is near the second-largest observation. These short runs cannot substantiate production p99 guarantees. Fixed-worker closed-loop results also do not expose overload queues at a fixed external arrival rate. A database/API capacity conclusion requires the separate SQL matrix and longer representative load.

Source-review hypothesis, not a measured attribution: [QueryBuilderCore.cs](G:/Projects/EasyDapper/EasyDapper/QueryBuilding/QueryBuilderCore.cs:119) allocates a new SqlBuilder and QueryCache for each builder; [QueryCache.cs](G:/Projects/EasyDapper/EasyDapper/Caching/QueryCache.cs:13) initializes eight SimpleConcurrentCache instances, each wrapping a ConcurrentDictionary. Prepared query binding does not consult that cache. These unused allocations are a candidate for a separate profile and measurement cycle; this matrix does not isolate their contribution to the one-clause allocation difference. No cache-sharing optimization was included in the measured/final B64 library. Forwarding the populated service cache would alter reference-retention lifetime, so it was left unchanged.

Preparing this analysis did not execute tests, builds, or benchmarks and did not modify production or harness source.



