# Repeated SQL load results — 2026-10-12

Current correctness findings, hashes and fresh load checks are in
[DEEP_REVIEW_2026_10_15.md](DEEP_REVIEW_2026_10_15.md). This table retains the measurements of
the October 12 binaries and is not a new benchmark of the corrected revision.

The optimized library passed all 72 of its main-matrix cases with zero warmup or
measured errors. The two failures in the inventory below are original-version
shared-connection probes at 64/256 workers; they are not throughput successes.
Before and direct Dapper passed all 72 cases each. Original probes have one
repetition and are descriptive; the other versions have three per shape.

This table is the main 234-case matrix only. Separate diagnostic, sustained and
longer basic-read checks are recorded in HIGH_LOAD_VALIDATION.md. Reproduction
uses EasyDapper.LoadTests/RunComparison.ps1. Local raw data, JSON/CSV aggregates
and the evidence-processing script are retained in:
C:\Users\Masoud\AppData\Local\Temp\EasyDapperHighLoad-20261012.
## Detailed aggregate tables

Status: **COMPLETE FILE INVENTORY**. Manifest 234/234; parsed reports 234. Generated 2026-10-12 18:06:01Z.

Passed reports: 232; failed reports: 2; metadata failures: 0; unreadable/metadata issues: 0.
Measured: 12955481 successful / 12955801 attempted operations; 10979675 reads; 1975806 updates; 320 errors. Warmup: 1970614 successful / 1970934 attempted; 320 errors.
Final correctness failures: 0; missing rows: 0; drain timeouts: 0; fixture cleanup failures: 0.

Values below are medians across recorded repetitions. CSV/JSON retain minimum/maximum and failed cases; all 234 source reports parsed successfully.

| Mode | Scenario | Clauses | Workers | Label | Runs/all passed | RPS median [min,max] | p95 ms | p99 ms | Alloc KiB/op | CPU us/success | GC 0/1/2 | Peak MiB |
| --- | --- | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| main | local-mixed | 1 | 1 | before | 3/True | 4192.2 [4028.1,4213.0] | 0.398 | 0.516 | 11.25 | 70.28 | 78/71/1 | 59.4 |
| main | local-mixed | 1 | 1 | dapper | 3/True | 4976.5 [4746.0,5099.4] | 0.288 | 0.360 | 4.87 | 46.38 | 40/0/0 | 55.3 |
| main | local-mixed | 1 | 1 | optimized | 3/True | 4123.6 [4029.7,4132.0] | 0.408 | 0.532 | 11.31 | 71.06 | 75/71/1 | 59.5 |
| main | local-mixed | 1 | 1 | original | 1/True | 5049.6 [5049.6,5049.6] | 0.274 | 0.344 | 5.25 | 50.63 | 44/0/0 | 55.6 |
| main | local-mixed | 1 | 16 | before | 3/True | 17621.4 [17242.9,18222.0] | 2.640 | 3.616 | 11.63 | 36.63 | 342/326/6 | 63.1 |
| main | local-mixed | 1 | 16 | dapper | 3/True | 25963.9 [23416.6,26198.6] | 0.984 | 1.448 | 4.94 | 21.23 | 211/1/1 | 57.3 |
| main | local-mixed | 1 | 16 | optimized | 3/True | 17105.1 [14614.8,17989.2] | 2.672 | 3.616 | 11.62 | 32.07 | 328/312/7 | 62.6 |
| main | local-mixed | 1 | 64 | before | 3/True | 15504.6 [15108.0,15766.4] | 8.704 | 11.328 | 11.68 | 51.17 | 154/146/5 | 78.4 |
| main | local-mixed | 1 | 64 | dapper | 3/True | 20600.5 [20576.4,23297.6] | 5.536 | 7.744 | 4.89 | 20.56 | 117/49/1 | 72.1 |
| main | local-mixed | 1 | 64 | optimized | 3/True | 18535.2 [15444.9,18876.0] | 7.296 | 9.088 | 11.63 | 37.00 | 183/171/5 | 79.1 |
| main | local-mixed | 1 | 64 | original | 1/True | 20899.9 [20899.9,20899.9] | 5.664 | 7.264 | 5.33 | 21.34 | 122/91/1 | 74.9 |
| main | local-mixed | 1 | 128 | before | 3/True | 15589.4 [14662.4,16855.5] | 16.896 | 23.040 | 12.07 | 61.63 | 159/150/6 | 94.2 |
| main | local-mixed | 1 | 128 | dapper | 3/True | 23841.2 [23404.0,23895.7] | 9.344 | 11.136 | 5.24 | 16.78 | 102/89/0 | 91.1 |
| main | local-mixed | 1 | 128 | optimized | 3/True | 17900.9 [17179.2,18488.7] | 14.080 | 17.664 | 12.26 | 47.85 | 191/179/9 | 96.2 |
| main | local-mixed | 1 | 256 | before | 3/True | 17382.4 [17085.4,19324.0] | 28.800 | 36.096 | 12.77 | 99.55 | 189/95/6 | 119.5 |
| main | local-mixed | 1 | 256 | dapper | 3/True | 23899.9 [22174.2,26615.7] | 25.728 | 32.512 | 5.27 | 66.84 | 111/104/1 | 108.9 |
| main | local-mixed | 1 | 256 | optimized | 3/True | 18954.6 [17385.2,19216.7] | 26.624 | 33.024 | 12.69 | 93.63 | 203/124/6 | 119.2 |
| main | local-mixed | 1 | 256 | original | 1/True | 26943.3 [26943.3,26943.3] | 23.424 | 27.904 | 5.69 | 47.25 | 128/119/1 | 110.3 |
| main | local-query | 1 | 1 | before | 3/True | 1501.2 [1461.4,1534.2] | 1.020 | 1.752 | 37.59 | 223.24 | 92/84/1 | 64.1 |
| main | local-query | 1 | 1 | dapper | 3/True | 5196.2 [4940.7,5319.4] | 0.274 | 0.344 | 6.58 | 49.27 | 55/0/0 | 57.3 |
| main | local-query | 1 | 1 | optimized | 3/True | 2865.5 [2793.2,2867.9] | 0.604 | 1.184 | 33.50 | 117.31 | 158/54/1 | 61.1 |
| main | local-query | 1 | 1 | original | 1/True | 1796.9 [1796.9,1796.9] | 0.792 | 1.160 | 21.09 | 157.97 | 62/0/0 | 62.1 |
| main | local-query | 1 | 64 | before | 3/True | 7459.4 [6491.6,7883.1] | 15.488 | 19.200 | 40.72 | 218.53 | 268/265/16 | 84.1 |
| main | local-query | 1 | 64 | dapper | 3/True | 38417.6 [33126.6,39895.9] | 3.168 | 4.192 | 6.69 | 56.03 | 251/219/1 | 75.8 |
| main | local-query | 1 | 64 | optimized | 3/True | 11465.7 [9916.3,11718.1] | 10.560 | 12.928 | 35.97 | 106.96 | 353/346/12 | 80.4 |
| main | local-query | 1 | 64 | original | 1/True | 11999.9 [11999.9,11999.9] | 9.920 | 12.736 | 21.49 | 282.74 | 214/204/0 | 81.0 |
| main | local-query | 1 | 256 | before | 3/True | 5883.7 [5310.5,6001.0] | 58.112 | 67.584 | 41.43 | 189.55 | 219/85/17 | 143.4 |
| main | local-query | 1 | 256 | dapper | 3/True | 39306.5 [37637.0,39790.7] | 11.584 | 14.400 | 6.94 | 50.52 | 228/223/1 | 110.0 |
| main | local-query | 1 | 256 | optimized | 3/True | 7714.6 [7307.0,7824.0] | 48.128 | 55.040 | 37.14 | 121.21 | 260/136/23 | 138.5 |
| main | local-query | 1 | 256 | original | 1/True | 9512.3 [9512.3,9512.3] | 34.560 | 43.264 | 22.55 | 292.70 | 179/176/1 | 104.5 |
| main | local-query | 64 | 1 | before | 3/True | 88.6 [88.2,90.2] | 14.528 | 15.488 | 599.48 | 3659.91 | 87/87/3 | 67.4 |
| main | local-query | 64 | 1 | dapper | 3/True | 1682.9 [1620.6,1690.1] | 1.040 | 1.328 | 44.11 | 142.47 | 120/0/0 | 56.4 |
| main | local-query | 64 | 1 | optimized | 3/True | 726.9 [668.0,766.7] | 2.704 | 3.680 | 257.95 | 410.33 | 284/282/12 | 64.0 |
| main | local-query | 64 | 1 | original | 1/True | 106.5 [106.5,106.5] | 11.904 | 13.440 | 430.14 | 1934.80 | 75/75/0 | 65.8 |
| main | local-query | 64 | 64 | before | 3/True | 321.5 [317.4,327.3] | 235.520 | 276.480 | 624.54 | 11748.19 | 184/49/15 | 105.0 |
| main | local-query | 64 | 64 | dapper | 3/True | 7118.0 [6860.2,7242.5] | 19.200 | 24.448 | 45.89 | 300.32 | 277/274/2 | 76.0 |
| main | local-query | 64 | 64 | optimized | 3/True | 1173.7 [1009.2,1224.3] | 75.264 | 128.000 | 293.54 | 711.55 | 332/77/26 | 98.0 |
| main | local-query | 64 | 64 | original | 1/True | 390.6 [390.6,390.6] | 188.416 | 216.064 | 439.45 | 9463.92 | 154/48/12 | 90.7 |
| main | local-query | 64 | 256 | before | 3/True | 305.2 [294.4,330.7] | 794.624 | 806.912 | 626.79 | 11782.09 | 166/35/3 | 161.6 |
| main | local-query | 64 | 256 | dapper | 3/True | 3590.8 [3211.1,4180.7] | 95.232 | 107.008 | 61.95 | 495.33 | 231/84/15 | 131.4 |
| main | local-query | 64 | 256 | optimized | 3/True | 1231.0 [1121.1,1272.8] | 231.424 | 249.856 | 294.25 | 725.51 | 344/75/16 | 177.5 |
| main | local-query | 64 | 256 | original | 1/True | 383.9 [383.9,383.9] | 663.552 | 675.840 | 439.63 | 9130.12 | 146/42/5 | 136.7 |
| main | local-query | 128 | 1 | before | 3/True | 42.7 [42.3,43.9] | 28.672 | 31.616 | 1288.00 | 15259.93 | 49/49/4 | 68.8 |
| main | local-query | 128 | 1 | dapper | 3/True | 715.7 [696.8,740.9] | 1.896 | 3.120 | 83.83 | 416.84 | 98/0/0 | 57.6 |
| main | local-query | 128 | 1 | optimized | 3/True | 334.0 [318.5,338.0] | 5.664 | 6.912 | 473.87 | 737.90 | 260/258/20 | 64.3 |
| main | local-query | 128 | 1 | original | 1/True | 54.1 [54.1,54.1] | 23.296 | 25.344 | 817.70 | 9571.03 | 36/36/0 | 69.0 |
| main | local-query | 128 | 64 | before | 3/True | 149.2 [129.7,154.7] | 483.328 | 518.144 | 1327.07 | 24971.92 | 190/37/8 | 115.2 |
| main | local-query | 128 | 64 | dapper | 3/True | 2113.4 [1930.2,2314.5] | 64.000 | 82.944 | 93.25 | 1620.25 | 193/144/13 | 85.4 |
| main | local-query | 128 | 64 | optimized | 3/True | 525.7 [516.9,548.3] | 167.936 | 246.784 | 541.71 | 2047.33 | 333/69/22 | 110.7 |
| main | local-query | 128 | 64 | original | 1/True | 165.5 [165.5,165.5] | 446.464 | 483.328 | 852.37 | 24625.75 | 129/40/10 | 93.0 |
| main | local-query | 128 | 256 | before | 3/True | 156.9 [154.0,159.0] | 647.168 | 675.840 | 1331.02 | 23880.48 | 185/33/3 | 156.5 |
| main | local-query | 128 | 256 | dapper | 3/True | 1387.6 [1359.2,1701.9] | 235.520 | 252.928 | 115.84 | 2223.62 | 178/62/13 | 148.9 |
| main | local-query | 128 | 256 | optimized | 3/True | 577.6 [550.3,595.9] | 460.800 | 477.184 | 541.99 | 1734.32 | 321/57/9 | 229.3 |
| main | local-query | 128 | 256 | original | 1/True | 185.6 [185.6,185.6] | 1409.024 | 1441.792 | 854.48 | 19393.68 | 140/37/3 | 149.0 |
| main | local-read | 1 | 1 | before | 3/True | 4208.8 [4187.8,4403.0] | 0.384 | 0.496 | 13.96 | 55.89 | 95/84/1 | 59.1 |
| main | local-read | 1 | 1 | dapper | 3/True | 5243.1 [5094.7,5323.7] | 0.266 | 0.336 | 5.95 | 53.90 | 51/0/0 | 55.0 |
| main | local-read | 1 | 1 | optimized | 3/True | 4331.4 [4262.8,4455.7] | 0.376 | 0.512 | 13.97 | 82.07 | 98/87/1 | 59.1 |
| main | local-read | 1 | 1 | original | 1/True | 5295.8 [5295.8,5295.8] | 0.262 | 0.330 | 6.19 | 31.23 | 54/0/0 | 55.2 |
| main | local-read | 1 | 16 | before | 3/True | 23325.9 [23284.8,23486.2] | 2.544 | 3.392 | 14.77 | 32.39 | 575/557/11 | 63.9 |
| main | local-read | 1 | 16 | dapper | 3/True | 40994.7 [40008.4,41393.8] | 0.660 | 1.408 | 5.94 | 21.52 | 402/1/1 | 57.2 |
| main | local-read | 1 | 16 | optimized | 3/True | 20584.8 [20157.1,22779.6] | 2.736 | 3.744 | 14.76 | 37.11 | 510/489/9 | 62.9 |
| main | local-read | 1 | 64 | before | 3/True | 24264.6 [23200.1,25484.1] | 6.336 | 7.744 | 14.88 | 58.00 | 312/300/12 | 79.4 |
| main | local-read | 1 | 64 | dapper | 3/True | 42389.7 [41399.9,42538.6] | 2.864 | 3.712 | 5.94 | 45.37 | 275/243/1 | 73.2 |
| main | local-read | 1 | 64 | optimized | 3/True | 22559.7 [22261.9,23580.7] | 6.656 | 8.576 | 14.98 | 72.76 | 291/281/11 | 79.2 |
| main | local-read | 1 | 64 | original | 1/True | 40817.7 [40817.7,40817.7] | 2.992 | 3.888 | 6.28 | 59.55 | 271/238/1 | 72.6 |
| main | local-read | 1 | 128 | before | 3/True | 22418.9 [21020.4,22978.3] | 11.008 | 13.440 | 15.61 | 55.72 | 302/291/12 | 96.0 |
| main | local-read | 1 | 128 | dapper | 3/True | 41011.6 [40111.9,41604.0] | 5.472 | 6.880 | 6.09 | 55.90 | 207/193/0 | 90.3 |
| main | local-read | 1 | 128 | optimized | 3/True | 22786.2 [22462.6,23546.8] | 10.688 | 12.544 | 15.49 | 54.46 | 307/299/14 | 98.4 |
| main | local-read | 1 | 256 | before | 3/True | 20491.5 [20443.3,21056.8] | 18.688 | 22.272 | 16.21 | 54.21 | 281/275/6 | 118.0 |
| main | local-read | 1 | 256 | dapper | 3/True | 41649.9 [37555.6,42304.1] | 10.624 | 13.056 | 6.26 | 52.85 | 217/211/1 | 108.4 |
| main | local-read | 1 | 256 | optimized | 3/True | 20900.2 [18951.9,21046.6] | 18.432 | 22.400 | 16.22 | 59.87 | 285/279/6 | 117.2 |
| main | local-read | 1 | 256 | original | 1/True | 40444.2 [40444.2,40444.2] | 10.944 | 13.376 | 6.48 | 61.82 | 218/212/1 | 108.5 |
| main | shared-read | 1 | 1 | before | 3/True | 4491.3 [4461.6,4504.1] | 0.364 | 0.498 | 13.94 | 59.65 | 102/89/1 | 59.4 |
| main | shared-read | 1 | 1 | dapper | 3/True | 5459.8 [5392.9,5487.8] | 0.242 | 0.298 | 5.91 | 45.71 | 53/0/0 | 54.9 |
| main | shared-read | 1 | 1 | optimized | 3/True | 4424.3 [4093.0,4480.4] | 0.380 | 0.512 | 13.96 | 60.64 | 100/88/1 | 59.2 |
| main | shared-read | 1 | 1 | original | 1/True | 5043.7 [5043.7,5043.7] | 0.260 | 0.324 | 6.21 | 41.48 | 51/0/0 | 55.5 |
| main | shared-read | 1 | 16 | before | 3/True | 4041.6 [3945.7,4042.7] | 5.984 | 7.744 | 14.47 | 64.86 | 96/77/2 | 61.3 |
| main | shared-read | 1 | 16 | dapper | 3/True | 4958.7 [4938.0,5000.4] | 4.224 | 5.184 | 6.31 | 46.77 | 49/0/0 | 57.7 |
| main | shared-read | 1 | 16 | optimized | 3/True | 3907.4 [3843.5,3929.6] | 6.240 | 7.968 | 14.48 | 80.26 | 93/75/2 | 61.0 |
| main | shared-read | 1 | 64 | before | 3/True | 3874.2 [3722.6,3938.4] | 23.808 | 27.776 | 14.49 | 65.69 | 88/58/3 | 72.5 |
| main | shared-read | 1 | 64 | dapper | 3/True | 4801.7 [4693.0,5024.4] | 16.512 | 18.688 | 6.28 | 43.43 | 49/1/1 | 64.9 |
| main | shared-read | 1 | 64 | optimized | 3/True | 3810.8 [3761.4,4011.6] | 23.296 | 27.136 | 14.40 | 74.47 | 83/52/3 | 73.4 |
| main | shared-read | 1 | 64 | original | 1/False | 0.0 [0.0,0.0] | 0.632 | 1.112 | 19.04 | n/a | 0/0/0 | 57.3 |
| main | shared-read | 1 | 128 | before | 3/True | 3822.7 [3724.5,3918.3] | 49.152 | 55.296 | 14.39 | 77.71 | 49/27/2 | 83.3 |
| main | shared-read | 1 | 128 | dapper | 3/True | 4978.4 [4781.4,5000.3] | 31.616 | 35.328 | 6.25 | 46.62 | 50/1/0 | 73.0 |
| main | shared-read | 1 | 128 | optimized | 3/True | 3755.9 [3737.3,3877.1] | 48.128 | 53.760 | 14.45 | 72.95 | 47/29/2 | 80.5 |
| main | shared-read | 1 | 256 | before | 3/True | 3734.4 [3627.3,3936.9] | 93.696 | 202.752 | 14.37 | 79.51 | 46/9/2 | 101.8 |
| main | shared-read | 1 | 256 | dapper | 3/True | 4893.9 [4723.0,4950.9] | 63.744 | 75.264 | 6.29 | 47.50 | 49/1/0 | 77.4 |
| main | shared-read | 1 | 256 | optimized | 3/True | 3771.0 [3710.3,3873.9] | 93.696 | 103.424 | 14.42 | 87.50 | 47/9/2 | 99.6 |
| main | shared-read | 1 | 256 | original | 1/False | 0.0 [0.0,0.0] | 0.584 | 1.096 | 14.95 | n/a | 0/0/0 | 75.8 |

| Mode | Scenario | Clauses | Workers | Reference | Eligible | RPS change % | Allocation reduction % | p95 reduction % | p99 reduction % |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: |
| main | local-mixed | 1 | 1 | before | True | -1.6 | -0.5 | -2.5 | -3.1 |
| main | local-mixed | 1 | 1 | dapper | True | -17.1 | -132.2 | -41.7 | -47.8 |
| main | local-mixed | 1 | 1 | original | True | -18.3 | -115.7 | -48.9 | -54.7 |
| main | local-mixed | 1 | 16 | before | True | -2.9 | 0.1 | -1.2 | 0.0 |
| main | local-mixed | 1 | 16 | dapper | True | -34.1 | -135.4 | -171.5 | -149.7 |
| main | local-mixed | 1 | 64 | before | True | 19.5 | 0.4 | 16.2 | 19.8 |
| main | local-mixed | 1 | 64 | dapper | True | -10.0 | -138.1 | -31.8 | -17.4 |
| main | local-mixed | 1 | 64 | original | True | -11.3 | -118.3 | -28.8 | -25.1 |
| main | local-mixed | 1 | 128 | before | True | 14.8 | -1.6 | 16.7 | 23.3 |
| main | local-mixed | 1 | 128 | dapper | True | -24.9 | -134.1 | -50.7 | -58.6 |
| main | local-mixed | 1 | 256 | before | True | 9.0 | 0.6 | 7.6 | 8.5 |
| main | local-mixed | 1 | 256 | dapper | True | -20.7 | -141.0 | -3.5 | -1.6 |
| main | local-mixed | 1 | 256 | original | True | -29.7 | -122.9 | -13.7 | -18.3 |
| main | local-query | 1 | 1 | before | True | 90.9 | 10.9 | 40.8 | 32.4 |
| main | local-query | 1 | 1 | dapper | True | -44.9 | -409.4 | -120.4 | -244.2 |
| main | local-query | 1 | 1 | original | True | 59.5 | -58.9 | 23.7 | -2.1 |
| main | local-query | 1 | 64 | before | True | 53.7 | 11.7 | 31.8 | 32.7 |
| main | local-query | 1 | 64 | dapper | True | -70.2 | -437.4 | -233.3 | -208.4 |
| main | local-query | 1 | 64 | original | True | -4.5 | -67.4 | -6.5 | -1.5 |
| main | local-query | 1 | 256 | before | True | 31.1 | 10.4 | 17.2 | 18.6 |
| main | local-query | 1 | 256 | dapper | True | -80.4 | -435.4 | -315.5 | -282.2 |
| main | local-query | 1 | 256 | original | True | -18.9 | -64.7 | -39.3 | -27.2 |
| main | local-query | 64 | 1 | before | True | 720.5 | 57.0 | 81.4 | 76.2 |
| main | local-query | 64 | 1 | dapper | True | -56.8 | -484.8 | -160.0 | -177.1 |
| main | local-query | 64 | 1 | original | True | 582.3 | 40.0 | 77.3 | 72.6 |
| main | local-query | 64 | 64 | before | True | 265.0 | 53.0 | 68.0 | 53.7 |
| main | local-query | 64 | 64 | dapper | True | -83.5 | -539.6 | -292.0 | -423.6 |
| main | local-query | 64 | 64 | original | True | 200.5 | 33.2 | 60.1 | 40.8 |
| main | local-query | 64 | 256 | before | True | 303.4 | 53.1 | 70.9 | 69.0 |
| main | local-query | 64 | 256 | dapper | True | -65.7 | -375.0 | -143.0 | -133.5 |
| main | local-query | 64 | 256 | original | True | 220.6 | 33.1 | 65.1 | 63.0 |
| main | local-query | 128 | 1 | before | True | 682.9 | 63.2 | 80.2 | 78.1 |
| main | local-query | 128 | 1 | dapper | True | -53.3 | -465.3 | -198.7 | -121.5 |
| main | local-query | 128 | 1 | original | True | 517.6 | 42.0 | 75.7 | 72.7 |
| main | local-query | 128 | 64 | before | True | 252.3 | 59.2 | 65.3 | 52.4 |
| main | local-query | 128 | 64 | dapper | True | -75.1 | -480.9 | -162.4 | -197.5 |
| main | local-query | 128 | 64 | original | True | 217.7 | 36.4 | 62.4 | 48.9 |
| main | local-query | 128 | 256 | before | True | 268.0 | 59.3 | 28.8 | 29.4 |
| main | local-query | 128 | 256 | dapper | True | -58.4 | -367.9 | -95.7 | -88.7 |
| main | local-query | 128 | 256 | original | True | 211.2 | 36.6 | 67.3 | 66.9 |
| main | local-read | 1 | 1 | before | True | 2.9 | -0.1 | 2.1 | -3.2 |
| main | local-read | 1 | 1 | dapper | True | -17.4 | -134.9 | -41.4 | -52.4 |
| main | local-read | 1 | 1 | original | True | -18.2 | -125.5 | -43.5 | -55.2 |
| main | local-read | 1 | 16 | before | True | -11.8 | 0.1 | -7.5 | -10.4 |
| main | local-read | 1 | 16 | dapper | True | -49.8 | -148.6 | -314.5 | -165.9 |
| main | local-read | 1 | 64 | before | True | -7.0 | -0.7 | -5.1 | -10.7 |
| main | local-read | 1 | 64 | dapper | True | -46.8 | -152.4 | -132.4 | -131.0 |
| main | local-read | 1 | 64 | original | True | -44.7 | -138.7 | -122.5 | -120.6 |
| main | local-read | 1 | 128 | before | True | 1.6 | 0.8 | 2.9 | 6.7 |
| main | local-read | 1 | 128 | dapper | True | -44.4 | -154.2 | -95.3 | -82.3 |
| main | local-read | 1 | 256 | before | True | 2.0 | -0.1 | 1.4 | -0.6 |
| main | local-read | 1 | 256 | dapper | True | -49.8 | -159.2 | -73.5 | -71.6 |
| main | local-read | 1 | 256 | original | True | -48.3 | -150.3 | -68.4 | -67.5 |
| main | shared-read | 1 | 1 | before | True | -1.5 | -0.1 | -4.4 | -2.8 |
| main | shared-read | 1 | 1 | dapper | True | -19.0 | -136.1 | -57.0 | -71.8 |
| main | shared-read | 1 | 1 | original | True | -12.3 | -124.9 | -46.2 | -58.0 |
| main | shared-read | 1 | 16 | before | True | -3.3 | -0.1 | -4.3 | -2.9 |
| main | shared-read | 1 | 16 | dapper | True | -21.2 | -129.4 | -47.7 | -53.7 |
| main | shared-read | 1 | 64 | before | True | -1.6 | 0.7 | 2.2 | 2.3 |
| main | shared-read | 1 | 64 | dapper | True | -20.6 | -129.2 | -41.1 | -45.2 |
| main | shared-read | 1 | 64 | original | False | n/a | 24.4 | -3586.1 | -2340.3 |
| main | shared-read | 1 | 128 | before | True | -1.7 | -0.4 | 2.1 | 2.8 |
| main | shared-read | 1 | 128 | dapper | True | -24.6 | -131.1 | -52.2 | -52.2 |
| main | shared-read | 1 | 256 | before | True | 1.0 | -0.3 | 0.0 | 49.0 |
| main | shared-read | 1 | 256 | dapper | True | -22.9 | -129.4 | -47.0 | -37.4 |
| main | shared-read | 1 | 256 | original | False | n/a | 3.5 | -15943.8 | -9336.5 |

- Fresh process per case; closed-loop fixed workers, not an arrival-rate or production-capacity certification.
- Successful RPS denominator includes drain. Main and provider-diagnostic cases are separate.
- p95/p99 values are per-run histogram upper bounds. Aggregate percentiles are medians of per-run percentiles, not pooled request percentiles.
- Allocated bytes include in-window harness/observer work; pre-window histogram allocation and post-window correctness checks are excluded.
- Original failed runs remain visible but are ineligible for successful-throughput comparisons.
- Basic GetById hot path invokes neither ParameterBuilder nor ExpressionParser; do not causally attribute its run-to-run variation to those two optimizations.
- Own-session DMV CPU differs from whole-server process CPU. Unavailable process counters remain null.

This script is evidence processing only; it performs no new benchmark, build, test or SQL command.


