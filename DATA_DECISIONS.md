# Data Decisions

## Approved Decisions

| Topic | Decision |
|---|---|
| Project domain | NYC taxi demand and weather |
| Taxi dataset family | NYC TLC Yellow Taxi |
| Temporal modeling granularity | Hourly |
| Geographic modeling concept | Taxi Zone |
| Target concept | Hourly `pickup_count` |
| Forecast horizons | 1h, 3h, and 6h |
| Processing technology | Apache Spark |
| Object store | MinIO |
| AI category | Machine learning / predictive forecasting |
| Raw weather format | JSON |
| Data architecture | Bronze / Silver / Gold |
| Study period | Calendar year 2025 (2025-01-01 through 2025-12-31) |
| Weather provider | Open-Meteo |
| Forecast-weather source | ECMWF IFS Single Runs with a conservative six-hour publication lag |
| Observed-weather role | Reference/diagnostic only; not a substitute for ex-ante predictors |
| Weather variables | `temperature_2m`, `relative_humidity_2m`, `precipitation`, `snowfall`, `weather_code`, `wind_speed_10m`, `wind_gusts_10m` |
| Weather spatial strategy | Five fixed NYC points; map each Taxi Zone centroid to the nearest point |
| Taxi Zone selection method | Smallest five-borough demand-ranked set covering at least 95% of pickups, validated over the full study period |

## Evidence and Trade-offs

| Decision | Evidence and rationale | Alternative/trade-off |
|---|---|---|
| Calendar year 2025 | All 12 official TLC files returned HTTP 200; total compressed size is 829,973,299 bytes (791.52 MiB). One calendar year covers all seasons and stays within the 2025 schema/policy regime. | More years add seasonal repetitions but increase compute and introduce schema and policy changes. |
| Open-Meteo | Official JSON APIs provide hourly UTC data, tested variables, multi-point requests, and an exact January 2025 ECMWF run. | NASA POWER has stronger primary-agency provenance but is coarse reanalysis and cannot reproduce forecasts available at prediction time. |
| ECMWF IFS Single Runs | Exact run `2025-01-01T00:00` returned successfully; the archive begins before the study period. A six-hour lag conservatively respects documented model processing time. | Stitched historical forecasts are easier but obscure issuance-time availability; observed/reanalysis weather risks leakage. |
| Seven weather variables | All seven were returned at hourly resolution; they cover thermal conditions, moisture, precipitation/snow, weather state, and wind without adding dozens of weakly motivated fields. | More variables may add signal but increase collinearity and explanation burden; importance remains an empirical modeling question. |
| Five weather points | January showed mean/p95 cross-point temperature ranges of 1.0332/2.7 °C, mean/p95 wind ranges of 4.4445/7.9 km/h, and 26 wet/dry disagreement hours. | One point is simpler but discards measurable spatial variation; per-zone weather is unnecessary at ~9 km model resolution. |
| 95% demand-coverage selection rule | January's smallest 95% set contains 53 zones and its least-active member has demand in 85.7527% of hours. Raising coverage to 99% requires 121 zones and lowers the minimum active-hour rate to 41.8011%. | A fixed arbitrary zone count is simpler but not evidence-based. Exact IDs must be recomputed over the full year. |

## Evidence-Dependent Decisions

| Topic | Status | Evidence needed |
|---|---|---|
| Final Taxi Zone IDs | TBD | Apply the approved 95% coverage rule to the full 2025 period and verify active-hour sparsity |
| Final ML algorithms | TBD | Baseline results, data scale, interpretability, and validation evidence |
| Elasticsearch and Kibana in final architecture | TBD | Core-pipeline stability and demonstrated analytical value |

Elasticsearch and Kibana are therefore provisional post-core components, not a completed architecture commitment.
