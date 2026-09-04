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
| Study period | Calendar year 2025 in `America/New_York`: local start inclusive `2025-01-01T00:00:00`, local end exclusive `2026-01-01T00:00:00` |
| Canonical join axis | UTC instants; the local modeling year maps to `[2025-01-01T05:00Z, 2026-01-01T05:00Z)` |
| TLC timestamp convention | Treat timezone-naive values as `America/New_York` wall time as an explicit modeling convention, not a TLC-documented source fact |
| DST anomaly handling | Quarantine nonexistent spring-forward and ambiguous fall-back taxi wall times; never shift or guess a fold |
| Final modeling Taxi Zones | 74-zone full-year set selected by the approved smallest-set-at-least-95% rule |
| Demand availability at fall-back | Mark both UTC instants for the unresolved repeated local 01 hour unavailable for every zone; never encode them as zero demand |
| Zone-to-weather mapping | Compute centroids in the official Taxi Zone archive's projected CRS, transform centroids to EPSG:4326, then select the nearest approved weather point by deterministic haversine distance |
| Weather provider | Open-Meteo |
| Forecast-weather source | ECMWF IFS Single Runs with a conservative six-hour publication lag |
| Observed-weather role | Reference/diagnostic only; not a substitute for ex-ante predictors |
| Weather variables | `temperature_2m`, `relative_humidity_2m`, `precipitation`, `snowfall`, `weather_code`, `wind_speed_10m`, `wind_gusts_10m` |
| Weather spatial strategy | Five fixed NYC points; map each Taxi Zone centroid to the nearest point |
| Taxi Zone selection method | Smallest five-borough demand-ranked set covering at least 95% of pickups, validated over the full study period |
| Local Spark deployment | Pinned Spark 3.5.7 standalone cluster with one master and one worker in Docker Compose |
| Spark object-store connector | Hadoop 3.3.4 S3A with `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262 |
| MinIO layout | `bigdata` bucket with Bronze, Silver, and Gold prefixes initialized idempotently |
| Unavailable archived forecasts | Preserve provider responses and report exact missing coverage; do not impute or substitute observed/reanalysis weather in Bronze |
| Demand-history feature timing | For target `t` and horizon `h`, every demand feature is computed per zone from data at or before cutoff `t-h`; rolling windows include and end at the cutoff |
| Same-hour demand lags | Resolve previous-day/week demand by `America/New_York` local wall hour; if that wall hour is an unresolved DST fold, keep the feature missing rather than choosing an instant |
| Modeling feature sets | A is demand/calendar only; B is the identical A columns plus the seven forecasts, per-variable missing flags, any-weather-missing, and source-response availability |
| Trailing demand variability | Include population standard deviation over the complete 24-hour window ending at cutoff; leave it missing when any window hour is unavailable |
| Paired evaluation population flag | Mark a row eligible only when every demand-history feature and every weather predictor is present; defer actual row selection until model evaluation |
| Chronological split protocol | Use target local time in `America/New_York`: train `[2025-01-01, 2025-09-01)`, validation `[2025-09-01, 2025-11-01)`, and frozen test `[2025-11-01, 2026-01-01)` for every zone, horizon, and feature set; never random split |
| Primary A/B comparison population | Use exactly `paired_evaluation_eligible == true` for both feature sets without imputation; preserve excluded-row reasons by split and horizon |
| Baseline selection rule | Select by overall validation MAE only; RMSE is secondary and test metrics cannot affect selection or future design |
| Selected non-ML baseline | Previous-week seasonal naive, selected from validation MAE 15.122268; the test set remains frozen and its results were not used for selection |
| Final frozen TEST evaluation protocol | Refit the Phase 5C-selected Regularized Linear Regression configuration (frozen hyperparameters per horizon/feature set) on train+validation only, score once on frozen test, and never change model/feature/hyperparameter choices afterward |

## Evidence and Trade-offs

| Decision | Evidence and rationale | Alternative/trade-off |
|---|---|---|
| Calendar year 2025 | All 12 official TLC files returned HTTP 200; total compressed size is 829,973,299 bytes (791.52 MiB). One calendar year covers all seasons and stays within the 2025 schema/policy regime. | More years add seasonal repetitions but increase compute and introduce schema and policy changes. |
| Open-Meteo | Official JSON APIs provide hourly UTC data, tested variables, multi-point requests, and an exact January 2025 ECMWF run. | NASA POWER has stronger primary-agency provenance but is coarse reanalysis and cannot reproduce forecasts available at prediction time. |
| ECMWF IFS Single Runs | Exact run `2025-01-01T00:00` returned successfully; the archive begins before the study period. A six-hour lag conservatively respects documented model processing time. | Stitched historical forecasts are easier but obscure issuance-time availability; observed/reanalysis weather risks leakage. |
| Seven weather variables | All seven were returned at hourly resolution; they cover thermal conditions, moisture, precipitation/snow, weather state, and wind without adding dozens of weakly motivated fields. | More variables may add signal but increase collinearity and explanation burden; importance remains an empirical modeling question. |
| Five weather points | January showed mean/p95 cross-point temperature ranges of 1.0332/2.7 °C, mean/p95 wind ranges of 4.4445/7.9 km/h, and 26 wet/dry disagreement hours. | One point is simpler but discards measurable spatial variation; per-zone weather is unnecessary at ~9 km model resolution. |
| 95% demand-coverage selection rule | January's smallest 95% set contains 53 zones and its least-active member has demand in 85.7527% of hours. Raising coverage to 99% requires 121 zones and lowers the minimum active-hour rate to 41.8011%. | A fixed arbitrary zone count is simpler but not evidence-based. Exact IDs must be recomputed over the full year. |
| One Spark master and one worker | The standalone worker executed the validated S3A round-trip job after both clean startup and service restart. This is the smallest cluster shape that exercises remote executor scheduling rather than driver-only local mode. | Local mode would use fewer containers but would not validate worker execution; adding more workers would add no Phase 2 evidence. |
| Hadoop 3.3.4 S3A dependency set | The official Spark 3.5.7 image bundles Hadoop client 3.3.4. Matching `hadoop-aws` 3.3.4 and its AWS SDK bundle 1.12.262 successfully wrote and read Parquet through MinIO. | Runtime package resolution is smaller initially but depends on mutable local caches and network access on every run. The custom image is larger because the SDK bundle is embedded. |
| One MinIO bucket with layer prefixes | The initializer created `bigdata` plus seven idempotent marker-backed prefixes, and MinIO readiness was checked before Spark startup. | Separate buckets can provide stronger policy boundaries but add configuration without a demonstrated local-project need. |
| Preserve archived-forecast gaps | Eight of 1,462 required ECMWF run positions were unavailable from the provider. Their responses are retained, and 9,810 missing predictor slots across 136 target hours are reported by run, target hour, horizon, point, and variable. | Imputation or observed-weather substitution would hide source limitations or violate the ex-ante research design; any modeling treatment requires Phase 4 evidence. |
| Local modeling-year time axis | TLC officially defines pickup time as when the meter was engaged, but its page and data dictionary provide no timezone/offset, and the Parquet timestamp is timezone-naive. NYC civil time is the relevant demand calendar, so `America/New_York` is declared as the reproducible modeling convention and converted to UTC for weather joins. | Treating naive values as UTC would shift local demand patterns by four or five hours. The local convention is evidence-limited and must remain explicit. |
| DST quarantine | In 2025 New York skips local 02:00 on 9 March and repeats local 01:00 on 2 November. A naive timestamp cannot distinguish the repeated folds. | Shifting nonexistent values or choosing a fold fabricates timing; quarantine preserves uncertainty but removes affected taxi records/targets from modeling. |
| Full-year 95% Taxi Zone set | The 74 selected zones contain 46,215,963 of 48,601,811 accepted five-borough pickups (95.0910307%). The least temporally active selected zone is Zone 66, active in 7,600 of 8,758 available hours (86.7778032%). Exact IDs are recorded in `docs/silver_etl_validation.md`. | January's 53-zone feasibility set was provisional. Applying the approved rule to the full year increases coverage stability without silently changing the rule. |
| Explicit complete demand grid | The 262-zone × 8,760-hour grid contains 2,295,120 rows and 883,059 measured zero-demand rows. The 524 zone-hours at the two unresolved fall-back instants remain null/unavailable. | Omitting zeros biases activity patterns; turning unresolved DST targets into zeros would fabricate negative evidence. |
| Weather missingness in Silver | Of 131,400 required records, 130,680 have source responses, 720 arise from unavailable runs, and 1,890 have at least one missing predictor. | Complete expected keys plus missingness flags preserve join cardinality without imputation or observed-weather substitution. |
| Leakage-safe modeling features | The 74-zone feature build produced 1,944,276 rows: 648,092 per horizon. All audited source timestamps were at or before cutoff, rolling-window end violations and duplicate keys were zero, and feature set B differs from A only by weather-related columns. | Source-time audit columns increase Silver size, but make temporal claims directly testable. |
| Missing-feature preservation | 43,438 rows lack at least one demand-history feature, 27,972 have a missing weather predictor, and 1,872,866 satisfy the paired-evaluation flag. No missing value was imputed or silently excluded from the persisted feature data. | Keeping all eligible-target rows preserves evidence for a later, explicitly approved missing-data strategy. |
| Fixed split populations | Train has 1,228,770 paired rows, validation 325,008, and test 319,088. Exclusions total 65,712/0/5,698 respectively; reasons are preserved independently and exclusively in the Silver manifest. | A random split would inflate similarity across time and cannot represent forward forecasting. The chronological split leaves only eight months for fitting but protects temporal validity. |
| Baseline validation evidence | Overall validation MAE/RMSE were 39.748108/75.011838 for persistence, 21.330961/44.829499 for previous-day naive, and 15.122268/29.725824 for previous-week naive. | Persistence is competitive at 1h but degrades sharply with horizon; the seasonal baseline is a stronger overall threshold for future ML. |
| ML candidate selection | Regularized Linear Regression on feature set A (no weather) was selected at every horizon from validation MAE only: 12.326950 (1h), 14.180445 (3h), 14.457571 (6h), each below the frozen previous-week baseline (15.122268). Full grid, weather deltas, and integrity checks are in `docs/ml_selection_validation.md`. | A small predeclared validation-only grid over two model families, fit on train only, avoids tuning against the frozen test set while still comparing linear and tree-ensemble capacity. |
| Weather A/B outcome | Adding approved weather predictors (feature set B) did not reduce validation MAE for either model family at any horizon; the measured change ranged from -0.18% to -1.13% (negative = MAE increased). | The result was measured, not assumed; feature set A is therefore selected. This does not by itself rule out weather value under different features, horizons, or evaluation windows. |
| Final frozen TEST evaluation | The frozen Phase 5C Regularized Linear Regression configuration (regParam 0.1/0.1/0.01 for 1h/3h/6h) was refit on train+validation and scored once on frozen test: MAE 13.789029 (1h A), 13.775351 (1h B), 17.476389 (3h A), 17.474982 (3h B), 18.350151 (6h A), 18.369958 (6h B); feature set A beats the frozen baseline test MAE at every horizon. Weather TEST deltas are small and change sign by horizon (+0.099%, +0.008%, -0.108%). Full detail in `docs/final_test_evaluation_validation.md`. | No model, feature, or hyperparameter change was made after observing TEST; Gradient-Boosted Trees was excluded because it was not selected in Phase 5C. |

## Evidence-Dependent Decisions

| Topic | Status | Evidence needed |
|---|---|---|
| Secondary treatment of incomplete predictor rows | TBD | Primary A/B evaluation is frozen to the complete paired population; decide whether a separately labeled secondary analysis adds value without contaminating the primary comparison |
| Elasticsearch and Kibana in final architecture | TBD | Core-pipeline stability and demonstrated analytical value |

Elasticsearch and Kibana are therefore provisional post-core components, not a completed architecture commitment.
