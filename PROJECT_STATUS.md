# Project Status

## Current Phase

Phase 6A final submission documentation is complete: design document,
architecture diagram, polished README, and requirements audit are done.
Phase 5D final frozen TEST evaluation remains complete and validated. Gold
predictions and metrics are published. Elasticsearch and Kibana are
excluded from the final architecture (see Closed Decisions).

## Completed and Validated

- GitHub repository initialized and pushed.
- Python 3.12.8 validated.
- WSL 2 and Ubuntu validated.
- Docker Engine and Docker Compose validated with the WSL 2 backend.
- Permanent governance and traceability documents established.
- Official January 2025 Yellow Taxi Parquet profiled without cleaning: 3,475,226 rows and 59,158,238 bytes.
- Official taxi lookup and geographic archive located; all observed pickup IDs were valid lookup IDs.
- All 12 official 2025 Yellow Taxi files verified by HEAD request; calendar year 2025 approved as the study period.
- Open-Meteo observed, stitched historical-forecast, and exact ECMWF single-run JSON endpoints validated for January 2025.
- NASA POWER hourly JSON validated as an observed/reanalysis comparison source.
- Five-point NYC weather strategy and a 95% demand-coverage Taxi Zone selection rule approved from measured evidence.
- Docker Compose model validated with only MinIO, an idempotent one-shot bucket initializer, one Spark master, and one Spark worker.
- MinIO `bigdata` bucket initialized with the seven planned Bronze, Silver, and Gold prefixes.
- Spark 3.5.7 and Hadoop 3.3.4 container image built with pinned S3A dependencies: `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262.
- Spark worker wrote a three-row DataFrame to MinIO as Parquet through `s3a://`, read it back, and validated the exact schema, row count, and values.
- The same Spark-MinIO round trip passed after restarting MinIO, the Spark master, and the Spark worker.
- Clean stack shutdown removed all Phase 2 containers and the Compose network while preserving the named MinIO data volume.
- All 12 official 2025 Yellow Taxi Parquet files were preserved unchanged in Bronze: 829,973,299 bytes total with per-object SHA-256 provenance.
- The official Taxi Zone lookup and geographic archive were preserved unchanged in Bronze reference storage.
- The corrected local-year weather plan contains 1,462 Open-Meteo ECMWF IFS Single Runs; 1,454 raw forecast JSON responses and eight raw provider-unavailable responses are preserved.
- Bronze contains 1,476 source objects totaling 843,318,877 bytes. The original 1,475 objects passed deep checksum/object verification; the one added boundary response passed upload checksum verification.
- An idempotent resume reused all 12 taxi files, both references, and all 1,461 weather outcomes without source downloads.
- Machine-readable missing coverage identifies 9,810 required predictor slots across 136 target hours; no observed/reanalysis substitution or imputation was performed.
- The complete six-test suite passed after the detailed missing-coverage implementation was added.
- The time-axis audit established an explicit `America/New_York` modeling year mapped to `[2025-01-01T05:00Z, 2026-01-01T05:00Z)` and documented the TLC source's missing timezone semantics.
- One additional boundary run (`2025-12-31T18:00Z`) was acquired successfully; all 1,461 existing weather outcomes were reused and missing coverage remained unchanged.
- Nine focused Bronze/time-axis tests validate year boundaries, leakage-safe run planning, and the 2025 spring-forward/fall-back behavior.
- Spark read 48,722,602 Bronze taxi rows, accepted 48,601,811 five-borough local-year pickups, rejected 109,104, and quarantined 11,687 ambiguous fall-back rows.
- The complete 262-zone × 8,760-hour demand grid contains 2,295,120 rows, including 883,059 legitimate zeros and 524 DST-unavailable zone-hours.
- The approved full-year 95% rule selected 74 zones covering 46,215,963 accepted pickups (95.0910307%); the least active selected zone is active in 86.7778032% of available hours.
- Silver weather contains 131,400 required records: 130,680 source-present, 720 from unavailable runs, and 1,890 with at least one missing predictor; leakage violations and duplicate keys are zero.
- The demand-weather join contains exactly 6,885,360 rows with zero unmatched rows, duplicate keys, or many-to-many expansion.
- Five focused synthetic Spark tests passed, and the full Silver inventory contains 85 data objects totaling 1,516,380,422 bytes before the validation report.
- The approved 74-zone modeling scope contains 1,944,720 candidates; 444 unavailable DST targets were explicitly excluded and 1,944,276 feature rows were persisted, exactly 648,092 per horizon.
- Zone-specific cutoff, lag, local previous-day/week, trailing mean, and trailing 24-hour standard-deviation features were built without target demand or future timestamps; all source-time and rolling-window audits passed with zero violations.
- Feature set A contains only 13 demand/calendar predictors. Feature set B adds only the seven approved forecast variables and their weather-related availability indicators.
- Missing values were preserved: 43,438 rows lack at least one demand-history feature, 27,972 have at least one weather predictor missing, and 1,872,866 rows satisfy the future paired-evaluation completeness flag.
- Six focused Phase 5A Spark tests passed. The modeling feature path contains 37 objects totaling 90,690,988 bytes, including `_SUCCESS`.
- Chronological local-time splits are frozen: train `[2025-01-01, 2025-09-01)`, validation `[2025-09-01, 2025-11-01)`, and test `[2025-11-01, 2026-01-01)`, with zero split overlap or key duplication.
- The single future A/B comparison population contains 1,228,770 train rows, 325,008 validation rows, and 319,088 frozen test rows; no imputation or full feature-data duplication was performed.
- Validation overall MAE/RMSE are 39.748108/75.011838 for persistence, 21.330961/44.829499 for previous-day seasonal naive, and 15.122268/29.725824 for previous-week seasonal naive.
- Previous-week seasonal naive was selected using validation MAE only. Its reported test MAE/RMSE are 20.991755/43.118488 and were not used for selection.
- All baseline source timestamps were at or before cutoff, target demand was not a predictor, and seven focused Phase 5B Spark tests passed.
- Phase 5C trained regularized Linear Regression and Gradient-Boosted Trees on train only and scored on validation only (409,590 train / 108,336 validation rows per horizon, identical for feature sets A and B, zero test rows read).
- The selected model at every horizon is regularized Linear Regression on feature set A (demand/calendar + zone, no weather): validation MAE/RMSE 12.326950/22.095700 (1h), 14.180445/26.519300 (3h), 14.457571/27.449900 (6h), each beating the frozen previous-week baseline (MAE 15.122268) by 18.48%, 6.23%, and 4.40% respectively.
- Measured weather A/B deltas were negative (weather did not reduce validation MAE) for both model families at all three horizons, ranging from -0.18% to -1.13%.
- Eight focused Phase 5C Spark tests passed; zero A/B population or key mismatches, zero invalid predictions, and the identical zero-clipping nonnegative rule applied to all models.
- Phase 5D refit the frozen Phase 5C Regularized Linear Regression configuration (Gradient-Boosted Trees excluded, not selected) on train+validation (517,926 rows per horizon) and evaluated once on the frozen test partitions (106,412/106,338/106,338 rows for 1h/3h/6h).
- Final TEST MAE/RMSE: 13.789029/24.889088 (1h A), 13.775351/24.882623 (1h B), 17.476389/33.478442 (3h A), 17.474982/33.467007 (3h B), 18.350151/35.703538 (6h A), 18.369958/35.706288 (6h B); the selected feature set A beats the frozen previous-week baseline test MAE (20.969355/20.996511/21.009413) by 7.180326/3.520122/2.659262 at every horizon.
- Measured weather TEST deltas are small and change sign by horizon: +0.099% (1h), +0.008% (3h), -0.108% (6h); this does not reverse the Phase 5C validation finding that weather's effect is near zero.
- Six focused Phase 5D Spark tests passed; zero A/B TEST key mismatches, zero test rows used for fitting/preprocessing, zero null/NaN predictions, and configurations frozen before TEST access.
- Gold predictions (638,176 rows, `s3a://bigdata/gold/predictions`) and the final metrics manifest (`s3a://bigdata/gold/metrics/final_test_evaluation_report.json`) are published.
- Phase 6A added `docs/design_document.md` (1-2 page summary) and `docs/architecture_diagram.md` (Mermaid source) and rewrote `README.md` with a research-question summary, results table, architecture overview, dataset sources, repository structure, validation summary, and limitations, using only already-measured evidence.

## Open Blockers

- **Administrative:** The assignment specifies teams of three students. Approval for solo work is not confirmed.

## Closed Decisions

- Elasticsearch and Kibana are **not included** in the final architecture; MinIO already satisfies the required Big Data storage technology and the extra layer adds no demonstrated value to the research question.
- No secondary post-TEST modeling analysis will be performed; the primary paired-population A/B evaluation is frozen.
- The frozen TEST evaluation will never be retuned.

## Next Action

Core pipeline, modeling, frozen TEST evaluation, and submission
documentation (design document, architecture diagram, polished README,
requirements audit) are complete. Remaining work is the presentation,
demo, Q&A rehearsal, and the still-open team-of-three administrative
approval. Do not re-run or re-tune the frozen TEST evaluation.
