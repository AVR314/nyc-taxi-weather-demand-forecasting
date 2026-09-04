# Project Status

## Current Phase

Phase 5A leakage-safe modeling features are complete and validated. Train/test
splitting, modeling, Gold outputs, Elasticsearch, and Kibana have not started.

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

## Open Blockers

- **Administrative:** The assignment specifies teams of three students. Approval for solo work is not confirmed.

## Decisions Awaiting Evidence

- Final train-time treatment of preserved demand-history and archived-forecast gaps.
- Final ML algorithms after baselines and feasibility evidence.
- Whether Elasticsearch and Kibana remain in the final architecture.

## Next Action

Await explicit authorization for the next phase. Do not start train/test
splitting, ML, Gold, Elasticsearch, or Kibana.
