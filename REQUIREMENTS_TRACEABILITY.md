# Requirements Traceability

Status values: `NOT STARTED`, `IN PROGRESS`, `VALIDATED`, `BLOCKED`.

| Requirement | Planned implementation | Evidence/artifact | Status |
|---|---|---|---|
| Real dataset | Use NYC TLC Yellow Taxi trip records and historical weather data. | All 12 official 2025 taxi files, 1,454 forecast responses, eight unavailable responses, and two references preserved in Bronze with provenance | VALIDATED |
| Working ETL or ELT | Ingest raw taxi Parquet and weather JSON, validate and transform with Spark, and publish Silver/Gold outputs. | Full 2025 Spark Silver ETL produced classified taxi, complete demand, weather, mapping, and joined Parquet outputs | VALIDATED |
| Semi-structured or unstructured data | Ingest raw hourly archived weather forecasts as JSON. | 1,454 successful JSON responses and eight provider-error JSON responses preserved unchanged | VALIDATED |
| Required Big Data technology | Use MinIO object storage for Bronze, Silver, and Gold layers. | `compose.yaml`; `bigdata` bucket and seven prefixes initialized; S3A Parquet round trip passed before and after restart | VALIDATED |
| Meaningful transformation | Clean records, report rejections, build Zone × Hour demand, join weather, and engineer leakage-safe modeling features. | Phase 4 validated join plus 1,944,276 Phase 5A feature rows; zero timestamp, rolling-end, or key violations | VALIDATED |
| Repository or storage for results | Store predictions, metrics, and analytical outputs in MinIO Gold; evaluate Elasticsearch as a downstream store. | Gold object inventory and, if retained, Elasticsearch indexes | NOT STARTED |
| Results and insights | Analyze forecast accuracy by horizon and the incremental value of weather. | Phase 5B validation/test MAE and RMSE for three non-ML baselines; Phase 5C measured weather A/B deltas (negative at every horizon) in `docs/ml_selection_validation.md`; frozen test-set evaluation of the selected model remains pending | IN PROGRESS |
| AI capability on project data | Train and evaluate demand-forecasting models using project-derived features. | Phase 5C trained regularized Linear Regression and Gradient-Boosted Trees on train, selected by validation MAE; `docs/ml_selection_validation.md` and `data/silver/ml_candidate_selection_report.json` | VALIDATED |
| Source code | Maintain reproducible pipeline, validation, modeling, and test code in Git. | Feasibility, Bronze, Silver ETL, modeling-feature, baseline, and ML-selection packages, infrastructure smoke tests, and focused Spark tests | IN PROGRESS |
| Reproducible containerized infrastructure | Keep Spark, Java, Hadoop, S3A, and MinIO versioned and runnable without host installations. | `compose.yaml`; `docker/spark/Dockerfile`; pinned versions; clean startup/restart/shutdown validation | VALIDATED |
| Taxi/weather time alignment | Define the modeling calendar, convert local civil time to UTC reproducibly, and handle DST without silent coercion. | `docs/time_axis_audit.md`; conversion helpers; nine focused time-axis/Bronze tests | VALIDATED |
| README with run instructions | Expand the README with prerequisites, setup, execution, validation, and troubleshooting steps. | Phase 2–5B setup, focused tests, ingestion, Silver ETL, features, and baseline execution commands in `README.md` | IN PROGRESS |
| 1–2 page design document | Summarize architecture, data flow, modeling, validation, and trade-offs. | Final design document | NOT STARTED |
| Architecture diagram | Diagram sources, processing, storage layers, modeling, and outputs. | Version-controlled diagram and exported image/PDF | NOT STARTED |
| Data-flow and technology explanation | Explain each component's role and why it is necessary. | README and design-document sections | NOT STARTED |
| Dataset link or sample | Provide official dataset links; add a tiny sample only if later needed. | Official links in both feasibility profiles and source URLs/parameters in the Bronze manifest | VALIDATED |
| Presentation slides | Prepare concise slides covering problem, design, implementation, results, and lessons. | Presentation deck | NOT STARTED |
| Live or recorded demo | Demonstrate reproducible pipeline execution and validated outputs. | Demo plan and live/recorded evidence | NOT STARTED |
| Results, challenges, and trade-offs explanation | Report only observed results and explain limitations and design compromises. | Final report, README, and presentation | NOT STARTED |
| Readiness for Q&A | Ensure the student can explain architecture, code, evidence, limitations, and alternatives. | Q&A notes and rehearsal checklist | NOT STARTED |
| Team of three students or approved exception | Obtain explicit university approval before representing solo work as compliant. | Written approval supplied by the student | BLOCKED |
