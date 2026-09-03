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

## Evidence-Dependent Decisions

| Topic | Status | Evidence needed |
|---|---|---|
| Exact taxi date range | TBD | Source availability, volume, seasonality coverage, and local resource feasibility |
| Exact weather provider | TBD | Historical coverage, licensing, reliability, variables, and reproducibility |
| Historical observed vs historical forecast weather | TBD | Provider availability and alignment with the forecasting claim |
| Exact weather variables | TBD | Coverage, data quality, domain relevance, and leakage assessment |
| Final Taxi Zone subset | TBD | Measured demand coverage and sparsity |
| Final ML algorithms | TBD | Baseline results, data scale, interpretability, and validation evidence |
| Elasticsearch and Kibana in final architecture | TBD | Core-pipeline stability and demonstrated analytical value |

Elasticsearch and Kibana are therefore provisional post-core components, not a completed architecture commitment.
