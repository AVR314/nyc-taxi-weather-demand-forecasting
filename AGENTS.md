# Project Instructions

## Project Goal

Build an end-to-end Big Data + AI project that is technically correct, reproducible, easy to explain, and fully aligned with the university assignment.

Research question: Can weather information improve short-term taxi demand forecasting across NYC taxi zones, and how does its predictive value change across 1-hour, 3-hour, and 6-hour forecasting horizons?

## Planned Core Architecture

NYC TLC Yellow Taxi Parquet + hourly historical weather JSON → MinIO Bronze → Apache Spark → cleaning and data-quality validation → Taxi Zone × Hour demand aggregation → weather join → MinIO Silver → leakage-safe feature engineering → demand forecasting ML → 1h / 3h / 6h horizons → comparison with weather vs without weather → MinIO Gold → Elasticsearch → Kibana.

Elasticsearch and Kibana are provisional downstream components. Add them only after the core pipeline works and the final architecture decision supports them.

## Architecture Rules

- Bronze data must preserve raw source data.
- Silver contains validated, cleaned, and transformed data.
- Gold contains predictions, metrics, and analytical outputs.
- Spark must perform meaningful transformation, not be included merely for appearance.
- Weather raw JSON provides semi-structured data.
- MinIO is the object-store Big Data technology.
- Elasticsearch and Kibana may be added after the core pipeline works.
- Do not add Kafka unless a genuine streaming requirement later justifies it.
- Prefer a smaller, understandable architecture over unnecessary complexity.

## ML and Research Rules

- Target: hourly `pickup_count` by Taxi Zone.
- Planned forecast horizons: 1h, 3h, and 6h.
- No future-data leakage.
- Lag and rolling features must use only information available before prediction time.
- Train, validation, and test splits must be chronological. Never randomly shuffle a time-series split.
- Establish simple forecasting baselines before evaluating ML.
- Weather and no-weather comparisons must otherwise use equivalent inputs and evaluation periods.
- Do not assume weather improves forecasting. Conclusions must follow actual results.
- If observed historical weather is used instead of historical forecast weather, explicitly document this limitation.
- Do not choose the number of Taxi Zones arbitrarily. Base it on measured demand coverage and sparsity.

## Data Quality Rules

- Never silently discard invalid records. Produce counts and reasons for rejected records.
- Validate timestamps, Taxi Zone IDs, temporal coverage, duplicates, and weather coverage.
- Keep zero-demand Zone × Hour records where appropriate.
- Preserve enough metadata to reproduce ingestion.

## Engineering Rules

For every implementation phase:

1. Inspect.
2. Implement.
3. Run.
4. Validate.
5. Test.
6. Fix failures.
7. Update documentation and status.
8. Commit.
9. Push.

- Never commit secrets.
- Never commit large raw datasets unless intentionally adding a tiny sample.
- Avoid machine-specific absolute paths.
- Keep commands reproducible.
- Tests must cover important data and leakage assumptions.
- Never invent metrics, outputs, results, or screenshots.
- Never declare a phase complete if validation failed.

## Documentation and Understanding Rules

Every significant design decision must document what was chosen, why it was chosen, relevant alternatives, the trade-off, and supporting evidence when applicable.

The final project must be explainable by the student. Avoid unnecessary abstractions or technologies that make Q&A harder without adding project value.

## Assignment Requirements to Track

Track all assignment requirements in `REQUIREMENTS_TRACEABILITY.md`, including the real dataset, ETL/ELT, semi-structured or unstructured data, required Big Data technology, meaningful transformation, results storage, results and insights, AI on project data, source code, run instructions, design document, architecture diagram, data-flow and technology explanation, dataset link or sample, presentation, demo, findings and trade-offs, and Q&A readiness.

The assignment states teams of three students. Approval for solo work is an open administrative blocker. Never claim it is resolved until the student explicitly confirms approval.
