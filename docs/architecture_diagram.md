# Architecture and Data-Flow Diagram

Source format is Mermaid (renders natively on GitHub). No local Mermaid
renderer (`mmdc`) is installed in this environment, so no SVG/PNG export is
committed — only this version-controlled diagram source, per the project
rule against fabricating artifacts that were not actually produced.

```mermaid
flowchart TD
    A["NYC TLC Yellow Taxi Parquet<br/>(12 official 2025 monthly files)"]
    B["Open-Meteo ECMWF IFS Single Run<br/>forecast JSON (5 points, 7 variables)"]

    A --> BR
    B --> BR

    subgraph BR["MinIO Bronze — raw, unmodified, with provenance"]
        direction LR
        BR1[taxi parquet]
        BR2[weather JSON]
        BR3[zone reference]
    end

    BR --> ETL

    subgraph ETL["Apache Spark ETL"]
        direction TB
        E1[Clean + validate taxi records] --> E2[Zone x Hour demand grid]
        E2 --> E3[Join weather by zone-centroid to nearest point]
        E3 --> E4["Leakage-safe feature engineering<br/>(cutoff = target_time - horizon)"]
    end

    ETL --> SV

    subgraph SV["MinIO Silver"]
        direction LR
        SV1[cleaned taxi]
        SV2[Zone x Hour demand]
        SV3[weather records]
        SV4[demand-weather join]
        SV5["modeling features<br/>Set A: demand/calendar + zone<br/>Set B: A + weather"]
    end

    SV --> SPLIT["Chronological split (America/New_York)<br/>Train [2025-01-01,2025-09-01)<br/>Validation [2025-09-01,2025-11-01)<br/>Test [2025-11-01,2026-01-01) — frozen"]

    SPLIT --> BASE["Non-ML baselines<br/>persistence / previous-day / previous-week<br/>selected by validation MAE"]

    SPLIT --> ML["Spark ML candidate selection<br/>Regularized Linear Regression vs GBT<br/>fit on Train, scored on Validation only<br/>Feature Set A vs B (weather) per horizon"]

    ML --> SEL["Selected: Regularized Linear Regression,<br/>Feature Set A, per-horizon hyperparameters<br/>frozen before TEST access"]

    SEL --> FINAL["Frozen final evaluation<br/>refit on Train+Validation only<br/>scored once on frozen Test<br/>A and B both scored, GBT excluded"]

    FINAL --> GOLD

    subgraph GOLD["MinIO Gold"]
        direction LR
        G1["predictions<br/>(location_id, target_time_utc,<br/>horizon_hours, actual, prediction,<br/>feature_set)"]
        G2["metrics<br/>(MAE/RMSE by horizon x feature_set,<br/>weather deltas, baseline comparison)"]
    end

    classDef horizon fill:#eef,stroke:#88a
    H["Horizons scored independently: 1h / 3h / 6h"]:::horizon
    H -.-> ML
    H -.-> FINAL
```

## Key separations shown

- **Weather vs no-weather**: every stage from feature engineering onward
  carries Feature Set A (no weather) and Feature Set B (A + weather) as
  parallel populations sharing identical keys.
- **Train / Validation / frozen Test**: the split is fixed and chronological;
  Test is read only once, at the final evaluation stage, after the model
  family and hyperparameters were frozen from Validation-only selection.
- **1h / 3h / 6h horizons**: features, splits, baselines, model selection,
  and final evaluation are all computed and scored separately per horizon.
