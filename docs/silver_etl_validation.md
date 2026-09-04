# Silver ETL Validation

Measured results below come from the complete 2025 Bronze inputs processed by
Spark 3.5.7 through MinIO S3A. No fare, distance, duration, or other post-trip
quality rule was used to reject a pickup-demand record.

## Taxi cleaning

| Measure | Rows |
|---|---:|
| Bronze input | 48,722,602 |
| Accepted five-borough pickups | 48,601,811 |
| Rejected | 109,104 |
| Quarantined | 11,687 |

Rejection and quarantine counts:

- `outside_five_boroughs`: 109,075
- `outside_local_year_2025`: 29
- `dst_ambiguous_fall_back`: 11,687
- `dst_nonexistent_spring_forward`: 0
- missing/invalid pickup timestamp or LocationID: 0

The local-year boundaries were applied before UTC conversion. Fall-back wall
times were not assigned a fold, and Spark never converted the spring gap or
fall fold silently.

## Demand and modeling zones

The official lookup contains 262 five-borough zones. Their complete 8,760-hour
grid has 2,295,120 rows, including 883,059 legitimate zero-demand rows. The two
unresolvable fall-back UTC target hours are unavailable for every zone, giving
524 unavailable rows with null—not zero—demand. The demand primary key has no
duplicates.

The smallest demand-ranked set reaching at least 95% contains 74 zones and
46,215,963 accepted pickups (95.0910307%):

`237, 161, 132, 236, 186, 230, 162, 142, 170, 234, 138, 68, 163, 79, 239, 48, 249, 164, 141, 107, 140, 246, 238, 263, 229, 90, 114, 113, 231, 100, 262, 43, 148, 144, 143, 137, 233, 158, 211, 151, 75, 87, 50, 13, 166, 261, 125, 41, 74, 88, 4, 42, 70, 24, 232, 209, 45, 224, 145, 244, 255, 7, 116, 226, 61, 256, 37, 112, 65, 97, 33, 80, 66, 152`

Across the 8,758 available hours, selected-zone active-hour coverage ranges
from 86.7778032% (Zone 66, 7,600 active hours) to 100%.

## Weather and join

Silver weather contains exactly 131,400 expected target × horizon × point
records. Raw forecast responses supply 130,680 records; 720 records correspond
to preserved unavailable runs. In total, 1,890 records have at least one
missing predictor. No values were imputed or replaced with observed weather.
There are zero leakage-rule violations and zero duplicate weather keys.

The complete demand-weather join contains exactly 6,885,360 rows, with zero
unmatched rows, zero duplicate `(location_id, target_time_utc, horizon_hours)`
keys, and no many-to-many expansion.

## Outputs and validation

- `silver/taxi_clean/records/`: classified taxi rows and UTC timestamps
- `silver/taxi_clean/hourly_demand/`: complete Zone × Hour demand grid
- `silver/taxi_clean/zone_weather_map/`: projected-geometry centroids mapped by
  deterministic nearest-point distance
- `silver/weather_clean/records/`: leakage-safe forecast predictors and
  missingness flags
- `silver/demand_weather/records/`: cardinality-validated joined records
- `silver/manifests/silver_etl_report.json`: schemas, counts, and object inventory

The inventory before writing the report contains 85 Silver data objects totaling
1,516,380,422 bytes. Five focused synthetic Spark tests passed for DST handling,
grid completion, weather leakage, missingness preservation, and join cardinality.
