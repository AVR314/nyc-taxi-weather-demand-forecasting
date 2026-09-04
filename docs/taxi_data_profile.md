# NYC TLC Yellow Taxi Data Feasibility Profile

## Scope and Provenance

Measured facts in this document come from the complete January 2025 feasibility file. No rows were cleaned or removed.

| Item | Value |
|---|---|
| Official catalog | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Direct source | [yellow_tripdata_2025-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) |
| Format | Apache Parquet |
| File size | 59,158,238 bytes (56.4177 MiB) |
| SHA-256 | `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a` |
| Rows | 3,475,226 |
| Row groups | 4 |
| Writer metadata | `parquet-cpp-arrow version 16.1.0` |
| Official dictionary | [Yellow Taxi Trip Records data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) |

January 2025 was retained because it is a complete official monthly release and no contrary feasibility evidence was found. HTTP HEAD checks returned 200 for every 2025 monthly file. Their combined compressed size is 829,973,299 bytes (791.52 MiB), making calendar year 2025 a practical full study period while avoiding cross-year schema and policy-regime changes. TLC notes that `cbd_congestion_fee` begins in 2025.

## Reproduction

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-feasibility.txt
.\.venv\Scripts\python.exe scripts\profile_taxi_data.py `
  data\raw\yellow_tripdata_2025-01.parquet `
  data\reference\taxi_zone_lookup.csv `
  --expected-month 2025-01 `
  --source-url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet `
  --zone-lookup-url https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv `
  --output data\profiling\taxi_profile_2025-01.json
```

The full Parquet file, lookup CSV, and generated JSON profile remain under ignored `data/` paths.

## Schema

All fields are nullable in the Parquet schema even when the measured null count is zero.

| Field | Arrow type |
|---|---|
| `VendorID` | `int32` |
| `tpep_pickup_datetime` | `timestamp[us]` |
| `tpep_dropoff_datetime` | `timestamp[us]` |
| `passenger_count` | `int64` |
| `trip_distance` | `double` |
| `RatecodeID` | `int64` |
| `store_and_fwd_flag` | `large_string` |
| `PULocationID` | `int32` |
| `DOLocationID` | `int32` |
| `payment_type` | `int64` |
| `fare_amount` | `double` |
| `extra` | `double` |
| `mta_tax` | `double` |
| `tip_amount` | `double` |
| `tolls_amount` | `double` |
| `improvement_surcharge` | `double` |
| `total_amount` | `double` |
| `congestion_surcharge` | `double` |
| `Airport_fee` | `double` |
| `cbd_congestion_fee` | `double` |

The timestamp type has no timezone metadata, and the official TLC page and data dictionary do not declare timezone semantics. The project therefore adopts `America/New_York` wall time as an explicit modeling convention—not a TLC source fact—and converts only unambiguous instants to UTC. See `docs/time_axis_audit.md` for the year boundary and DST quarantine policy.

## Timestamp Coverage

| Field | Minimum | Maximum | Rows outside January 2025 |
|---|---:|---:|---:|
| Pickup | 2024-12-31 20:47:55 | 2025-02-01 00:00:44 | 22 |
| Drop-off | 2024-12-18 07:52:40 | 2025-02-01 23:44:11 | 1,811 |

The monthly file is not a perfect timestamp partition. These are measured anomalies, not rows removed by this phase.

## Null Counts

| Field group | Null count | Percent of rows |
|---|---:|---:|
| `passenger_count` | 540,149 | 15.5428% |
| `RatecodeID` | 540,149 | 15.5428% |
| `store_and_fwd_flag` | 540,149 | 15.5428% |
| `congestion_surcharge` | 540,149 | 15.5428% |
| `Airport_fee` | 540,149 | 15.5428% |
| Every other field | 0 | 0% |

The shared null pattern should be investigated by vendor/record source in the cleaning phase. It does not prevent the demand target because pickup timestamp and `PULocationID` have no nulls.

## Pickup Location Distribution

There are 261 distinct non-null pickup IDs. All are within the official lookup range and all occur in the official lookup; invalid/out-of-range count is zero.

Top pickup zones:

| Rank | LocationID | Zone | Pickups | Share |
|---:|---:|---|---:|---:|
| 1 | 161 | Midtown Center | 169,977 | 4.89% |
| 2 | 237 | Upper East Side South | 163,703 | 4.71% |
| 3 | 236 | Upper East Side North | 155,647 | 4.48% |
| 4 | 132 | JFK Airport | 146,137 | 4.21% |
| 5 | 230 | Times Sq/Theatre District | 125,829 | 3.62% |
| 6 | 186 | Penn Station/Madison Sq West | 119,131 | 3.43% |
| 7 | 162 | Midtown East | 117,930 | 3.39% |
| 8 | 142 | Lincoln Square East | 110,585 | 3.18% |
| 9 | 239 | Upper West Side South | 96,614 | 2.78% |
| 10 | 163 | Midtown North | 95,906 | 2.76% |
| 11 | 234 | Union Sq | 95,896 | 2.76% |
| 12 | 170 | Murray Hill | 95,636 | 2.75% |
| 13 | 68 | East Chelsea | 91,241 | 2.63% |
| 14 | 138 | LaGuardia Airport | 89,658 | 2.58% |
| 15 | 48 | Clinton East | 84,137 | 2.42% |

Special lookup IDs appear but are not five-borough modeling zones: ID 1 (Newark Airport) has 377 pickups, ID 264 (Unknown) has 8,141, and ID 265 (Outside NYC) has 1,380.

Across the 262 five-borough lookup zones, 3,465,328 pickups were measured. Demand concentration and sparsity are substantial:

| Cumulative pickup coverage | Minimum zones | Actual coverage | Lowest selected active-hour coverage |
|---:|---:|---:|---:|
| 90% | 41 | 90.2906% | 93.0108% |
| 95% | 53 | 95.0054% | 85.7527% |
| 99% | 121 | 99.0066% | 41.8011% |

Recommendation: use the smallest five-borough, demand-ranked set covering at least 95% of pickups, then verify every selected zone's active-hour rate on the full 2025 period. January yields 53 candidate zones and a clear sparsity cost when moving to 99%. The exact final IDs remain TBD until the full-year profile is available.

## Data-Quality Flags

These counts are screening evidence only; no flag is yet an automatic rejection rule.

| Check | Count | Percent |
|---|---:|---:|
| Zero trip distance | 90,893 | 2.6155% |
| Negative trip distance | 0 | 0% |
| Trip distance over 100 miles | 162 | 0.0047% |
| Zero `fare_amount` | 1,398 | 0.0402% |
| Negative `fare_amount` | 144,118 | 4.1470% |
| `fare_amount` over $500 | 55 | 0.0016% |
| Zero `total_amount` | 559 | 0.0161% |
| Negative `total_amount` | 63,037 | 1.8139% |
| `total_amount` over $1,000 | 3 | 0.0001% |
| Zero-duration trip | 1,927 | 0.0554% |
| Negative-duration trip | 124 | 0.0036% |
| Duration over 24 hours | 20 | 0.0006% |

The measured duration range is -3,088,339 to 337,579 seconds, confirming extreme timestamp anomalies. Negative fares are common enough that their semantics must be investigated rather than treated as accidental noise.

## Fields Useful to the Future Demand Pipeline

- Core target construction: `tpep_pickup_datetime`, `PULocationID`.
- Validation and audit: `tpep_dropoff_datetime`, `DOLocationID`, `trip_distance`, `VendorID`, `store_and_fwd_flag`.
- Quality segmentation: `passenger_count`, `RatecodeID`, `payment_type`, fare and surcharge fields.
- Known-at-prediction-time features should come from calendar, lagged demand, and archived weather forecasts. Post-trip fare, duration, drop-off, and payment fields must not enter forecasting features.
- After validation, aggregation should build a complete Zone × Hour grid so legitimate zero-demand hours are retained.

## Taxi Zone References

- [Official lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv): 265 IDs, range 1–265.
- [Official geographic archive](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip): shapefile needed for zone centroids and future maps.

## Limitations

- This profile measures one month; seasonality and the final zone list require the full approved period.
- TLC states that vendor-submitted trip data may contain inaccuracies and does not guarantee completeness.
- Suspicious thresholds are transparent feasibility screens and require documented cleaning decisions later.
