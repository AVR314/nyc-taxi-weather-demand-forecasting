# Time-Axis Audit

## Source evidence and limitation

The official [TLC trip-record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
states that Yellow Taxi records contain pickup and drop-off dates/times and are
provided by authorized technology providers. The official March 18, 2025
[Yellow Taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
defines `tpep_pickup_datetime` as the date and time when the meter was engaged.
Neither source declares a timezone or UTC offset. The measured Parquet type is
`timestamp[us]` without timezone metadata. Therefore, `America/New_York` is an
explicit project modeling convention, not a claimed TLC source guarantee.

## Modeling-year policy

The study population is pickups whose timezone-naive pickup wall time is in the
half-open interval `2025-01-01 00:00:00` through `2026-01-01 00:00:00` in
`America/New_York`. This local-year convention is appropriate for NYC demand,
whose calendar, weekday, and hour-of-day effects follow New York civil time.

Weather remains stored and joined on UTC instants. The local modeling year maps
to the half-open UTC interval `2025-01-01T05:00:00Z` through
`2026-01-01T05:00:00Z`, or 8,760 hourly target instants ending at
`2026-01-01T04:00:00Z`. The leakage-safe weather plan now contains 1,462 model
runs, from `2024-12-31T12:00:00Z` through `2025-12-31T18:00:00Z`.

## DST policy

[NIST's local-time guidance](https://www.nist.gov/pml/time-and-frequency-division/local-time-faqs)
states that U.S. daylight saving time starts at 02:00 local time on the second
Sunday in March and ends at 02:00 local time on the first Sunday in November.
For New York in 2025:

- On 9 March, local times `02:00`–`02:59:59` do not exist. Such naive TLC
  values are invalid local instants and must be quarantined, never shifted.
- On 2 November, local times `01:00`–`01:59:59` identify two possible UTC
  instants. The source has no offset/fold field, so those taxi records must be
  quarantined and both affected demand targets treated as unavailable rather
  than assigning an arbitrary fold or counting missing demand as zero.
- The canonical UTC axis naturally skips the spring `02` wall hour and contains
  both fall `01` hours, distinguished by UTC instant and `fold` after conversion.

The conversion helper rejects both nonexistent and ambiguous wall times. Focused
tests verify the year boundaries, the spring transition from EST to EDT, both
fall-back candidates, and the 8,760-hour UTC target interval.

## Bronze effect

Only the newly required `2025-12-31T18:00:00Z` ECMWF run was requested. All
1,461 earlier plan artifacts were reused. The request succeeded with no retry;
the corrected plan has 1,454 successful responses and eight preserved
provider-unavailable responses. Recomputed missing coverage is unchanged at
9,810 predictor slots across 136 target hours.
