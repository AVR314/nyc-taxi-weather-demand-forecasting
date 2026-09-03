# Hourly Weather Data Feasibility Profile

## Evaluation Standard

The research claim concerns forecasting, so weather used as a predictor should represent information available before each taxi-demand target hour. Historical observations or reanalysis are valuable references, but using future observed weather as a predictor would overstate real forecasting performance.

Live checks were run on the candidate JSON endpoints for 1–2 January 2025. A separate five-point check covered all 744 hours of January 2025. All five final validation requests returned HTTP 200.

## Candidate 1: Open-Meteo

### Official Documentation and Endpoints

- [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api): `https://archive-api.open-meteo.com/v1/archive`
- [Historical Forecast API](https://open-meteo.com/en/docs/historical-forecast-api): `https://historical-forecast-api.open-meteo.com/v1/forecast`
- [Single Runs API](https://open-meteo.com/en/docs/single-runs-api): `https://single-runs-api.open-meteo.com/v1/forecast?run=YYYY-MM-DDTHH:MM`
- [Previous Runs API](https://open-meteo.com/en/docs/previous-runs-api): fixed 1–7 day lead-time offsets
- [GFS and HRRR model documentation](https://open-meteo.com/en/docs/gfs-api)
- [Licence](https://open-meteo.com/en/license) and [pricing/limits](https://open-meteo.com/en/pricing)

### Data Contract

Responses are JSON objects with location and time metadata (`latitude`, `longitude`, `elevation`, `timezone`, `utc_offset_seconds`), `hourly_units`, and an `hourly` object containing a `time` array plus parallel variable arrays. Multiple coordinate requests return a list of these objects.

The tested core variables are:

- `temperature_2m`
- `relative_humidity_2m`
- `precipitation`
- `snowfall`
- `weather_code`
- `wind_speed_10m`
- `wind_gusts_10m`

All are hourly and available from the tested January 2025 endpoints. Requests should use `timezone=UTC`; the test returned `GMT` with offset 0. Taxi timestamps must be explicitly converted to UTC before joining.

### Observed/Reanalysis Nature

The Historical Weather API provides model-based reanalysis/analysis: ERA5 (~25 km), ERA5-Land (~9–11 km), and, from 2017, ECMWF IFS analysis (~9 km). It is gap-free and consistent but uses information assimilated around or after the valid time. It is suitable as an observed-weather reference, not as the primary ex-ante predictor.

The two-day test returned 48 hourly records and zero nulls for the seven core variables.

### Historical Forecast Nature and Availability

- The Historical Forecast API stitches the first hours from successive operational model runs into a continuous series. It is convenient and tested successfully for January 2025, but it does not preserve one explicit issuance-time/run relationship for each prediction. It is therefore vulnerable to availability ambiguity for strict leakage control.
- The Previous Runs API preserves fixed offsets measured in days, so it does not directly encode the project's 1h/3h/6h horizons.
- The Single Runs API preserves an exact model run selected by UTC initialization time. Official documentation reports ECMWF IFS HRES 9 km runs from 14 March 2024 onward. A live request for run `2025-01-01T00:00`, model `ecmwf_ifs`, returned 24 hourly rows, proving that exact-run forecasts are realistically available for the chosen period.
- The official documentation warns that global model results generally become available 4–6 hours after initialization. The pipeline must use a conservative publication lag and select only the newest run that would have been available at the taxi prediction cutoff.

The tested single-run response had one missing final-hour value for precipitation, snowfall, weather code, and wind gusts, while temperature, humidity, and wind speed were complete. Ingestion therefore needs per-variable coverage checks and must not silently impute missing forecast fields.

### Access, Reproducibility, and Trade-offs

- Free non-commercial access requires no API key and is limited to 600 calls/minute, 5,000/hour, 10,000/day, and 300,000/month; there is no uptime guarantee.
- API data are CC BY 4.0 and require attribution.
- Advantages: JSON, hourly data, UTC support, multi-coordinate requests, broad variables, and preserved 2025 ECMWF forecast runs that support the research claim.
- Disadvantages: Open-Meteo is an aggregator rather than the originating forecast center; model/version changes require recorded request URLs, run timestamps, retrieval dates, and raw responses. Exact-run acquisition is more involved than downloading one stitched series.

## Candidate 2: NASA POWER

### Official Documentation and Endpoint

- [Hourly API](https://power.larc.nasa.gov/docs/services/api/temporal/hourly/): `https://power.larc.nasa.gov/api/temporal/hourly/point`
- [Meteorology methodology](https://power.larc.nasa.gov/docs/methodology/meteorology/)
- [Data sources](https://power.larc.nasa.gov/docs/methodology/data/sources/)
- [Referencing guide](https://power.larc.nasa.gov/docs/referencing/)

NASA POWER returns a GeoJSON-like object. Hourly values are mappings under `properties.parameter`, keyed by compact UTC timestamps such as `2025010100`; metadata includes source, API version, time standard, coordinates, and fill value.

The tested variables were `T2M`, `RH2M`, `PRECTOTCORR`, `WS10M`, and `WD10M`. The two-day request returned 48 UTC hours with no null or `-999` fill values.

### Nature, Resolution, and Access

- Meteorology is based on NASA MERRA-2 reanalysis and near-real-time GEOS processing, not an archive of forecasts issued at historical cutoffs.
- Hourly data are available from 2001 to near real time through the API; the underlying meteorology grid is 0.5° × 0.625° (roughly 50 km around NYC).
- Data are free and the tested endpoint required no authentication. Official guidance asks clients not to send excessive synchronous requests; numeric limits are not published, and HTTP 429 is documented.
- NASA requests citation including service, version, and access date.

Advantages are primary NASA provenance, stable metadata, public JSON access, and long historical coverage. Disadvantages are coarse resolution for NYC, UTC/local-solar-time complexity, occasional retrospective quality replacement, and no preserved ex-ante forecast runs. It is a strong observed/reanalysis reference but a weak primary source for the causal forecasting claim.

## Recommendation

Use Open-Meteo as the project weather provider:

1. Use ECMWF IFS Single Runs as the weather inputs for the forecasting comparison.
2. Record run initialization and apply a conservative six-hour availability lag before a run is eligible.
3. Extract the seven tested core variables at each 1h/3h/6h target time.
4. Use Open-Meteo Historical Weather only as an observed/reanalysis reference for coverage checks and limitations, not as a substitute for ex-ante forecast inputs.
5. Retain NASA POWER as the documented fallback/reference candidate, not the selected predictor source.

This is more defensible than selecting the easiest continuous historical series because it matches the information set that could have existed at prediction time.

## Spatial Strategy

A single NYC point is simple but loses measured intra-city variation. For five representative points (Lower Manhattan, Bronx, Brooklyn, JFK/Queens, and Staten Island) during January 2025:

| Measure across the five points at each hour | Result |
|---|---:|
| Mean temperature range | 1.0332 °C |
| 95th-percentile temperature range | 2.7 °C |
| Maximum temperature range | 4.0 °C |
| Hours with at least 2 °C temperature range | 97 of 744 |
| Mean wind-speed range | 4.4445 km/h |
| 95th-percentile wind-speed range | 7.9 km/h |
| Maximum wind-speed range | 15.7 km/h |
| Hours with wet/dry precipitation disagreement | 26 of 744 |

Recommendation: use the five fixed points and map each Taxi Zone centroid to its nearest point. This captures measured borough-scale variation with one multi-coordinate API request per model run and avoids the complexity of querying all Taxi Zones. Store the requested and provider-returned grid coordinates because Open-Meteo snaps requests to model grid cells.

## Missing-Data and Reproducibility Rules

- Preserve raw JSON plus request URL, retrieval timestamp, run initialization, selected model, units, coordinates, timezone, and provider-returned grid point.
- Validate exactly one expected value per point, target hour, variable, and eligible model run.
- Report missing counts by variable and point; never silently forward-fill or substitute observed weather.
- Attribute Open-Meteo and its upstream model source in final artifacts.
- Re-run the provider availability check before bulk acquisition because API coverage and terms can change.

## Reproduction

```powershell
.\.venv\Scripts\python.exe scripts\check_weather_feasibility.py `
  --output data\profiling\weather_feasibility.json
```

The generated JSON is ignored local evidence; the script and documented endpoints reproduce the checks.
