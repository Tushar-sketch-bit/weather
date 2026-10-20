# Air Pollution Analytics Pipeline

Production-oriented architecture and implementation plan for collecting OpenWeatherMap air-pollution data for Delhi, Uttar Pradesh, Punjab and Rajasthan, then publishing trusted analytical tables to Power BI.

> **Important status note:** this repository currently contains a dbt starter scaffold. This README is the target architecture, connection runbook and delivery plan; it does not claim that every cloud resource or model has already been deployed.

## 1. Project outcome

The pipeline must provide:

- Historical air-pollution observations for four configured locations.
- Recurring current-pollution observations for the same locations.
- Optional hourly forecast observations, kept separate from measured observations.
- Immutable raw landing data in Google Cloud Storage (GCS).
- An Apache Iceberg table layer with managed metadata, snapshots and schema evolution.
- Clean, deduplicated Silver models and dashboard-ready Gold models in BigQuery, built with dbt.
- A Power BI report for trends, pollutant composition, state/location comparisons and freshness.
- A recovery path when an API call, sync, schema change, table update or dashboard refresh fails.

The reporting window in the delivery plan is **October 1, 2026 through December 31, 2026**. The first Delhi historical test covers **October 1–19, 2026**. Those are project dates, not assumptions about the current calendar date.

## 2. Architecture at a glance

```text
                         HTTPS + API key
  OpenWeatherMap  ------------------------------+
  /air_pollution/current                         |
  /air_pollution/history                         v
  /air_pollution/forecast                 Airbyte Cloud
  (four configured locations)        4 historical + 4 current
                                           source connections
                                                  |
                                append-only files; sync metadata
                                                  v
                              Google Cloud Storage: BRONZE
                     raw JSONL/JSONL.GZ or Parquet objects
                                                  |
                             explicit parse/load/table operations
                                                  v
                          BigQuery-managed Apache Iceberg layer
                    GCS = bytes | Iceberg = table format/metadata
                                                  |
                          BigQuery SQL + dbt transformations
                                                  v
                  BigQuery SILVER                 BigQuery GOLD
              clean observations, dedupe      daily metrics, comparisons,
              timestamps, quality flags       freshness and dashboard marts
                                                  |
                              read-only BI connection
                                                  v
                                             Power BI
```

### The role separation is non-negotiable

| Component | Role | Does not mean |
| --- | --- | --- |
| GCS | Durable object storage for raw files and Iceberg data files | A folder of files is not automatically an Iceberg table |
| Apache Iceberg | Table format: schema, snapshots, manifests and table state | Iceberg is not the storage bucket or query engine |
| BigQuery | SQL query and processing engine; hosts Silver/Gold and can manage/query Iceberg | BigQuery does not infer table semantics from arbitrary landed files |
| Airbyte Cloud | API extraction, scheduling, retries and delivery to GCS | Airbyte does not create Iceberg metadata merely by writing files |
| dbt | Versioned SQL transformations, tests, documentation and lineage | dbt is not the API extractor or object-storage catalog |
| Power BI | Semantic model and visualization | Power BI should not be the primary transformation layer |

### Connection matrix

| Direction | Protocol/connector | Identity or secret | Validation boundary |
| --- | --- | --- | --- |
| OpenWeatherMap → Airbyte | HTTPS API request | OpenWeather API key | HTTP status, response schema, rate-limit behavior |
| Airbyte → GCS | Airbyte GCS destination over GCS XML/S3-compatible API | Dedicated GCS service account HMAC key | Probe object, object listing, file readability |
| GCS → BigQuery/Iceberg | BigQuery-supported load/ingestion/table operation | BigQuery service agent or documented cloud-resource connection | Load audit, schema, snapshot and row-count checks |
| Iceberg → dbt | BigQuery SQL relation/source | dbt BigQuery identity | `dbt debug`, source query, model/test results |
| dbt Gold → Power BI | Google BigQuery connector | Read-only Google identity or service account | Navigator visibility, query permission, refresh success |

The arrows are intentional. Power BI reads Gold; it does not write back to dbt. dbt reads/queries the table layer and writes Silver/Gold; it does not manage Airbyte. Airbyte writes Bronze; it does not manage Iceberg snapshots.

## 3. Design decisions and risks

### 3.1 Validate the Airbyte source capability before building eight connections

The current Airbyte Marketplace OpenWeather source documentation describes a connector for the **OpenWeather One Call API** and its `openweather_one_call` stream. The pollution endpoints in this project are separate Air Pollution API endpoints. Do not assume that selecting a connector named “OpenWeather” can extract `/air_pollution`, `/air_pollution/history` or `/air_pollution/forecast`.

Use this decision gate before implementation:

1. Open the connector's **Discover schema** screen.
2. Confirm that the source can call the required pollution endpoint and expose `dt`, `main.aqi` and `components.*` fields.
3. Run one Delhi request for October 1–19, 2026 and inspect the raw payload in the destination.
4. If the stock connector cannot do this, use an Airbyte HTTP API source, a low-code/custom Airbyte connector, or a small separately deployed extractor that still writes through the same Bronze contract.

This prevents a common failure mode: a successful weather sync that produces no pollution data.

### 3.2 Use a location registry, not hard-coded state names

The OpenWeather Air Pollution API accepts latitude and longitude. Create a version-controlled configuration or BigQuery seed with one approved point per reporting location. Example location keys are illustrative and must be approved by the business owner:

| `location_id` | State/territory label | Example city anchor | Required configuration |
| --- | --- | --- | --- |
| `delhi` | Delhi | Delhi | latitude, longitude, timezone |
| `up_lucknow` | Uttar Pradesh | Lucknow | latitude, longitude, timezone |
| `pb_ludhiana` | Punjab | Ludhiana | latitude, longitude, timezone |
| `rj_jaipur` | Rajasthan | Jaipur | latitude, longitude, timezone |

If the dashboard is intended to represent an entire state, label the result as **the selected city/coordinate**, not as a statewide sensor average. Add more stations later with a new `location_id`; never silently change the coordinates behind an existing key.

### 3.3 Separate measured data from forecasts

Use `is_forecast` and `observation_type` columns:

- `measured_current`: response from the current endpoint.
- `measured_historical`: response from the history endpoint.
- `forecast`: response from the forecast endpoint.

Forecast values must not be mixed into historical trend charts or state comparisons unless the visual explicitly says “forecast”.

## 4. Source contract: OpenWeatherMap

### Endpoints

```text
Current:  /data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}
History:  /data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={START}&end={END}&appid={API_KEY}
Forecast: /data/2.5/air_pollution/forecast?lat={lat}&lon={lon}&appid={API_KEY}
```

The history endpoint uses Unix timestamps in UTC. Store the source `dt` as `observed_at_utc`; do not convert it to local time until Silver modeling. Store both UTC and local timestamps so cross-state comparisons remain unambiguous.

The expected source payload contains:

- `coord.lat`, `coord.lon`.
- `list[].dt`.
- `list[].main.aqi`.
- `list[].components.co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`.

Pollutant concentrations are documented by OpenWeather in `µg/m³`. Preserve the raw numeric values and the source unit in Bronze. Do not recalculate the OpenWeather AQI as an Indian, European, UK, US or other national AQI without a separately documented conversion rule.

### Source rules

1. Keep the API key in Airbyte's secret store or a cloud secret manager; never put it in SQL, Git, a README example, a dashboard, or a log message.
2. Use exponential backoff for `429`, `5xx` and transient network errors.
3. Treat `401`, malformed coordinates and invalid date windows as non-retryable configuration failures.
4. Chunk long historical windows if the connector/API returns a response-size or time-window limit.
5. Record the request window, endpoint, location key, HTTP status, Airbyte sync ID and ingestion timestamp in operational metadata.
6. Preserve the complete response in Bronze before flattening it.

### Bronze record envelope

Every normalized record should retain enough provenance to answer “where did this value come from?”:

```text
source_system       = openweathermap
source_endpoint     = current | history | forecast
location_id         = stable project location key
source_observed_at  = source dt converted to UTC timestamp
ingested_at_utc     = time the record entered the pipeline
airbyte_sync_id     = Airbyte sync identifier, if applicable
source_file_uri     = gs://... object URI
record_hash         = deterministic hash of source payload
raw_payload         = original nested JSON, where practical
```

## 5. Airbyte Cloud ingestion plan

### Connection inventory

Create separate connections so one bad location does not block every location and so backfills can be rerun independently:

| Connection family | Delhi | Uttar Pradesh | Punjab | Rajasthan |
| --- | --- | --- | --- | --- |
| Historical backfill | `owm_history_delhi` | `owm_history_up` | `owm_history_punjab` | `owm_history_rajasthan` |
| Current recurring | `owm_current_delhi` | `owm_current_up` | `owm_current_punjab` | `owm_current_rajasthan` |

If the chosen connector supports multiple streams safely, the same logical names can be implemented as streams in fewer connections. The operational requirement is still eight independently observable extraction units.

### Historical backfill

1. Configure the source with one location's latitude, longitude and API secret.
2. Set the history request window to the required UTC range.
3. Run a full append backfill for Delhi, October 1–19, 2026.
4. Validate the destination object count, minimum/maximum `dt`, pollutant columns and duplicate rate.
5. Repeat for Uttar Pradesh, Punjab and Rajasthan.
6. Record the backfill run IDs and source windows in the run log.

Do not use “overwrite” for a shared raw prefix. Historical replays must append new objects and be reconciled by `record_hash`/business key downstream.

### Current recurring syncs

Recommended starting schedule: **hourly at a staggered minute**, for example Delhi at `:05`, UP at `:15`, Punjab at `:25` and Rajasthan at `:35` UTC. The interval can be tightened only after API limits, Airbyte sync duration and BigQuery cost are measured.

For each current connection:

- Use append mode so each observation remains auditable.
- Keep a small overlap window when supported, such as re-reading the last hour, because current observations can arrive late or be corrected.
- Use the source `dt` for the business key, not the Airbyte extraction time.
- Alert on two consecutive failures or when the newest observation is older than the agreed SLA.

### Airbyte to GCS: exact connection checklist

Airbyte's GCS destination uses the GCS XML/S3-compatible interface and requires an HMAC access key and secret for the destination connector. Prepare a dedicated service account and bucket before opening the Airbyte destination form.

1. Create a dedicated bucket and a dedicated prefix, for example `gs://PROJECT-air-pollution-raw/raw/openweather/`.
2. Enable uniform bucket-level access and public-access prevention.
3. Create a service account used only by Airbyte.
4. Grant that identity `Storage Object User` (`roles/storage.objectUser`) on the bucket, or a narrowly scoped custom role with create/list/delete permissions required by the connector's connection check and chosen sync mode.
5. Create an HMAC key for the Airbyte service account and save the secret immediately; it cannot be retrieved later.
6. In Airbyte, choose **Google Cloud Storage (GCS)** as the destination and enter:
   - bucket name;
   - bucket path/prefix;
   - bucket region;
   - HMAC access key;
   - HMAC secret;
   - output format.
7. Prefer JSONL/JSONL.GZ with no flattening for the first raw contract. Choose Parquet only when schema handling and downstream loading have been tested.
8. Configure a path that includes stream and ingestion date, for example:

```text
raw/openweather/${STREAM_NAME}/${YEAR}/${MONTH}/${DAY}/
```

9. Test the Airbyte destination. The connection test writes, lists and deletes a probe object, so retention policies or object holds must not block those operations.
10. Run one Delhi sync and verify the object with `gcloud storage ls` or the GCS console before enabling the remaining seven connections.

Never use a production-wide bucket path for a Full Refresh - Overwrite connection. That mode deletes existing objects under the stream prefix.

## 6. GCS Bronze layout and controls

### Recommended object layout

```text
gs://<raw-bucket>/
  raw/openweather/
    stream=owm_history_delhi/
      ingestion_year=2026/ingestion_month=10/ingestion_day=20/
        <airbyte-sync-and-part>.jsonl.gz
    stream=owm_current_delhi/
      ingestion_year=2026/ingestion_month=10/ingestion_day=20/
        <airbyte-sync-and-part>.jsonl.gz
  quarantine/openweather/
  manifests/openweather/
```

The exact Airbyte path variables depend on the connector version. If Airbyte cannot create the `stream=` syntax, use the closest supported path and add the logical partition columns during the load. The raw object path is an ingestion partition, not a replacement for the source `observed_at_utc`.

### Bronze requirements

- Immutable append-only raw objects; never update an object in place.
- Capture `ingestion_date`, `source_endpoint`, `location_id` and `source_file_uri` during loading.
- Keep a quarantine prefix for malformed JSON, schema failures and rejected rows.
- Retain raw files long enough to replay at least the full reporting window plus an agreed recovery period.
- Enable object versioning or soft delete where the retention/cost policy justifies it.
- Configure lifecycle rules for old objects only after retention and audit requirements are approved.
- Keep the bucket and BigQuery/Iceberg data location compatible to avoid avoidable cross-region cost and unsupported combinations.

### Bronze validation

For every sync, record:

```text
sync_id, connection_name, source_endpoint, location_id,
object_uri, object_generation, object_size_bytes,
first_observed_at_utc, last_observed_at_utc,
record_count, malformed_count, loaded_at_utc, status, error_message
```

An object existing in GCS is not proof that its records were parsed, loaded into Iceberg, or exposed to Power BI. Each transition needs its own success marker and metrics.

## 7. Apache Iceberg table layer

### What this layer does

The Iceberg layer is the governed table representation between raw files and analytical transformations. It provides a table schema, snapshots, manifests, partition metadata and controlled updates. BigQuery is the processing engine used to query and operate the table; GCS stores the underlying data and metadata files for the managed table configuration.

### What this layer does not do

Landing JSONL or Parquet files in GCS does **not** create an Iceberg table. A table must be explicitly created and maintained with supported BigQuery/Iceberg operations. Never hand-edit Iceberg metadata or copy files into an Iceberg data directory without using the supported table APIs and documented workflows.

### Recommended table boundary

Use one Iceberg table for normalized pollution observations, rather than one table per location:

```text
<bq_project>.iceberg_bronze.air_pollution_observations
```

Partition and cluster choices should be validated with real data. A reasonable starting point is:

- partition by `observation_date`;
- cluster by `location_id`, `observation_type`;
- retain `ingestion_date` for replay/audit queries.

Avoid a partition for every stream or every API sync; that creates small-file and metadata pressure.

### BigQuery/GCS/Iceberg connection sequence

1. Choose one BigQuery region and a compatible GCS bucket location before creating datasets or tables.
2. Enable the APIs required by BigQuery, BigLake/Iceberg and Cloud Storage.
3. Create the raw bucket and a separate prefix for managed Iceberg data if the platform configuration requires it. Do not point a managed Iceberg table at the Airbyte raw prefix.
4. Create the documented BigQuery cloud-resource connection/service identity for the managed Iceberg table workflow.
5. Grant only the documented bucket-level permissions to the BigQuery service agent or connection identity. Typical operations include reading source objects, writing table data and managing table metadata; exact permissions depend on the selected managed-table workflow and organization policy.
6. Create the Iceberg-managed table through the BigQuery console, SQL DDL or supported ingestion operation documented for the selected BigQuery release.
7. Load parsed records from the Bronze/raw landing zone using an explicit load or insert operation.
8. Verify that the table appears in BigQuery, can be queried, and has a new snapshot after the load.
9. Exercise a controlled schema addition and a deduplication/upsert test in a non-production dataset.
10. Keep the raw GCS objects unchanged so a failed table load can be replayed.

Do not mix two ownership models. If BigQuery manages the Iceberg table, let BigQuery manage table operations. If an external Iceberg catalog is selected instead, document that catalog and use its supported writer/reader path; do not call an externally managed table “BigQuery-managed”.

### Bootstrap load pattern

```text
GCS raw objects
    -> temporary raw/external or load table
    -> parse and add provenance columns
    -> validate schema and row counts
    -> INSERT/MERGE into Iceberg-managed table
    -> record Iceberg snapshot ID and load audit row
```

Use a temporary table or staging query for malformed-row isolation. A single failed file must not make an already committed Iceberg snapshot invisible. Commit the next snapshot only after the batch passes validation.

### Iceberg acceptance tests

- `SELECT COUNT(*)` is stable across a replay after deduplication.
- A table snapshot is visible after every successful batch.
- A failed load leaves raw objects and the prior valid snapshot intact.
- A new nullable source field can be added without rewriting historical business values.
- Updates are performed through supported table operations.
- A time-travel or snapshot inspection query can identify the table state used for a report refresh.
- A recovery run can rebuild a test table from the raw GCS prefix.

## 8. BigQuery and dbt Silver/Gold design

### Dataset layout

Use separate datasets with explicit ownership and access boundaries:

| Dataset | Contents | Writers | Consumers |
| --- | --- | --- | --- |
| `iceberg_bronze` | Iceberg-managed normalized observations and load audit | ingestion/table job | dbt staging, data engineering |
| `silver` | cleaned and deduplicated observation models | dbt service account | Gold models, analysts |
| `gold` | dashboard marts and freshness views | dbt service account | Power BI, analysts |
| `ops` | sync runs, object manifests, rejected records, data-quality results | ingestion/dbt jobs | operations |

Keep Gold small and purpose-built. Power BI should not query raw nested JSON or run expensive deduplication logic on every visual interaction.

### Suggested dbt model graph

```text
source(iceberg_bronze.air_pollution_observations)
        |
        v
stg_air_pollution_observations
        |
        +--> int_air_pollution_deduplicated
        |          |
        |          +--> mart_daily_pollution
        |          +--> mart_state_comparison
        |          +--> mart_pollutant_profile
        |          +--> mart_current_status
        |
        +--> ops_data_freshness
```

### Canonical Silver schema

At minimum, expose these fields after flattening and type conversion:

| Column | Type/meaning |
| --- | --- |
| `observation_id` | deterministic hash of location, observation timestamp and observation type |
| `location_id` | stable registry key |
| `state_name` | reporting label from the location registry |
| `city_name` | selected city/coordinate label |
| `latitude`, `longitude` | source/request coordinates |
| `observed_at_utc` | source `dt` converted from Unix seconds |
| `observed_at_local` | UTC timestamp converted using the registry timezone |
| `observation_date` | local reporting date |
| `observation_type` | `measured_current`, `measured_historical` or `forecast` |
| `aqi` | OpenWeather AQI value, retained with its source definition |
| `pm2_5`, `pm10`, `co`, `no`, `no2`, `o3`, `so2`, `nh3` | numeric concentrations in `µg/m³` |
| `ingested_at_utc` | pipeline ingestion timestamp |
| `airbyte_sync_id` | source sync provenance |
| `source_file_uri` | raw GCS object URI |
| `record_hash` | hash of the raw/normalized source record |
| `is_valid_record` | quality result used to exclude invalid rows from Gold |
| `quality_issue_code` | nullable reason for a rejected or suspicious record |

### Deduplication policy

The primary measured-data key is:

```text
(location_id, observed_at_utc, observation_type)
```

Use `record_hash` to identify exact replays. For duplicate business keys, retain the record using this deterministic ordering:

1. valid record before invalid record;
2. measured historical before measured current when both represent the same actual timestamp;
3. latest successful ingestion for a corrected source value;
4. lowest `record_hash` as a final tie breaker.

Do not deduplicate forecasts into measured observations. Forecast identity should also include the forecast generation time, for example `(location_id, forecast_generated_at_utc, forecast_for_at_utc)`.

### Data-quality tests

Add dbt tests for:

- not-null `location_id`, `observed_at_utc`, `observation_type` and `ingested_at_utc`;
- accepted values for `observation_type`;
- uniqueness of `observation_id` in Silver;
- accepted range for `aqi` of 1–5 when present;
- non-negative pollutant concentrations;
- valid latitude/longitude ranges;
- no future measured timestamps beyond a small clock-skew tolerance;
- source freshness for each location;
- expected location coverage for the reporting window;
- relationship from observation `location_id` to the location registry;
- rejected-row counts below an agreed threshold.

Do not silently turn bad numeric values into zero. Preserve the raw value, set a quality issue and exclude it from metric calculations.

### Gold marts

#### `mart_daily_pollution`

One row per local date and location. Include average, minimum, maximum and observation count for PM2.5, PM10 and each pollutant, plus the latest measured timestamp and completeness percentage.

#### `mart_state_comparison`

One row per date, state label and location. Include rank, PM2.5/PM10 averages, change from the previous day and comparison to the period average. Keep the city/location grain visible so a city is not mistaken for a statewide aggregate.

#### `mart_pollutant_profile`

Long-form or carefully modeled wide-form pollutant metrics for composition visuals. Include pollutant name, concentration, unit, date, location and observation type.

#### `mart_current_status`

One latest valid measured row per location with `latest_observed_at_utc`, minutes since observation, freshness status, latest AQI and pollutant values.

#### `ops_data_freshness`

One row per connection/location with last successful Airbyte sync, last object arrival, last Iceberg load, last dbt build, latest measured observation and current status. This powers the dashboard freshness page and operations alerts.

## 9. Connecting dbt to BigQuery

### Required setup

Create a dedicated dbt identity and grant only the permissions needed for the target datasets. The identity needs to run BigQuery jobs, read metadata and source data, and create/update/delete the dbt-managed Silver/Gold relations. Prefer Workload Identity Federation or OAuth where available; use a service-account JSON key only when the deployment environment requires it.

The local `profiles.yml` should not be committed. A safe service-account profile shape is:

```yaml
air_pollution:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: silver_dev
      location: "{{ env_var('BQ_LOCATION') }}"
      threads: 4
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
```

Use environment-specific targets such as `dev`, `staging` and `prod`; do not point a developer profile at production by default.

### Connection smoke test

Run from the directory containing `dbt_project.yml`:

```bash
dbt debug --target dev
dbt parse --target dev
dbt build --target dev --select state:modified+
```

The first run should create or use only the development dataset. Verify the BigQuery job location matches the dataset location.

### dbt operating rules

- Use `source()` for the Iceberg/bronze input and `ref()` for model dependencies.
- Keep raw parsing and data-quality flags in staging/intermediate models.
- Materialize Silver as partitioned/clustered BigQuery tables when the data volume warrants it.
- Materialize Gold as stable tables or views with documented grain.
- Add schema tests before exposing a new column to Power BI.
- Run `dbt build` only after the Iceberg load audit says the input batch is complete.
- Publish a dbt manifest and run results with each deployment for lineage and incident review.

## 10. Connecting Power BI to BigQuery

### Recommended serving contract

Power BI should read only the `gold` dataset or a dedicated authorized view dataset. Grant the Power BI identity read access to Gold and the ability to create query jobs in the billing project. Do not grant Power BI write access to Bronze, Iceberg or Silver.

Use **Import** mode for the default dashboard when the hourly freshness requirement is acceptable. Use **DirectQuery** only when near-real-time queries are required and the cost, concurrency and query-latency behavior have been tested. If using the BigQuery Storage API for large result sets, grant the additional read-session permissions required by the selected identity.

### Power BI Desktop connection

1. Open **Get data** and choose **Google BigQuery**.
2. Sign in with an approved Google organizational account, or choose service-account login where organizational policy requires it.
3. Set the billing project explicitly if the authenticated identity does not have job-creation permission in the first project returned by the connector.
4. In Navigator, select the `gold` dataset and the required marts.
5. Prefer `mart_daily_pollution`, `mart_state_comparison`, `mart_pollutant_profile`, `mart_current_status` and `ops_data_freshness` over raw tables.
6. Load the data, define a date table and create relationships only where the Gold model documents them.
7. Publish the report to the Power BI service.
8. Configure the cloud-to-cloud BigQuery connection and scheduled refresh. Use the same billing project and region assumptions as the desktop test.
9. Test refresh with a read-only identity before sharing the report.

### Power BI model rules

- Display the selected city/coordinate in titles and tooltips; do not call it a state-wide measurement unless it truly is one.
- Label units as `µg/m³` and distinguish OpenWeather AQI from any other AQI scale.
- Show data freshness beside every “latest” visual.
- Exclude `forecast` rows from measured trend pages by default.
- Keep a “Data quality” page showing missing locations, stale syncs and rejected-row counts.
- Hide engineering columns from report users but retain them in the source tables for auditability.

### Suggested report pages

1. **Executive overview** — latest AQI, PM2.5 and PM10 cards for all locations.
2. **Geographic comparison** — selected cities/states, rankings and period averages.
3. **Pollution trends** — daily PM2.5, PM10 and pollutant time series from October through December 2026.
4. **Pollutant composition** — PM2.5, PM10, CO, NO, NO2, O3, SO2 and NH3 analysis.
5. **Historical analysis** — October 1–19 validation period and the full reporting window.
6. **Data freshness** — last observation, last sync, last load, dbt status and missing records.

## 11. End-to-end execution order

The pipeline should run in this order for each ingestion cycle:

```text
Airbyte sync completes
    -> GCS object manifest is updated
    -> raw files are parsed and validated
    -> Iceberg batch is committed
    -> dbt Silver/Gold build runs
    -> freshness audit is updated
    -> Power BI refresh is allowed
```

If any step fails, stop downstream promotion. Keep the last known-good Gold tables available and expose the failure through `ops_data_freshness` rather than publishing partial data as current.

## 12. Execution roadmap and acceptance criteria

### 01 — Complete API ingestion

- [x] Test Delhi historical pollution data for October 1–19, 2026.
- [ ] Configure historical streams for Uttar Pradesh, Punjab and Rajasthan.
- [ ] Test the current pollution endpoint for all four locations.
- [ ] Set up four recurring current-data syncs.

**Exit criteria:** each location has a repeatable source request, valid UTC timestamps, expected pollutant fields, a documented source type and an observable failure status.

### 02 — Connect GCP and Airbyte

- [ ] Create a GCS bucket for raw data.
- [ ] Configure the Airbyte GCS destination and permissions.
- [ ] Run a test sync and validate the output files.

**Exit criteria:** one Delhi sync writes an object to the intended prefix, the object is readable by the load identity, the Airbyte connection check passes, and the raw contract is documented with a sample schema.

### 03 — Implement the Iceberg layer

- [ ] Configure the required BigQuery/GCS integration.
- [ ] Create an Iceberg-managed table using supported operations.
- [ ] Load and validate records from the raw landing zone.
- [ ] Test table metadata, updates and deduplication.

**Exit criteria:** a clean test batch creates a new table snapshot, a replay does not multiply Silver business rows, and a failed load leaves the prior valid snapshot queryable.

### 04 — Build Silver and Gold models

- [ ] Standardize timestamps and pollutant measurements.
- [ ] Deduplicate by location and observation timestamp.
- [ ] Build daily pollution aggregates and state-level comparisons using dbt.

**Exit criteria:** `dbt build` passes source, model and custom quality tests in a non-production target; every Gold table has a documented grain and freshness field.

### 05 — Build the Power BI dashboard

- [ ] Connect Power BI to the final analytical tables.
- [ ] Visualize pollution trends, pollutant composition and location comparisons.
- [ ] Continue scheduled ingestion through December 31, 2026.

**Exit criteria:** a refresh succeeds with a read-only Gold identity, every page shows the reporting period and unit, and the freshness page detects a deliberately stale test connection.

## 13. Operations runbook

### Daily checks

1. Check the eight Airbyte connection statuses and last successful sync times.
2. Check whether all four locations have new current observations.
3. Check raw object counts and sizes in GCS.
4. Check Bronze-to-Iceberg load status and latest snapshot ID.
5. Run the dbt build/test job and review rejected-row counts.
6. Confirm Gold freshness and Power BI refresh status.

### If Airbyte fails

- Inspect the source HTTP status and connector logs.
- For `429`, increase backoff/staggering and avoid launching all backfills together.
- For `401`, rotate/replace the secret in Airbyte; do not write it to a ticket or log.
- For a source schema change, preserve the raw payload, quarantine the batch, update the source contract and add a regression fixture before retrying.
- Retry only the failed location/connection where possible.

### If GCS delivery fails

- Test HMAC access, bucket/prefix spelling and outbound access to `storage.googleapis.com`.
- Verify the bucket IAM binding includes create/list/delete permissions required by the connector mode.
- Confirm no retention policy, hold or overwrite setting blocks the probe or expected write.
- Do not broaden the service account to project Owner as a first response.

### If Iceberg load fails

- Keep the raw object and load audit row.
- Check schema/type errors and malformed records in quarantine.
- Do not manually delete table metadata or data files.
- Re-run from the immutable raw batch into a temporary/test table.
- Commit a new snapshot only after row counts, uniqueness and null checks pass.

### If dbt fails

- Stop Power BI promotion for that batch.
- Inspect the first failing model/test, not only the final summary.
- Compare source freshness and schema to the last successful run.
- Fix the model or quarantine bad records, then rerun the same batch deterministically.
- Keep the last valid Gold tables available and mark the dashboard freshness status as delayed.

### If Power BI refresh fails

- Verify the BigQuery identity still has dataset read and query-job permissions.
- Set the billing project explicitly if the error mentions `bigquery.jobs.create`.
- Check whether the Power BI connector is using Import or DirectQuery as designed.
- Query the Gold mart directly in BigQuery to distinguish a warehouse problem from a Power BI model problem.
- Never repair a Power BI refresh by granting broad write access to the warehouse.

## 14. Security, cost and governance controls

### Secrets

- Use Airbyte secret storage for the OpenWeather API key and GCS HMAC secret.
- Use Secret Manager, Workload Identity Federation or an approved CI secret store for dbt deployment credentials.
- Keep `.env`, service-account JSON files, `profiles.yml`, HMAC secrets and API keys out of Git.
- If a credential is ever committed or pasted into a ticket, revoke and rotate it immediately.

### IAM separation

Use separate identities for:

| Identity | Minimum responsibility |
| --- | --- |
| Airbyte source | Call OpenWeather endpoints |
| Airbyte GCS destination | Write/list/delete only its raw prefix |
| Iceberg/BigQuery table job | Read Bronze and manage the selected Iceberg table |
| dbt | Run jobs and manage Silver/Gold relations |
| Power BI | Read Gold and create query jobs only |
| Engineer/admin | Provisioning, break-glass recovery and IAM changes |

### Cost controls

- Use a single GCS bucket and sensible prefixes rather than one bucket per state.
- Start current syncs hourly; measure before increasing frequency.
- Partition and cluster Silver/Gold tables based on query evidence.
- Keep Power BI pointed at aggregated Gold marts.
- Set BigQuery budgets, job labels and maximum bytes billed for development queries.
- Retain raw data according to the agreed audit window and use lifecycle policies after approval.

## 15. Local development and repository notes

This directory is a dbt project. The intended local workflow is:

```bash
cd weather_project/weather_pipeline
dbt debug
dbt parse
dbt build
```

The commands require a valid local dbt installation and a configured BigQuery profile. They do not create Airbyte, GCS, Iceberg or Power BI resources. Cloud provisioning and connection setup must be completed separately using the runbook above.

Before adding models, add the repository-level reproducibility items required by the deployment environment:

- pinned Python/dbt runtime;
- a dependency lockfile;
- a `Dockerfile` for offline/repeatable project checks;
- a single test command such as `make test`;
- an `.env.example` containing names only, never secrets;
- CI checks for `dbt parse`, `dbt build` against a test target and documentation validation.

## 16. Official references

The following links are the source of truth for connector/API behavior and should be rechecked when versions change:

- [OpenWeather Air Pollution API](https://openweathermap.org/api/air-pollution)
- [Airbyte OpenWeather source](https://docs.airbyte.com/integrations/sources/openweather)
- [Airbyte Google Cloud Storage destination](https://docs.airbyte.com/integrations/destinations/gcs)
- [Google Cloud Storage: create a bucket](https://cloud.google.com/storage/docs/creating-buckets)
- [Google Cloud Storage: manage HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys)
- [BigQuery Apache Iceberg managed tables](https://cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery)
- [BigQuery: transfer data into an Iceberg managed table](https://cloud.google.com/bigquery/docs/iceberg-ingestion)
- [dbt: connect BigQuery](https://docs.getdbt.com/docs/local/connect-data-platform/bigquery-setup)
- [Power Query Google BigQuery connector](https://learn.microsoft.com/en-us/power-query/connectors/google-bigquery)
- [BigQuery IAM roles and permissions](https://cloud.google.com/bigquery/docs/access-control)

Connector capabilities, permissions, versions and UI labels change. Reconfirm the official documentation during implementation, especially the Airbyte OpenWeather capability and BigQuery-managed Iceberg prerequisites.
