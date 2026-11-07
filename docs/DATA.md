# Data Inventory and Data-Management Plan

This file is the source of truth for datasets used by the project. Update it whenever a dataset, release, geography, API query, or transformation changes.

## Data Principles

- Raw data is **not committed to Git**.
- Every source must have a documented version/release and retrieval date.
- Keep source data immutable after download; transformations belong in `data/interim` or `data/processed`.
- Record units before combining datasets.
- Align timestamps explicitly, including timezone, daylight-saving behavior, and leap-year handling.
- Do not describe simulated ResStock buildings as actual Fort Collins households.

## Dataset Inventory

### 1. ResStock Hourly Demand

Use ResStock 2024 Release 2, AMY 2018, baseline (`upgrade=0`), with Colorado metadata filtered to `in.county=G0800690` (Larimer County). The downloaded inventory contains 573 profiles totaling 3,006,131,341 bytes. Of these, 382 metadata records are labeled `Single-Family Detached` in `in.geometry_building_type_acs`. Use that cohort for the initial placement experiment. Larimer County is a regional proxy for Fort Collins. The profiles represent simulated buildings.

The metadata and profiles are stored outside Git. To reproduce the download, use `scripts/download_larimer_baseline.py` with the Colorado metadata CSV or the filtered workbook, and use the [baseline profile URL](https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade=0/state=CO/418477-0.parquet) as the template. Retain the original metadata URL and confirm its release before freezing the inventory.

The pilot uses profiles 128, 1414, and 3575. It sums four 15-minute interval energy values from `out.electricity.total.energy_consumption` for each hour. Timestamps are interval ends. The code preserves the source wall clock through aggregation. Confirm the source time zone before joining PV or emissions data.

#### Pilot verification

| Building ID | 15-minute intervals | Hourly rows | Total demand (kWh) |
|---|---:|---:|---:|
| 128 | 35,040 | 8,760 | 8,346.248 |
| 1414 | 35,040 | 8,760 | 5,689.824 |
| 3575 | 35,040 | 8,760 | 9,684.960 |

The three files passed the interval spacing and completeness checks. Aggregation preserved energy to floating-point precision. The rolling same-hour-last-week baseline scored 6,624 building-hours from Oct 1 through Dec 31, 2018. Pilot MAE was 0.380 kWh and RMSE was 0.891 kWh per hourly interval. The solar and emissions example uses synthetic inputs to check the pipeline.

#### Reproduce the pilot

With Java 17 and PySpark 4.0.1 installed, run this from the repository root. Replace the paths with your local paths and use a new output directory each time.

```bash
DATA_DIR="$HOME/**YOUR FILE ROOT DIRECTORY**"
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=4 pyspark-shell' \
python3 scripts/resstock_pilot.py "$DATA_DIR/larimer_baseline" /tmp/resstock-pilot-hourly \
  --metadata "$DATA_DIR/CO_baseline_metadata_and_annual_results.csv" \
  --test-start 2018-10-01
```

Next, confirm metadata provenance and the source clock, validate the 382-home cohort, then freeze a chronological train/validation/test split before comparing Random Forest and LSTM.

---

### 2. U.S. Census / American Community Survey

**Role:** Fort Collins housing context and estimate of potentially eligible housing stock.

**Candidate measures from proposal:**

- occupied housing units;
- owner-occupied housing units;
- single-family detached housing units.

**Important:** Census data provides population-level context. It should not be joined to a simulated ResStock record in a way that implies the record corresponds to a real household unless a defensible mapping methodology is established.

**Query log:**

- [ ] ACS product/year:
- [ ] table/variables:
- [ ] geography:
- [ ] API query or download URL:
- [ ] retrieval date:

---

### 3. PVWatts V8

**Role:** Generate a representative hourly solar-production profile directly from standardized PV-system assumptions.

**Inputs to freeze for each experiment:**

- [ ] system capacity (kW DC):
- [ ] module type:
- [ ] array type:
- [ ] system losses (%):
- [ ] tilt:
- [ ] azimuth:
- [ ] location / weather source:
- [ ] API version:

**Outputs to retain:**

- hourly AC production;
- API/request parameters;
- retrieval date;
- any unit conversions.

Never commit API keys.

---

### 4. Solar-to-Grid

**Role:** Historical solar-generation context and possible validation/reference source.

**Initial proposal size:** approximately 8.69 GB unfiltered.

**Proposed geography:** Larimer County where appropriate.

**Coverage noted in proposal:** 2012-2020 estimates for existing residential and commercial solar installations.

**Final methodological role:**

- [ ] primary solar profile;
- [ ] validation/context only;
- [ ] not used in final pipeline.

Record this decision explicitly to avoid mixing incompatible solar estimates.

---

### 5. EPA CAMPD

**Role:** Historical fossil-generation / emissions-related context.

**Initial proposal size:** approximately 40 MB for the intended extract.

**Questions to resolve:**

- What spatial region maps most appropriately to the demand/solar simulation?
- Are emissions estimated from direct plant data, regional aggregation, or another factor?
- What temporal resolution is available and defensible?

Record any derived emissions factor with units such as `kg CO2 / kWh` or `lb CO2 / MWh`.

---

### 6. EIA-930

**Role:** Regional electricity demand, generation by source, and interchange information.

**Potential use:** grid context and a possible fossil-generation-share indicator.

**Questions to resolve:**

- Which balancing authority/region represents Fort Collins for this analysis?
- Will EIA-930 influence the emissions calculation, or only provide context?
- What timestamp/timezone transformations are required?

---

### 7. Cambium

**Role:** Candidate source for emissions factors / sensitivity assumptions.

The proposal bibliography lists Cambium, but the exact use should be decided before final experiments.

**Decision:**

- [ ] used for primary emissions factor;
- [ ] used only in sensitivity analysis;
- [ ] not used.

## Common Processed Schema

The final schema will depend on the selected releases, but a forecast-ready record should conceptually contain:

```text
building_id
forecast_timestamp
hour
calendar features
weather features (if available at forecast time)
building characteristics
lag_1h
lag_24h
lag_168h
rolling demand statistics
target_demand_kwh
```

Feature names and windows are placeholders until the team freezes the forecasting design.

## Data Lineage

For each processed artifact, log:

```text
input dataset/version -> script/job -> configuration -> output path -> row count -> timestamp
```

Suggested metadata file per run:

```json
{
  "run_id": "YYYYMMDD-HHMM-description",
  "inputs": [],
  "geography": "",
  "weather_year": "",
  "time_zone": "",
  "output": "",
  "row_count": 0,
  "notes": ""
}
```

## Git Policy for Data

Recommended `.gitignore` entries:

```gitignore
data/raw/**
data/interim/**
data/processed/**
!data/raw/.gitkeep
!data/interim/.gitkeep
!data/processed/.gitkeep
*.parquet
*.csv
*.csv.gz
*.zip
*.tar.gz
```

Small hand-created test fixtures may be committed under `tests/fixtures/`.
