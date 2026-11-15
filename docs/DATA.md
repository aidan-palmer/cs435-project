# Data Inventory and Data-Management Plan

The demand pilot uses ResStock. Its solar and emissions smoke check uses synthetic inputs. A separate script prepares PVWatts exports for the Fort Collins rooftop scenario. Census/ACS remains a planned input. Solar-to-Grid, EPA CAMPD, EIA-930, and Cambium are candidate reference or emissions sources. The project has yet to select its emissions methodology.

## Storage and API keys

Use the repository-relative locations below for a new setup, or point the scripts at a separate local data directory. Keep downloaded inputs under `data/raw/`, transformed copies under `data/interim/`, and model-ready outputs under `data/processed/`. The existing ResStock files are in the sibling directory `../DATA_for_project/`. These data directories are outside version control.

| Source | Storage for a new setup | Personal API key |
|---|---|---|
| ResStock | `data/raw/resstock/` | None for public OEDI downloads |
| Census ACS | `data/raw/census/` | `CENSUS_API_KEY` for the Census Data API. Public Summary File downloads require no key. |
| PVWatts V8 | `data/raw/pvwatts/` | `PVWATTS_API_KEY` from the NLR Developer Network |
| Solar-to-Grid | `data/raw/solar-to-grid/` | None for OEDI downloads |
| EPA CAMPD | `data/raw/emissions/campd/` | `CAMPD_API_KEY` for direct CAM API calls. Browser downloads require no personal key. |
| EIA-930 | `data/raw/eia930/` | `EIA_API_KEY` for direct EIA API calls. Grid Monitor file downloads require no key. |
| Cambium | `data/raw/emissions/cambium/` | None for Scenario Viewer downloads |

The variable names are for local use and documentation. The repository currently has no API clients. Keep keys in your shell environment or an ignored `.env` file. Omit them from Git, request logs, and cached responses.

## Dataset inventory

### 1. ResStock hourly demand

The pilot uses the National Laboratory of the Rockies (NLR, formerly NREL) ResStock 2024 Release 2, actual meteorological year (AMY) 2018, baseline (`upgrade=0`). Colorado metadata is filtered to `in.county=G0800690` for Larimer County. The initial placement cohort is the 382 records labeled `Single-Family Detached` in `in.geometry_building_type_acs`. Larimer County is a regional proxy for Fort Collins. The records are simulated dwellings.

| File or directory within `../DATA_for_project/` | Format and contents | Local size |
|---|---|---:|
| `CO_baseline_metadata_and_annual_results.csv` | Colorado baseline metadata and annual results, keyed by `bldg_id`. Required by the Spark pilot. | 28,555,099 bytes |
| `CO_baseline_metadata_and_annual_results.parquet` | Same source table in Parquet | 3,643,481 bytes |
| `All_larimer_baseline.xlsx` | Local filtered metadata workbook. The downloader accepts its first sheet with `openpyxl` installed. | 994,315 bytes |
| `larimer_baseline/` | 573 individual `<bldg_id>-0.parquet` files with 15-minute profiles. | 3,006,131,341 bytes (about 3.01 GB) |
| `larimer-geography-weather.csv` | Local companion table with 573 geography and weather-metadata rows. | 74,232 bytes |
| `resstock-all-larimer-hourly/` | Local hourly Parquet output with 32 part files. | 21,057,860 bytes of Parquet |

**Download.** Get the [Colorado baseline metadata CSV](https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/metadata_and_annual_results/by_state/state=CO/csv/CO_baseline_metadata_and_annual_results.csv) from the AMY 2018 release. The same release offers the [Parquet metadata](https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/metadata_and_annual_results/by_state/state=CO/parquet/CO_baseline_metadata_and_annual_results.parquet) and a data dictionary through the [release catalog](https://resstock.nlr.gov/datasets). From the repository root, run the following against a new data directory.

```bash
DATA_DIR=data/raw/resstock
mkdir -p "$DATA_DIR"
RESSTOCK_URL='https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2'
METADATA="$DATA_DIR/CO_baseline_metadata_and_annual_results.csv"
curl -fL -o "$METADATA" "$RESSTOCK_URL/metadata_and_annual_results/by_state/state=CO/csv/CO_baseline_metadata_and_annual_results.csv"
python scripts/download_larimer_baseline.py \
  "$METADATA" "$DATA_DIR/larimer_baseline" \
  --sample-url "$RESSTOCK_URL/timeseries_individual_buildings/by_state/upgrade=0/state=CO/418477-0.parquet" \
  --dry-run
```

Remove `--dry-run` to download the 573 files. Add `--detached-only` to select the 382 detached profiles. The downloader skips existing valid Parquet files. The feature-building script reads `/larimer_baseline/*.parquet` through Spark's configured filesystem. If using the in-place timestamp converter for that workflow, run it on copies under `data/interim/` to preserve source files.

**Use and citation.** The OEDI End-Use Load Profiles dataset is published under CC BY 4.0. Credit NLR and OEDI, include the dataset DOI and specific release path, link the license, and indicate changes when redistributing adapted data. [OEDI dataset and citation](https://data.openei.org/submissions/4520) · [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

The pilot uses profiles 128, 1414, and 3575. It sums four 15-minute interval energy values from `out.electricity.total.energy_consumption` for each hour. Timestamps are interval ends. The code preserves the source wall clock through aggregation. The [ResStock FAQ](https://natlabrockies.github.io/ResStock.github.io/docs/FAQ.html) describes the source clock as fixed Eastern Standard Time (UTC−05:00). Confirm the clock when aligning the three sources, including interval labels, daylight saving, and leap-year handling.

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
DATA_DIR=../DATA_for_project
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=4 pyspark-shell' \
python scripts/resstock_pilot.py \
  "$DATA_DIR/larimer_baseline" \
  "$(mktemp -d)/resstock-pilot-hourly" \
  --metadata "$DATA_DIR/CO_baseline_metadata_and_annual_results.csv" \
  --test-start 2018-10-01
```

Before comparing Random Forest and LSTM, validate the full 382-profile cohort and freeze a chronological train/validation/test split.

### 2. U.S. Census / American Community Survey

ACS tract, place, or other area estimates provide housing context. The planned geography is Fort Collins city, Colorado. A starting point is the 2024 ACS 5-year Detailed Tables with `B25002` (occupancy), `B25003` (tenure), and `B25024` (units in structure). Retain estimates, margins of error, and geographic IDs. The single-family detached and owner-occupied counts come from different tables. They do not by themselves establish how many units meet both criteria.

**Download and size.** Request a [Census Data API key](https://api.census.gov/data/key_signup.html). Use the [2024 ACS 5-year API](https://www.census.gov/data/developers/data-sets/acs-5year/2024.html) at `/data/2024/acs/acs5`, pass the key in `key`, and request place `27425` within Colorado (`state:08`). The API returns a JSON array with a header row. Allow a few KB for one place and a small variable selection. For a keyless download, use the [2024 ACS Summary File](https://www.census.gov/programs-surveys/acs/data/summary-file.2024.html). Each detailed table is a delimited `.dat` file covering multiple geographies. For example, the [5-year file index](https://www2.census.gov/programs-surveys/acs/summary_file/2024/table-based-SF/data/5YRData/) lists the occupancy table `acsdt5y2024-b25002.dat` at about 22 MB. The final ACS vintage and variable list are pending.

**Use and citation.** Credit the Census Bureau for its source data. The API terms call for an attribution notice in applications, prohibit implying Census Bureau endorsement, and prohibit using the data to identify survey respondents. Use ACS at the area level when comparing with simulated households. [Census API terms](https://www.census.gov/data/developers/about/terms-of-service.html) · [Census citation guidance](https://www.census.gov/about/policies/citation.html)

### 3. PVWatts V8

PVWatts provides modeled hourly solar production for a representative installation. The Fort Collins rooftop scenario averages four weather grid cells, with each run using a 4 kW DC system, standard modules, a fixed roof mount, 20° tilt, 180° azimuth, and 14.08% system losses. The four source CSVs are in `data/PVWATTS/Roof Mount/` and total 2,408,810 bytes.

#### Prepare the Fort Collins rooftop profile

In the [PVWatts calculator](https://pvwatts.nlr.gov/), use **Results → Hourly** to export each location. The browser export requires no API key. From the repository root, run:

```bash
python3 scripts/build_pvwatts_profile.py \
  "data/PVWATTS/Roof Mount"/pvwatts_hourly_FCRoof_{1,2,3,4}.csv \
  --output data/processed/pvwatts/fort-collins-rooftop-hourly.csv
```

When running from an IDE, set the working directory to the repository root and pass four separate CSV paths followed by `--output` and a new output path. The shell's `{1,2,3,4}` shorthand is only for the terminal.

The script checks that each export has the same 8,760 hourly keys and system settings. It gives each location equal weight and converts watts to kWh for each one-hour interval. The output year is 2018 to match ResStock's pilot calendar. The solar input still represents typical-year weather.

PVWatts source hours use local standard time, which is UTC−07:00 for these exports. The generated CSV includes the original fixed-MST hour, an `America/Denver` hour with the applicable MST or MDT offset, and UTC. Keep the offset when reading Mountain timestamps so the repeated fall hour stays distinct. Use `hour_start_utc` when joining to ResStock after normalizing its published fixed Eastern Standard Time hour starts. The pilot currently stores the ResStock wall-clock labels with UTC encoding. Interpret those labels as EST (UTC−05:00) before converting to actual UTC. ResStock wraps the last two Colorado hours into the first two EST hours, so restore those two records to the end of the 2018 local year if joining the full year. [PVWatts time convention](https://pvwatts.nrel.gov/downloads/pvwattsv5.pdf) · [ResStock timestamp FAQ](https://natlabrockies.github.io/ResStock.github.io/docs/FAQ.html)

#### Join 2018 demand and rooftop solar

Use Java 17 and PySpark 4.0.1 in the project's Python environment. If the environment has PySpark 3.0.x and the legacy `pyspark-stubs` package, remove those stubs before upgrading. PySpark 4.0.1 includes its own type hints.

```bash
.venv/bin/python -m pip uninstall -y pyspark-stubs
.venv/bin/python -m pip install 'pyspark==4.0.1'
```

From the repository root, pass all three paths. Choose a fresh output path when rerunning the join.

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/join_resstock_pvwatts.py \
  ../DATA_for_project/resstock-all-larimer-hourly \
  data/processed/pvwatts/fort-collins-rooftop-hourly.csv \
  data/processed/resstock-pvwatts-2018
```

The join accepts complete 2018 hourly Parquet from the ResStock pilot and writes one Parquet record per building and UTC hour. It keeps the original EST label for traceability and the offset-aware Colorado timestamp for reporting. Each building gets the same representative 4 kW rooftop profile. `solar_generation_kwh` is a per-installation scenario value. Apply the placement selection before summing solar across buildings. Filter to the 382 detached profiles using Colorado baseline metadata and `building_id` when running the initial placement experiment.

**Download and size.** Get an [NLR Developer Network key](https://developer.nlr.gov/signup/). Call `https://developer.nlr.gov/api/pvwatts/v8.json` with the system inputs, `timeframe=hourly`, and the key in the `X-Api-Key` header. Expect JSON (XML is also available), with hourly AC power in `outputs.ac` in watts and weather source and timezone in `station_info`. An uncompressed hourly JSON response is typically on the order of hundreds of KB per request, depending on the returned arrays. Save a sanitized response with the request inputs, model version, weather metadata, and retrieval date. The response can echo `inputs.api_key`. Divide hourly AC watts by 1,000 and multiply by the one-hour interval to get kWh. [PVWatts V8 request and response reference](https://developer.nlr.gov/docs/solar/pvwatts/v8/) · [API key usage](https://developer.nlr.gov/docs/api-key/)

**Use and citation.** NLR permits use of its API data subject to its terms, including restrictions on using DOE, NLR, or Alliance names to imply endorsement. The default developer limit is 1,000 requests per hour per key across NLR APIs. [Developer Network terms](https://developer.nlr.gov/terms/) · [Rate limits](https://developer.nlr.gov/docs/rate-limits/)

### 4. Solar-to-Grid

Berkeley Lab's 2012–2020 modeled generation data is a candidate for historical solar context or validation. The distributed PV files report county-level estimates for existing residential and non-residential installations. Confirm coverage for Larimer County before using it.

**Download and size.** From the [OEDI release](https://data.openei.org/submissions/4503), download **Hourly generation by plant and county** (8.69 GB ZIP). The archive contains plant and county CSVs. Use county FIPS `08069` for Larimer and retain the reference documentation. Extract only the relevant files if disk space is limited. [Berkeley Lab file guide](https://eta-publications.lbl.gov/sites/default/files/s2g_2021_user_guide_for_data_file.pdf)

**Use and citation.** CC BY 4.0. Credit the Berkeley Lab authors, cite DOI `10.25984/1825661`, link the license, and indicate changes when redistributing adapted data. [DOE catalog and license](https://catalog.data.gov/dataset/solar-to-grid-public-data-file-for-utility-scale-upv-and-distributed-photovoltai-2012-2020)

### 5. EPA Clean Air Markets Program Data (CAMPD)

CAMPD provides reported or apportioned hourly emissions and operating data for covered units, including CO₂ mass and gross load. The final unit list, period, grid mapping, and methodology are pending. The proposal budgets about 40 MB for the intended extract. Actual size depends on the filters and output format.

**Download and format.** In [CAMPD Custom Data Download](https://campd.epa.gov/data/custom-data-download), select **Emissions → Hourly Emissions**, choose the date range and states or facilities, and export CSV or JSON. The [Bulk Data Files page](https://campd.epa.gov/data/bulk-data-files) also provides prepackaged files. For direct API access, obtain an api.data.gov key via the [CAM API portal](https://www.epa.gov/power-sector/cam-api-portal) and follow the endpoint's authentication and paging instructions. Preserve facility and unit IDs, date and hour, and the published units.

**Use and citation.** EPA publishes these data for public use. Cite EPA's Power Sector Data and identify the filters and whether the data were apportioned. CAMPD reports emissions mass from covered facilities. Any regional factor derived from it needs a documented generation denominator, coverage, and units. [EPA data and suggested citation](https://www.epa.gov/power-sector/power-sector-data) · [EPA open data license](https://edg.epa.gov/epa_data_license.html)

### 6. EIA-930 Hourly Electric Grid Monitor

EIA-930 is a candidate for hourly demand, generation mix, interchange, and regional grid context. EIA's published hourly CO₂ estimates begin on July 1, 2018. Select the balancing authority or region before using the data in the analysis.

**Download and size.** Open the [Grid Monitor dashboard](https://www.eia.gov/electricity/gridmonitor/) and select **Download Data → Six-Month Files** for hourly balance, interchange, or subregion CSVs, or **Balancing Authority/Regional Files** for historical XLSX workbooks, which include published CO₂ estimates where available. Budget tens of MB per full six-month balance CSV as a planning estimate. Workbook sizes vary by region and coverage. To use the JSON API, [register for an EIA key](https://www.eia.gov/opendata/register.php) and follow the [EIA API documentation](https://www.eia.gov/developer/). Use the dashboard files for 2018 because historical API submissions start in 2019. [EIA-930 file definitions and coverage](https://www.eia.gov/electricity/930-content/about.html)

**Use and citation.** EIA permits reuse and redistribution of its data and asks users to acknowledge EIA and include the publication date. Direct API access is also subject to EIA's terms. [EIA reuse policy](https://www.eia.gov/about/copyrights_reuse.php) · [EIA API terms](https://www.eia.gov/opendata/terms-of-service.php)

### 7. Cambium

Cambium is a candidate for forward-looking emissions factors or sensitivity analysis. Its 2024 release provides modeled hourly scenarios for the contiguous U.S. through 2050. The release, scenario, modeled year, geography, and metric for this project are pending.

**Download and size.** Open [NLR's Cambium 2024 Scenario Viewer](https://scenarioviewer.nlr.gov/?layout=Default&mode=download&project=5c7bef16-7e38-4094-92ce-8b03dfa93380) and use **Download** to select the scenario and file at the desired resolution. Expect CSV for hourly data, with bulk files potentially packaged in ZIP. The [Cambium 2024 workbook](https://data.openei.org/submissions/8395) provides an XLSX subset with levelization calculations. The size depends on the chosen archive or workbook. Record the selected asset's byte size when the project pins the scenario and geography. [Cambium metric definitions and limitations](https://docs.nlr.gov/docs/fy25osti/93005.pdf)

**Use and citation.** The Scenario Viewer asks users to credit DOE, NLR, and Alliance in resulting publications and restricts use of their names for endorsement. Cite the release and scenario using the Viewer citation. Record whether the factor is average or marginal, along with the temporal resolution and units. [Scenario Viewer terms and citation](https://scenarioviewer.nlr.gov/?layout=Default&mode=download&project=5c7bef16-7e38-4094-92ce-8b03dfa93380)

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

For each source, record the release or API version, key-free URL or query, retrieval date, geography, file size, and units. For each processed artifact, also record the transformation and output:

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

The repository already ignores these data paths and file types.

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
