# System Architecture

## Goal

Build a reproducible pipeline that connects distributed residential-demand preprocessing to forecasting, solar simulation, emissions estimation, and fixed-budget allocation.

## Components

### 1. Data ingestion

**Technology:** PySpark / Spark DataFrames

Responsibilities:

- read raw source files;
- apply geography and building-type filters;
- retain required metadata;
- standardize column names and data types;
- create an auditable data inventory.

Suggested module:

```text
src/data/ingest.py
```

### 2. Data cleaning and time alignment

Responsibilities:

- detect duplicates and missing intervals;
- normalize electricity units;
- align timestamps;
- document daylight-saving and leap-year behavior;
- reject or flag incomplete demand profiles.

Suggested module:

```text
src/data/clean.py
```

### 3. Feature engineering

Responsibilities:

- demand lags;
- rolling historical summaries;
- hour/day/week/season features;
- available weather features;
- selected static building characteristics.

All features must be computable from information available when the forecast is issued.

Suggested module:

```text
src/features/build_features.py
```

### 4. Forecasting

All forecasting methods target the same next-day hourly period.

#### Seasonal naive baseline

Possible initial forms:

- same hour previous day (`t - 24h`);
- same hour previous week (`t - 168h`).

Freeze the exact baseline before model comparison.

#### Random Forest

**Technology:** Spark MLlib

Inputs: engineered tabular features.

Output: hourly demand prediction.

#### LSTM

**Technology:** TensorFlow / Keras

Inputs: historical demand sequences plus any frozen contextual features.

Output: same forecast horizon as baseline and Random Forest.

Suggested layout:

```text
src/models/baseline.py
src/models/random_forest.py
src/models/lstm.py
```

### 5. Solar generation

Generate or load an hourly production profile for a standardized residential PV system.

The simulator must record assumptions including:

- capacity;
- losses;
- tilt;
- azimuth;
- source/API version;
- production unit.

Suggested module:

```text
src/simulation/solar.py
```

### 6. Grid-import simulation

For each simulated building and UTC hour, all energy values are in kWh:

| Output column | Calculation |
|---|---|
| `grid_import_without_solar_kwh` | Hourly demand |
| `self_consumed_solar_kwh` | Smaller of demand and solar generation |
| `grid_import_with_solar_kwh` | Demand minus self-consumed solar |
| `excess_solar_kwh` | Solar generation minus self-consumed solar |
| `grid_reduction_kwh` | Import without solar minus import with solar |

Surplus is recorded in its own column. It represents energy available for export or curtailment under a later policy. The hourly model uses solar within the hour it is generated, with zero export credit and no battery storage. Each row represents an installation scenario for one building. Apply the placement selection before aggregating solar benefit across buildings.

Run the [simulation script](../scripts/simulate_grid_import.py) from the repository root after creating the joined Parquet described in [DATA.md](DATA.md#join-2018-demand-and-rooftop-solar):

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/simulate_grid_import.py \
  data/processed/resstock-pvwatts-2018 \
  data/processed/grid-import-2018
```

The output retains the input building ID, UTC timestamp, and offset-aware Mountain Time label. Choose a new output directory for each run. Run the unit tests with `SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python -m unittest discover -s tests`.

#### Explore the hourly simulation

Run the local dashboard after generating the grid-import Parquet:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/serve_grid_dashboard.py data/processed/grid-import-2018
```

Open `http://127.0.0.1:8765` while the process is running. In an IDE, use the repository root as the working directory, pass `data/processed/grid-import-2018` as the script parameter, and set the same two environment variables. Pass `--port 8766` if the default port is in use.

#### Fort Collins area heatmap and building location

The dashboard pairs one city heatmap with a comparison bar chart. **Main map metric** selects the sole solid map fill: solar generation (amber), grid import without solar (purple), grid import with solar (red), grid reduction from solar (green), or excess solar (cyan). The map defaults to solar generation. The former overlapping diagonal-band design has been removed.

The **Other metrics by city** chart beside the map compares the remaining metrics for Fort Collins and Loveland. Each metric uses its own color and has one labeled horizontal bar per city, with the numeric value at the end. **Show comparison bars** checkboxes hide or restore chart metrics independently of the map. The main metric is omitted from those checkboxes and bars automatically. Switching the main metric moves it onto the map and makes the previous metric available in the chart, preserving checkbox preferences. If no comparison metrics are enabled, the chart displays a prompt while the main map remains visible. At narrow screen widths the chart stacks beneath the map. Click a city for the main metric and enabled comparison values.

The **Simulation hour** slider inside the map section updates the main map, comparison bars, and numeric city values together. **Previous hour**, **Next hour**, and **Play day / Pause** support stepping or playback. Playback stops at the last available hour, when a new day/building loads, when switching to daily totals, or when the page is hidden. Moving the slider or stepping manually also pauses playback. Slider labels use offset-aware Mountain Time, so both repeated fall hours appear distinctly as MDT and MST. The slider follows the actual available hourly timestamps, including 23-hour and 25-hour days. The first load starts at noon when available; subsequent loads preserve the chosen local hour when possible.

**Map period** switches between hourly values (default) and daily totals. Daily mode disables playback and the slider. **Map values** switches between average per simulated building (default) and total for simulated buildings. Units are kWh/building/hour or kWh/hour in hourly mode and kWh/building/day or kWh/day in daily mode. These are energy values for the displayed interval, not citywide stock estimates or instantaneous kW. All modeled homes still receive the same representative rooftop solar profile.

Both the map and comparison bars summarize **all simulated buildings in that city for the displayed interval**, even when the hourly chart shows one building. The selected-building summary follows the map hour, and the chart cursor points to the same hour. City tags come from `in.city`. Hourly city averages divide by the number of contributing buildings in that hour; daily averages divide by the distinct contributing buildings that day. The `/api/map-day` response includes daily aggregates and a UTC-ordered `hours` array. A small broadcast join attaches the geographic tags, and Spark groups city/census-area hours before collecting them; raw building-hour records are not sent to the browser. Changing the slider, playback time, units, main metric, or comparison visibility makes no new data request.

The map has a zero-to-maximum color scale for the chosen main metric across both cities and every available hour of the loaded day. The bars have their own common zero-to-maximum length scale across all five measures, both cities, and the entire loaded day, including hidden metrics and the main metric. These independently labeled scales stay fixed as the slider moves. Changing the main metric recomputes the map's scale without changing the bar scale; hiding comparison bars changes neither scale. Daily mode uses daily aggregates and daily scales; unit changes or a new date recompute both scales. Gray means missing map data, and bars distinguish **No data** from valid zero values. City values and scales remain available when external map assets cannot load. **Compare numeric city values** lists the main metric and enabled comparison metrics.

The local 573-building dataset has 256 Fort Collins tags, 128 Loveland tags, and 189 without a specific mapped city. The latter remain in the overall chart but are excluded from city colors; the dashboard reports coverage for the buildings actually present on that date. Missing location columns in older metadata are supported and leave buildings unplaced.

Selecting a building highlights its recorded city with a blue outline and automatically loads its hourly results. **Show building area** recenters the map. If the city is unspecified, the map uses the published `in.puma` tag to show a dashed outline of its broader 2010 Census Public Use Microdata Area. Labels distinguish city-level locations from broad census areas. Unknown tags produce a location-unavailable message. The map does not infer an address, invent a point inside an area, or use weather-station coordinates as household coordinates. A search that selects a matching building updates its outline; **Show day** loads that building's energy summary.

`scripts/larimer_map_areas.geojson` bundles simplified official U.S. Census Bureau [TIGERweb ACS 2018 boundaries](https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2018/MapServer), matching the simulation calendar: layer 26 for Fort Collins (`0827425`) and Loveland (`0846465`) city boundaries, and layer 0 for Larimer's 2010 PUMAs (`0800102`, `0800103`). The feature collection records the exact source query URLs and simplification tolerance (0.0003 degrees). PUMA metadata GISJOINs `G08000102` and `G08000103` map to those Census GEOIDs. Boundaries are served locally in dashboard metadata; no live geocoding is needed.

Drag to pan, use zoom buttons, and choose **Reset map view** to restore the regional extent. The map’s layer control toggles the solar reference markers; the main metric uses **Main map metric**, and chart visibility uses **Show comparison bars**. The markers use `FORT_COLLINS_POINTS` from `scripts/pvwatts_api.py`; they represent the four equally weighted solar reference locations, not households. Mouse-wheel zoom is disabled to keep page scrolling predictable.

The map uses [Leaflet 1.9.4](https://leafletjs.com/examples-v1/quick-start/) from unpkg and [OpenStreetMap tiles](https://operations.osmfoundation.org/policies/tiles/), with visible attribution. It requires internet access for those assets and tiles, sends tile requests from the browser, and needs no API key. If the map cannot load, the hourly chart and numeric city results remain usable. A failed `/api/map-day` request clears the previous heatmap and shows a message while preserving successful hourly results. No tiles are prefetched or downloaded for offline use.

Summary text uses IDs separate from the sole map container (`region-map`). The HTML regression check in `tests/test_grid_dashboard_html.py` prevents duplicate element IDs: a previous duplicate `map-reduction` ID allowed summary updates to target and destroy the reduction map's contents.

Run the HTML regression check with `.venv/bin/python -m unittest discover -s tests -p test_grid_dashboard_html.py`. Run the geography and aggregation checks with `.venv/bin/python -m unittest discover -s tests -p test_grid_dashboard.py`. The full test suite also requires the Spark environment described above.

#### Find a simulated building

The dashboard reads `../DATA_for_project/CO_baseline_metadata_and_annual_results.csv` by default. If you keep the Colorado baseline metadata elsewhere, add `--metadata "path/to/CO_baseline_metadata_and_annual_results.csv"` to the dashboard command or the IDE Parameters field. The [ResStock data instructions](DATA.md#1-resstock-hourly-demand) link to the original CSV. The dashboard matches its baseline `bldg_id` to the simulation's `building_id`. Buildings without a matching metadata record appear by ID.

Each result reads like **Single-Family Detached · 1,698 sq ft · 2000s · Natural Gas · ID 128**. The labels describe simulated dwellings.

| Label | Meaning | Example search |
|---|---|---|
| Building type | Modeled housing structure | `detached` |
| Square footage | Modeled floor area in square feet | `1698` or `1,698` |
| Construction decade | Modeled construction vintage | `2000s` |
| Heating fuel | Modeled heating fuel | `natural gas` |
| ID | ResStock building identifier | `id 128` |

Type one or more terms into **Search buildings**. For example, `detached 2000s natural gas` matches labels containing all those terms. Then choose a result from **Building**. To return to the aggregate view, clear the search and choose **All buildings**.

Set the Mountain Time date and click **Show day**. **Most productive day** jumps to the date with the greatest total solar generation for the selected scope. Ties use the earliest date. The chart maintains the separate repeated hour on the fall clock change.

### 7. Avoided-CO2 estimation

Basic form:

```text
avoided_co2 = grid_reduction_kwh * emissions_factor
```

The emissions-factor source, spatial resolution, temporal resolution, and unit conversions must be explicit.

Suggested module:

```text
src/simulation/emissions.py
```

### 8. Fixed-budget allocation

The implemented entry point is `scripts/allocate_solar.py`. It consumes a scored household CSV and an explicit experiment-assumptions JSON. See [ALLOCATION.md](ALLOCATION.md) for schemas, formulas, commands, and saved artifacts.

The default prioritized strategy ranks by predicted avoided CO2 per USD; a total-predicted-benefit ranking is also available. It selects whole installations using exact cent arithmetic, skips homes that do not fit the remaining budget, and continues down the ranking. This is a greedy heuristic rather than a globally optimal knapsack solver.

A seeded random baseline uses the identical eligible pool and budget cap, with 100 runs by default. Results record selected households, estimated benefits, spending, remaining budget, seeds, and gains against the random mean. Variable installation costs may lead to different counts and spending under the same cap.

Optional held-out realized benefits are used only to evaluate the selected homes. They never participate in ranking or random selection. Runs without realized outcomes are explicitly labeled estimated-only. The allocator does not derive scores from realized demand or introduce default installation costs or emissions factors; upstream forecast-derived scoring and the emissions methodology remain explicit inputs.

### 9. Evaluation and reporting

Forecast metrics:

- MAE;
- RMSE.

Decision metrics:

- grid reduction;
- avoided CO2;
- benefit per dollar;
- allocation regret or difference relative to an oracle strategy, if added.

System metrics:

- preprocessing runtime;
- training runtime;
- inference runtime;
- scaling with data size.

Suggested modules:

```text
src/evaluation/forecast_metrics.py
src/evaluation/allocation_metrics.py
src/evaluation/runtime.py
```

## Leakage Prevention

The pipeline should make chronological ordering a first-class design constraint.

```text
TRAIN -------- VALIDATION -------- TEST
past                                  future
```

Rules:

- choose time boundaries before fitting transformations;
- rolling features must use only prior observations;
- do not calculate normalizers/statistics from future/test data;
- tune models using validation data only;
- perform final decision evaluation on held-out test demand.

## Suggested End-to-End CLI

A future interface could look like:

```bash
python -m src.data.prepare --config configs/data.yaml
python -m src.models.train --model rf --config configs/rf.yaml
python -m src.models.train --model lstm --config configs/lstm.yaml
python -m src.simulation.run --forecast outputs/... --config configs/simulation.yaml
python -m src.allocation.run --config configs/allocation.yaml
python -m src.evaluation.report --run-id <run-id>
```

This is a target structure, not a requirement to implement immediately.
