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

For each household/hour:

```text
grid_without_solar = demand
self_consumed_solar = min(demand, solar_generation)
grid_with_solar = max(demand - solar_generation, 0)
grid_reduction = grid_without_solar - grid_with_solar
```

If exported solar is later modeled, it must be treated separately and documented.

Suggested module:

```text
src/simulation/grid.py
```

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

At minimum compare:

1. random allocation;
2. predicted-benefit prioritization.

Potential score:

```text
predicted_benefit_per_dollar = predicted_avoided_co2 / installation_cost
```

A key evaluation requirement is that households selected using predictions are scored on **held-out realized demand**, not only on their predicted benefit.

Suggested module:

```text
src/allocation/strategies.py
```

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
