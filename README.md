# Forecasting Residential Electricity Demand to Guide Solar Investment in Fort Collins

## Overview

This term project investigates whether machine-learning forecasts of residential electricity demand can improve residential solar placement decisions in Fort Collins, Colorado.

The project combines residential electricity demand, housing characteristics, solar-production estimates, and regional grid/emissions data in a reproducible Big Data pipeline. We will compare a seasonal-naive baseline, a Random Forest Regressor, and an LSTM model for next-day hourly electricity-demand forecasting. Forecasts will then feed a solar and emissions simulation used to compare installation strategies under a fixed budget.

The core deliverable is a reproducible **forecasting -> solar simulation -> emissions estimation -> allocation** pipeline.

## Research Question

> How accurately can a machine-learning model forecast residential electricity demand, and how do those forecasts affect estimated avoided CO2 and placement of residential solar in Fort Collins under a fixed budget?

Supporting questions:

- Does Random Forest or LSTM outperform a seasonal forecasting baseline?
- Does better forecasting accuracy lead to better solar-placement decisions?
- How sensitive are the results to installation cost, system capacity, and emissions assumptions?

## Project Scope

### In scope

- Residential electricity-demand preparation with PySpark
- Chronological train/validation/test splits
- Seasonal-naive demand forecasting baseline
- Random Forest regression using Spark MLlib
- LSTM forecasting using TensorFlow/Keras
- Hourly solar-production estimates
- Grid-import simulation with and without solar
- Avoided-CO2 estimation
- Fixed-budget solar-allocation strategies
- Sensitivity analysis
- Runtime/scaling measurements
- Reproducible figures and final demonstration

### Secondary / stretch goals

- Battery-storage simulation
- More detailed roof or parcel modeling
- Advanced allocation methods such as genetic algorithms

These should not displace the core pipeline.

## Planned Data Sources

| Dataset | Purpose | Initial scope |
|---|---|---|
| ResStock | Hourly residential demand and building characteristics | Fort Collins / relevant Colorado geography |
| U.S. Census / ACS | Housing context and estimates of eligible housing stock | Fort Collins |
| PVWatts V8 | Representative hourly PV-production profile | Standardized solar package |
| Solar-to-Grid | Historical solar-generation context / validation | Larimer County where appropriate |
| EPA CAMPD | Fossil generation and emissions-related data | Supporting emissions context |
| EIA-930 | Regional load, generation mix, and interchange | Grid context / possible emissions proxy |
| Cambium | Potential emissions-factor source or sensitivity input | If used by final methodology |

See [`docs/DATA.md`](docs/DATA.md) before downloading or adding data.

## High-Level Pipeline

```text
Raw Data
  |
  v
PySpark preprocessing
  - filter geography/building types
  - normalize units
  - align timestamps
  - validate missing/duplicate records
  - engineer lag/rolling/calendar features
  |
  v
Partitioned Parquet
  |
  +-------------------------+
  |                         |
  v                         v
Seasonal baseline      Random Forest / LSTM
  |                         |
  +------------+------------+
               |
               v
       24-hour demand forecasts
               |
               v
       Hourly solar simulation
               |
               v
 Grid import before / after solar
               |
               v
     Avoided CO2 estimation
               |
               v
 Fixed-budget allocation strategies
               |
               v
 Evaluation + sensitivity + figures
```

More detail is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Repository Layout

```text
.
├── README.md
├── CONTRIBUTING.md
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DATA.md
│   ├── DEVELOPMENT.md
│   ├── EXPERIMENTS.md
│   └── PROJECT_PLAN.md
├── data/
│   ├── raw/              # ignored; never commit large source datasets
│   ├── interim/          # ignored
│   └── processed/        # ignored
├── notebooks/            # exploratory work only
├── src/
│   ├── data/             # PySpark ingestion + preprocessing
│   ├── features/         # lag, rolling, calendar, weather features
│   ├── models/           # baseline, Random Forest, LSTM
│   ├── simulation/       # PV, grid-import, emissions simulation
│   ├── allocation/       # fixed-budget placement strategies
│   └── evaluation/       # metrics, sensitivity, runtime tests
├── tests/
├── outputs/
│   ├── figures/
│   ├── metrics/
│   └── logs/
└── scripts/
```

Directories can be added as implementation begins. Avoid placing important project logic only in notebooks.

## Evaluation

### Forecast quality

- Mean Absolute Error (MAE)
- Root Mean Squared Error (RMSE)
- Comparison against seasonal-naive forecasting
- Held-out chronological test data

### Solar-allocation impact

- Total simulated grid-electricity reduction
- Total estimated avoided CO2
- Environmental benefit per dollar invested
- Performance of random allocation vs. benefit-prioritized allocation
- Realized benefit evaluated against held-out demand profiles

### Computational performance

- Preprocessing time
- Model training time
- Inference time
- Runtime as dataset size increases

## Reproducibility Rules

1. **Do not commit raw datasets.** Record source URLs, versions, query parameters, geography, and retrieval dates in `docs/DATA.md`.
2. **Keep units explicit.** Every energy/power field should document its unit and conversion.
3. **Use chronological splits.** No future information may leak into training features.
4. **Fit preprocessing on training data only** when fitting is required.
5. **Version experiment assumptions.** Solar capacity, cost, emissions factors, and budget must be logged with each result.
6. **Distinguish simulated households from real properties.** Do not imply that ResStock records are actual Fort Collins households.
7. **Prefer scripts/modules over manual notebook steps** for final results.
8. **Save random seeds** wherever stochastic models or allocations are used.

## Getting Started

Until the implementation environment is finalized, use this workflow:

```bash
git clone <repository-url>
cd <repository-name>

# create a feature branch
git checkout -b feature/<short-description>

# create local data folders
mkdir -p data/raw data/interim data/processed outputs/figures outputs/metrics outputs/logs
```

Then review:

1. [`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md)
2. [`docs/DATA.md`](docs/DATA.md)
3. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
4. [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
5. [`docs/EXPERIMENTS.md`](docs/EXPERIMENTS.md)

## Team Workstreams

The project is divided into four connected areas:

- **Data and Spark:** dataset selection, cleaning, integration, and inventory
- **Forecasting:** seasonal baseline, Random Forest, LSTM, and validation
- **Simulation:** solar production, grid imports, and emissions calculations
- **Allocation and reporting:** placement comparisons, sensitivity analysis, figures, and integration

See [`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md) for the initial milestone breakdown.

## Definition of Done

The project is ready for final submission when a clean checkout can reproduce the major results from documented source data and configuration, including:

- prepared analysis data;
- baseline, Random Forest, and LSTM forecast metrics;
- solar/grid-import simulation results;
- at least two allocation strategies under the same fixed budget;
- held-out evaluation of selected installations;
- one documented sensitivity analysis;
- runtime measurements; and
- final figures used in the report/presentation.

## Team

- Reese Edens
- Calvin Edlund
- Kaleb Flowers
- Aidan Palmer

## Course Project Note

This repository supports a Big Data term project. The emphasis is not only predictive accuracy, but also the distributed-data pipeline, reproducibility, computational tradeoffs, and how forecast quality affects a downstream solar-investment decision.
