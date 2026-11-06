# Experiment and Results Protocol

This document defines how experiments should be recorded so model comparisons and downstream allocation results remain reproducible.

## Experiment ID

Use a stable run ID such as:

```text
2026-11-20_rf_lag24-lag168_v1
```

Each run should save configuration, metrics, and relevant logs under the same ID.

## Forecasting Experiments

### Models

- Seasonal naive baseline
- Random Forest Regressor
- LSTM

### Required split information

Record exact chronological boundaries:

```text
training:   YYYY-MM-DD to YYYY-MM-DD
validation: YYYY-MM-DD to YYYY-MM-DD
test:       YYYY-MM-DD to YYYY-MM-DD
```

### Metrics

At minimum:

```text
MAE
RMSE
training runtime
inference runtime
```

Optional breakdowns:

- hour of day;
- season/month;
- building segment;
- demand quantile.

## Downstream Solar Experiment

Every allocation experiment must identify:

```text
forecast model/run ID
PV profile/version
PV capacity
installation cost
budget
emissions factor/source
allocation strategy
random seed (if applicable)
held-out evaluation period
```

## Allocation Strategies

Minimum comparison:

### Random

Select eligible homes randomly until the installation budget is exhausted.

Run multiple seeds if variance is meaningful.

### Predicted environmental benefit

Rank homes by forecast-derived benefit, ideally normalized by cost if costs vary.

### Optional oracle/reference

For analysis only, rank using held-out realized benefit. This can provide an upper-bound comparison, but must not be presented as an implementable forecasting strategy.

## Avoided-CO2 Calculation

Document the exact formula and units used by the final implementation.

Template:

```text
Grid reduction:              kWh
Emissions factor:            kg CO2 / kWh
Avoided emissions:           kg CO2
Avoided emissions per cost:  kg CO2 / USD
```

## Sensitivity Analysis

The proposal identifies three assumptions to vary:

1. installation cost;
2. solar-system capacity;
3. emissions assumptions.

Recommended format:

| Scenario | Cost | PV capacity | Emissions assumption | Result |
|---|---:|---:|---|---|
| Base | ... | ... | ... | ... |
| Low-cost | ... | ... | ... | ... |
| High-capacity | ... | ... | ... | ... |
| Alternate emissions | ... | ... | ... | ... |

Keep forecasting models frozen while varying simulation assumptions unless the experiment explicitly studies both.

## Scaling Experiment

Because this is a Big Data project, include at least one runtime/scaling result.

Suggested approach:

Run preprocessing/training on increasing fractions or sizes of the dataset while keeping the environment as consistent as possible.

Record:

```text
input rows/buildings
input size
Spark partitions
cluster/executor configuration
preprocessing runtime
training runtime
inference runtime
```

## Result Directory Convention

```text
outputs/
  metrics/<run-id>.json
  logs/<run-id>.log
  figures/<run-id>_<figure-name>.png
```

Example metric record:

```json
{
  "run_id": "2026-11-20_rf_v1",
  "model": "random_forest",
  "mae": null,
  "rmse": null,
  "train_seconds": null,
  "inference_seconds": null,
  "git_commit": "",
  "notes": ""
}
```

## Planned Figures

From the proposal:

- actual vs. predicted hourly demand;
- MAE/RMSE model comparison;
- household demand and solar-generation overlap;
- aggregate grid imports before and after solar;
- avoided CO2 versus installation budget;
- geographic or household-segment summaries when supported by the available data.
