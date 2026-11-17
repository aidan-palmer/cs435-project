# Project Plan

## Core Deliverable

A reproducible forecasting, simulation, and allocation pipeline for evaluating residential solar-investment strategies in Fort Collins.

## Workstreams

### Data and Spark

- dataset selection and inventory;
- geography filtering;
- cleaning and integration;
- timestamp/unit alignment;
- lag and rolling features;
- partitioned Parquet outputs.

### Forecasting

- seasonal-naive baseline;
- Random Forest;
- LSTM;
- chronological validation;
- MAE/RMSE and runtime comparison.

### Simulation

- PV-generation assumptions;
- hourly demand/PV overlap;
- grid import before/after solar;
- avoided-CO2 calculation.

### Allocation and reporting

- random allocation;
- predicted-benefit allocation;
- held-out realized-benefit evaluation;
- sensitivity analysis;
- figures and final integration.

## Initial Timeline

### Week 11 - Nov 2-9

**Lead listed in proposal:** Kaleb Flowers

- lock scope, datasets, targets, and assumptions;
- run a small end-to-end example;
- verify geography, emissions data, and required software/data.

**Exit criteria:**

- exact ResStock release/weather year/geography chosen;
- emissions approach narrowed;
- small data sample flows through a prototype pipeline.

### Week 12 - Nov 10-16

**Lead listed in proposal:** Reese Edens (simulation / pipeline start)

- prepare Spark pipeline and data inventory;
- implement solar/emissions simulator;
- document units and time alignment.

**Exit criteria:**

- processed Parquet sample exists;
- core data-quality checks exist;
- simulator works on known test values.

### Week 13 - Nov 17-23

**Lead listed in proposal:** Aidan Palmer

- train/tune Random Forest and neural network;
- produce validation results;
- integrate forecast outputs into simulator.

**Exit criteria:**

- baseline, RF, and LSTM produce the same forecast horizon;
- validation MAE/RMSE recorded;
- forecast output format is stable.

### Week 14 - Nov 24-30

**Lead listed in proposal:** Calvin Edlund

- freeze models;
- evaluate forecasts and allocations;
- run a sensitivity scenario.

**Exit criteria:**

- final model configurations selected;
- allocation strategies compared under identical budget;
- held-out simulated benefits computed.

### Week 15 - Dec 1-8

**All team members**

- freeze features early in the week;
- finish report and figures;
- finalize software instructions;
- run reproducibility checks;
- practice presentation/demo.

## Immediate GitHub Issues to Create

Suggested first issue set:

1. **Freeze ResStock release, weather year, and geography**
2. **Create data inventory and download instructions**
3. **Build small end-to-end fixture**
4. **Implement timestamp and unit normalization**
5. **Write seasonal-naive baseline**
6. **Create Random Forest training pipeline**
7. **Create LSTM sequence pipeline**
8. **Implement PVWatts profile retrieval/cache**
9. **Implement grid-import simulation**
10. **Choose/document emissions-factor methodology**
11. **Implement fixed-budget random allocation**
12. **Implement predicted-benefit allocation**
13. **Create forecast evaluation report**
14. **Create allocation evaluation report**
15. **Add runtime/scaling benchmark**
16. **Create final reproducibility script/instructions**

## Backlog

### Simulate home battery storage

Start this as a separate issue after the baseline → forecast → simulation → allocation pipeline runs end to end. Compare PV-only and PV-plus-battery scenarios for the same simulated buildings and rooftop systems. For a motivating example, building 532866 had 18.23 kWh of excess solar and still imported 4.25 kWh on May 30, 2018. The hourly timing determines how much storage could offset.

Use the [hourly grid-import output](ARCHITECTURE.md#6-grid-import-simulation). Group by `building_id` and process consecutive `hour_start_utc` records in order. Carry battery state of charge across days. Set usable capacity (kWh), charge and discharge limits (kW), charge and discharge efficiencies, and initial state of charge explicitly. Start the baseline storage run empty and use PV surplus as the charging source.

1. Serve demand with same-hour solar as in the PV-only case.
2. Charge from `excess_solar_kwh`, limited by available surplus, the hourly charge limit, and the remaining usable capacity after charging losses.
3. Discharge to cover `grid_import_with_solar_kwh`, limited by remaining demand, the hourly discharge limit, and the energy the battery can deliver after discharge losses.
4. Update state of charge. Record hourly charge, energy delivered from the battery, end-of-hour state of charge, losses, grid imports with storage, and remaining surplus.
5. Compare annual grid imports and surplus for PV-only and PV-plus-battery runs. Report ending state of charge and repeat for several capacities. Use annual results to assess storage benefits.

**Acceptance criteria**

- Produce hourly storage results and annual comparisons per building.
- Preserve energy balance, respect capacity and power limits, and carry state correctly overnight.
- Add tests for a zero-capacity battery, empty and full states, efficiency losses, power limits, and surplus on one day serving demand the next.

## Scope Control

Before adding a feature, ask whether it is required to answer the research question.

Do not prioritize battery optimization, complex roof geometry, parcel-level claims, or advanced optimization until the baseline -> forecast -> simulation -> allocation pipeline runs end to end.
