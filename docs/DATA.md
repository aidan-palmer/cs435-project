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

**Role:** Primary technical dataset for residential electricity demand and building characteristics.

**Initial proposal estimate:** approximately 1.68 GB for the Fort Collins subset.

**Source:** National Laboratory of the Rockies / ResStock datasets and documentation.

**Planned use:**

- hourly residential electricity demand;
- building characteristics;
- available weather information;
- model inputs for baseline, Random Forest, and LSTM forecasts.

**Decisions to record before processing:**

- [ ] ResStock release/version:
- [ ] weather year:
- [ ] exact geography/filter used for Fort Collins area:
- [ ] included building types:
- [ ] electricity field(s) selected:
- [ ] timestamp convention/timezone:
- [ ] interval-energy vs. interval-power interpretation:

**Validation checks:**

- [ ] missing timestamps;
- [ ] duplicate records;
- [ ] incomplete daily profiles;
- [ ] negative or implausible demand;
- [ ] consistent 24-hour forecast targets;
- [ ] expected number of records per building/time period.

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
