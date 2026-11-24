# Predicted-benefit solar allocation

`scripts/allocate_solar.py` ranks scored households, selects whole installations within a fixed budget, and compares them with seeded random allocation. It uses the Python standard library and does not require Spark for this household-level selection step.

## Create the files from your combined data

`scripts/prepare_allocation.py` now converts the existing joined demand/solar data into the two allocator inputs. It can also run the $15,000 and $20,000 comparisons in the same command. Run from the repository root:

```bash
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell'

.venv/bin/python scripts/prepare_allocation.py \
  data/processed/resstock-pvwatts-2018 \
  data/processed/allocation-ready \
  --evaluation-start 2018-10-01 \
  --evaluation-end 2019-01-01 \
  --budgets-usd 15000 20000
```

The script prompts for three explicit scenario inputs:

1. Installation cost **per household system**, in USD. This is separate from the total allocation budget.
2. A constant emissions factor in **kg CO2 per kWh**.
3. The factor's source/methodology, or a label clearly identifying a sensitivity assumption.

No default cost or emissions factor is inserted. For noninteractive execution, supply `--installation-cost-usd`, `--emissions-factor-kg-per-kwh`, and `--emissions-source` explicitly. `--pv-capacity-kw` records the capacity represented by the input solar profile (the project default is 4 kW); it does not resize the profile.

The output directory must be new. The command above creates:

```text
allocation-ready/
  allocation-candidates.csv
  allocation-assumptions.json
  budget-15000/
    ranked_households.csv
    selected_households.csv
    strategy_results.csv
    summary.json
  budget-20000/
    ...same four result files...
```

To show the $15,000 run in the dashboard, use the same terminal environment:

```bash
.venv/bin/python scripts/serve_grid_dashboard.py \
  data/processed/grid-import-2018 \
  --allocation-results data/processed/allocation-ready/budget-15000 \
  --port 8767
```

Open `http://127.0.0.1:8767/allocation`. To display the $20,000 run, restart with `budget-20000`. Stop an existing dashboard on that port before starting another one.

### Forecast and scoring method

This preparation command creates a **frozen historical baseline forecast**: for each building, it averages demand by UTC hour of week using only the 28 elapsed days before the evaluation start. That profile stays fixed throughout the evaluation period. The historical baseline gets the combined-data pipeline running without requiring a separately saved model output; it is not a Random Forest or LSTM forecast. Saved model predictions require a separate adapter with verified timestamp conventions.

The evaluation dates are Mountain Time calendar boundaries, with an exclusive end. Training covers a whole number of elapsed UTC weeks (`--training-days`, default 28). This means clock-change days retain their actual 23 or 25 hours, while the forecast profile uses UTC weekday/hour, not local weekday/hour. The script checks complete, unique, aligned hours and finite nonnegative demand/solar for every building in both windows.

Predicted and realized-demand self-consumption are calculated separately as `min(demand, solar)`, summed over the evaluation period, then multiplied by the supplied constant CO2 factor. There is zero export credit. The realized column evaluates held-out demand with the same modeled PV profile and emissions assumptions; it is not measured avoided emissions. The CSV also saves both grid-reduction totals in kWh for auditing. Only per-building aggregates are collected to Python.

By default, the preparation uses `../DATA_for_project/CO_baseline_metadata_and_annual_results.csv` and marks only **Single-Family Detached** profiles eligible, following the initial placement scope. Use `--metadata` if that CSV is elsewhere. `--all-buildings` explicitly broadens eligibility to every modeled profile. Ineligible buildings can remain in the CSV but are excluded from both allocation strategies. Every building must have complete matching baseline metadata when using the default filter.

The assumptions file records the training cutoff/model, input path, system capacity, supplied cost/factor/source, evaluation window, and eligibility scope. With `--budgets-usd`, those preparation details are also preserved in each allocation summary. Omitting that option creates only the input files; the standalone allocator commands below remain available.

Preparation tests:

```bash
PYTHONDONTWRITEBYTECODE=1 SPARK_LOCAL_IP=127.0.0.1 \
.venv/bin/python -m unittest discover -s tests -p test_prepare_allocation.py -v
```

## Inputs

Supply a CSV with one row per household:

| Column | Meaning |
|---|---|
| `building_id` | Unique nonempty household identifier, preserved as text. |
| `installation_cost_usd` | Positive installation cost in USD, with no fractions of a cent. |
| `predicted_avoided_co2_kg` | Finite nonnegative forecast-derived avoided CO2, summed over the experiment's common evaluation period. |
| `realized_avoided_co2_kg` | Optional held-out realized avoided CO2 over the same period; supply values for every household if this column is present. |
| `eligible` | Optional `true`/`false` or `1`/`0`; defaults to `true`. Both strategies use the same eligible pool. |

IDs must be unique, including ineligible rows. Blank benefits, nonfinite values, negative benefits, malformed rows, and invalid costs are rejected before creating outputs. Unknown extra columns are ignored. Zero predicted benefit is valid, remains in the eligible pool, and ranks after positive benefit; exclude a household explicitly with `eligible=false` if necessary.

The allocator consumes **precomputed household scores**, not the existing realized-demand grid-import Parquet as a forecast. The project has not yet adopted production installation costs or an emissions methodology. The preparation command above supplies forecast-derived scores using explicit cost and emissions inputs; the allocator itself introduces no default cost or emissions factor.

For the current PV-only simulation with zero export credit, an upstream scoring step can compute:

```text
predicted_avoided_co2_kg = sum over hours(
    min(predicted_demand_kwh, solar_generation_kwh) * documented_emissions_factor_kg_per_kwh
)
```

Use aligned hours, finite nonnegative energy inputs, the chosen emissions methodology, and the same system configuration and horizon for all households. Forecasts and eligibility must use only information available at the allocation decision time. For evaluation, compute the separate realized benefit using held-out realized demand. Do not substitute realized demand into the ranking score. This allocator cannot verify how externally supplied scores were produced.

## Experiment assumptions

Pass `--assumptions path/to/assumptions.json`. All six fields are required and saved in the summary:

```json
{
  "forecast_run_id": "identify-your-forecast-model-and-run",
  "pv_profile_id": "identify-your-PV-profile-and-version",
  "pv_capacity_kw": 4,
  "emissions_source": "identify-factor-source-methodology-and-version",
  "evaluation_start": "2018-10-01",
  "evaluation_end": "2019-01-01"
}
```

This is a configuration template, not an adopted emissions or cost assumption. Use your actual experiment configuration; the end date is exclusive. The cost of each candidate is recorded in the ranked and selected household files. All monetary values use USD. Benefits are kg CO2 **over the stated period**, not automatically annual or lifetime benefits.

## Run

From the repository root:

```bash
.venv/bin/python scripts/allocate_solar.py \
  data/processed/allocation-candidates.csv \
  data/processed/allocation-run-001 \
  --budget-usd 100000 \
  --assumptions path/to/assumptions.json \
  --ranking benefit_per_dollar \
  --seed 42 \
  --random-runs 100
```

Choose a new output directory for each run. Existing results are never overwritten. The script validates the scored candidate CSV and assumptions before creating an output directory.

## Selection and comparison

The default score is `predicted_avoided_co2_kg / installation_cost_usd`. `--ranking predicted_benefit` instead ranks by total predicted avoided CO2. Scores sort descending; ties use the building ID in lexicographic order. Ratio ordering uses exact fractions, while exported ratio values are displayed to Decimal precision.

The prioritized strategy scans that order, selecting an eligible installation if its entire cost fits the remaining budget. It skips unaffordable homes and continues to later, cheaper candidates. Budget arithmetic uses integer cents, preventing floating-point overspending. Zero budgets and budgets below the cheapest installation produce empty selections with valid result files.

The random strategy shuffles the same eligible candidate pool, then uses the same whole-installation budget check. It starts from a canonical ID order, uses an independent random-number generator, and runs seeds `seed` through `seed + random_runs - 1`. The default is 100 runs. Selection is reproducible and invariant to input CSV row ordering. Neither strategy uses realized outcomes to select or break ties.

Both strategies obey the **same budget cap**, not necessarily identical spending or installation counts when costs vary. Reported spending and unused budget make this visible. At equal installation costs, the counts are equal. The prioritized method is a greedy ranking heuristic, not a globally optimal knapsack solver; a random run or its mean can outperform it for some variable-cost inputs. Negative comparison gains are reported honestly.

The comparison reports predicted avoided CO2, and separately realized avoided CO2 when supplied. It includes random-run mean, minimum, maximum, sample standard deviation, and prioritized gain over the random mean. Sample standard deviation is `null` for a single random run. Percentage gain is `null` when the random mean is zero. Without realized scores, the summary explicitly labels the comparison as estimated-only; it is not evidence of improved held-out environmental impact.

## Saved artifacts

| File | Contents |
|---|---|
| `ranked_households.csv` | Every eligible candidate, rank, cost, estimated benefit, benefit per dollar, and optional realized benefit. |
| `selected_households.csv` | Selected homes for the prioritized strategy and every random run, identified by strategy and seed, with selection order, rank, cost, and benefits. |
| `strategy_results.csv` | One prioritized row and one row per random seed: selected count, spending, unused budget, predicted and optional realized CO2, and benefit per dollar. |
| `summary.json` | Budget, ranking, counts, seeds, input SHA-256, experiment assumptions, aggregate random statistics, gains, and evaluation limitations. |

Empty selections still produce CSV headers and summary statistics. Benefit-per-dollar is blank/`null` when no money was spent, and unavailable realized outcomes are blank/`null`, not zero.

## Verify

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -p test_allocate_solar.py -v
```

Tests cover normalized and raw-benefit ranking, affordable-candidate skipping, cent-exact budgets, random reproducibility, eligibility, realized-outcome isolation, missing/invalid input, empty selections, variable-cost greedy limitations, CLI outputs, and protection against overwriting an existing experiment. Tests use synthetic scores and do not validate a production emissions model.


## Dashboard results page

The dashboard includes **Simulation** and **Allocation results** navigation links. To load a completed experiment, start it with the new optional argument:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_SUBMIT_ARGS='--master local[2] --conf spark.sql.shuffle.partitions=8 pyspark-shell' \
.venv/bin/python scripts/serve_grid_dashboard.py data/processed/grid-import-2018 \
  --allocation-results data/processed/allocation-run-001
```

Open `http://127.0.0.1:8765/allocation`, or select **Allocation results** from the simulation page. Add `--port 8767` if using that port for your local dashboard.

The page shows the budget cap, prioritized spending and installation count, prioritized-versus-random benefit bars, random-run range, comparison table, and experiment assumptions. **Benefit comparison** switches between predicted and held-out realized CO2. If realized outcomes were not supplied, that option is disabled and the page states that the results are estimated-only. Negative gains and zero random means are handled without implying prioritization must win.

**Selected households** can show the prioritized allocation or any saved random seed, with 25 homes per page. Costs and predicted/realized benefits remain distinct. Homes present in the loaded simulation link back to that building's hourly energy view; an unknown ID is displayed without a misleading link. The comparison uses the entire saved allocation period and is independent of the simulation page's selected day.

The results reader loads only `summary.json`, `strategy_results.csv`, and `selected_households.csv` from the configured folder. It verifies budget use, unique run/household records, selection counts, and summed costs and benefits before displaying results. **Refresh results** rereads changed artifacts. This page is read-only; it does not execute a new allocation or modify saved results. Use a different `--allocation-results` folder when starting the dashboard to display another run.

Without a configured folder, the page explains how to load one. **Explore a synthetic example** explicitly opens an in-memory, reproducible example generated by the allocator. A prominent banner labels it as synthetic, including when bookmarked at `/allocation?example=1`. It is never used as a silent fallback for missing or invalid project results. **Back to saved results** returns to the configured run or empty state.

Tests for result loading and pagination:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -p test_allocation_results.py -v
```
