# Final study protocol and reproduction

## Question and scope

The final study asks whether residential demand forecasts improve simulated solar placement under fixed budgets. Forecasting uses all 573 aligned simulated Larimer County baseline profiles; 382 detached profiles are eligible for installations. Profiles are unweighted simulations, not identifiable Fort Collins properties. The full held-out window is 8 November 2018 00:00 UTC to 1 January 2019 00:00 UTC, end exclusive.

`configs/final-study-v1.json` freezes the experiment. Earlier saved logs and the ten-home pilot remain separate snapshots. They are not pooled with the final aligned results or counted as independent repetitions.

## Full-cohort results

The matched evaluation contains 742,608 test targets for each method. RF with temperature and housing has the lowest observed MAE, 0.403447 kWh; LSTM with temperature has the lowest RMSE, 0.857991 kWh. Demand-only RF scores 0.419719/0.938238 MAE/RMSE, temperature RF 0.406690/0.900184, and demand-only LSTM 0.438829/0.915226. Previous-day and previous-week errors are 0.490481/1.089628 and 0.526289/1.172454. Housing lowers temperature-RF MAE by about 0.80% and RMSE by 0.73% for these validation-selected variants.

At the $54,000 base budget, each prospective method installs five systems. Realized modeled benefit ranges from 2,227.60 to 2,241.94 kg CO2, compared with a paired random mean of 1,274.48 kg. Temperature LSTM is highest in this case; RF with housing has the lowest demand MAE but realizes 2,236.50 kg. The frozen actual-history reference reaches 2,239.57 kg. These closely spaced portfolio totals do not establish that an advanced model reliably beats the simpler historical reference. Complete budget and sensitivity results appear in `outputs/final-study-v1/evaluation/allocation_comparison.csv`.

All 24 fixed-protocol scaling measurements completed. At 573 buildings, median preparation/fit/prediction times are 3.119/12.752/0.242 seconds for RF and 7.329/5.138/0.423 seconds for the one-epoch LSTM. RF fit medians rise from 3.086 seconds at 50 buildings to 12.752 seconds at 573; LSTM fit medians rise from 0.753 to 5.138 seconds. These measurements describe the fixed short benchmark, not the fully tuned study fits or cluster performance. Raw repeats and medians/minima/maxima/IQR are retained in `outputs/final-study-v1/scaling/`.

## Forecasting and housing inputs

Training targets cover 9 January–14 September 2018 (3,424,248 hourly targets / 142,677 sequences). Validation covers 15 September–7 November (742,608 targets / 30,942 sequences); test covers 8 November–31 December (the same counts). All models use the same building/hour keys. Each daily issue observes 168 complete hours and predicts 24 subsequent hours. `forecast_issued_at` labels the start of the last observed hour; availability is one hour later.

The five trained variants are demand-only RF, temperature RF, temperature-plus-housing RF, demand-only LSTM, and temperature LSTM. Previous-day and previous-week baselines use identical test keys. Three RF configurations are selected by validation RMSE. LSTM uses the fixed teammate architecture and validation-loss early stopping, restoring the best weights. Seed 42 is fixed. Test data does not choose settings.

The housing ablation adds static floor area and one-hot heating fuel to temperature RF. It excludes annual energy and outcome fields. All buildings appear in training; static cohort metadata is assumed available before forecasting. LSTM retains history channels without explicit housing inputs, an intentional architecture scope decision disclosed in the paper. Housing differences describe tuned fitted variants, not a causal effect of one attribute.

## One-time installation timing

The decision is 8 November 2018 00:00 UTC. For each selected model, use validation forecasts for the preceding 28 complete days, all available by the decision. Average predicted demand by building and UTC hour of week, then freeze that 168-hour profile throughout evaluation. Actual demand from the test period cannot change the profile. Missing, duplicate, mismatched, or late records are rejected.

The selected models reconstruct prior-period forecasts at the decision using only information from before the cutoff. Historical issue labels identify input windows; they do not claim those final, validation-selected models were deployed live on the earlier dates. Model-selection and installation processing delays are idealized away. This is a model-derived periodic planning proxy, not a direct multi-month forecast. The idealized installation becomes effective immediately at the cutoff; no permitting or construction delay is modeled. Seasonal forecasts undergo the same pre-decision averaging. A frozen mean of actual historical demand is a separate reference. Typical-year PV is assumed known as the installation scenario.

Direct daily test forecasts are also evaluated in a separately labeled retrospective ranking diagnostic. They must not be used to claim installations decided before those forecasts existed. No hindsight-selected portfolio informs a prospective choice; the hindsight portfolio is only an evaluation reference.

## Physical scoring and allocation

Predicted self-consumption is `sum(min(max(predicted_kwh, 0), solar_kwh))`. Realized scoring substitutes held-out simulated demand. A common constant emissions factor converts kWh to kg CO2; excess solar has zero credit. Cost arithmetic uses integer cents. Selection ranks predicted benefit per installation dollar, applies whole-system affordability, and evaluates the chosen portfolio on held-out benefits.

Budgets are $15,000, $20,000, $21,600, $54,000, $108,000, and $216,000. Eight one-assumption-at-a-time cases vary cost, PV capacity, and emissions factor. Positive uniform emissions changes should scale totals without changing rankings. PV cases rescale the fixed 4 kW hourly shape before overlap is recalculated and hold cost fixed; they do not represent physical inverter/clipping reruns or market pricing.

Eight prospective ranking methods and seven retrospective methods yield 720 model/mode/scenario/budget cases. Every case has 100 paired random seeds, 42–141. All models use identical candidates and random controls within the same budget/assumption case. Outputs retain predicted and realized benefits, grid reduction, spending, unused budget, benefit per dollar, random sample SD, and hindsight capture.

## Controlled local scaling

Nested seeded cohorts contain 50, 150, 300, and 573 buildings. Three repeats at each size are run in seeded shuffled order following a ten-building warm-up. Training is fixed to July 2018, validation to 1–7 August, and test to 8–14 August. Historical inputs start 24 June to provide complete windows.

RF is fixed at 50 trees/depth 8; temperature LSTM runs one epoch at batch size 128. This smaller fixed computational protocol is separate from final accuracy tuning. Preparation includes materialized feature/sequence generation and vector/tensor readiness; prediction materializes outputs on prepared inputs. The same warm Spark JVM and OS cache are reused. Medians, IQRs, minima, and maxima describe local measurements. Spark local[2] and TensorFlow CPU thread limits are recorded. This does not establish multi-machine speedup; cluster benchmarking is an explicitly unimplemented extension.

## Environment and commands

Use Python 3.12, Java 17, and `requirements-forecasting.lock.txt`. The tested TensorFlow 2.19 / PyArrow 17 combination avoids the Mac ARM import conflict encountered with newer incompatible wheels. Models and generated datasets are ignored by Git. Place source inputs according to `docs/DATA.md`; preserve their manifests and units.

From the repository root in the pinned environment:

```bash
python scripts/reproduce_final_study.py --stage train --resume
python scripts/reproduce_final_study.py --stage evaluate --resume
python scripts/reproduce_final_study.py --stage scaling --resume
python scripts/reproduce_final_study.py --stage verify --resume
python scripts/build_results_dashboard.py
```

Training defaults to local[2] with 4 GB driver memory and 16 shuffle partitions. TensorFlow uses two intra-operation and one inter-operation thread. For an entirely fresh run, supply the aligned dataset or rebuild it using the shared interval-end preparation, then choose fresh output identifiers before running stages. `--resume` skips only completed artifacts. Do not overwrite a partial run without examining its logs; move to a new run ID when changing settings or inputs.

## Artifact inventory

- `outputs/final-study-v1/{rf-demand,rf-temperature,rf-housing,lstm-demand,lstm-temperature}/`: models, exact test forecasts, validation forecasts and run metrics. Housing vocabulary and LSTM histories/normalizers accompany their respective variants.
- `data/processed/final-study-v1/sequences/`: exact UTC sequence partitions and split metadata.
- `outputs/final-study-v1/evaluation/`: common-target accuracy/breakdowns, prospective and retrospective case folders, assumptions and source hashes.
- `outputs/final-study-v1/scaling/`: raw repeat measurements, summary, input cohorts/hashes, and execution protocol.
- `results/performance/final-*.json`: actual phase times, settings, environment and source references.
- `outputs/final-study-v1/run_sources/`: retained training source preceding a memory-only preparation optimization.
- `outputs/final-study-v1/verification.json` and `reproducibility/`: independent checks and retained code snapshot.
- `data/processed/dashboard-results/`: generated app snapshot and CSVs. The app exposes saved cases without running training on user interactions.

The base commit alone does not identify locally edited executed code. Retain the code snapshot, run-specific source copies, configuration, lock file, input hashes and trained artifacts together. The verification stage reloads every fitted model, reproduces its held-out predictions, recalculates errors from saved predictions, and audits allocation/scaling invariants; it is not a claim of bitwise-identical retraining on a different machine.

## Reporting limits

Report single-seed forecasting differences descriptively, with no significance or population-generalization claim. Distinguish full-year all-profile solar potential from shorter budget portfolios. Report regional simulated scope rather than real parcel eligibility. Cost is a historical 2018 USD scenario; emissions are a constant proxy. Source changes relative to the proposal, the LSTM housing scope decision, local execution, periodic planning proxy, and PV scaling approximation belong in the final paper and application.
