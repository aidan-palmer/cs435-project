# Development Guide

## Branching

Use short-lived feature branches:

```text
main
feature/data-ingestion
feature/random-forest
feature/lstm
feature/solar-simulator
feature/allocation
fix/timestamp-alignment
```

Avoid committing directly to `main` once multiple people are working in parallel.

## Pull Requests

A pull request should answer:

1. What changed?
2. Why was it needed?
3. How was it tested?
4. Does it change data assumptions, units, features, or experiment configuration?
5. What output should reviewers inspect?

Keep PRs focused enough that another team member can review the logic.

## Coding Conventions

### Python

- use clear function/module boundaries;
- use type hints where practical;
- prefer configuration files/arguments over hard-coded paths;
- keep unit conversions in named functions;
- document expected input and output units;
- log row counts around major Spark transformations.

### Spark

- prefer DataFrame operations over collecting large data to the driver;
- avoid unnecessary `collect()` or `toPandas()` calls;
- repartition only with a reason;
- cache/persist only reused expensive intermediates;
- record partition counts for performance experiments;
- use Parquet for processed datasets.

### Notebooks

Notebooks are appropriate for:

- initial exploration;
- checking distributions;
- prototyping plots;
- debugging small samples.

Final preprocessing, training, simulation, and evaluation logic should be moved into `src/` modules or reproducible scripts.

## Testing Strategy

### Unit tests

Good early targets:

- unit conversion;
- timestamp alignment;
- lag creation;
- rolling-window behavior;
- solar/grid-import equations;
- emissions conversion;
- budget allocation.

### Data-quality tests

Examples:

```text
no duplicate (building_id, timestamp)
expected timestamp spacing
non-negative demand
non-negative PV generation
consistent energy units
chronological split ordering
```

### Integration test

Maintain a tiny fixture that can run the entire pipeline quickly:

```text
small demand sample
 -> features
 -> baseline/RF stub or model
 -> solar profile
 -> grid reduction
 -> emissions
 -> allocation
 -> metrics
```

This should be the first thing run after major integration changes.

## Commit Style

Examples:

```text
feat: add ResStock hourly aggregation
feat: implement seasonal naive baseline
fix: prevent future demand leakage in rolling feature
refactor: split solar and emissions simulation
docs: record PVWatts assumptions
```

## Secrets

Never commit:

- API keys;
- tokens;
- private credentials;
- machine-specific secrets.

Use environment variables, for example:

```bash
export PVWATTS_API_KEY="..."
```

Add `.env` to `.gitignore` if a local `.env` file is used.

## Review Checklist

Before merging:

- [ ] code runs from repository-relative paths;
- [ ] no raw/large data accidentally added;
- [ ] units are documented;
- [ ] no test-set leakage introduced;
- [ ] assumptions/config changes are documented;
- [ ] outputs are reproducible from the PR instructions;
- [ ] tests or validation checks pass.
