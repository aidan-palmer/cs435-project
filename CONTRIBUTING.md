# Contributing

## Before Starting Work

1. Pull the latest `main`.
2. Check existing issues/PRs to avoid duplicate work.
3. Create a feature branch.
4. Confirm any changed assumptions with the team when they affect data, units, forecast targets, emissions, solar capacity, cost, or evaluation.

## Pull Request Expectations

A PR should contain:

- a concise description of the change;
- testing/validation performed;
- affected datasets or assumptions;
- example output when useful;
- documentation updates when behavior changes.

## Project-Specific Rules

- Never commit large raw datasets.
- Never commit API keys.
- Do not use future demand observations to create forecast features.
- Do not imply simulated ResStock buildings correspond to identifiable Fort Collins properties.
- Keep energy and emissions units explicit.
- Prefer reproducible scripts over undocumented manual processing.
- Record random seeds for stochastic experiments.

## Suggested Review Questions

- Is this scientifically consistent with the proposal?
- Can another team member reproduce it?
- Are time boundaries and units clear?
- Could this introduce data leakage?
- Does it change the downstream solar/allocation interpretation?
