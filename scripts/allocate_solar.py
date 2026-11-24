"""Compare predicted-benefit solar allocation with seeded random selection.

Consume one scored CSV row per household. Costs are USD; avoided CO2 is kg
summed over a common evaluation period. This stage does not infer forecasts,
installation costs, or emissions factors.
"""

import argparse
import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import random
from statistics import mean, stdev


@dataclass(frozen=True)
class Candidate:
    building_id: str
    cost_cents: int
    predicted_co2_kg: Decimal
    realized_co2_kg: Decimal | None = None
    eligible: bool = True

    def __post_init__(self):
        if not isinstance(self.building_id, str) or not self.building_id.strip():
            raise ValueError("building_id must be nonempty text")
        if type(self.cost_cents) is not int or self.cost_cents <= 0:
            raise ValueError("Installation cost must be a positive integer number of cents")
        for value in (self.predicted_co2_kg, self.realized_co2_kg):
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value < 0):
                raise ValueError("CO2 benefits must be finite nonnegative Decimals")
        if self.predicted_co2_kg is None or type(self.eligible) is not bool:
            raise ValueError("Predicted benefit and boolean eligibility are required")

    @property
    def benefit_per_dollar(self):
        return Fraction(self.predicted_co2_kg) * 100 / self.cost_cents


def nonnegative_number(value, name):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a finite nonnegative number") from None
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} must be a finite nonnegative number")
    return number


def money_cents(value, name, *, positive=False):
    cents = Fraction(nonnegative_number(value, name)) * 100
    if cents.denominator != 1:
        raise ValueError(f"{name} must have no fractions of a cent")
    if positive and cents <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return int(cents)


def dollars(cents):
    return f"{cents // 100}.{cents % 100:02d}"


def decimal_text(value):
    return format(value, "f")


def ratio_text(value, cost_cents):
    if not cost_cents:
        return None
    with localcontext() as context:
        context.prec = 28
        return decimal_text(value * 100 / Decimal(cost_cents))


def read_candidates(path):
    required = {"building_id", "installation_cost_usd", "predicted_avoided_co2_kg"}
    result, seen = [], set()
    with Path(path).open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise ValueError("Candidate CSV has duplicate column names")
        if missing := required - set(fields):
            raise ValueError("Missing candidate columns: " + ", ".join(sorted(missing)))
        has_realized = "realized_avoided_co2_kg" in fields
        for line, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Candidate CSV line {line}: wrong number of fields")
            building = row["building_id"].strip()
            if not building or building in seen:
                raise ValueError(f"Candidate CSV line {line}: empty or duplicate building_id")
            seen.add(building)
            eligible = row.get("eligible", "true").strip().lower()
            if eligible not in {"true", "false", "1", "0"}:
                raise ValueError(f"Candidate CSV line {line}: eligible must be true/false or 1/0")
            result.append(Candidate(
                building_id=building,
                cost_cents=money_cents(row["installation_cost_usd"], f"line {line} cost", positive=True),
                predicted_co2_kg=nonnegative_number(row["predicted_avoided_co2_kg"], f"line {line} predicted CO2"),
                realized_co2_kg=(nonnegative_number(row["realized_avoided_co2_kg"],
                                                  f"line {line} realized CO2") if has_realized else None),
                eligible=eligible in {"true", "1"},
            ))
    if not result:
        raise ValueError("Candidate CSV contains no households")
    return result


def rank_candidates(candidates, ranking="benefit_per_dollar"):
    if ranking not in {"benefit_per_dollar", "predicted_benefit"}:
        raise ValueError("Unknown ranking strategy")
    score = (lambda row: row.benefit_per_dollar) if ranking == "benefit_per_dollar" else (
        lambda row: Fraction(row.predicted_co2_kg)
    )
    # Realized outcomes never participate in ranking or tie-breaking.
    return sorted((row for row in candidates if row.eligible),
                  key=lambda row: (-score(row), row.building_id))


def select_with_budget(ordered_candidates, budget_cents):
    if type(budget_cents) is not int or budget_cents < 0:
        raise ValueError("Budget must be a nonnegative integer number of cents")
    selected, remaining, seen = [], budget_cents, set()
    for candidate in ordered_candidates:
        if candidate.building_id in seen:
            raise ValueError("Duplicate candidate in allocation order")
        seen.add(candidate.building_id)
        if candidate.eligible and candidate.cost_cents <= remaining:
            selected.append(candidate)
            remaining -= candidate.cost_cents
    return selected


def selection_metrics(selected, budget_cents, has_realized):
    spent = sum(row.cost_cents for row in selected)
    predicted = sum((row.predicted_co2_kg for row in selected), Decimal(0))
    realized = sum((row.realized_co2_kg for row in selected), Decimal(0)) if has_realized else None
    return {
        "selected_count": len(selected), "total_cost_usd": dollars(spent),
        "unspent_budget_usd": dollars(budget_cents - spent),
        "predicted_avoided_co2_kg": decimal_text(predicted),
        "predicted_co2_kg_per_usd": ratio_text(predicted, spent),
        "realized_avoided_co2_kg": decimal_text(realized) if realized is not None else None,
        "realized_co2_kg_per_usd": ratio_text(realized, spent) if realized is not None else None,
    }


def compare_allocations(candidates, budget_cents, *, ranking="benefit_per_dollar", seed=42, random_runs=100):
    if type(random_runs) is not int or random_runs < 1:
        raise ValueError("random_runs must be at least one")
    if type(seed) is not int:
        raise ValueError("Random seed must be an integer")
    if len({row.building_id for row in candidates}) != len(candidates):
        raise ValueError("Duplicate candidate building_id")
    realized_flags = {row.realized_co2_kg is not None for row in candidates}
    if len(realized_flags) > 1:
        raise ValueError("Realized CO2 must be supplied for every candidate or none")
    has_realized = realized_flags == {True}
    ranked = rank_candidates(candidates, ranking)
    selections = [("prioritized", None, select_with_budget(ranked, budget_cents))]
    # Canonical starting order makes random runs invariant to CSV row order.
    canonical = sorted(ranked, key=lambda row: row.building_id)
    for offset in range(random_runs):
        order = canonical.copy()
        run_seed = seed + offset
        random.Random(run_seed).shuffle(order)
        selections.append(("random", run_seed, select_with_budget(order, budget_cents)))
    results = [{"strategy": strategy, "seed": run_seed,
                **selection_metrics(selected, budget_cents, has_realized)}
               for strategy, run_seed, selected in selections]
    return ranked, selections, results


def read_assumptions(path):
    with Path(path).open(encoding="utf-8") as source:
        assumptions = json.load(source)
    required = {"forecast_run_id", "pv_profile_id", "pv_capacity_kw", "emissions_source",
                "evaluation_start", "evaluation_end"}
    if not isinstance(assumptions, dict) or required - assumptions.keys():
        raise ValueError("Assumptions JSON requires: " + ", ".join(sorted(required)))
    for name in required - {"pv_capacity_kw"}:
        if not isinstance(assumptions[name], str) or not assumptions[name].strip():
            raise ValueError(f"Assumptions {name} must be nonempty text")
    if not nonnegative_number(assumptions["pv_capacity_kw"], "PV capacity"):
        raise ValueError("PV capacity must be greater than zero")
    start, end = (date.fromisoformat(assumptions[name]) for name in ("evaluation_start", "evaluation_end"))
    if start >= end:
        raise ValueError("evaluation_end must be later than evaluation_start (end is exclusive)")
    return {name: str(assumptions[name]) if name == "pv_capacity_kw" else assumptions[name]
            for name in sorted(required)}


def experiment_summary(candidates, results, budget_cents, ranking, seed, assumptions, input_digest):
    prioritized, baseline = results[0], results[1:]
    random_summary = {"runs": len(baseline), "seeds": [row["seed"] for row in baseline]}
    comparison = {}
    for name in ("predicted_avoided_co2_kg", "realized_avoided_co2_kg"):
        if prioritized[name] is None:
            random_summary[name], comparison[name] = None, None
            continue
        values = [Decimal(row[name]) for row in baseline]
        average = sum(values) / len(values)
        gain = Decimal(prioritized[name]) - average
        random_summary[name] = {
            "mean": decimal_text(average), "min": decimal_text(min(values)),
            "max": decimal_text(max(values)),
            "sample_standard_deviation": decimal_text(stdev(values)) if len(values) > 1 else None,
        }
        comparison[name] = {"gain_over_random_mean": decimal_text(gain),
                            "percent_gain_over_random_mean": decimal_text(gain / average * 100) if average else None}
    random_summary["mean_cost_usd"] = decimal_text(mean(Decimal(row["total_cost_usd"]) for row in baseline))
    random_summary["mean_selected_count"] = sum(row["selected_count"] for row in baseline) / len(baseline)
    return {
        "ranking": ranking, "budget_usd": dollars(budget_cents), "base_seed": seed,
        "candidate_count": len(candidates), "eligible_count": sum(row.eligible for row in candidates),
        "input_sha256": input_digest, "assumptions": assumptions,
        "units": {"cost": "USD", "benefit": "kg CO2 over the stated evaluation period"},
        "prioritized": prioritized, "random_baseline": random_summary, "comparison": comparison,
        "evaluation": ("Held-out realized benefits supplied; used only for evaluation."
                       if prioritized["realized_avoided_co2_kg"] is not None
                       else "Estimated benefits only. No held-out realized-benefit comparison is available."),
        "method": "Greedy whole-installation selection; skip unaffordable homes and continue. Not a globally optimal knapsack solver.",
    }


def candidate_row(candidate):
    with localcontext() as context:
        context.prec = 28
        score = Decimal(candidate.benefit_per_dollar.numerator) / Decimal(candidate.benefit_per_dollar.denominator)
    return {"building_id": candidate.building_id, "installation_cost_usd": dollars(candidate.cost_cents),
            "predicted_avoided_co2_kg": decimal_text(candidate.predicted_co2_kg),
            "predicted_co2_kg_per_usd": decimal_text(score),
            "realized_avoided_co2_kg": (decimal_text(candidate.realized_co2_kg)
                                       if candidate.realized_co2_kg is not None else None)}


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def save_experiment(output, candidates, ranked, selections, results, summary):
    # Claim a new destination; never overwrite an existing experiment.
    output.mkdir(parents=True, exist_ok=False)
    fields = list(candidate_row(candidates[0]))
    ranks = {row.building_id: index for index, row in enumerate(ranked, 1)}
    write_csv(output / "ranked_households.csv", ["rank", *fields], (
        {"rank": ranks[row.building_id], **candidate_row(row)} for row in ranked
    ))
    write_csv(output / "selected_households.csv", ["strategy", "seed", "selection_order", "rank", *fields], (
        {"strategy": strategy, "seed": run_seed, "selection_order": index,
         "rank": ranks[row.building_id], **candidate_row(row)}
        for strategy, run_seed, selected in selections for index, row in enumerate(selected, 1)
    ))
    write_csv(output / "strategy_results.csv", list(results[0]), results)
    with (output / "summary.json").open("w", encoding="utf-8") as destination:
        json.dump(summary, destination, indent=2, allow_nan=False)
        destination.write("\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidates", type=Path, help="CSV of scored households; see docs/ALLOCATION.md")
    parser.add_argument("output", type=Path, help="New experiment output directory")
    parser.add_argument("--budget-usd", required=True, help="Budget cap in USD at cent precision")
    parser.add_argument("--assumptions", type=Path, required=True, help="JSON identifying forecast, solar, emissions, and evaluation period")
    parser.add_argument("--ranking", choices=("benefit_per_dollar", "predicted_benefit"), default="benefit_per_dollar")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--random-runs", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        budget = money_cents(args.budget_usd, "Budget")
        assumptions = read_assumptions(args.assumptions)
        candidates = read_candidates(args.candidates)
        ranked, selections, results = compare_allocations(
            candidates, budget, ranking=args.ranking, seed=args.seed, random_runs=args.random_runs
        )
        digest = hashlib.sha256(args.candidates.read_bytes()).hexdigest()
        summary = experiment_summary(candidates, results, budget, args.ranking, args.seed, assumptions, digest)
        save_experiment(args.output, candidates, ranked, selections, results, summary)
    except (OSError, ValueError, InvalidOperation) as error:
        parser.error(str(error))
    print(f"Saved allocation comparison to {args.output}")
    print(f"Prioritized: {results[0]['selected_count']} homes; ${results[0]['total_cost_usd']} spent; "
          f"{results[0]['predicted_avoided_co2_kg']} kg predicted avoided CO2")
    print(f"Random baseline: {args.random_runs} runs under the same ${dollars(budget)} budget cap")


if __name__ == "__main__":
    main()
