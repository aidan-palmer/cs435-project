"""Read-only, paginated views of saved solar allocation experiments."""

import csv
from decimal import Decimal
import json
from pathlib import Path

if __package__:
    from .allocate_solar import (
        Candidate, candidate_row, compare_allocations, dollars, experiment_summary,
        money_cents, nonnegative_number, ratio_text, rank_candidates,
    )
else:
    from allocate_solar import (
        Candidate, candidate_row, compare_allocations, dollars, experiment_summary,
        money_cents, nonnegative_number, ratio_text, rank_candidates,
    )


class AllocationResults:
    FILES = ("summary.json", "strategy_results.csv", "selected_households.csv")

    def __init__(self, directory=None):
        self.directory = Path(directory).resolve() if directory is not None else None
        self._stamp = None
        self._cached = None
        self._demo = None

    @staticmethod
    def _result(row):
        strategy = row.get("strategy")
        if strategy not in {"prioritized", "random"}:
            raise ValueError("Unknown allocation strategy in saved results")
        seed = None if row.get("seed") in (None, "") else int(row["seed"])
        if (strategy == "prioritized") != (seed is None):
            raise ValueError("Only random results must have a seed")
        count = int(row["selected_count"])
        if count < 0:
            raise ValueError("Selected count cannot be negative")
        cost = money_cents(row["total_cost_usd"], "Saved spending")
        unspent = money_cents(row["unspent_budget_usd"], "Saved unused budget")
        predicted = nonnegative_number(row["predicted_avoided_co2_kg"], "Saved predicted CO2")
        realized = (None if row.get("realized_avoided_co2_kg") in (None, "") else
                    nonnegative_number(row["realized_avoided_co2_kg"], "Saved realized CO2"))
        return {"strategy": strategy, "seed": seed, "selected_count": count,
                "total_cost_usd": dollars(cost), "unspent_budget_usd": dollars(unspent),
                "predicted_avoided_co2_kg": str(predicted),
                "realized_avoided_co2_kg": str(realized) if realized is not None else None}

    @classmethod
    def _validate(cls, summary, raw_results, raw_selected):
        required = {"budget_usd", "assumptions", "prioritized", "random_baseline", "eligible_count",
                    "candidate_count", "ranking", "input_sha256", "method"}
        if not isinstance(summary, dict) or required - summary.keys():
            raise ValueError("Saved allocation summary is missing required fields")
        if summary["ranking"] not in {"benefit_per_dollar", "predicted_benefit"}:
            raise ValueError("Unknown saved allocation ranking")
        budget = money_cents(summary["budget_usd"], "Saved budget")
        if not isinstance(summary["assumptions"], dict):
            raise ValueError("Saved experiment assumptions must be an object")
        if {"forecast_run_id", "pv_profile_id", "pv_capacity_kw", "emissions_source", "evaluation_start", "evaluation_end"} - summary["assumptions"].keys():
            raise ValueError("Saved experiment assumptions are incomplete")
        results = [cls._result(row) for row in raw_results]
        by_key = {(row["strategy"], row["seed"]): row for row in results}
        if len(results) != len(by_key) or ("prioritized", None) not in by_key:
            raise ValueError("Saved results require one prioritized result and distinct random seeds")
        random_rows = [row for row in results if row["strategy"] == "random"]
        if not random_rows or len(random_rows) != int(summary["random_baseline"]["runs"]):
            raise ValueError("Saved random-run count does not match summary")
        if cls._result(summary["prioritized"]) != by_key[("prioritized", None)]:
            raise ValueError("Saved prioritized result does not match summary")
        if len({row["realized_avoided_co2_kg"] is None for row in results}) != 1:
            raise ValueError("Saved results have incomplete realized-benefit coverage")
        for row in results:
            cost = money_cents(row["total_cost_usd"], "Saved spending")
            unspent = money_cents(row["unspent_budget_usd"], "Saved unused budget")
            if cost > budget or cost + unspent != budget:
                raise ValueError("Saved result violates the stated budget")
        groups = {key: [] for key in by_key}
        seen = set()
        for raw in raw_selected:
            seed = None if raw.get("seed") in (None, "") else int(raw["seed"])
            key = (raw["strategy"], seed)
            building = raw["building_id"].strip()
            if key not in groups or not building or (*key, building) in seen:
                raise ValueError("Selected homes contain an unknown run or duplicate household")
            seen.add((*key, building))
            cost = money_cents(raw["installation_cost_usd"], "Selected cost", positive=True)
            predicted = nonnegative_number(raw["predicted_avoided_co2_kg"], "Selected predicted CO2")
            realized = (None if raw.get("realized_avoided_co2_kg") in (None, "") else
                        nonnegative_number(raw["realized_avoided_co2_kg"], "Selected realized CO2"))
            rank, order = int(raw["rank"]), int(raw["selection_order"])
            if rank < 1 or order < 1:
                raise ValueError("Saved ranks and selection order must be positive")
            groups[key].append({"building_id": building, "rank": rank, "selection_order": order,
                                "installation_cost_usd": dollars(cost),
                                "predicted_avoided_co2_kg": str(predicted),
                                "predicted_co2_kg_per_usd": ratio_text(predicted, cost),
                                "realized_avoided_co2_kg": str(realized) if realized is not None else None})
        for key, homes in groups.items():
            result = by_key[key]
            if len(homes) != result["selected_count"]:
                raise ValueError("Selected-household count does not match run results")
            if {home["selection_order"] for home in homes} != set(range(1, len(homes) + 1)):
                raise ValueError("Invalid saved selection order")
            if sum(money_cents(home["installation_cost_usd"], "cost") for home in homes) != money_cents(result["total_cost_usd"], "cost"):
                raise ValueError("Selected costs do not match run results")
            for field in ("predicted_avoided_co2_kg", "realized_avoided_co2_kg"):
                if result[field] is None:
                    if any(home[field] is not None for home in homes):
                        raise ValueError("Inconsistent saved realized-benefit coverage")
                elif any(home[field] is None for home in homes) or sum((Decimal(home[field]) for home in homes), Decimal(0)) != Decimal(result[field]):
                    raise ValueError("Selected benefits do not match run results")
            homes.sort(key=lambda home: home["selection_order"])
        return {"summary": summary, "results": results, "groups": groups}

    def _read(self):
        if self.directory is None:
            return None
        try:
            paths = [self.directory / name for name in self.FILES]
            ranking_path = self.directory / "ranked_households.csv"
            if ranking_path.exists():
                paths.append(ranking_path)
            stamp = tuple((path.stat().st_mtime_ns, path.stat().st_size) for path in paths)
            if stamp != self._stamp:
                with paths[0].open(encoding="utf-8") as source:
                    summary = json.load(source)
                tables = []
                for path in paths[1:3]:
                    with path.open(encoding="utf-8-sig", newline="") as source:
                        tables.append(list(csv.DictReader(source)))
                validated = self._validate(summary, *tables)
                validated["ranked"] = None
                if ranking_path.exists():
                    with ranking_path.open(encoding="utf-8-sig", newline="") as source:
                        saved = list(csv.DictReader(source))
                    candidates = [Candidate(
                        raw["building_id"].strip(),
                        money_cents(raw["installation_cost_usd"], "Ranked cost", positive=True),
                        nonnegative_number(raw["predicted_avoided_co2_kg"], "Ranked predicted CO2"),
                        None if raw.get("realized_avoided_co2_kg") in (None, "") else
                        nonnegative_number(raw["realized_avoided_co2_kg"], "Ranked realized CO2"),
                    ) for raw in saved]
                    if len({home.building_id for home in candidates}) != len(candidates):
                        raise ValueError("Duplicate ranked household")
                    ranked = rank_candidates(candidates, summary["ranking"])
                    if len(ranked) != int(summary["eligible_count"]) or len(saved) != len(ranked):
                        raise ValueError("Ranked-household count does not match eligible count")
                    for index, (raw, home) in enumerate(zip(saved, ranked), 1):
                        if int(raw["rank"]) != index or raw["building_id"] != home.building_id:
                            raise ValueError("Ranked households are not in the saved ranking order")
                    normalized = [{"rank": index, **candidate_row(home)}
                                  for index, home in enumerate(ranked, 1)]
                    by_id = {home["building_id"]: home for home in normalized}
                    for homes in validated["groups"].values():
                        for home in homes:
                            if any(home[field] != by_id.get(home["building_id"], {}).get(field)
                                   for field in ("rank", "installation_cost_usd", "predicted_avoided_co2_kg", "realized_avoided_co2_kg")):
                                raise ValueError("Selected household does not match ranked household")
                    validated["ranked"] = normalized
                self._cached = validated
                self._stamp = stamp
            return self._cached
        except (OSError, ValueError, KeyError, TypeError, ArithmeticError) as error:
            raise ValueError(f"Could not load the saved allocation experiment: {error}") from error

    def _example(self):
        if self._demo is None:
            candidates = [Candidate("example-A", 600000, Decimal(1200), Decimal(1000)),
                          Candidate("example-B", 500000, Decimal(900), Decimal(800)),
                          Candidate("example-C", 400000, Decimal(400), Decimal(300))]
            ranked, selections, results = compare_allocations(candidates, 1000000, seed=42, random_runs=100)
            assumptions = {"forecast_run_id": "synthetic-example", "pv_profile_id": "synthetic-example",
                           "pv_capacity_kw": "4", "emissions_source": "Synthetic example only; no real emissions factor",
                           "evaluation_start": "2018-10-01", "evaluation_end": "2019-01-01"}
            summary = experiment_summary(candidates, results, 1000000, "benefit_per_dollar", 42, assumptions, "synthetic-example")
            ranks = {home.building_id: index for index, home in enumerate(ranked, 1)}
            selected = [{"strategy": strategy, "seed": seed, "selection_order": index,
                         "rank": ranks[home.building_id], **candidate_row(home)}
                        for strategy, seed, homes in selections for index, home in enumerate(homes, 1)]
            self._demo = self._validate(summary, results, selected)
            self._demo["ranked"] = [{"rank": index, **candidate_row(home)} for index, home in enumerate(ranked, 1)]
        return self._demo

    def view(self, *, example=False, strategy="prioritized", seed=None, page=1, page_size=25):
        data = self._example() if example else self._read()
        if data is None:
            return {"available": False, "is_example": False}
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("Page size must be between 1 and 100")
        if strategy not in {"prioritized", "random"} or type(page) is not int or page < 1:
            raise ValueError("Choose a valid strategy and page")
        if strategy == "random":
            seed = int(seed) if seed is not None else next(row["seed"] for row in data["results"] if row["strategy"] == "random")
        elif seed is not None:
            raise ValueError("Prioritized selection has no random seed")
        key = (strategy, seed)
        if key not in data["groups"]:
            raise ValueError("That random seed is absent from the saved experiment")
        homes = data["groups"][key]
        pages = max(1, (len(homes) + page_size - 1) // page_size)
        if page > pages:
            raise ValueError("Selected-household page is out of range")
        chosen = {home["building_id"] for home in homes}
        prioritized = {home["building_id"] for home in data["groups"][("prioritized", None)]}
        def annotate(home):
            return {**home, "selected_in_run": home["building_id"] in chosen,
                    "selected_prioritized": home["building_id"] in prioritized}
        ranked = data.get("ranked")
        return {"available": True, "is_example": example, "summary": data["summary"],
                "ranking": {"available": ranked is not None, "total": len(ranked) if ranked is not None else 0,
                            "homes": [annotate(home) for home in (ranked or [])[:20]]},
                "results": data["results"], "selection": {
                    "strategy": strategy, "seed": seed, "page": page, "pages": pages,
                    "total": len(homes), "page_size": page_size,
                    "homes": [annotate(home) for home in homes[(page - 1) * page_size:page * page_size]],
                }}
