"""Retrieve, validate, and cache the Fort Collins PVWatts V8 runs."""

import hashlib
import json
import math
import os
import ssl
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ENDPOINT = "https://developer.nlr.gov/api/pvwatts/v8.json"
FORT_COLLINS_POINTS = ((40.61, -105.10), (40.61, -105.06), (40.57, -105.10), (40.57, -105.06))
SYSTEM = {
    "system_capacity": 4,
    "module_type": 0,
    "array_type": 1,
    "tilt": 20,
    "azimuth": 180,
    "losses": 14.08,
    "dc_ac_ratio": 1.2,
    "inv_eff": 96,
    "bifaciality": 0,
    "use_wf_albedo": 1,
    "dataset": "nsrdb",
    "timeframe": "hourly",
}
HOURS_PER_YEAR = 8760
DEFAULT_CACHE_DIR = Path("data/raw/pvwatts/cache")


def fetch_response(params, api_key):
    if not api_key:
        raise ValueError("Set PVWATTS_API_KEY to download a missing PVWatts response")

    request = Request(f"{ENDPOINT}?{urlencode(params)}")
    request.add_unredirected_header("X-Api-Key", api_key)
    context = ssl.create_default_context()
    system_bundle = Path("/etc/ssl/cert.pem")
    if not context.cert_store_stats()["x509_ca"] and system_bundle.is_file():
        context.load_verify_locations(cafile=system_bundle)
    try:
        with urlopen(request, timeout=60, context=context) as response:
            return json.load(response)
    except HTTPError as error:
        if error.code == 429:
            raise ValueError("PVWatts rate limit reached. Retry later to download missing responses.") from None
        raise ValueError(f"PVWatts request returned HTTP {error.code}") from None
    except (URLError, TimeoutError) as error:
        raise ValueError("Could not reach PVWatts. Check network access and try again.") from error


def validate_response(response):
    if not isinstance(response, dict) or response.get("errors") != []:
        raise ValueError("PVWatts returned an invalid response or reported an API error")
    try:
        station = response["station_info"]
        coordinates = (station["lat"], station["lon"])
        model = (str(response["version"]), station["weather_data_source"])
        watts = response["outputs"]["ac"]
        offset = station["tz"]
    except (KeyError, TypeError) as error:
        raise ValueError("PVWatts response is missing hourly output or weather metadata") from error

    if offset != -7:
        raise ValueError("The Fort Collins PVWatts weather file must use fixed MST (UTC−07:00)")
    if not all(type(value) in (int, float) and math.isfinite(value) for value in coordinates):
        raise ValueError("PVWatts response has invalid weather coordinates")
    if not isinstance(watts, list) or len(watts) != HOURS_PER_YEAR or any(
        type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in watts
    ):
        raise ValueError("PVWatts must supply 8,760 finite, nonnegative hourly AC power values")

    return coordinates, model, watts


def read_cached(path, request):
    with path.open(encoding="utf-8") as source:
        entry = json.load(source)
    if not isinstance(entry, dict) or entry.get("request") != request:
        raise ValueError(f"{path}: cache entry does not match the PVWatts request")
    return validate_response(entry.get("response"))


def load_profile(cache_dir, params, api_key):
    request = {"endpoint": ENDPOINT, "parameters": params}
    key = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
    path = cache_dir / f"{key}.json"
    if path.exists():
        return read_cached(path, request)

    response = fetch_response(params, api_key)
    profile = validate_response(response)
    # The API can echo credentials in its input metadata.
    if isinstance(response.get("inputs"), dict):
        response["inputs"].pop("api_key", None)
    entry = {
        "request": request,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "response": response,
    }

    cache_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=cache_dir) as temporary:
        temp_path = Path(temporary) / "response.json"
        with temp_path.open("w", encoding="utf-8") as output:
            json.dump(entry, output, allow_nan=False)
        # Publish the complete file without replacing another run's snapshot.
        try:
            os.link(temp_path, path)
        except FileExistsError:
            return read_cached(path, request)
    return profile


def get_fort_collins_profiles(cache_dir):
    api_key = os.environ.get("PVWATTS_API_KEY")
    profiles = [
        load_profile(cache_dir, {**SYSTEM, "lat": lat, "lon": lon}, api_key)
        for lat, lon in FORT_COLLINS_POINTS
    ]
    if len({location for location, _, _ in profiles}) != len(profiles):
        raise ValueError("The four PVWatts requests must resolve to distinct weather locations")
    if any(model != profiles[0][1] for _, model, _ in profiles):
        raise ValueError("PVWatts responses must use the same model version and weather dataset")
    return [watts for _, _, watts in profiles]
