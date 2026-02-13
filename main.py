from flask import Flask, request, redirect, render_template, url_for, jsonify
from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
import random
import requests
import time
import re
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from scrapy import Selector
import career_sleeves_v2 as sleeves_v2

# Create an instance of the Flask class
app = Flask(__name__)

# Cache the latest XKCD id to avoid hardcoded limits and repeated API calls
latest_comic_cache = {"id": None, "fetched_at": 0}
CACHE_TTL_SECONDS = 3600
DEFAULT_COMIC_ID = 3000
JOBS_CACHE_TTL_SECONDS = 600
source_cache = {}
source_health = {}
SOURCE_FAILURE_THRESHOLD = 3
SOURCE_FAILURE_COOLDOWN_SECONDS = 900

# Location anchor: Copernicuslaan 105, 's-Hertogenbosch
HOME_LAT = 51.6978
HOME_LON = 5.3037

CITY_COORDS_NL = {
    "s-hertogenbosch": (51.6978, 5.3037),
    "den bosch": (51.6978, 5.3037),
    "amsterdam": (52.3676, 4.9041),
    "rotterdam": (51.9244, 4.4777),
    "utrecht": (52.0907, 5.1214),
    "eindhoven": (51.4416, 5.4697),
    "tilburg": (51.5555, 5.0913),
    "breda": (51.5719, 4.7683),
    "arnhem": (51.9851, 5.8987),
    "nijmegen": (51.8420, 5.8528),
    "groningen": (53.2194, 6.5665),
    "leiden": (52.1601, 4.4970),
    "maastricht": (50.8514, 5.6900),
    "haarlem": (52.3874, 4.6462),
    "delft": (52.0116, 4.3571),
    "enschede": (52.2215, 6.8937),
}

NETHERLANDS_KEYWORDS = [
    "netherlands", "nederland", "dutch", "holland",
    "amsterdam", "rotterdam", "utrecht", "the hague", "den haag",
    "eindhoven", "groningen", "tilburg", "breda", "arnhem",
    "nijmegen", "haarlem", "leiden", "maastricht", "zwolle",
    "almere", "delft", "s-hertogenbosch", "den bosch", "enschede",
]
OTHER_COUNTRY_KEYWORDS = [
    "usa", "united states", "canada", "australia", "new zealand",
    "india", "singapore", "philippines",
    "germany", "deutschland", "berlin", "munich", "frankfurt",
    "belgium", "france", "spain", "portugal", "italy",
    "poland", "romania", "hungary", "czech republic",
    "united kingdom", "uk", "england", "ireland", "sweden",
    "norway", "denmark", "finland", "austria", "switzerland",
]
NON_EU_COUNTRY_KEYWORDS = [
    "usa", "united states", "canada", "australia", "new zealand",
    "india", "singapore", "philippines", "mexico", "brazil",
    "argentina", "chile", "colombia", "japan", "china",
    "hong kong", "south korea", "korea", "uae", "saudi",
    "egypt", "south africa", "nigeria",
]
EU_REMOTE_HINTS = [
    "europe", "european", "eu", "european union", "emea",
    "netherlands", "nederland", "belgium", "france", "spain", "italy",
    "portugal", "ireland", "sweden", "denmark", "finland", "poland",
    "romania", "czech", "austria", "switzerland",
]
GLOBAL_REMOTE_HINTS = ["worldwide", "anywhere", "global"]

REMOTIVE_API = "https://remotive.com/api/remote-jobs"
REMOTEOK_API = "https://remoteok.com/api"
THEMUSE_API = "https://www.themuse.com/api/public/jobs"
THEMUSE_PAGES = 3
ARBEITNOW_API = "https://www.arbeitnow.com/api/job-board-api"
JOBICY_API = "https://jobicy.com/api/v2/remote-jobs"
HIMALAYAS_API = "https://himalayas.app/jobs/api"
ADZUNA_API_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/nl/search/{page}"
SERPAPI_URL = "https://serpapi.com/search.json"
INDEED_SEARCH_URL = "https://www.indeed.com/jobs"
INDEED_SEARCH_URL_NL = "https://nl.indeed.com/jobs"
INDEED_SEARCH_URL_BY_MODE = {
    "nl_only": INDEED_SEARCH_URL_NL,
    "nl_eu": INDEED_SEARCH_URL_NL,
    "global": INDEED_SEARCH_URL,
}
LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
LINKEDIN_GEO_ID_BY_MODE = {
    "nl_only": "102890719",  # Netherlands
}
SERPAPI_MARKET_BY_MODE = {
    "nl_only": {
        "google_domain": "google.nl",
        "gl": "nl",
        "hl": "nl",
    },
    "nl_eu": {
        "google_domain": "google.nl",
        "gl": "nl",
        "hl": "en",
    },
    "global": {
        "google_domain": "google.com",
        "hl": "en",
    },
}
DEFAULT_MAX_PAGES = 8
DEFAULT_TARGET_RAW_PER_SLEEVE = 150
DEFAULT_RATE_LIMIT_RPS = 1.5
DEFAULT_DETAIL_RATE_LIMIT_RPS = 1.2
DEFAULT_HTTP_TIMEOUT = 14
DEFAULT_HTTP_RETRIES = 2
PASS_FALLBACK_MIN_COUNT = 10
DEFAULT_NO_NEW_UNIQUE_PAGES = 2
DEFAULT_DETAIL_FETCH_BASE_BUDGET = 10
DEFAULT_DETAIL_FETCH_REDUCED_BUDGET = 4
SNAPSHOT_DIR = Path(os.getenv("SCRAPE_SNAPSHOT_DIR", "debug_snapshots"))
STATE_DIR = Path(os.getenv("SCRAPE_STATE_DIR", "debug_state"))
QUERY_PERFORMANCE_STATE_PATH = STATE_DIR / "query_performance_state.json"
SEEN_JOBS_STATE_PATH = STATE_DIR / "seen_jobs_state.json"
RUNTIME_CONFIG_PATH = Path(os.getenv("SCRAPE_RUNTIME_CONFIG", "scrape_runtime_config.json"))
DEFAULT_INCREMENTAL_WINDOW_DAYS = 14
MAX_REASON_COUNT = 3
TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "trk",
    "trkEmail",
}
CANONICAL_QUERY_PARAMS = {"jk", "vjk", "jobId", "currentJobId", "id"}
TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}
PRIMARY_DIRECT_SOURCES = {"indeed_web": "Indeed", "linkedin_web": "LinkedIn"}
AUTO_FAILOVER_SOURCES = ["jobicy", "himalayas", "remotive", "remoteok", "serpapi"]


def _normalize_text(*parts):
    return " ".join(str(part or "") for part in parts).lower()


def _infer_work_mode(text):
    if "remote" in text:
        return "Remote"
    if "hybrid" in text:
        return "Hybrid"
    if "on-site" in text or "onsite" in text:
        return "On-site"
    return "Unknown"


def _clean_value(value, fallback="Unknown"):
    if value is None:
        return fallback
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned if cleaned else fallback


def _normalized_location_mode(location_mode):
    mode = _clean_value(location_mode, "nl_only").lower()
    if mode in {"nl_only", "nl_eu", "global"}:
        return mode
    return "nl_only"


def _indeed_search_url_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    return INDEED_SEARCH_URL_BY_MODE.get(mode, INDEED_SEARCH_URL)


def _linkedin_geo_id_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    return LINKEDIN_GEO_ID_BY_MODE.get(mode, "")


def _serpapi_market_params_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    params = SERPAPI_MARKET_BY_MODE.get(mode, {"hl": "en"})
    return dict(params)


def _strip_html(value):
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _clean_value(text, "")


def _now_utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _slugify(value):
    cleaned = re.sub(r"[^\w\-]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "na"


def _ensure_snapshot_dir():
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_state_dir():
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _load_json_file(path, fallback):
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def _save_json_file(path, payload):
    try:
        _ensure_state_dir()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except OSError:
        return False


def _load_runtime_config():
    default_config = {
        "config_version": "1.0",
        "query_overrides": {},
        "threshold_overrides": {
            "min_primary_score": sleeves_v2.MIN_PRIMARY_SLEEVE_SCORE_TO_SHOW,
            "min_total_hits": sleeves_v2.MIN_TOTAL_HITS_TO_SHOW,
            "min_maybe_primary_score": sleeves_v2.MIN_PRIMARY_SLEEVE_SCORE_TO_MAYBE,
            "min_maybe_total_hits": sleeves_v2.MIN_TOTAL_HITS_TO_MAYBE,
        },
        "detail_fetch": {
            "base_budget_per_page": DEFAULT_DETAIL_FETCH_BASE_BUDGET,
            "reduced_budget_per_page": DEFAULT_DETAIL_FETCH_REDUCED_BUDGET,
        },
        "crawl": {
            "no_new_unique_pages_stop": DEFAULT_NO_NEW_UNIQUE_PAGES,
        },
        "query_performance": {
            "min_runs_before_prune": 3,
            "min_avg_parsed_per_page": 0.5,
            "min_queries_to_keep": 8,
        },
    }
    loaded = _load_json_file(RUNTIME_CONFIG_PATH, {})
    if not isinstance(loaded, dict):
        return default_config
    merged = dict(default_config)
    for key in ("query_overrides", "threshold_overrides", "detail_fetch", "crawl", "query_performance"):
        incoming = loaded.get(key)
        if isinstance(incoming, dict):
            merged[key] = {**default_config.get(key, {}), **incoming}
    if loaded.get("config_version"):
        merged["config_version"] = str(loaded["config_version"])
    return merged


RUNTIME_CONFIG = _load_runtime_config()


def _log_event(event, **payload):
    record = {
        "ts": _now_utc_stamp(),
        "event": event,
        **payload,
    }
    print(json.dumps(record, ensure_ascii=False, default=str))


def _save_debug_event(source, query, page, reason, diagnostics, **metadata):
    _ensure_snapshot_dir()
    timestamp = _now_utc_stamp()
    filename = (
        f"{timestamp}_{_slugify(source)}_{_slugify(query)[:60]}_p{int(page)}_"
        f"{_slugify(reason)[:30]}.json"
    )
    path = SNAPSHOT_DIR / filename
    payload = {
        "timestamp": timestamp,
        "source": source,
        "query": query,
        "page": int(page),
        "reason": reason,
        "metadata": metadata,
    }
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        diagnostics.setdefault("snapshots", []).append(
            {
                "source": source,
                "query": query,
                "page": int(page),
                "reason": reason,
                "path": str(path),
            }
        )
        return str(path)
    except OSError:
        return ""


def _save_html_snapshot(source, query, page, html_text, reason, diagnostics):
    if not html_text:
        return _save_debug_event(
            source,
            query,
            page,
            reason,
            diagnostics,
            note="html_missing",
        )
    try:
        _ensure_snapshot_dir()
        timestamp = _now_utc_stamp()
        filename = (
            f"{timestamp}_{_slugify(source)}_{_slugify(query)[:60]}_p{int(page)}_"
            f"{_slugify(reason)[:30]}.html"
        )
        path = SNAPSHOT_DIR / filename
        path.write_text(html_text, encoding="utf-8", errors="ignore")
        snapshot_path = str(path)
        diagnostics.setdefault("snapshots", []).append(
            {
                "source": source,
                "query": query,
                "page": int(page),
                "reason": reason,
                "path": snapshot_path,
            }
        )
        return snapshot_path
    except OSError:
        return ""


def _canonicalize_url(url):
    raw_url = _clean_value(url, "")
    if not raw_url:
        return ""
    parsed = urlparse(raw_url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_norm = key.strip()
        if key_norm in TRACKING_QUERY_PARAMS:
            continue
        if key_norm in CANONICAL_QUERY_PARAMS:
            query_pairs.append((key_norm, value))
    canonical_query = urlencode(sorted(query_pairs))
    return urlunparse((scheme, netloc, path.rstrip("/"), "", canonical_query, ""))


def _extract_job_id_from_url(url):
    parsed = urlparse(_clean_value(url, ""))
    for key, value in parse_qsl(parsed.query):
        if key in CANONICAL_QUERY_PARAMS and value:
            return value
    path_parts = [part for part in (parsed.path or "").split("/") if part]
    for part in reversed(path_parts):
        if re.search(r"\d{5,}", part):
            return part
    return ""


def _build_dedupe_key(item):
    title_key = sleeves_v2.normalize_for_match(item.get("title"))
    company_key = sleeves_v2.normalize_for_match(item.get("company"))
    canonical_url = _canonicalize_url(item.get("link") or item.get("url"))
    job_id = _clean_value(item.get("job_id"), "") or _extract_job_id_from_url(canonical_url)
    anchor = job_id or canonical_url
    if not anchor:
        location_key = sleeves_v2.normalize_for_match(item.get("location"))
        date_key = sleeves_v2.normalize_for_match(item.get("date") or item.get("date_posted"))
        anchor = f"{location_key}|{date_key}"
    return (title_key, company_key, anchor), canonical_url, job_id


def _count_unique_items(items):
    seen = set()
    for item in items:
        dedupe_key, _, _ = _build_dedupe_key(item)
        seen.add(dedupe_key)
    return len(seen)


def _load_query_performance_state():
    payload = _load_json_file(QUERY_PERFORMANCE_STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _save_query_performance_state(state):
    return _save_json_file(QUERY_PERFORMANCE_STATE_PATH, state)


def _prioritize_queries(sleeve_key, queries):
    state = _load_query_performance_state()
    sleeve_state = state.get((sleeve_key or "").upper(), {})
    qp_cfg = RUNTIME_CONFIG.get("query_performance", {})
    min_runs = int(qp_cfg.get("min_runs_before_prune", 3))
    min_avg = float(qp_cfg.get("min_avg_parsed_per_page", 0.5))
    min_keep = int(qp_cfg.get("min_queries_to_keep", 8))

    scored = []
    for query in queries:
        q_state = sleeve_state.get(query, {})
        runs = int(q_state.get("runs", 0))
        parsed_total = int(q_state.get("parsed_total", 0))
        pages_total = int(q_state.get("pages_total", 0))
        avg = (parsed_total / pages_total) if pages_total else 0.0
        scored.append((query, runs, avg))

    scored.sort(key=lambda item: (item[2], item[1]), reverse=True)
    kept = []
    for query, runs, avg in scored:
        if runs >= min_runs and avg < min_avg and len(scored) > min_keep:
            continue
        kept.append(query)
    if len(kept) < min_keep:
        kept = [query for query, _, _ in scored[: max(min_keep, len(scored))]]
    return kept or queries


def _update_query_performance_from_diagnostics(diagnostics, sleeve_key):
    state = _load_query_performance_state()
    sleeve = (sleeve_key or "").upper()
    sleeve_state = state.setdefault(sleeve, {})
    for entry in (diagnostics.get("source_query_summary") or {}).values():
        query = _clean_value(entry.get("query"), "")
        if not query:
            continue
        q_state = sleeve_state.setdefault(
            query,
            {
                "runs": 0,
                "parsed_total": 0,
                "pages_total": 0,
                "last_updated": "",
            },
        )
        q_state["runs"] = int(q_state.get("runs", 0)) + 1
        q_state["parsed_total"] = int(q_state.get("parsed_total", 0)) + int(entry.get("parsed_count", 0))
        q_state["pages_total"] = int(q_state.get("pages_total", 0)) + int(entry.get("pages_attempted", 0))
        q_state["last_updated"] = _now_utc_stamp()
    _save_query_performance_state(state)


def _load_seen_jobs_state():
    payload = _load_json_file(SEEN_JOBS_STATE_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _save_seen_jobs_state(state):
    return _save_json_file(SEEN_JOBS_STATE_PATH, state)


def _prune_seen_jobs_state(state, window_days):
    now = datetime.now(timezone.utc)
    max_age_seconds = max(1, int(window_days)) * 86400
    pruned = {}
    for key, value in state.items():
        last_seen = value.get("last_seen") if isinstance(value, dict) else ""
        try:
            parsed = datetime.fromisoformat(str(last_seen))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if (now - parsed).total_seconds() <= max_age_seconds:
            pruned[key] = value
    return pruned


def _seen_key_for_job(job):
    dedupe_key, _, _ = _build_dedupe_key(job)
    return "|".join(dedupe_key)


def _apply_incremental_filter(jobs, window_days):
    state = _load_seen_jobs_state()
    state = _prune_seen_jobs_state(state, window_days)
    now_iso = datetime.now(timezone.utc).isoformat()
    fresh = []
    skipped = 0
    for job in jobs:
        key = _seen_key_for_job(job)
        if key in state:
            skipped += 1
            state[key]["last_seen"] = now_iso
            continue
        fresh.append(job)
        state[key] = {"first_seen": now_iso, "last_seen": now_iso}
    _save_seen_jobs_state(state)
    return fresh, skipped


def _new_diagnostics():
    return {
        "run_id": "",
        "source_query_pages": [],
        "source_query_summary": {},
        "blocked_detected": {},
        "snapshots": [],
        "funnel": {},
        "fallbacks_applied": [],
        "fail_reasons": {},
        "dedupe_ratio_by_source": {},
        "auto_failover": [],
        "config_version": RUNTIME_CONFIG.get("config_version", "1.0"),
    }


def _log_page_metrics(diagnostics, **kwargs):
    diagnostics.setdefault("source_query_pages", []).append(kwargs)
    source = kwargs.get("source", "unknown")
    query = kwargs.get("query", "")
    location = kwargs.get("location", "")
    summary_key = f"{source}|{query}|{location}"
    summary = diagnostics.setdefault("source_query_summary", {}).setdefault(
        summary_key,
        {
            "source": source,
            "query": query,
            "location": location,
            "pages_attempted": 0,
            "raw_count": 0,
            "parsed_count": 0,
            "new_unique_count": 0,
            "detailpages_fetched": 0,
            "full_description_count": 0,
            "error_count": 0,
            "blocked_detected": False,
        },
    )
    summary["pages_attempted"] += 1
    summary["raw_count"] += int(kwargs.get("cards_found", 0))
    summary["parsed_count"] += int(kwargs.get("parsed_count", 0))
    summary["new_unique_count"] += int(kwargs.get("new_unique_count", 0))
    summary["detailpages_fetched"] += int(kwargs.get("detailpages_fetched", 0))
    summary["full_description_count"] += int(kwargs.get("full_description_count", 0))
    summary["error_count"] += int(kwargs.get("error_count", 0))
    summary["blocked_detected"] = bool(summary["blocked_detected"] or kwargs.get("blocked_detected"))


def _record_blocked(diagnostics, source):
    diagnostics.setdefault("blocked_detected", {})[source] = True


def _rate_limited_get(
    session,
    url,
    params,
    headers,
    domain_state,
    requests_per_second,
    timeout_seconds,
    max_retries,
):
    parsed = urlparse(url)
    domain = parsed.netloc.lower() or "unknown"
    min_interval = 0
    if requests_per_second and requests_per_second > 0:
        min_interval = 1.0 / requests_per_second
    last_seen = domain_state.get(domain, 0.0)
    wait_for = min_interval - (time.time() - last_seen)
    if wait_for > 0:
        time.sleep(wait_for)

    last_exc = ""
    for attempt in range(max_retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout_seconds,
            )
            domain_state[domain] = time.time()
            if response.status_code in TRANSIENT_HTTP_STATUSES and attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            return response, ""
        except requests.RequestException as exc:
            last_exc = str(exc)
            if attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            break
    return None, last_exc or "request_failed"


def _haversine_km(lat1, lon1, lat2, lon2):
    radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return radius_km * c


def _estimate_distance_km(location_text):
    text = _normalize_text(location_text)
    for city, (lat, lon) in CITY_COORDS_NL.items():
        if city in text:
            return round(_haversine_km(HOME_LAT, HOME_LON, lat, lon), 1), city
    return None, None


def _passes_location_gate(text, location_mode):
    if location_mode == "global":
        return True

    has_non_eu_hint = any(keyword in text for keyword in NON_EU_COUNTRY_KEYWORDS)
    is_netherlands = _is_netherlands_job(text)
    if location_mode == "nl_only":
        return is_netherlands and not has_non_eu_hint

    if is_netherlands:
        return True

    is_remote_or_hybrid = "remote" in text or "hybrid" in text
    has_eu_hint = any(keyword in text for keyword in EU_REMOTE_HINTS)
    has_global_remote_hint = any(keyword in text for keyword in GLOBAL_REMOTE_HINTS)

    if not is_remote_or_hybrid:
        return False

    if location_mode == "nl_eu":
        return (has_eu_hint or has_global_remote_hint) and not has_non_eu_hint

    return False


def _is_netherlands_job(*parts):
    text = _normalize_text(*parts)
    has_nl_signal = any(keyword in text for keyword in NETHERLANDS_KEYWORDS)
    has_other_country_signal = any(keyword in text for keyword in OTHER_COUNTRY_KEYWORDS)
    has_nl_code = bool(re.search(r"\b(nl|netherlands)\b", text))

    if has_nl_signal or has_nl_code:
        return True
    if has_other_country_signal:
        return False
    return False


def _location_passes_for_mode(location_mode):
    pass_ids = sleeves_v2.LOCATION_MODE_PASSES.get(location_mode, ["nl"])
    locations = []
    for pass_id in pass_ids:
        locations.extend(sleeves_v2.SEARCH_LOCATIONS.get(pass_id, []))
    return locations or ["Netherlands"]


def _query_bundle_for_sleeve(sleeve_key):
    sleeve = (sleeve_key or "").upper()
    overrides = (RUNTIME_CONFIG.get("query_overrides") or {}).get(sleeve, [])
    terms = overrides if isinstance(overrides, list) and overrides else sleeves_v2.SLEEVE_SEARCH_TERMS.get(sleeve, [])
    ordered_terms = [term for term in terms if term]
    return _prioritize_queries(sleeve, ordered_terms)


def _source_headers(source_name, location_mode="nl_only"):
    mode = _normalized_location_mode(location_mode)
    if mode in {"nl_only", "nl_eu"}:
        accept_language = "nl-NL,nl;q=0.9,en-US;q=0.7,en;q=0.6"
        referer = "https://www.google.nl/"
    else:
        accept_language = "en-US,en;q=0.9,nl;q=0.8"
        referer = "https://www.google.com/"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": accept_language,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer,
        "X-Source": source_name,
    }


def _compact_whitespace(values):
    if isinstance(values, str):
        values = [values]
    return _clean_value(" ".join(str(value or "") for value in values), "")


def _parse_indeed_cards(selector, response_url):
    cards = selector.css(
        "div.job_seen_beacon, div.slider_container, div[data-testid='jobSeenBeacon']"
    )
    parsed = []
    for card in cards:
        link = (
            card.css("a.jcs-JobTitle::attr(href)").get()
            or card.css("h2.jobTitle a::attr(href)").get()
            or card.css("a[data-jk]::attr(href)").get()
        )
        snippet_parts = card.css("div.job-snippet *::text, [data-testid='text-snippet'] *::text").getall()
        parsed_item = {
            "title": _clean_value(
                card.css("a.jcs-JobTitle span::text").get()
                or card.css("h2.jobTitle span::text").get()
                or card.css("h2 a::attr(aria-label)").get(),
                "",
            ),
            "company": _clean_value(
                card.css("[data-testid='company-name']::text").get()
                or card.css("span.companyName::text").get(),
                "",
            ),
            "location": _clean_value(
                card.css("[data-testid='text-location']::text").get()
                or card.css("div.companyLocation::text").get(),
                "",
            ),
            "link": requests.compat.urljoin(response_url, link) if link else "",
            "snippet": _compact_whitespace(snippet_parts),
            "date": _clean_value(
                card.css("span.date::text").get()
                or card.css("span[data-testid='myJobsStateDate']::text").get(),
                "Unknown",
            ),
            "source": "Indeed",
        }
        if parsed_item["title"] or parsed_item["link"]:
            parsed.append(parsed_item)
    return cards, parsed


def _parse_linkedin_cards(selector):
    cards = selector.css("li.base-card, div.base-card, div.result-card")
    parsed = []
    for card in cards:
        metadata = card.css(
            "div.base-search-card__metadata *::text, "
            "ul.job-search-card__job-insight *::text, "
            "div.result-card__meta *::text"
        ).getall()
        parsed_item = {
            "title": _clean_value(
                card.css("h3.base-search-card__title::text").get()
                or card.css("h3.result-card__title::text").get(),
                "",
            ),
            "company": _clean_value(
                card.css("h4.base-search-card__subtitle::text").get()
                or card.css("h4.result-card__subtitle::text").get(),
                "",
            ),
            "location": _clean_value(
                card.css("span.job-search-card__location::text").get()
                or card.css("span.job-result-card__location::text").get(),
                "",
            ),
            "link": _clean_value(
                card.css("a.base-card__full-link::attr(href)").get()
                or card.css("a.result-card__full-card-link::attr(href)").get(),
                "",
            ),
            "snippet": _compact_whitespace(metadata),
            "date": _clean_value(
                card.css("time::attr(datetime)").get() or card.css("time::text").get(),
                "Unknown",
            ),
            "source": "LinkedIn",
        }
        if parsed_item["title"] and parsed_item["link"]:
            parsed.append(parsed_item)
    return cards, parsed


def _fetch_detail_page_text(
    session,
    url,
    source_name,
    diagnostics,
    domain_state,
    detail_rps,
    location_mode="nl_only",
):
    link = _clean_value(url, "")
    if not link:
        return "", True, 0

    response, error = _rate_limited_get(
        session,
        link,
        params=None,
        headers=_source_headers(source_name, location_mode),
        domain_state=domain_state,
        requests_per_second=detail_rps,
        timeout_seconds=DEFAULT_HTTP_TIMEOUT,
        max_retries=DEFAULT_HTTP_RETRIES,
    )
    error_count = 1 if error else 0
    if error or response is None:
        return "", True, error_count

    blocked = response.status_code in {401, 403, 429} or sleeves_v2.detect_blocked_html(response.text)
    if blocked:
        _record_blocked(diagnostics, source_name)
        _save_html_snapshot(
            source_name,
            "detail-page",
            0,
            response.text,
            "blocked",
            diagnostics,
        )
        return "", True, error_count + 1

    selector = Selector(text=response.text)
    if source_name == "Indeed":
        chunks = selector.css(
            "#jobDescriptionText *::text, "
            "div#jobDescriptionText *::text, "
            "div.jobsearch-jobDescriptionText *::text"
        ).getall()
    else:
        chunks = selector.css(
            "div.show-more-less-html__markup *::text, "
            "div.description__text *::text, "
            "div.core-section-container__content *::text"
        ).getall()
    description = _compact_whitespace(chunks)
    if not description:
        return "", True, error_count
    return description, False, error_count


def _fetch_indeed_jobs_direct(
    sleeve_key,
    location_mode="nl_only",
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    diagnostics=None,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
):
    diagnostics = diagnostics or _new_diagnostics()
    search_url = _indeed_search_url_for_mode(location_mode)
    jobs = []
    seen_unique = set()
    domain_state = {}
    queries = _query_bundle_for_sleeve(sleeve_key)
    locations = _location_passes_for_mode(location_mode)
    session = requests.Session()
    detail_base_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("base_budget_per_page", DEFAULT_DETAIL_FETCH_BASE_BUDGET))
    detail_reduced_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("reduced_budget_per_page", DEFAULT_DETAIL_FETCH_REDUCED_BUDGET))
    detail_failures = 0
    detail_attempts = 0

    for query in queries:
        for location in locations:
            previous_jobs = len(jobs)
            no_new_unique_streak = 0
            last_response_body = ""
            for page_idx in range(max_pages):
                start = page_idx * 10
                params = {"q": query, "l": location, "start": start}
                response, error = _rate_limited_get(
                    session,
                    search_url,
                    params=params,
                    headers=_source_headers("Indeed", location_mode),
                    domain_state=domain_state,
                    requests_per_second=requests_per_second,
                    timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                    max_retries=DEFAULT_HTTP_RETRIES,
                )
                status = response.status_code if response is not None else 0
                body = response.text if response is not None else ""
                if body:
                    last_response_body = body
                blocked = bool(
                    status in {401, 403, 429}
                    or sleeves_v2.detect_blocked_html(body)
                )
                if blocked:
                    _record_blocked(diagnostics, "Indeed")

                cards_found = 0
                parsed_count = 0
                new_unique_count = 0
                detailpages_fetched = 0
                full_description_count = 0
                error_count = 1 if error else 0
                parsed_items = []
                request_url = response.url if response is not None else search_url

                if response is not None and response.ok and not blocked:
                    selector = Selector(text=body)
                    cards, parsed_items = _parse_indeed_cards(selector, request_url)
                    cards_found = len(cards)
                    parsed_count = len(parsed_items)

                    if cards_found == 0 or parsed_count == 0:
                        _save_html_snapshot(
                            "Indeed",
                            query,
                            page_idx + 1,
                            body,
                            "parse-empty",
                            diagnostics,
                        )

                    fail_rate = (detail_failures / detail_attempts) if detail_attempts else 0.0
                    detail_budget = min(detail_base_budget, len(parsed_items))
                    if blocked or fail_rate > 0.5:
                        detail_budget = min(detail_reduced_budget, len(parsed_items))

                    for idx, item in enumerate(parsed_items):
                        dedupe_key, _, _ = _build_dedupe_key(item)
                        if dedupe_key in seen_unique:
                            continue
                        seen_unique.add(dedupe_key)
                        new_unique_count += 1

                        full_description = ""
                        detail_failed = True
                        detail_errors = 0
                        if idx < detail_budget:
                            full_description, detail_failed, detail_errors = _fetch_detail_page_text(
                                session,
                                item.get("link"),
                                "Indeed",
                                diagnostics,
                                domain_state,
                                detail_rps,
                                location_mode,
                            )
                            detail_attempts += 1
                            if detail_failed:
                                detail_failures += 1
                            detailpages_fetched += 1
                            error_count += detail_errors
                            if full_description:
                                full_description_count += 1

                        item["full_description"] = full_description
                        item["detail_fetch_failed"] = bool(detail_failed)
                        item["query"] = query
                        item["query_location"] = location
                        item["source"] = "Indeed"
                        jobs.append(item)
                else:
                    if response is not None:
                        _save_html_snapshot(
                            "Indeed",
                            query,
                            page_idx + 1,
                            body,
                            f"status-{status}",
                            diagnostics,
                        )

                _log_page_metrics(
                    diagnostics,
                    source="Indeed",
                    query=query,
                    location=location,
                    page=page_idx + 1,
                    url=request_url,
                    status=status,
                    cards_found=cards_found,
                    parsed_count=parsed_count,
                    new_unique_count=new_unique_count,
                    detailpages_fetched=detailpages_fetched,
                    full_description_count=full_description_count,
                    error_count=error_count,
                    blocked_detected=blocked,
                )
                if new_unique_count == 0:
                    no_new_unique_streak += 1
                else:
                    no_new_unique_streak = 0
                if cards_found == 0 or no_new_unique_streak >= max(1, int(no_new_unique_pages)):
                    break
                if len(seen_unique) >= target_raw:
                    return jobs, diagnostics
            if len(jobs) == previous_jobs:
                _save_debug_event(
                    "Indeed",
                    query,
                    0,
                    "no-new-items",
                    diagnostics,
                    location=location,
                    pages_attempted=max_pages,
                    unique_items=len(seen_unique),
                )
                if last_response_body:
                    _save_html_snapshot(
                        "Indeed",
                        query,
                        0,
                        last_response_body,
                        "no-new-items",
                        diagnostics,
                    )
    return jobs, diagnostics


def _fetch_linkedin_jobs_direct(
    sleeve_key,
    location_mode="nl_only",
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    diagnostics=None,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
):
    diagnostics = diagnostics or _new_diagnostics()
    jobs = []
    seen_unique = set()
    domain_state = {}
    geo_id = _linkedin_geo_id_for_mode(location_mode)
    queries = _query_bundle_for_sleeve(sleeve_key)
    locations = _location_passes_for_mode(location_mode)
    session = requests.Session()
    detail_base_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("base_budget_per_page", DEFAULT_DETAIL_FETCH_BASE_BUDGET))
    detail_reduced_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("reduced_budget_per_page", DEFAULT_DETAIL_FETCH_REDUCED_BUDGET))
    detail_failures = 0
    detail_attempts = 0

    for query in queries:
        for location in locations:
            no_new_unique_streak = 0
            last_response_body = ""
            for page_idx in range(max_pages):
                start = page_idx * 25
                params = {"keywords": query, "location": location, "start": start}
                if geo_id:
                    params["geoId"] = geo_id
                response, error = _rate_limited_get(
                    session,
                    LINKEDIN_SEARCH_URL,
                    params=params,
                    headers=_source_headers("LinkedIn", location_mode),
                    domain_state=domain_state,
                    requests_per_second=requests_per_second,
                    timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                    max_retries=DEFAULT_HTTP_RETRIES,
                )
                status = response.status_code if response is not None else 0
                body = response.text if response is not None else ""
                if body:
                    last_response_body = body
                blocked = bool(
                    status in {401, 403, 429}
                    or sleeves_v2.detect_blocked_html(body)
                )
                if blocked:
                    _record_blocked(diagnostics, "LinkedIn")

                cards_found = 0
                parsed_count = 0
                new_unique_count = 0
                detailpages_fetched = 0
                full_description_count = 0
                error_count = 1 if error else 0
                parsed_items = []
                request_url = response.url if response is not None else LINKEDIN_SEARCH_URL

                if response is not None and response.ok and not blocked:
                    selector = Selector(text=body)
                    cards, parsed_items = _parse_linkedin_cards(selector)
                    cards_found = len(cards)
                    parsed_count = len(parsed_items)
                    if cards_found == 0 or parsed_count == 0:
                        _save_html_snapshot(
                            "LinkedIn",
                            query,
                            page_idx + 1,
                            body,
                            "parse-empty",
                            diagnostics,
                        )

                    fail_rate = (detail_failures / detail_attempts) if detail_attempts else 0.0
                    detail_budget = min(detail_base_budget, len(parsed_items))
                    if blocked or fail_rate > 0.5:
                        detail_budget = min(detail_reduced_budget, len(parsed_items))

                    for idx, item in enumerate(parsed_items):
                        dedupe_key, _, _ = _build_dedupe_key(item)
                        if dedupe_key in seen_unique:
                            continue
                        seen_unique.add(dedupe_key)
                        new_unique_count += 1

                        full_description = ""
                        detail_failed = True
                        detail_errors = 0
                        if idx < detail_budget:
                            full_description, detail_failed, detail_errors = _fetch_detail_page_text(
                                session,
                                item.get("link"),
                                "LinkedIn",
                                diagnostics,
                                domain_state,
                                detail_rps,
                                location_mode,
                            )
                            detail_attempts += 1
                            if detail_failed:
                                detail_failures += 1
                            detailpages_fetched += 1
                            error_count += detail_errors
                            if full_description:
                                full_description_count += 1

                        item["full_description"] = full_description
                        item["detail_fetch_failed"] = bool(detail_failed)
                        item["query"] = query
                        item["query_location"] = location
                        item["source"] = "LinkedIn"
                        jobs.append(item)
                else:
                    if response is not None:
                        _save_html_snapshot(
                            "LinkedIn",
                            query,
                            page_idx + 1,
                            body,
                            f"status-{status}",
                            diagnostics,
                        )

                _log_page_metrics(
                    diagnostics,
                    source="LinkedIn",
                    query=query,
                    location=location,
                    page=page_idx + 1,
                    url=request_url,
                    status=status,
                    cards_found=cards_found,
                    parsed_count=parsed_count,
                    new_unique_count=new_unique_count,
                    detailpages_fetched=detailpages_fetched,
                    full_description_count=full_description_count,
                    error_count=error_count,
                    blocked_detected=blocked,
                )
                if new_unique_count == 0:
                    no_new_unique_streak += 1
                else:
                    no_new_unique_streak = 0
                if cards_found == 0 or no_new_unique_streak >= max(1, int(no_new_unique_pages)):
                    break
                if len(seen_unique) >= target_raw:
                    return jobs, diagnostics
            if no_new_unique_streak >= max(1, int(no_new_unique_pages)):
                _save_debug_event(
                    "LinkedIn",
                    query,
                    0,
                    "no-new-items",
                    diagnostics,
                    location=location,
                    pages_attempted=max_pages,
                    unique_items=len(seen_unique),
                )
                if last_response_body:
                    _save_html_snapshot(
                        "LinkedIn",
                        query,
                        0,
                        last_response_body,
                        "no-new-items",
                        diagnostics,
                    )
    return jobs, diagnostics


def rank_and_filter_jobs(
    items,
    target_sleeve=None,
    min_target_score=4,
    location_mode="nl_only",
    strict_sleeve=True,
    include_fail=False,
    return_diagnostics=False,
    diagnostics=None,
):
    diagnostics = diagnostics or _new_diagnostics()
    scored_jobs = []
    dedupe_seen = set()
    raw_by_source = Counter()
    kept_by_source = Counter()

    for job in items:
        source = _clean_value(job.get("source"), "unknown")
        raw_by_source[source] += 1
        dedupe_key, canonical_url, job_id = _build_dedupe_key(job)
        if dedupe_key in dedupe_seen:
            continue
        dedupe_seen.add(dedupe_key)
        kept_by_source[source] += 1

        title = _clean_value(job.get("title"), "")
        company = _clean_value(job.get("company"), "")
        location = _clean_value(job.get("location"), "")
        snippet = _clean_value(job.get("snippet"), "")
        full_description = _clean_value(job.get("full_description"), "")
        link = _clean_value(job.get("link") or job.get("url"), "")
        if canonical_url:
            link = canonical_url
        date_posted = _clean_value(job.get("date") or job.get("date_posted"), "Unknown")
        salary = _clean_value(job.get("salary"), "Not listed")

        raw_text = _clean_value(
            " ".join(
                [
                    title,
                    company,
                    location,
                    snippet,
                    full_description,
                ]
            ),
            "",
        )
        prepared_text = sleeves_v2.prepare_text(raw_text)
        title_text = _normalize_text(title)

        language_flags, language_notes = sleeves_v2.detect_language_flags(raw_text)
        sleeve_scores, sleeve_details = sleeves_v2.score_all_sleeves(raw_text, title_text)
        primary_sleeve, natural_primary_score = max(
            sleeve_scores.items(),
            key=lambda pair: pair[1],
        )

        scoring_sleeve = target_sleeve if target_sleeve in sleeves_v2.VALID_SLEEVES else primary_sleeve
        primary_score = sleeve_scores.get(scoring_sleeve, natural_primary_score)
        primary_sleeve_details = sleeve_details.get(scoring_sleeve, {})
        total_positive_hits = int(primary_sleeve_details.get("total_positive_hits", 0))

        hard_reject_reason = sleeves_v2.detect_hard_reject(title, raw_text)
        abroad_score, abroad_badges, _ = sleeves_v2.score_abroad(raw_text)
        synergy_score, synergy_hits = sleeves_v2.score_synergy(raw_text)
        penalty_points, penalty_reasons = sleeves_v2.evaluate_soft_penalties(raw_text)

        weighted_score = (
            (abroad_score * sleeves_v2.RANKING_WEIGHTS["abroad_score"])
            + (primary_score * sleeves_v2.RANKING_WEIGHTS["primary_sleeve_score"])
            + (synergy_score * sleeves_v2.RANKING_WEIGHTS["synergy_score"])
        )
        location_gate_match = _passes_location_gate(raw_text, location_mode)
        location_penalty = 0 if location_gate_match else 4
        rank_score = (weighted_score * 20) - penalty_points - location_penalty

        primary_sleeve_config = sleeves_v2.SLEEVE_CONFIG[scoring_sleeve]
        reasons = [
            (
                f"Sleeve {scoring_sleeve} fit {primary_score}/5 "
                f"(A:{sleeve_scores['A']} B:{sleeve_scores['B']} "
                f"C:{sleeve_scores['C']} D:{sleeve_scores['D']} E:{sleeve_scores['E']})"
            ),
            f"Abroad score {abroad_score}/4 via {', '.join(abroad_badges) or 'no explicit signal'}",
            f"Keyword coverage {total_positive_hits} hits for sleeve {scoring_sleeve}",
        ]
        if language_notes:
            reasons.append(language_notes[0])
        if penalty_reasons:
            reasons.append(penalty_reasons[0])
        if not location_gate_match:
            reasons.append("Locatie buiten voorkeursregio; als lagere prioriteit gemarkeerd.")
        reasons = reasons[:MAX_REASON_COUNT]

        scored_jobs.append(
            {
                "title": title or "Unknown role",
                "company": company or "Unknown company",
                "location": location or "Unknown",
                "url": link,
                "source": source,
                "date_posted": date_posted,
                "snippet": snippet,
                "full_description": full_description,
                "raw_text": raw_text,
                "prepared_text": prepared_text.strip(),
                "primary_sleeve_id": scoring_sleeve,
                "primary_sleeve_name": primary_sleeve_config.get("name", ""),
                "primary_sleeve_tagline": primary_sleeve_config.get("tagline", ""),
                "sleeve_scores": sleeve_scores,
                "primary_sleeve_score": primary_score,
                "abroad_score": abroad_score,
                "abroad_badges": abroad_badges,
                "decision": "FAIL",
                "reasons": reasons,
                "hard_reject_reason": hard_reject_reason or None,
                "language_flags": language_flags,
                "language_notes": language_notes,
                "query": _clean_value(job.get("query"), ""),
                "query_location": _clean_value(job.get("query_location"), ""),
                "detail_fetch_failed": bool(job.get("detail_fetch_failed", False)),
                "canonical_url_or_job_id": job_id or link,
                "link": link,
                "date": date_posted,
                "salary": salary,
                "primary_sleeve": scoring_sleeve,
                "why_relevant": reasons,
                "_base_reasons": list(reasons),
                "_score_components": {
                    "synergy_score": synergy_score,
                    "penalty_points": penalty_points,
                    "total_positive_hits": total_positive_hits,
                    "location_gate_match": location_gate_match,
                    "strict_target_mismatch": bool(
                        strict_sleeve and target_sleeve and primary_sleeve != target_sleeve
                    ),
                },
                "_fail_reason": "",
                "_rank": (
                    round(rank_score, 4),
                    weighted_score,
                    primary_score,
                    synergy_score,
                ),
            }
        )

    threshold_cfg = RUNTIME_CONFIG.get("threshold_overrides", {})
    base_min_total_hits = int(threshold_cfg.get("min_total_hits", sleeves_v2.MIN_TOTAL_HITS_TO_SHOW))
    base_min_primary = int(threshold_cfg.get("min_primary_score", min_target_score))
    base_min_maybe_total = int(threshold_cfg.get("min_maybe_total_hits", sleeves_v2.MIN_TOTAL_HITS_TO_MAYBE))
    base_min_maybe_primary = int(threshold_cfg.get("min_maybe_primary_score", sleeves_v2.MIN_PRIMARY_SLEEVE_SCORE_TO_MAYBE))

    threshold_profiles = [
        {
            "name": "default",
            "min_total_hits": base_min_total_hits,
            "min_primary_score": base_min_primary,
            "min_maybe_total_hits": base_min_maybe_total,
            "min_maybe_primary_score": base_min_maybe_primary,
        },
        {
            "name": "fallback:min_total_hits-1",
            "min_total_hits": max(1, base_min_total_hits - 1),
            "min_primary_score": base_min_primary,
            "min_maybe_total_hits": 1,
            "min_maybe_primary_score": base_min_maybe_primary,
        },
        {
            "name": "fallback:min_primary_sleeve_score-1",
            "min_total_hits": max(1, base_min_total_hits - 1),
            "min_primary_score": max(1, base_min_primary - 1),
            "min_maybe_total_hits": 1,
            "min_maybe_primary_score": max(1, base_min_maybe_primary - 1),
        },
        {
            "name": "fallback:soften_maybe_floor",
            "min_total_hits": max(1, base_min_total_hits - 1),
            "min_primary_score": max(1, base_min_primary - 1),
            "min_maybe_total_hits": 1,
            "min_maybe_primary_score": 1,
        },
    ]

    chosen_profile = threshold_profiles[0]
    fallback_steps = []
    for profile in threshold_profiles:
        pass_count = 0
        for scored in scored_jobs:
            scored["reasons"] = list(scored.get("_base_reasons") or [])
            hard_reject_reason = scored.get("hard_reject_reason")
            primary_score = float(scored.get("primary_sleeve_score", 0))
            total_hits = int(scored.get("_score_components", {}).get("total_positive_hits", 0))
            location_gate_match = bool(scored.get("_score_components", {}).get("location_gate_match", True))
            mismatch = scored.get("_score_components", {}).get("strict_target_mismatch", False)
            fail_reason = ""

            if hard_reject_reason:
                decision = "FAIL"
                fail_reason = hard_reject_reason
            elif location_mode != "global" and not location_gate_match:
                decision = "FAIL"
                fail_reason = "location_out_of_scope"
            elif mismatch:
                decision = "FAIL"
                fail_reason = "target_sleeve_mismatch"
            elif primary_score >= profile["min_primary_score"] and total_hits >= profile["min_total_hits"]:
                decision = "PASS"
            elif (
                primary_score >= profile["min_maybe_primary_score"]
                and total_hits >= profile["min_maybe_total_hits"]
            ):
                decision = "MAYBE"
            else:
                decision = "FAIL"
                if primary_score < profile["min_maybe_primary_score"]:
                    fail_reason = "primary_sleeve_score_too_low"
                else:
                    fail_reason = "insufficient_keyword_hits"

            scored["decision"] = decision
            if fail_reason:
                scored["_fail_reason"] = fail_reason
                reasons = list(scored.get("reasons") or [])
                reasons.append(f"Fail reason: {fail_reason}")
                scored["reasons"] = reasons[:MAX_REASON_COUNT]
            scored["_applied_threshold_profile"] = profile["name"]
            if decision == "PASS":
                pass_count += 1

        chosen_profile = profile
        if profile["name"] != "default":
            fallback_steps.append(profile["name"])
        if pass_count >= PASS_FALLBACK_MIN_COUNT:
            break

    priority = {"PASS": 2, "MAYBE": 1, "FAIL": 0}
    scored_jobs.sort(
        key=lambda job: (priority.get(job.get("decision", "FAIL"), 0), job.get("_rank", (0, 0, 0, 0))),
        reverse=True,
    )

    pass_jobs = [job for job in scored_jobs if job.get("decision") == "PASS"]
    maybe_jobs = [job for job in scored_jobs if job.get("decision") == "MAYBE"]
    fail_jobs = [job for job in scored_jobs if job.get("decision") == "FAIL"]
    if not pass_jobs and not maybe_jobs and scored_jobs:
        promoted = []
        for failed in fail_jobs:
            if failed.get("hard_reject_reason"):
                continue
            if (failed.get("_fail_reason") or "").startswith("location_"):
                continue
            failed["decision"] = "MAYBE"
            reasons = list(failed.get("reasons") or [])
            reasons.append("Promoted to MAYBE to avoid empty output while raw matches exist.")
            failed["reasons"] = reasons[:MAX_REASON_COUNT]
            promoted.append(failed)
            if len(promoted) >= 10:
                break
        maybe_jobs = [job for job in scored_jobs if job.get("decision") == "MAYBE"]
        fail_jobs = [job for job in scored_jobs if job.get("decision") == "FAIL"]

    fail_reason_counter = Counter()
    for failed in fail_jobs:
        fail_reason_counter[failed.get("_fail_reason") or "unknown_fail"] += 1

    if len(pass_jobs) < PASS_FALLBACK_MIN_COUNT:
        selected_jobs = pass_jobs + maybe_jobs
    else:
        selected_jobs = pass_jobs
    if include_fail:
        selected_jobs = selected_jobs + fail_jobs

    dedupe_ratio_by_source = {}
    for source, raw_count in raw_by_source.items():
        kept = kept_by_source.get(source, 0)
        ratio = round((kept / raw_count), 4) if raw_count else 0
        dedupe_ratio_by_source[source] = {
            "raw_count": raw_count,
            "after_dedupe": kept,
            "dedupe_ratio": ratio,
        }

    top_fail_reasons = [
        {"reason": reason, "count": count}
        for reason, count in fail_reason_counter.most_common(5)
    ]
    full_description_count = sum(1 for job in scored_jobs if _clean_value(job.get("full_description"), ""))
    full_description_coverage = round((full_description_count / len(scored_jobs)), 4) if scored_jobs else 0
    funnel = {
        "raw": len(items),
        "after_dedupe": len(scored_jobs),
        "scored": len(scored_jobs),
        "pass_count": len(pass_jobs),
        "maybe_count": len(maybe_jobs),
        "fail_count": len(fail_jobs),
        "full_description_count": full_description_count,
        "full_description_coverage": full_description_coverage,
        "top_fail_reasons": top_fail_reasons,
    }
    diagnostics["funnel"] = funnel
    diagnostics["fail_reasons"] = {item["reason"]: item["count"] for item in top_fail_reasons}
    diagnostics["fallbacks_applied"] = fallback_steps
    diagnostics["dedupe_ratio_by_source"] = dedupe_ratio_by_source
    diagnostics["threshold_profile"] = chosen_profile

    for job in scored_jobs:
        job.pop("_rank", None)
        job.pop("_score_components", None)
        job.pop("_applied_threshold_profile", None)
        job.pop("_fail_reason", None)
        job.pop("_base_reasons", None)

    if return_diagnostics:
        return {
            "jobs": selected_jobs,
            "all_jobs": scored_jobs,
            "funnel": funnel,
            "fallbacks_applied": fallback_steps,
            "top_fail_reasons": top_fail_reasons,
            "dedupe_ratio_by_source": dedupe_ratio_by_source,
        }
    return selected_jobs


def fetch_latest_comic_id():
    """Fetch and cache the latest XKCD comic id."""
    now = time.time()
    if (
        latest_comic_cache["id"] is not None
        and now - latest_comic_cache["fetched_at"] < CACHE_TTL_SECONDS
    ):
        return latest_comic_cache["id"]

    response = requests.get("https://xkcd.com/info.0.json", timeout=8)
    response.raise_for_status()
    latest_id = response.json().get("num", DEFAULT_COMIC_ID)

    latest_comic_cache["id"] = latest_id
    latest_comic_cache["fetched_at"] = now
    return latest_id


def fetch_comic(comic_id):
    """Fetch a specific XKCD comic payload."""
    response = requests.get(f"https://xkcd.com/{comic_id}/info.0.json", timeout=8)
    response.raise_for_status()
    return response.json()


def _sleeve_query_string(sleeve_key):
    terms = sleeves_v2.SLEEVE_SEARCH_TERMS.get((sleeve_key or "").upper(), [])
    quoted_terms = [f"\"{term}\"" for term in terms[:6]]
    return " OR ".join(quoted_terms) if quoted_terms else ""


def _fetch_remotive_jobs():
    items = []
    response = requests.get(REMOTIVE_API, timeout=12)
    response.raise_for_status()
    for job in response.json().get("jobs", []):
        location = _clean_value(job.get("candidate_required_location"), "")
        category = _clean_value(job.get("category"), "")
        tags = " ".join(job.get("tags") or [])
        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": _clean_value(job.get("company_name"), ""),
                "location": location,
                "link": _clean_value(job.get("url"), ""),
                "snippet": _strip_html(job.get("description")),
                "salary": _clean_value(job.get("salary"), "Not listed"),
                "date": _clean_value(job.get("publication_date"), "Unknown"),
                "work_mode_hint": _normalize_text(job.get("job_type"), location, category, tags),
                "source": "Remotive",
            }
        )
    return items


def _fetch_remoteok_jobs():
    items = []
    response = requests.get(REMOTEOK_API, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    for job in response.json():
        if not isinstance(job, dict) or not job.get("position"):
            continue
        location = _clean_value(job.get("location"), "")
        tags = " ".join(job.get("tags") or [])
        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        if salary_min and salary_max:
            salary = f"{salary_min}-{salary_max}"
        else:
            salary = "Not listed"
        items.append(
            {
                "title": _clean_value(job.get("position"), ""),
                "company": _clean_value(job.get("company"), ""),
                "location": location,
                "link": _clean_value(job.get("apply_url") or job.get("url"), ""),
                "snippet": _strip_html(job.get("description")),
                "salary": salary,
                "date": _clean_value(job.get("date"), "Unknown"),
                "work_mode_hint": _normalize_text(location, tags),
                "source": "RemoteOK",
            }
        )
    return items


def _fetch_themuse_jobs():
    items = []
    for page in range(1, THEMUSE_PAGES + 1):
        response = requests.get(THEMUSE_API, timeout=12, params={"page": page})
        response.raise_for_status()
        payload = response.json()
        for job in payload.get("results", []):
            location_names = ", ".join(
                location.get("name", "")
                for location in (job.get("locations") or [])
                if isinstance(location, dict)
            )
            categories = ", ".join(
                category.get("name", "")
                for category in (job.get("categories") or [])
                if isinstance(category, dict)
            )
            levels = ", ".join(
                level.get("name", "")
                for level in (job.get("levels") or [])
                if isinstance(level, dict)
            )
            tags = ", ".join(
                tag.get("name", "")
                for tag in (job.get("tags") or [])
                if isinstance(tag, dict)
            )
            items.append(
                {
                    "title": _clean_value(job.get("name"), ""),
                    "company": _clean_value((job.get("company") or {}).get("name"), ""),
                    "location": _clean_value(location_names, ""),
                    "link": _clean_value((job.get("refs") or {}).get("landing_page"), ""),
                    "snippet": _strip_html(job.get("contents")),
                    "salary": "Not listed",
                    "date": _clean_value(job.get("publication_date"), "Unknown"),
                    "work_mode_hint": _normalize_text(location_names, categories, levels, tags),
                    "source": "The Muse",
                }
            )
    return items


def _fetch_arbeitnow_jobs():
    items = []
    response = requests.get(ARBEITNOW_API, timeout=12)
    response.raise_for_status()
    for job in response.json().get("data", []):
        created_at = job.get("created_at")
        if isinstance(created_at, int):
            date_value = time.strftime("%Y-%m-%d", time.gmtime(created_at))
        else:
            date_value = "Unknown"
        tags = " ".join(job.get("tags") or [])
        job_types = " ".join(job.get("job_types") or [])
        location = _clean_value(job.get("location"), "")
        remote_flag = "remote" if job.get("remote") else ""
        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": _clean_value(job.get("company_name"), ""),
                "location": location,
                "link": _clean_value(job.get("url"), ""),
                "snippet": _strip_html(job.get("description")),
                "salary": "Not listed",
                "date": date_value,
                "work_mode_hint": _normalize_text(remote_flag, location, tags, job_types),
                "source": "Arbeitnow",
            }
        )
    return items


def _format_salary_range(minimum, maximum, currency=""):
    if minimum and maximum:
        return f"{minimum}-{maximum} {currency}".strip()
    return "Not listed"


def _fetch_jobicy_jobs(_sleeve_key=None, location_mode="nl_only", **_kwargs):
    params = {"count": 100}
    if location_mode == "nl_only":
        params["geo"] = "Netherlands"
    elif location_mode == "nl_eu":
        params["geo"] = "Europe"

    response = requests.get(
        JOBICY_API,
        timeout=12,
        params=params,
        headers={"User-Agent": "Mozilla/5.0 (existence.app)"},
    )
    response.raise_for_status()
    payload = response.json()
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    items = []
    for job in jobs:
        tags = job.get("jobTags") or job.get("tags") or []
        if isinstance(tags, str):
            tags = [tags]
        tags_text = " ".join(str(tag) for tag in tags)

        industries = job.get("jobIndustry") or []
        if isinstance(industries, str):
            industries = [industries]
        industry_text = " ".join(str(industry) for industry in industries)

        job_types = job.get("jobType") or []
        if isinstance(job_types, str):
            job_types = [job_types]
        job_type_text = " ".join(str(job_type) for job_type in job_types)

        location = _clean_value(job.get("jobGeo") or job.get("location"), "")
        items.append(
            {
                "title": _clean_value(job.get("jobTitle") or job.get("title"), ""),
                "company": _clean_value(job.get("companyName"), ""),
                "location": location,
                "link": _clean_value(job.get("url"), ""),
                "snippet": _clean_value(job.get("jobExcerpt") or _strip_html(job.get("jobDescription")), ""),
                "salary": "Not listed",
                "date": _clean_value(job.get("pubDate"), "Unknown"),
                "work_mode_hint": _normalize_text(job_type_text, location, tags_text, industry_text),
                "source": "Jobicy",
            }
        )
    return items


def _fetch_himalayas_jobs():
    response = requests.get(HIMALAYAS_API, timeout=12, params={"limit": 80})
    response.raise_for_status()
    payload = response.json()
    jobs = payload.get("jobs", payload if isinstance(payload, list) else [])
    items = []
    for job in jobs:
        company_info = job.get("company") if isinstance(job.get("company"), dict) else {}
        company = _clean_value(
            company_info.get("name") or job.get("companyName"),
            "",
        )
        location_restrictions = job.get("locationRestrictions") or job.get("location_restrictions") or []
        if isinstance(location_restrictions, str):
            location_restrictions = [location_restrictions]
        timezone_restrictions = job.get("timezoneRestrictions") or job.get("timezone_restrictions") or []
        if isinstance(timezone_restrictions, str):
            timezone_restrictions = [timezone_restrictions]
        categories = job.get("categories") or []
        if isinstance(categories, str):
            categories = [categories]
        category_text = " ".join(str(category) for category in categories)
        location = _clean_value(
            ", ".join(str(part) for part in location_restrictions) or job.get("location"),
            "",
        )
        salary = _format_salary_range(
            job.get("salaryMin") or job.get("minSalary"),
            job.get("salaryMax") or job.get("maxSalary"),
            job.get("salaryCurrency") or "",
        )
        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": company,
                "location": location,
                "link": _clean_value(
                    job.get("applicationLink") or job.get("applyUrl") or job.get("url"),
                    "",
                ),
                "snippet": _clean_value(job.get("excerpt") or _strip_html(job.get("description")), ""),
                "salary": salary,
                "date": _clean_value(job.get("pubDate") or job.get("publishedAt"), "Unknown"),
                "work_mode_hint": _normalize_text(
                    " ".join(str(part) for part in location_restrictions),
                    " ".join(str(part) for part in timezone_restrictions),
                    job.get("employmentType"),
                    job.get("type"),
                    category_text,
                ),
                "source": "Himalayas",
            }
        )
    return items


def _fetch_adzuna_jobs(sleeve_key, location_mode="nl_only", **_kwargs):
    app_id = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key = os.getenv("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        raise ValueError("Adzuna credentials are missing")
    query = _sleeve_query_string(sleeve_key)
    if location_mode in {"nl_only", "nl_eu"}:
        query = f"({query}) AND (remote OR hybrid OR travel OR on-site)"
    response = requests.get(
        ADZUNA_API_TEMPLATE.format(page=1),
        timeout=12,
        params={
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": 40,
            "what": query,
            "where": "Netherlands",
            "content-type": "application/json",
        },
    )
    response.raise_for_status()
    items = []
    for job in response.json().get("results", []):
        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        if salary_min and salary_max:
            salary = f"{int(salary_min)}-{int(salary_max)}"
        else:
            salary = "Not listed"
        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": _clean_value((job.get("company") or {}).get("display_name"), ""),
                "location": _clean_value((job.get("location") or {}).get("display_name"), ""),
                "link": _clean_value(job.get("redirect_url"), ""),
                "snippet": _strip_html(job.get("description")),
                "salary": salary,
                "date": _clean_value(job.get("created"), "Unknown"),
                "work_mode_hint": _normalize_text(query, job.get("contract_time"), job.get("contract_type")),
                "source": "Adzuna",
            }
        )
    return items


def _fetch_serpapi_jobs(sleeve_key, location_mode="nl_only", provider_filter=None, **_kwargs):
    api_key = os.getenv("SERPAPI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SerpApi key is missing")
    location_mode = _normalized_location_mode(location_mode)
    query = _sleeve_query_string(sleeve_key)
    if location_mode in {"nl_only", "nl_eu"}:
        query = f"({query}) (remote OR hybrid OR travel) Netherlands"

    search_location = "Netherlands" if location_mode == "nl_only" else "Europe"
    serpapi_params = {
        "engine": "google_jobs",
        "q": query,
        "location": search_location,
        "api_key": api_key,
    }
    serpapi_params.update(_serpapi_market_params_for_mode(location_mode))
    response = requests.get(
        SERPAPI_URL,
        timeout=12,
        params=serpapi_params,
    )
    response.raise_for_status()
    payload = response.json()
    items = []
    for job in payload.get("jobs_results", []):
        via = _clean_value(job.get("via"), "")
        if provider_filter and provider_filter.lower() not in via.lower():
            continue

        options = job.get("apply_options") or []
        apply_link = ""
        if options and isinstance(options[0], dict):
            apply_link = options[0].get("link", "")

        extensions = job.get("detected_extensions") or []
        if isinstance(extensions, list):
            extension_text = " ".join(str(item) for item in extensions)
            date_value = extensions[0] if extensions else "Unknown"
        else:
            extension_text = str(extensions)
            date_value = "Unknown"

        source_label = "SerpApi Google Jobs"
        if provider_filter:
            provider_name = "LinkedIn" if provider_filter.lower() == "linkedin" else "Indeed"
            source_label = f"{provider_name} via SerpApi"

        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": _clean_value(job.get("company_name"), ""),
                "location": _clean_value(job.get("location"), ""),
                "link": _clean_value(apply_link, ""),
                "snippet": _clean_value(job.get("description"), ""),
                "salary": _clean_value(job.get("salary"), "Not listed"),
                "date": _clean_value(date_value, "Unknown"),
                "work_mode_hint": _normalize_text(
                    extension_text,
                    via,
                    job.get("location"),
                ),
                "source": source_label,
            }
        )
    return items


def _fetch_serpapi_indeed_jobs(sleeve_key, location_mode="nl_only", **_kwargs):
    return _fetch_serpapi_jobs(
        sleeve_key,
        location_mode=location_mode,
        provider_filter="indeed",
    )


def _fetch_serpapi_linkedin_jobs(sleeve_key, location_mode="nl_only", **_kwargs):
    return _fetch_serpapi_jobs(
        sleeve_key,
        location_mode=location_mode,
        provider_filter="linkedin",
    )


def _fetch_indeed_web_jobs(
    sleeve_key,
    location_mode="nl_only",
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    diagnostics=None,
    **_kwargs,
):
    return _fetch_indeed_jobs_direct(
        sleeve_key,
        location_mode=location_mode,
        max_pages=max_pages,
        target_raw=target_raw,
        diagnostics=diagnostics,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
    )


def _fetch_linkedin_web_jobs(
    sleeve_key,
    location_mode="nl_only",
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    diagnostics=None,
    **_kwargs,
):
    return _fetch_linkedin_jobs_direct(
        sleeve_key,
        location_mode=location_mode,
        max_pages=max_pages,
        target_raw=target_raw,
        diagnostics=diagnostics,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
    )


SOURCE_REGISTRY = {
    "indeed_web": {
        "label": "Indeed (direct scraping)",
        "default_enabled": True,
        "requires_env": [],
        "query_based": True,
        "fetcher": _fetch_indeed_web_jobs,
    },
    "linkedin_web": {
        "label": "LinkedIn (direct scraping)",
        "default_enabled": True,
        "requires_env": [],
        "query_based": True,
        "fetcher": _fetch_linkedin_web_jobs,
    },
    "remotive": {
        "label": "Remotive",
        "default_enabled": False,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_remotive_jobs,
    },
    "remoteok": {
        "label": "RemoteOK",
        "default_enabled": False,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_remoteok_jobs,
    },
    "jobicy": {
        "label": "Jobicy",
        "default_enabled": False,
        "requires_env": [],
        "query_based": True,
        "cache_by_sleeve": False,
        "fetcher": _fetch_jobicy_jobs,
    },
    "himalayas": {
        "label": "Himalayas",
        "default_enabled": False,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_himalayas_jobs,
    },
    "themuse": {
        "label": "The Muse",
        "default_enabled": False,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_themuse_jobs,
    },
    "arbeitnow": {
        "label": "Arbeitnow",
        "default_enabled": False,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_arbeitnow_jobs,
    },
    "adzuna": {
        "label": "Adzuna (API key)",
        "default_enabled": False,
        "requires_env": ["ADZUNA_APP_ID", "ADZUNA_APP_KEY"],
        "query_based": True,
        "fetcher": _fetch_adzuna_jobs,
    },
    "indeed_serpapi": {
        "label": "Indeed via SerpApi (API key)",
        "default_enabled": False,
        "requires_env": ["SERPAPI_API_KEY"],
        "query_based": True,
        "fetcher": _fetch_serpapi_indeed_jobs,
    },
    "linkedin_serpapi": {
        "label": "LinkedIn via SerpApi (API key)",
        "default_enabled": False,
        "requires_env": ["SERPAPI_API_KEY"],
        "query_based": True,
        "fetcher": _fetch_serpapi_linkedin_jobs,
    },
    "serpapi": {
        "label": "Google Jobs via SerpApi (API key)",
        "default_enabled": False,
        "requires_env": ["SERPAPI_API_KEY"],
        "query_based": True,
        "fetcher": _fetch_serpapi_jobs,
    },
}


def _source_env_missing(source_key):
    config = SOURCE_REGISTRY[source_key]
    missing = []
    for env_name in config["requires_env"]:
        if not os.getenv(env_name, "").strip():
            missing.append(env_name)
    return missing


def _source_health_block_reason(source_key):
    health = source_health.get(source_key) or {}
    failure_streak = health.get("failure_streak", 0)
    last_failure_at = health.get("last_failure_at", 0)
    if failure_streak < SOURCE_FAILURE_THRESHOLD:
        return ""
    if time.time() - last_failure_at > SOURCE_FAILURE_COOLDOWN_SECONDS:
        return ""
    return _clean_value(health.get("last_error"), "Recent repeated source failures")


def _source_available(source_key):
    if _source_env_missing(source_key):
        return False
    if _source_health_block_reason(source_key):
        return False
    return True


def _source_availability_reason(source_key):
    missing_env = _source_env_missing(source_key)
    if missing_env:
        return f"Missing env: {', '.join(missing_env)}"
    health_reason = _source_health_block_reason(source_key)
    if health_reason:
        return f"Temporarily disabled after repeated failures: {health_reason}"
    return ""


def _record_source_health(source_key, error):
    health = source_health.get(source_key, {"failure_streak": 0, "last_failure_at": 0, "last_error": ""})
    if error:
        health["failure_streak"] = health.get("failure_streak", 0) + 1
        health["last_failure_at"] = time.time()
        health["last_error"] = _clean_value(error, "Unknown source error")
    else:
        health["failure_streak"] = 0
        health["last_error"] = ""
    source_health[source_key] = health


def _default_sources():
    return [
        source_key
        for source_key, config in SOURCE_REGISTRY.items()
        if config["default_enabled"] and _source_available(source_key)
    ]


def _cache_key_for(source_key, sleeve_key, location_mode, max_pages, target_raw, no_new_unique_pages):
    config = SOURCE_REGISTRY[source_key]
    if config["query_based"] and config.get("cache_by_sleeve", True):
        return f"{source_key}:{sleeve_key}:{location_mode}:p{max_pages}:t{target_raw}:n{no_new_unique_pages}"
    if config["query_based"]:
        return f"{source_key}:{location_mode}:p{max_pages}:t{target_raw}:n{no_new_unique_pages}"
    return source_key


def _fetch_source_with_cache(
    source_key,
    sleeve_key,
    location_mode,
    force_refresh=False,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
):
    cache_key = _cache_key_for(
        source_key,
        sleeve_key,
        location_mode,
        max_pages,
        target_raw,
        no_new_unique_pages,
    )
    now = time.time()
    cache_entry = source_cache.get(cache_key)
    if (
        cache_entry
        and not force_refresh
        and now - cache_entry["fetched_at"] < JOBS_CACHE_TTL_SECONDS
    ):
        return cache_entry["items"], cache_entry["error"], cache_entry.get("diagnostics") or _new_diagnostics()

    fetcher = SOURCE_REGISTRY[source_key]["fetcher"]
    query_based = SOURCE_REGISTRY[source_key]["query_based"]
    source_diag = _new_diagnostics()
    try:
        if query_based:
            result = fetcher(
                sleeve_key,
                location_mode=location_mode,
                max_pages=max_pages,
                target_raw=target_raw,
                requests_per_second=requests_per_second,
                detail_rps=detail_rps,
                no_new_unique_pages=no_new_unique_pages,
                diagnostics=source_diag,
            )
        else:
            result = fetcher()
        if isinstance(result, tuple) and len(result) == 2:
            items, source_diag = result
        else:
            items = result
        error = None
    except Exception as exc:  # pragma: no cover
        items = []
        error = str(exc)
        source_diag.setdefault("source_query_pages", []).append(
            {
                "source": source_key,
                "query": "",
                "location": location_mode,
                "page": 0,
                "url": "",
                "status": 0,
                "cards_found": 0,
                "parsed_count": 0,
                "detailpages_fetched": 0,
                "error_count": 1,
                "blocked_detected": False,
            }
        )
    _record_source_health(source_key, error)

    source_cache[cache_key] = {
        "items": items,
        "error": error,
        "diagnostics": source_diag,
        "fetched_at": now,
    }
    return items, error, source_diag


def fetch_jobs_from_sources(
    selected_sources,
    sleeve_key,
    location_mode="nl_only",
    force_refresh=False,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    allow_failover=True,
    run_id="",
):
    requested = [source for source in selected_sources if source in SOURCE_REGISTRY]
    if not requested:
        requested = _default_sources()
    usable_sources = [source for source in requested if _source_available(source)]
    if not usable_sources:
        return [], ["No available sources for the selected options."], [], _new_diagnostics()

    items = []
    errors = []
    diagnostics = _new_diagnostics()
    diagnostics["run_id"] = run_id
    for source_key in usable_sources:
        source_items, source_error, source_diag = _fetch_source_with_cache(
            source_key,
            sleeve_key,
            location_mode,
            force_refresh=force_refresh,
            max_pages=max_pages,
            target_raw=target_raw,
            requests_per_second=requests_per_second,
            detail_rps=detail_rps,
            no_new_unique_pages=no_new_unique_pages,
        )
        items.extend(source_items)
        if source_error:
            errors.append(f"{source_key}: {source_error}")
        diagnostics["source_query_pages"].extend(source_diag.get("source_query_pages", []))
        diagnostics["snapshots"].extend(source_diag.get("snapshots", []))
        for source, blocked in (source_diag.get("blocked_detected") or {}).items():
            diagnostics["blocked_detected"][source] = bool(
                diagnostics["blocked_detected"].get(source, False) or blocked
            )
        for key, value in (source_diag.get("source_query_summary") or {}).items():
            diagnostics["source_query_summary"][key] = value

    unique_count = _count_unique_items(items)
    blocked_primary = []
    for source_key, source_name in PRIMARY_DIRECT_SOURCES.items():
        if source_key not in usable_sources:
            continue
        if diagnostics["blocked_detected"].get(source_name):
            blocked_primary.append(source_key)
    low_yield = unique_count < max(20, int(target_raw * 0.35))
    should_failover = bool(allow_failover and (blocked_primary or low_yield))

    if should_failover:
        reason = "blocked_primary" if blocked_primary else "low_yield"
        diagnostics["auto_failover"].append(
            {
                "reason": reason,
                "blocked_primary_sources": blocked_primary,
                "unique_count_before_failover": unique_count,
            }
        )
        for source_key in AUTO_FAILOVER_SOURCES:
            if source_key in usable_sources:
                continue
            if source_key not in SOURCE_REGISTRY:
                continue
            if not _source_available(source_key):
                continue

            source_items, source_error, source_diag = _fetch_source_with_cache(
                source_key,
                sleeve_key,
                location_mode,
                force_refresh=force_refresh,
                max_pages=max_pages,
                target_raw=target_raw,
                requests_per_second=requests_per_second,
                detail_rps=detail_rps,
                no_new_unique_pages=no_new_unique_pages,
            )
            usable_sources.append(source_key)
            items.extend(source_items)
            if source_error:
                errors.append(f"{source_key}: {source_error}")
            diagnostics["source_query_pages"].extend(source_diag.get("source_query_pages", []))
            diagnostics["snapshots"].extend(source_diag.get("snapshots", []))
            for source, blocked in (source_diag.get("blocked_detected") or {}).items():
                diagnostics["blocked_detected"][source] = bool(
                    diagnostics["blocked_detected"].get(source, False) or blocked
                )
            for key, value in (source_diag.get("source_query_summary") or {}).items():
                diagnostics["source_query_summary"][key] = value
            diagnostics["auto_failover"].append(
                {
                    "source_activated": source_key,
                    "new_items": len(source_items),
                }
            )
            unique_count = _count_unique_items(items)
            if unique_count >= target_raw:
                break

    _update_query_performance_from_diagnostics(diagnostics, sleeve_key)

    return items, errors, usable_sources, diagnostics


def _public_scrape_config():
    sources = []
    for source_key, config in SOURCE_REGISTRY.items():
        available = _source_available(source_key)
        reason = _source_availability_reason(source_key)
        sources.append(
            {
                "id": source_key,
                "label": config["label"],
                "available": available,
                "default_enabled": bool(config["default_enabled"] and available),
                "reason": reason,
            }
        )

    return {
        "sources": sources,
        "config_version": RUNTIME_CONFIG.get("config_version", "1.0"),
        "defaults": {
            "sources": _default_sources(),
            "location_mode": "nl_only",
            "strict": False,
            "max_results": 30,
            "max_pages": DEFAULT_MAX_PAGES,
            "target_raw": DEFAULT_TARGET_RAW_PER_SLEEVE,
            "requests_per_second": DEFAULT_RATE_LIMIT_RPS,
            "detail_requests_per_second": DEFAULT_DETAIL_RATE_LIMIT_RPS,
            "no_new_unique_pages": int((RUNTIME_CONFIG.get("crawl") or {}).get("no_new_unique_pages_stop", DEFAULT_NO_NEW_UNIQUE_PAGES)),
            "incremental": False,
            "state_window_days": DEFAULT_INCREMENTAL_WINDOW_DAYS,
            "use_cache": True,
            "failover": False,
        },
        "location_modes": [
            {"id": "nl_only", "label": sleeves_v2.LOCATION_MODE_LABELS["nl_only"]},
            {"id": "nl_eu", "label": sleeves_v2.LOCATION_MODE_LABELS["nl_eu"]},
            {"id": "global", "label": sleeves_v2.LOCATION_MODE_LABELS["global"]},
        ],
    }


# Root URL maps to this function
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


# Redirect '/index.html' to '/'
@app.route('/index.html')
def redirect_to_index():
    return redirect('/')


# Routes for different decades
@app.route('/genesis')
def decade_2000():
    return render_template('2000.html')


@app.route('/aspiration')
def decade_2010():
    return render_template('2010.html')


@app.route('/enlightenment')
def decade_2020():
    return render_template('2020.html')


@app.route('/synergy')
def decade_2030():
    return render_template('2030.html')


@app.route('/immersion')
def decade_2040():
    return render_template('2040.html')


@app.route('/transcendence')
def decade_2050():
    return render_template('2050.html')


# Handles form submission for XKCD comic ID
@app.route('/comic', methods=['GET', 'POST'])
def read_form():
    comic_id_value = request.form.get('comic_id', '').strip()

    try:
        latest_id = fetch_latest_comic_id()
    except requests.RequestException:
        latest_id = DEFAULT_COMIC_ID

    if comic_id_value:
        try:
            comic_id = int(comic_id_value)
            if not 1 <= comic_id <= latest_id:
                raise ValueError('comic_id out of range')
            invalid_id = False
        except ValueError:
            invalid_id = True
            comic_id = random.randint(1, latest_id)
    else:
        invalid_id = True
        comic_id = random.randint(1, latest_id)

    return redirect(url_for('show_comic', comic_id=comic_id, invalid=int(invalid_id)))


# Show the XKCD comic based on comic_id
@app.route('/comic/<int:comic_id>')
def show_comic(comic_id):
    invalid_id = request.args.get('invalid', '0') == '1'
    try:
        data = fetch_comic(comic_id)
        error = None
    except requests.RequestException:
        data = None
        error = "We couldn't reach XKCD right now. Please try again in a moment."

    return render_template('comic.html', data=data, invalid_id=invalid_id, error=error)


@app.route('/scrape-config')
def scrape_config():
    return jsonify(_public_scrape_config())


# Route to trigger scraping
@app.route('/scrape')
def scrape():
    run_id = uuid.uuid4().hex[:12]
    sleeve_key = request.args.get("sleeve", "").upper().strip()
    if sleeve_key not in sleeves_v2.VALID_SLEEVES:
        allowed = ", ".join(sorted(sleeves_v2.VALID_SLEEVES))
        return jsonify({"error": f"Invalid sleeve. Use one of: {allowed}."}), 400

    location_mode = request.args.get("location_mode", "nl_only").strip()
    if location_mode not in {"nl_only", "nl_eu", "global"}:
        location_mode = "nl_only"

    strict_sleeve = request.args.get("strict", "0") == "1"
    force_refresh = request.args.get("refresh", "0") == "1"
    include_fail = request.args.get("include_fail", "0") == "1"
    use_legacy_response = request.args.get("legacy", "0") == "1"

    max_results_raw = request.args.get("max_results", "20")
    try:
        max_results = int(max_results_raw)
    except ValueError:
        max_results = 20
    max_results = max(5, min(max_results, 100))

    max_pages_raw = request.args.get("max_pages", str(DEFAULT_MAX_PAGES))
    try:
        max_pages = int(max_pages_raw)
    except ValueError:
        max_pages = DEFAULT_MAX_PAGES
    max_pages = max(1, min(max_pages, 12))

    target_raw_raw = request.args.get("target_raw", str(DEFAULT_TARGET_RAW_PER_SLEEVE))
    try:
        target_raw = int(target_raw_raw)
    except ValueError:
        target_raw = DEFAULT_TARGET_RAW_PER_SLEEVE
    target_raw = max(20, min(target_raw, 500))

    requests_per_second_raw = request.args.get("rps", str(DEFAULT_RATE_LIMIT_RPS))
    detail_rps_raw = request.args.get("detail_rps", str(DEFAULT_DETAIL_RATE_LIMIT_RPS))
    try:
        requests_per_second = float(requests_per_second_raw)
    except ValueError:
        requests_per_second = DEFAULT_RATE_LIMIT_RPS
    try:
        detail_rps = float(detail_rps_raw)
    except ValueError:
        detail_rps = DEFAULT_DETAIL_RATE_LIMIT_RPS
    requests_per_second = max(0.2, min(requests_per_second, 5.0))
    detail_rps = max(0.2, min(detail_rps, 5.0))
    no_new_unique_pages_raw = request.args.get(
        "no_new_unique_pages",
        str((RUNTIME_CONFIG.get("crawl") or {}).get("no_new_unique_pages_stop", DEFAULT_NO_NEW_UNIQUE_PAGES)),
    )
    try:
        no_new_unique_pages = int(no_new_unique_pages_raw)
    except ValueError:
        no_new_unique_pages = DEFAULT_NO_NEW_UNIQUE_PAGES
    no_new_unique_pages = max(1, min(no_new_unique_pages, 6))

    incremental_mode = request.args.get("incremental", "0") == "1"
    state_window_days_raw = request.args.get("state_window_days", str(DEFAULT_INCREMENTAL_WINDOW_DAYS))
    try:
        state_window_days = int(state_window_days_raw)
    except ValueError:
        state_window_days = DEFAULT_INCREMENTAL_WINDOW_DAYS
    state_window_days = max(1, min(state_window_days, 90))

    sources_param = request.args.get("sources", "")
    selected_sources = [source.strip().lower() for source in sources_param.split(",") if source.strip()]
    failover_raw = request.args.get("failover", "").strip()
    if failover_raw in {"0", "1"}:
        allow_failover = failover_raw == "1"
    else:
        allow_failover = not bool(selected_sources)

    items, fetch_errors, used_sources, fetch_diagnostics = fetch_jobs_from_sources(
        selected_sources,
        sleeve_key,
        location_mode=location_mode,
        force_refresh=force_refresh,
        max_pages=max_pages,
        target_raw=target_raw,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        allow_failover=allow_failover,
        run_id=run_id,
    )
    if not items and fetch_errors:
        return jsonify(
            {
                "error": "All selected sources failed.",
                "details": fetch_errors,
                "diagnostics": fetch_diagnostics,
            }
        ), 502

    ranking_result = rank_and_filter_jobs(
        items,
        target_sleeve=sleeve_key,
        min_target_score=sleeves_v2.MIN_PRIMARY_SLEEVE_SCORE_TO_SHOW,
        location_mode=location_mode,
        strict_sleeve=strict_sleeve,
        include_fail=include_fail,
        return_diagnostics=True,
        diagnostics=fetch_diagnostics,
    )
    candidate_items = ranking_result.get("jobs") or []
    incremental_skipped = 0
    if incremental_mode:
        candidate_items, incremental_skipped = _apply_incremental_filter(candidate_items, state_window_days)
    response_items = candidate_items[:max_results]
    funnel = ranking_result.get("funnel") or {}
    summary = {
        "run_id": run_id,
        "config_version": RUNTIME_CONFIG.get("config_version", "1.0"),
        "sleeve": sleeve_key,
        "location_mode": location_mode,
        "requested_sources": selected_sources,
        "sources_used": used_sources,
        "failover_enabled": allow_failover,
        "raw_count": int(funnel.get("raw", 0)),
        "deduped_count": int(funnel.get("after_dedupe", 0)),
        "pass_count": int(funnel.get("pass_count", 0)),
        "maybe_count": int(funnel.get("maybe_count", 0)),
        "fail_count": int(funnel.get("fail_count", 0)),
        "top_fail_reasons": ranking_result.get("top_fail_reasons") or [],
        "fallbacks_applied": ranking_result.get("fallbacks_applied") or [],
        "incremental_mode": incremental_mode,
        "incremental_skipped": incremental_skipped,
    }
    pages_attempted_per_source = Counter()
    for entry in fetch_diagnostics.get("source_query_summary", {}).values():
        source_name = _clean_value(entry.get("source"), "")
        pages_attempted_per_source[source_name] += int(entry.get("pages_attempted", 0))
    min_pages_attempted = min(pages_attempted_per_source.values()) if pages_attempted_per_source else 0
    summary["targets"] = {
        "raw_or_pages_goal_met": bool(
            summary["deduped_count"] >= target_raw or min_pages_attempted >= 5
        ),
        "pass_or_maybe_goal_met": bool(
            summary["pass_count"] >= 20 or summary["maybe_count"] >= 40
        ),
        "non_zero_when_raw": bool(
            summary["raw_count"] == 0 or (summary["pass_count"] + summary["maybe_count"]) > 0
        ),
        "min_pages_attempted_per_source": min_pages_attempted,
        "pages_attempted_per_source": dict(pages_attempted_per_source),
    }
    summary["kpi_gate_passed"] = bool(
        summary["targets"]["raw_or_pages_goal_met"]
        and summary["targets"]["pass_or_maybe_goal_met"]
        and summary["targets"]["non_zero_when_raw"]
    )
    summary["full_description_coverage"] = float(funnel.get("full_description_coverage", 0))
    summary["full_description_count"] = int(funnel.get("full_description_count", 0))

    _log_event(
        "scrape_summary",
        run_id=run_id,
        sleeve=sleeve_key,
        location_mode=location_mode,
        raw=summary["raw_count"],
        deduped=summary["deduped_count"],
        pass_count=summary["pass_count"],
        maybe_count=summary["maybe_count"],
        fail_count=summary["fail_count"],
        targets=summary["targets"],
    )
    for entry in fetch_diagnostics.get("source_query_summary", {}).values():
        _log_event(
            "source_query_summary",
            run_id=run_id,
            source=entry.get("source"),
            query=entry.get("query"),
            location=entry.get("location"),
            pages_attempted=entry.get("pages_attempted"),
            raw_count=entry.get("raw_count"),
            parsed_count=entry.get("parsed_count"),
            new_unique_count=entry.get("new_unique_count"),
            error_count=entry.get("error_count"),
            blocked_detected=entry.get("blocked_detected"),
        )

    if use_legacy_response:
        response = jsonify(response_items)
        response.headers["X-Sources-Used"] = ",".join(used_sources)
        return response

    response_payload = {
        "jobs": response_items,
        "summary": summary,
        "diagnostics": fetch_diagnostics,
        "errors": fetch_errors,
    }
    response = jsonify(response_payload)
    response.headers["X-Sources-Used"] = ",".join(used_sources)
    return response


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
