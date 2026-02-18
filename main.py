"""Main Flask application for existence.app."""

from flask import Flask, request, redirect, render_template, url_for, jsonify
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import html
import ipaddress
import itertools
import json
import math
import os
import random
import requests
import time
import re
import threading
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse
from parsel import Selector
import career_sleeves as sleeves

# Create an instance of the Flask class
app = Flask(__name__)
# cPanel/Passenger compatibility: some setups expect "application".
application = app

# Cache the latest XKCD id to avoid hardcoded limits and repeated API calls
latest_comic_cache = {"id": None, "fetched_at": 0}
CACHE_TTL_SECONDS = 3600
DEFAULT_COMIC_ID = 3000
JOBS_CACHE_TTL_SECONDS = 600
JOBS_CACHE_EMPTY_TTL_SECONDS = 45
MAX_STALE_CACHE_FALLBACK_SECONDS = 1800
source_cache = {}
source_health = {}
SOURCE_HEALTH_DEFAULT_BLOCK_THRESHOLD = 2
SOURCE_HEALTH_DEFAULT_BLOCK_COOLDOWN_SECONDS = 3600
SOURCE_HEALTH_DEFAULT_ERROR_COOLDOWN_SECONDS = 600

# Location anchor: Copernicuslaan 105, 5223EC, 's-Hertogenbosch (BAG/PDOK centroid)
HOME_LAT = 51.69531823
HOME_LON = 5.28914427
HOME_LOCATION_LABEL = "Home"
HOME_LOCATION_FULL_LABEL = "Copernicuslaan 105, 5223EC 's-Hertogenbosch"

CITY_COORDS_NL = {
    "'s-hertogenbosch": (51.6978, 5.3037),
    "s hertogenbosch": (51.6978, 5.3037),
    "s-hertogenbosch": (51.6978, 5.3037),
    "den bosch": (51.6978, 5.3037),
    "amsterdam": (52.3676, 4.9041),
    "den haag": (52.0705, 4.3007),
    "'s-gravenhage": (52.0705, 4.3007),
    "s gravenhage": (52.0705, 4.3007),
    "the hague": (52.0705, 4.3007),
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
    "almere": (52.3508, 5.2647),
    "zwolle": (52.5168, 6.0830),
    "amersfoort": (52.1561, 5.3878),
    "dordrecht": (51.8133, 4.6901),
    "apeldoorn": (52.2112, 5.9699),
    "leeuwarden": (53.2012, 5.7999),
    "venlo": (51.3704, 6.1724),
    "hengelo": (52.2676, 6.7930),
    "enschede": (52.2215, 6.8937),
}

NETHERLANDS_KEYWORDS = [
    "netherlands", "nederland", "dutch", "holland",
    "amsterdam", "rotterdam", "utrecht", "the hague", "den haag",
    "eindhoven", "groningen", "tilburg", "breda", "arnhem",
    "nijmegen", "haarlem", "leiden", "maastricht", "zwolle",
    "almere", "delft", "s-hertogenbosch", "den bosch", "enschede",
]
VIETNAM_KEYWORDS = [
    "vietnam",
    "viet nam",
    "hanoi",
    "ha noi",
    "ho chi minh",
    "ho chi minh city",
    "hcmc",
    "saigon",
    "da nang",
    "danang",
    "hai phong",
    "binh duong",
    "bac ninh",
]
OTHER_COUNTRY_KEYWORDS = [
    "usa", "united states", "canada", "australia", "new zealand",
    "india", "singapore", "philippines", "vietnam", "viet nam",
    "germany", "deutschland", "berlin", "munich", "frankfurt",
    "belgium", "france", "spain", "portugal", "italy",
    "poland", "romania", "hungary", "czech republic",
    "united kingdom", "uk", "england", "ireland", "sweden",
    "norway", "denmark", "finland", "austria", "switzerland",
]
NON_EU_COUNTRY_KEYWORDS = [
    "usa", "united states", "canada", "australia", "new zealand",
    "india", "singapore", "philippines", "vietnam", "viet nam", "mexico", "brazil",
    "argentina", "chile", "colombia", "japan", "china",
    "hong kong", "south korea", "korea", "uae", "saudi",
    "egypt", "south africa", "nigeria",
]
TARGET_REMOTE_HINTS = [
    "international",
    "global",
    "worldwide",
    "work from abroad",
    "relocation",
    "visa sponsorship",
    "work permit",
    "expat",
    "apac",
    "asean",
    "vietnam",
]
ABROAD_PERCENT_CONTEXT_KEYWORDS = [
    "travel",
    "travelling",
    "traveling",
    "international",
    "abroad",
    "buitenland",
    "overseas",
    "cross-border",
    "multi-country",
    "site visit",
    "site visits",
    "client site",
    "client sites",
    "on site",
    "on-site",
    "onsite",
    "op locatie",
    "klantlocatie",
    "reisbereid",
    "reizen",
    "internationaal reizen",
]
ABROAD_CONTEXT_TERMS = [
    "international",
    "global",
    "abroad",
    "overseas",
    "travel",
    "travelling",
    "traveling",
    "buitenland",
    "cross-border",
    "multi-country",
    "site visit",
    "client site",
    "op locatie",
    "klantlocatie",
    "emea",
    "apac",
    "asean",
    "vietnam",
    "visa sponsorship",
    "work permit",
    "relocation",
    "european union",
    "europe",
]
ABROAD_GEO_TERMS = {
    "countries": [
        ("Netherlands", ["netherlands", "nederland"]),
        ("Belgium", ["belgium", "belgie"]),
        ("Germany", ["germany", "deutschland", "duitsland"]),
        ("France", ["france", "frankrijk"]),
        ("Spain", ["spain", "spanje"]),
        ("Italy", ["italy", "italie"]),
        ("Portugal", ["portugal"]),
        ("Poland", ["poland", "polen"]),
        ("Romania", ["romania", "roemenie"]),
        ("Czech Republic", ["czech republic", "czechia", "tsjechie"]),
        ("Austria", ["austria", "oostenrijk"]),
        ("Switzerland", ["switzerland", "zwitserland"]),
        ("United Kingdom", ["united kingdom", "uk", "england", "verenigd koninkrijk"]),
        ("Ireland", ["ireland", "ierland"]),
        ("United States", ["usa", "united states", "verenigde staten", "vs"]),
        ("Canada", ["canada"]),
        ("Mexico", ["mexico"]),
        ("Brazil", ["brazil"]),
        ("Argentina", ["argentina"]),
        ("Chile", ["chile"]),
        ("Colombia", ["colombia"]),
        ("India", ["india"]),
        ("Singapore", ["singapore"]),
        ("Philippines", ["philippines", "filippijnen"]),
        ("Vietnam", ["vietnam", "viet nam"]),
        ("Japan", ["japan"]),
        ("China", ["china"]),
        ("Hong Kong", ["hong kong", "hongkong"]),
        ("South Korea", ["south korea", "korea", "zuid korea"]),
        ("United Arab Emirates", ["uae", "united arab emirates", "verenigde arabische emiraten"]),
        ("Saudi Arabia", ["saudi arabia", "saudi", "saudie arabie"]),
        ("Egypt", ["egypt", "egypte"]),
        ("South Africa", ["south africa", "zuid afrika"]),
        ("Nigeria", ["nigeria"]),
        ("Australia", ["australia", "australie"]),
        ("New Zealand", ["new zealand", "nieuw zeeland"]),
    ],
    "regions": [
        ("EU", ["eu", "european union", "europese unie"]),
        ("EMEA", ["emea"]),
        ("Benelux", ["benelux"]),
        ("DACH", ["dach"]),
        ("Nordics", ["nordics", "scandinavia", "scandinavie"]),
        ("APAC", ["apac", "asia pacific", "azie pacific"]),
        ("ASEAN", ["asean"]),
        ("LATAM", ["latam", "latin america", "latijns amerika"]),
        ("Middle East", ["middle east", "mena", "midden oosten"]),
    ],
    "continents": [
        ("Europe", ["europe", "europa"]),
        ("Asia", ["asia", "azie"]),
        ("Africa", ["africa", "afrika"]),
        ("North America", ["north america", "noord amerika"]),
        ("South America", ["south america", "zuid amerika"]),
        ("Oceania", ["oceania"]),
    ],
}

INDEED_SEARCH_URL = "https://www.indeed.com/jobs"
INDEED_SEARCH_URL_NL = "https://nl.indeed.com/jobs"
INDEED_SEARCH_URL_BY_MODE = {
    "nl_vn": INDEED_SEARCH_URL,
}
LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
LINKEDIN_JOB_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
NL_WEB_SEARCH_URL = "https://duckduckgo.com/html/"
NL_WEB_PAGE_SIZE = 30
NL_WEB_OPENING_HINTS = [
    "job opening",
    "job openings",
    "vacature",
    "vacatures",
    "baan",
    "banen",
    "werken bij",
    "solliciteer",
    "sollicitatie",
    "hiring",
    "career",
    "careers",
    "functie",
    "rol",
    "position",
    "positions",
]
DEFAULT_MAX_PAGES = 4
DEFAULT_TARGET_RAW_PER_SLEEVE = 150
DEFAULT_RATE_LIMIT_RPS = 0.45
DEFAULT_DETAIL_RATE_LIMIT_RPS = 0.25
DEFAULT_HTTP_TIMEOUT = 14
DEFAULT_HTTP_RETRIES = 2
PASS_FALLBACK_MIN_COUNT = 10
DEFAULT_NO_NEW_UNIQUE_PAGES = 2
DEFAULT_DETAIL_FETCH_BASE_BUDGET = 4
DEFAULT_DETAIL_FETCH_REDUCED_BUDGET = 2
SNAPSHOT_DIR = Path(os.getenv("SCRAPE_SNAPSHOT_DIR", "debug_snapshots"))
STATE_DIR = Path(os.getenv("SCRAPE_STATE_DIR", "debug_state"))
QUERY_PERFORMANCE_STATE_PATH = STATE_DIR / "query_performance_state.json"
SEEN_JOBS_STATE_PATH = STATE_DIR / "seen_jobs_state.json"
CUSTOM_SLEEVES_STATE_PATH = STATE_DIR / "custom_sleeves_state.json"
RUNTIME_CONFIG_PATH = Path(os.getenv("SCRAPE_RUNTIME_CONFIG", "scrape_runtime_config.json"))
DEFAULT_INCREMENTAL_WINDOW_DAYS = 14
MAX_REASON_COUNT = 3
FIXED_SYNERGY_SLEEVE_LETTERS = ("A", "B", "C", "D")
CUSTOM_SYNERGY_MIN_LETTER = "E"
CUSTOM_SYNERGY_MAX_LETTER = "Z"
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
MVP_SOURCE_IDS = ["indeed_web", "linkedin_web", "nl_web_openings"]
MVP_LOCATION_MODE = "nl_vn"
SCRAPE_MODE = "mvp"
SCRAPE_VARIANT_DEFAULT = "default"
SCRAPE_VARIANT_ULTRA_FAST = "ultra_fast"
ULTRA_FAST_SOURCE_IDS = ["linkedin_web"]
ULTRA_FAST_MAX_PAGES = 2
ULTRA_FAST_TARGET_RAW = 90
ULTRA_FAST_MIN_RPS = 1.1
ULTRA_FAST_MIN_DETAIL_RPS = 1.0
ULTRA_FAST_NO_NEW_UNIQUE_PAGES = 1
SCRAPE_PROGRESS_TTL_SECONDS = 1800
SCRAPE_PROGRESS_MAX_EVENTS = 220
REQUEST_USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
]
scrape_progress_state = {}
scrape_progress_lock = threading.Lock()
custom_sleeves_lock = threading.Lock()
source_cache_lock = threading.Lock()
source_health_lock = threading.Lock()


def _normalize_text(*parts):
    return " ".join(str(part or "") for part in parts).lower()


def _infer_work_mode(text):
    normalized = _normalize_text(text)
    hybrid_terms = [
        "hybrid",
        "hybride",
    ]
    remote_terms = [
        "remote",
        "op afstand",
        "thuiswerk",
        "werk vanuit huis",
        "work from home",
        "wfh",
    ]
    onsite_terms = [
        "on-site",
        "onsite",
        "on site",
        "op locatie",
        "op kantoor",
    ]

    if any(term in normalized for term in hybrid_terms):
        return "Hybrid"
    if any(term in normalized for term in remote_terms):
        return "Remote"
    if any(term in normalized for term in onsite_terms):
        return "On-site"
    return "Unknown"


def _clean_value(value, fallback="Unknown"):
    if value is None:
        return fallback
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned if cleaned else fallback


def _normalized_location_mode(location_mode):
    mode = _clean_value(location_mode, MVP_LOCATION_MODE).lower()
    if mode in {"nl_vn", "nl_only", "nl_eu", "global", "vn_only", "vn_plus_discovery"}:
        return MVP_LOCATION_MODE
    return MVP_LOCATION_MODE


def _indeed_search_url_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    return INDEED_SEARCH_URL_BY_MODE.get(mode, INDEED_SEARCH_URL)


def _strip_html(value):
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _clean_value(text, "")


def _now_utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _slugify(value):
    cleaned = re.sub(r"[^\w\-]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "na"


def _progress_cleanup_locked(now=None):
    now = now or time.time()
    stale_ids = []
    for run_id, payload in scrape_progress_state.items():
        updated_at = float(payload.get("updated_at", 0) or 0)
        if updated_at and now - updated_at > SCRAPE_PROGRESS_TTL_SECONDS:
            stale_ids.append(run_id)
    for run_id in stale_ids:
        scrape_progress_state.pop(run_id, None)


def _progress_start(run_id, **meta):
    if not run_id:
        return
    now = time.time()
    payload = {
        "run_id": run_id,
        "status": "running",
        "started_at": now,
        "updated_at": now,
        "meta": meta or {},
        "events": [],
        "summary": {},
        "error": "",
    }
    with scrape_progress_lock:
        _progress_cleanup_locked(now)
        scrape_progress_state[run_id] = payload


def _progress_update(run_id, stage, message, **data):
    if not run_id:
        return
    now = time.time()
    event = {
        "ts": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "stage": _clean_value(stage, "info"),
        "message": _clean_value(message, ""),
        "data": data or {},
    }
    with scrape_progress_lock:
        payload = scrape_progress_state.get(run_id)
        if not payload:
            return
        payload["updated_at"] = now
        payload.setdefault("events", []).append(event)
        if len(payload["events"]) > SCRAPE_PROGRESS_MAX_EVENTS:
            payload["events"] = payload["events"][-SCRAPE_PROGRESS_MAX_EVENTS:]


def _progress_finish(run_id, status="done", summary=None, error=""):
    if not run_id:
        return
    now = time.time()
    with scrape_progress_lock:
        payload = scrape_progress_state.get(run_id)
        if not payload:
            return
        payload["status"] = "error" if str(status).lower() == "error" else "done"
        payload["updated_at"] = now
        payload["summary"] = summary or {}
        payload["error"] = _clean_value(error, "") if error else ""
        _progress_cleanup_locked(now)


def _progress_snapshot(run_id, tail=30):
    if not run_id:
        return None
    with scrape_progress_lock:
        payload = scrape_progress_state.get(run_id)
        if not payload:
            return None
        safe_tail = max(5, min(int(tail), 120))
        events = list(payload.get("events") or [])
        snapshot = {
            "run_id": payload.get("run_id"),
            "status": payload.get("status", "running"),
            "started_at": payload.get("started_at"),
            "updated_at": payload.get("updated_at"),
            "meta": dict(payload.get("meta") or {}),
            "events": events[-safe_tail:],
            "event_count": len(events),
            "summary": dict(payload.get("summary") or {}),
            "error": payload.get("error", ""),
        }
        return snapshot


def _is_absolute_http_url(url):
    parsed = urlparse(_clean_value(url, ""))
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and not parsed.username
        and not parsed.password
    )


def _host_for_url(url):
    return (urlparse(_clean_value(url, "")).hostname or "").lower().rstrip(".")


def _is_indeed_host(host):
    normalized = str(host or "").strip().lower().rstrip(".")
    return bool(normalized) and (
        normalized == "indeed.com" or normalized.endswith(".indeed.com")
    )


def _is_linkedin_host(host):
    normalized = str(host or "").strip().lower().rstrip(".")
    return bool(normalized) and (
        normalized == "linkedin.com"
        or normalized.endswith(".linkedin.com")
        or normalized == "lnkd.in"
        or normalized.endswith(".lnkd.in")
    )


def _is_public_hostname(host):
    normalized = str(host or "").strip().lower().rstrip(".")
    if not normalized:
        return False
    if (
        normalized == "localhost"
        or normalized.endswith(".localhost")
        or normalized.endswith(".local")
    ):
        return False
    try:
        ip = ipaddress.ip_address(normalized.strip("[]"))
    except ValueError:
        return True
    return bool(getattr(ip, "is_global", False))


def _is_public_destination_url(url):
    if not _is_absolute_http_url(url):
        return False
    return _is_public_hostname(_host_for_url(url))


def _is_allowed_platform_lookup_url(url):
    if not _is_absolute_http_url(url):
        return False
    host = _host_for_url(url)
    return _is_indeed_host(host) or _is_linkedin_host(host)


def _is_platform_job_host(url):
    host = _host_for_url(url)
    if not host:
        return False
    return _is_indeed_host(host) or _is_linkedin_host(host)


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


def _normalize_career_sleeve_letter(value):
    cleaned = _clean_value(value, "").strip().upper()
    if len(cleaned) != 1 or not cleaned.isalpha():
        return ""
    return cleaned


def _is_fixed_career_sleeve_letter(letter):
    return letter in FIXED_SYNERGY_SLEEVE_LETTERS


def _is_custom_career_sleeve_letter(letter):
    if not letter or len(letter) != 1:
        return False
    if not letter.isalpha():
        return False
    if _is_fixed_career_sleeve_letter(letter):
        return False
    return ord(CUSTOM_SYNERGY_MIN_LETTER) <= ord(letter) <= ord(CUSTOM_SYNERGY_MAX_LETTER)


def _parse_queries_for_storage(raw_queries):
    if isinstance(raw_queries, str):
        parsed = _parse_search_queries(raw_queries)
    elif isinstance(raw_queries, list):
        parsed = _dedupe_queries(raw_queries)
    else:
        parsed = []

    normalized_queries = []
    for query in parsed:
        cleaned = _clean_value(query, "")
        if len(cleaned) < 2:
            continue
        normalized_queries.append(cleaned[:96])
    return _dedupe_queries(normalized_queries)[:80]


def _default_custom_location_preferences():
    return {
        "countries": [],
        "regions": [],
        "abroad_min_percent": 0,
        "abroad_max_percent": 100,
    }


def _parse_geo_preferences_for_storage(raw_queries, limit=24):
    parsed = _parse_queries_for_storage(raw_queries)
    return parsed[: max(1, int(limit))]


def _parse_abroad_percent_for_storage(raw_value, fallback=0):
    try:
        value = int(float(str(raw_value).strip()))
    except (TypeError, ValueError):
        value = int(fallback)
    return max(0, min(100, value))


def _parse_custom_location_preferences(raw_payload):
    defaults = _default_custom_location_preferences()
    if not isinstance(raw_payload, dict):
        return defaults

    countries = _parse_geo_preferences_for_storage(raw_payload.get("countries"))
    regions = _parse_geo_preferences_for_storage(raw_payload.get("regions"))
    min_percent = _parse_abroad_percent_for_storage(
        raw_payload.get("abroad_min_percent"),
        fallback=defaults["abroad_min_percent"],
    )
    max_percent = _parse_abroad_percent_for_storage(
        raw_payload.get("abroad_max_percent"),
        fallback=defaults["abroad_max_percent"],
    )
    if max_percent < min_percent:
        max_percent = min_percent

    return {
        "countries": countries,
        "regions": regions,
        "abroad_min_percent": min_percent,
        "abroad_max_percent": max_percent,
    }


def _next_available_custom_career_sleeve_letter(records):
    used_letters = set()
    for entry in records or []:
        if not isinstance(entry, dict):
            continue
        letter = _normalize_career_sleeve_letter(entry.get("letter"))
        if _is_custom_career_sleeve_letter(letter):
            used_letters.add(letter)

    for code in range(ord(CUSTOM_SYNERGY_MIN_LETTER), ord(CUSTOM_SYNERGY_MAX_LETTER) + 1):
        candidate = chr(code)
        if candidate not in used_letters:
            return candidate
    return ""


def _fixed_career_sleeves():
    records = []
    for letter in FIXED_SYNERGY_SLEEVE_LETTERS:
        config = sleeves.CAREER_SLEEVE_CONFIG.get(letter, {})
        records.append(
            {
                "letter": letter,
                "title": _clean_value(config.get("name"), f"Career Sleeve {letter}"),
                "queries": _dedupe_queries(sleeves.CAREER_SLEEVE_SEARCH_QUERIES.get(letter, [])),
                "location_preferences": _default_custom_location_preferences(),
                "locked": True,
                "scope": "fixed",
                "updated_at": _now_utc_stamp(),
            }
        )
    return records


def _load_custom_career_sleeves():
    payload = _load_json_file(CUSTOM_SLEEVES_STATE_PATH, {"version": "1.0", "custom_sleeves": []})
    raw_entries = payload.get("custom_sleeves") if isinstance(payload, dict) else []
    if not isinstance(raw_entries, list):
        return []

    by_letter = {}
    for entry in raw_entries:
        if not isinstance(entry, dict):
            continue
        letter = _normalize_career_sleeve_letter(entry.get("letter"))
        if not _is_custom_career_sleeve_letter(letter):
            continue
        title = _clean_value(entry.get("title"), "")[:120]
        queries = _parse_queries_for_storage(entry.get("queries"))
        location_preferences = _parse_custom_location_preferences(entry.get("location_preferences"))
        if not title:
            continue
        by_letter[letter] = {
            "letter": letter,
            "title": title,
            "queries": queries,
            "location_preferences": location_preferences,
            "locked": False,
            "scope": "custom",
            "updated_at": _clean_value(entry.get("updated_at"), _now_utc_stamp()),
        }

    return [by_letter[key] for key in sorted(by_letter)]


def _save_custom_career_sleeves(records):
    serializable = []
    for entry in records or []:
        if not isinstance(entry, dict):
            continue
        letter = _normalize_career_sleeve_letter(entry.get("letter"))
        if not _is_custom_career_sleeve_letter(letter):
            continue
        title = _clean_value(entry.get("title"), "")[:120]
        queries = _parse_queries_for_storage(entry.get("queries"))
        location_preferences = _parse_custom_location_preferences(entry.get("location_preferences"))
        if not title:
            continue
        serializable.append(
            {
                "letter": letter,
                "title": title,
                "queries": queries,
                "location_preferences": location_preferences,
                "updated_at": _clean_value(entry.get("updated_at"), _now_utc_stamp()),
            }
        )

    serializable.sort(key=lambda item: item.get("letter", ""))
    payload = {
        "version": "1.0",
        "updated_at": _now_utc_stamp(),
        "custom_sleeves": serializable,
    }
    return _save_json_file(CUSTOM_SLEEVES_STATE_PATH, payload)


def _career_sleeve_catalog():
    fixed = _fixed_career_sleeves()
    custom = _load_custom_career_sleeves()
    all_entries = list(fixed) + list(custom)
    return {
        "fixed": fixed,
        "custom": custom,
        "all": all_entries,
        "locked_letters": list(FIXED_SYNERGY_SLEEVE_LETTERS),
        "custom_letter_min": CUSTOM_SYNERGY_MIN_LETTER,
        "custom_letter_max": CUSTOM_SYNERGY_MAX_LETTER,
    }


def _load_runtime_config():
    default_config = {
        "config_version": "1.0",
        "query_overrides": {},
        "threshold_overrides": {
            "min_primary_score": sleeves.MIN_PRIMARY_CAREER_SLEEVE_SCORE_TO_SHOW,
            "min_total_hits": sleeves.MIN_TOTAL_HITS_TO_SHOW,
            "min_maybe_primary_score": sleeves.MIN_PRIMARY_CAREER_SLEEVE_SCORE_TO_MAYBE,
            "min_maybe_total_hits": sleeves.MIN_TOTAL_HITS_TO_MAYBE,
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
        "source_health": {
            "block_threshold": SOURCE_HEALTH_DEFAULT_BLOCK_THRESHOLD,
            "blocked_cooldown_seconds": SOURCE_HEALTH_DEFAULT_BLOCK_COOLDOWN_SECONDS,
            "error_cooldown_seconds": SOURCE_HEALTH_DEFAULT_ERROR_COOLDOWN_SECONDS,
        },
        "anti_block": {
            "prefer_rss_first": True,
            "skip_html_if_rss_has_items": True,
            "rss_skip_threshold_per_query": 5,
            "disable_detail_fetch": True,
            "warmup_gate_to_rss_only": True,
        },
    }
    loaded = _load_json_file(RUNTIME_CONFIG_PATH, {})
    if not isinstance(loaded, dict):
        return default_config
    merged = dict(default_config)
    for key in (
        "query_overrides",
        "threshold_overrides",
        "detail_fetch",
        "crawl",
        "query_performance",
        "source_health",
        "anti_block",
    ):
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
    if not parsed.netloc:
        return ""
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
    title_key = sleeves.normalize_for_match(item.get("title"))
    company_key = sleeves.normalize_for_match(item.get("company"))
    raw_url = _clean_value(item.get("link") or item.get("url"), "")
    canonical_url = _canonicalize_url(raw_url)
    job_id = _clean_value(item.get("job_id"), "") or _extract_job_id_from_url(canonical_url or raw_url)
    anchor = job_id or canonical_url
    if not anchor:
        location_key = sleeves.normalize_for_match(item.get("location"))
        date_key = sleeves.normalize_for_match(item.get("date") or item.get("date_posted"))
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


def _prioritize_queries(career_sleeve_key, queries):
    state = _load_query_performance_state()
    career_sleeve_state = state.get((career_sleeve_key or "").upper(), {})
    qp_cfg = RUNTIME_CONFIG.get("query_performance", {})
    min_runs = int(qp_cfg.get("min_runs_before_prune", 3))
    min_avg = float(qp_cfg.get("min_avg_parsed_per_page", 0.5))
    min_keep = int(qp_cfg.get("min_queries_to_keep", 8))

    scored = []
    for query in queries:
        q_state = career_sleeve_state.get(query, {})
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


def _update_query_performance_from_diagnostics(diagnostics, career_sleeve_key):
    state = _load_query_performance_state()
    career_sleeve = (career_sleeve_key or "").upper()
    career_sleeve_state = state.setdefault(career_sleeve, {})
    for entry in (diagnostics.get("source_query_summary") or {}).values():
        query = _clean_value(entry.get("query"), "")
        if not query:
            continue
        q_state = career_sleeve_state.setdefault(
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
    run_id = _clean_value(diagnostics.get("run_id"), "")
    if run_id:
        source = kwargs.get("source", "source")
        page = kwargs.get("page", "?")
        status = kwargs.get("status", 0)
        cards = int(kwargs.get("cards_found", 0))
        parsed = int(kwargs.get("parsed_count", 0))
        new_unique = int(kwargs.get("new_unique_count", 0))
        detailpages = int(kwargs.get("detailpages_fetched", 0))
        errors = int(kwargs.get("error_count", 0))
        message = (
            f"{source} p{page}: status {status}, cards {cards}, parsed {parsed}, "
            f"new {new_unique}, detail {detailpages}, errors {errors}"
        )
        _progress_update(
            run_id,
            "page",
            message,
            source=source,
            query=kwargs.get("query", ""),
            location=kwargs.get("location", ""),
            page=page,
            status=status,
            cards_found=cards,
            parsed_count=parsed,
            new_unique_count=new_unique,
            detailpages_fetched=detailpages,
            error_count=errors,
            blocked_detected=bool(kwargs.get("blocked_detected")),
        )


def _progress_from_diagnostics(diagnostics, stage, message, **data):
    run_id = _clean_value((diagnostics or {}).get("run_id"), "")
    if not run_id:
        return
    _progress_update(run_id, stage, message, **data)


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
    # Small jitter helps avoid deterministic request signatures.
    time.sleep(random.uniform(0.05, 0.18))

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


def _display_city_label(city_key):
    key = _clean_value(city_key, "").lower()
    special = {
        "'s-hertogenbosch": "'s-Hertogenbosch",
        "s-hertogenbosch": "'s-Hertogenbosch",
        "s hertogenbosch": "'s-Hertogenbosch",
        "den bosch": "Den Bosch",
        "'s-gravenhage": "'s-Gravenhage",
        "s gravenhage": "'s-Gravenhage",
        "the hague": "The Hague",
        "den haag": "Den Haag",
    }
    if key in special:
        return special[key]
    if not key:
        return ""
    tokens = re.split(r"[\s\-]+", key)
    return " ".join(token.capitalize() for token in tokens if token)


def _score_location_proximity(location_text, raw_text="", work_mode="Unknown"):
    location_value = _clean_value(location_text, "")
    distance_km, matched_city = _estimate_distance_km(location_value)
    inferred_mode = _infer_work_mode(_normalize_text(location_value, raw_text, work_mode))
    mode = inferred_mode if inferred_mode != "Unknown" else _clean_value(work_mode, "Unknown")
    location_label = _clean_value(location_value, "Unknown")
    if matched_city:
        location_label = _display_city_label(matched_city)

    if distance_km is None:
        if mode in {"Remote", "Hybrid"} and _is_netherlands_job(location_value, raw_text):
            score = 2.4
            tier = "remote_or_hybrid"
        elif _is_netherlands_job(location_value, raw_text):
            score = 2.0
            tier = "nl_unknown_distance"
        else:
            score = 0.8
            tier = "outside_focus"
    elif distance_km <= 20:
        score = 4.0
        tier = "very_close"
    elif distance_km <= 40:
        score = 3.6
        tier = "close"
    elif distance_km <= 80:
        score = 3.1
        tier = "commutable"
    elif distance_km <= 120:
        score = 2.6
        tier = "extended_commute"
    elif distance_km <= 180:
        score = 2.0
        tier = "far"
    elif distance_km <= 260:
        score = 1.3
        tier = "very_far"
    else:
        score = 0.8
        tier = "outside_focus"

    if distance_km is not None and mode in {"Remote", "Hybrid"} and score < 2.2:
        score = 2.2
        tier = f"{tier}_remote_or_hybrid"

    return {
        "main_location": location_label,
        "distance_km": distance_km,
        "matched_city": _display_city_label(matched_city),
        "score": round(float(score), 2),
        "tier": tier,
        "anchor": HOME_LOCATION_LABEL,
    }


def _context_has_abroad_keywords(raw_text, start, end):
    text = str(raw_text or "").lower()
    left = max(0, start - 64)
    right = min(len(text), end + 64)
    context = sleeves.normalize_for_match(text[left:right])
    if not context:
        return False
    return any(keyword in context for keyword in _expanded_abroad_percent_context_keywords())


_ABROAD_PERCENT_CONTEXT_CACHE = None
_ABROAD_CONTEXT_TERMS_CACHE = None
_ABROAD_GEO_TERMS_CACHE = None


def _expanded_abroad_percent_context_keywords():
    global _ABROAD_PERCENT_CONTEXT_CACHE
    if _ABROAD_PERCENT_CONTEXT_CACHE is None:
        _ABROAD_PERCENT_CONTEXT_CACHE = _expand_terms_with_bilingual_variants(
            ABROAD_PERCENT_CONTEXT_KEYWORDS
        )
    return _ABROAD_PERCENT_CONTEXT_CACHE


def _expanded_abroad_context_terms():
    global _ABROAD_CONTEXT_TERMS_CACHE
    if _ABROAD_CONTEXT_TERMS_CACHE is None:
        _ABROAD_CONTEXT_TERMS_CACHE = _expand_terms_with_bilingual_variants(
            ABROAD_CONTEXT_TERMS
        )
    return _ABROAD_CONTEXT_TERMS_CACHE


def _expanded_abroad_geo_terms():
    global _ABROAD_GEO_TERMS_CACHE
    if _ABROAD_GEO_TERMS_CACHE is None:
        expanded = {}
        for category, entries in ABROAD_GEO_TERMS.items():
            expanded_entries = []
            for label, aliases in entries:
                expanded_entries.append(
                    (label, _expand_terms_with_bilingual_variants(aliases))
                )
            expanded[category] = expanded_entries
        _ABROAD_GEO_TERMS_CACHE = expanded
    return _ABROAD_GEO_TERMS_CACHE


def _extract_abroad_percentage(raw_text):
    text = str(raw_text or "")
    if not text:
        return None, ""
    normalized_text = text.replace("\u2013", "-").replace("\u2014", "-")

    candidates = []
    range_spans = []
    for match in re.finditer(
        r"(\d{1,3})\s*(?:-|to|tot)\s*(\d{1,3})\s*(?:%|percent|procent|percentage|pct)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        start, end = match.span()
        if not _context_has_abroad_keywords(normalized_text, start, end):
            continue
        first = int(match.group(1))
        second = int(match.group(2))
        low = max(0, min(100, min(first, second)))
        high = max(0, min(100, max(first, second)))
        candidates.append((high, f"{low}-{high}%"))
        range_spans.append((start, end))

    for match in re.finditer(
        r"(\d{1,3})\s*(?:%|percent|procent|percentage|pct)",
        normalized_text,
        flags=re.IGNORECASE,
    ):
        start, end = match.span()
        if any(start < span_end and end > span_start for span_start, span_end in range_spans):
            continue
        if not _context_has_abroad_keywords(normalized_text, start, end):
            continue
        value = max(0, min(100, int(match.group(1))))
        candidates.append((value, f"{value}%"))

    if not candidates:
        return None, ""
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][0], candidates[0][1]


def _has_abroad_context(raw_text):
    text = sleeves.normalize_for_match(raw_text)
    if not text:
        return False
    return any(term in text for term in _expanded_abroad_context_terms())


def _alias_has_abroad_context(raw_text, alias):
    text = sleeves.normalize_for_match(raw_text)
    alias_text = sleeves.normalize_for_match(alias)
    if not text or not alias_text:
        return False
    parts = [re.escape(part) for part in alias_text.split()]
    if not parts:
        return False
    pattern = r"\b" + r"\s+".join(parts) + r"\b"
    for match in re.finditer(pattern, text):
        if _context_has_abroad_keywords(text, *match.span()):
            return True
    return False


def _extract_abroad_geo_mentions(raw_text):
    prepared_text = sleeves.prepare_text(raw_text)
    has_context = _has_abroad_context(raw_text)
    geo = {"countries": [], "regions": [], "continents": []}
    for category in ("countries", "regions", "continents"):
        for label, aliases in _expanded_abroad_geo_terms().get(category, []):
            hits = sleeves.find_hits(prepared_text, aliases)
            if not hits:
                continue
            if label == "Netherlands":
                continue
            contextual_hit = has_context or any(_alias_has_abroad_context(raw_text, alias) for alias in aliases)
            if contextual_hit and label not in geo[category]:
                geo[category].append(label)
    locations = geo["countries"] + geo["regions"] + geo["continents"]
    return geo, locations


def _enhance_abroad_score(base_score, base_badges, abroad_meta, raw_text):
    score = float(base_score)
    badges = []
    seen = set()
    for badge in list(base_badges or []):
        normalized = _clean_value(badge, "")
        if normalized and normalized not in seen:
            seen.add(normalized)
            badges.append(normalized)

    percentage = abroad_meta.get("percentage")
    geo_mentions = abroad_meta.get("locations") or []
    text = _normalize_text(raw_text)
    has_intensity_phrase = any(
        phrase in text
        for phrase in (
            "frequent travel",
            "regelmatig reizen",
            "extensive travel",
            "travel heavy",
            "high travel",
            "veel reizen",
            "travel required",
            "international mobility",
        )
    )

    if percentage is not None:
        if percentage >= 50:
            score += 1.2
        elif percentage >= 30:
            score += 0.8
        elif percentage >= 15:
            score += 0.5
        if "travel_percentage" not in seen:
            badges.append("travel_percentage")
            seen.add("travel_percentage")
    elif has_intensity_phrase:
        score += 0.5
        if "travel_intensity" not in seen:
            badges.append("travel_intensity")
            seen.add("travel_intensity")

    if len(geo_mentions) >= 3:
        score += 0.9
        if "geo_scope" not in seen:
            badges.append("geo_scope")
            seen.add("geo_scope")
    elif len(geo_mentions) >= 1:
        score += 0.5
        if "geo_scope" not in seen:
            badges.append("geo_scope")
            seen.add("geo_scope")

    score = max(0.0, min(float(sleeves.ABROAD_SCORE_CAP), round(score, 2)))
    return score, badges


def _derive_abroad_identifiers(percentage, locations, raw_text, badges=None):
    identifiers = []
    seen = set()

    def add_identifier(value):
        normalized = sleeves.normalize_for_match(value)
        if not normalized:
            return
        slug = normalized.replace(" ", "_")
        if slug in seen:
            return
        seen.add(slug)
        identifiers.append(slug)

    for badge in badges or []:
        add_identifier(badge)
    if percentage is not None:
        add_identifier("travel_percentage")
    if locations:
        add_identifier("geo_scope")
    if _has_abroad_context(raw_text):
        add_identifier("international_context")
    return identifiers


def _extract_abroad_metadata(raw_text):
    percentage, percentage_text = _extract_abroad_percentage(raw_text)
    geo, locations = _extract_abroad_geo_mentions(raw_text)
    return {
        "percentage": percentage,
        "percentage_text": percentage_text,
        "countries": geo["countries"],
        "regions": geo["regions"],
        "continents": geo["continents"],
        "locations": locations,
        "identifiers": _derive_abroad_identifiers(percentage, locations, raw_text),
    }


def _passes_location_gate(text, location_mode):
    _ = _normalized_location_mode(location_mode)
    normalized_text = _normalize_text(text).strip()
    if not normalized_text:
        return True

    is_netherlands = _is_netherlands_job(normalized_text)
    is_vietnam = _is_vietnam_job(normalized_text)
    if is_netherlands or is_vietnam:
        return True

    has_non_eu_hint = any(keyword in normalized_text for keyword in NON_EU_COUNTRY_KEYWORDS)
    if has_non_eu_hint and not is_vietnam:
        return False

    is_remote_or_hybrid = any(
        marker in normalized_text
        for marker in (
            "remote",
            "hybrid",
            "hybride",
            "wfh",
            "work from home",
            "thuiswerk",
            "op afstand",
        )
    )
    has_target_hint = any(marker in normalized_text for marker in TARGET_REMOTE_HINTS)
    has_travel_or_visa_context = any(
        marker in normalized_text
        for marker in (
            "travel",
            "reizen",
            "visa",
            "work permit",
            "relocation",
            "international",
            "global mobility",
        )
    )
    if is_remote_or_hybrid and has_target_hint:
        return True
    if has_target_hint and has_travel_or_visa_context:
        return True
    return False


def _build_location_gate_text(location, query_location="", work_mode_hint="", raw_text=""):
    parts = []
    location_cleaned = _clean_value(location, "")
    location_known = bool(location_cleaned) and location_cleaned.lower() not in {
        "unknown",
        "not listed",
        "n/a",
        "na",
    }
    if location_known:
        parts.append(location_cleaned)
    else:
        query_cleaned = _clean_value(query_location, "")
        if query_cleaned and query_cleaned.lower() not in {"unknown", "not listed", "n/a", "na"}:
            parts.append(query_cleaned)
    hint_cleaned = _clean_value(work_mode_hint, "")
    if hint_cleaned and hint_cleaned.lower() not in {"unknown", "not listed", "n/a", "na"}:
        parts.append(hint_cleaned)
    narrowed = _normalize_text(*parts).strip()
    if narrowed:
        return narrowed
    return _normalize_text(raw_text)


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


def _is_vietnam_job(*parts):
    text = _normalize_text(*parts)
    return any(keyword in text for keyword in VIETNAM_KEYWORDS)


def _location_passes_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    pass_ids = sleeves.LOCATION_MODE_PASSES.get(
        mode,
        sleeves.LOCATION_MODE_PASSES.get(MVP_LOCATION_MODE, ["nl", "vn"]),
    )
    locations = []
    seen = set()
    for pass_id in pass_ids:
        for location in sleeves.SEARCH_LOCATIONS.get(pass_id, []):
            cleaned = _clean_value(location, "")
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            locations.append(cleaned)
    return locations or ["Netherlands", "Vietnam"]


_BILINGUAL_TOKEN_GROUPS = [
    {"operations", "operaties", "operationeel", "operationele", "operational"},
    {"operation", "operatie"},
    {"analyst", "analist"},
    {"analysis", "analyse"},
    {"insights", "inzichten"},
    {"workflow", "werkstroom"},
    {"delivery", "levering"},
    {"implementation", "implementatie"},
    {"rollout", "uitrol"},
    {"manager"},
    {"coordinator", "coordinatie"},
    {"specialist"},
    {"strategy", "strategie"},
    {"strategic", "strategisch"},
    {"technical", "technisch"},
    {"engineer", "ingenieur"},
    {"infrastructure", "infrastructuur"},
    {"reliability", "betrouwbaarheid"},
    {"facility", "faciliteit"},
    {"facilities", "faciliteiten"},
    {"critical", "kritisch", "kritieke"},
    {"supply", "toelevering"},
    {"chain", "keten"},
    {"logistics", "logistiek"},
    {"vendor", "leverancier"},
    {"partner"},
    {"guest", "gast"},
    {"experience", "ervaring"},
    {"theme", "thema"},
    {"park", "pretpark"},
    {"travel", "travelling", "traveling", "reizen", "reisbereidheid"},
    {"international", "internationaal", "internationale"},
    {"global", "wereldwijd", "wereldwijde"},
    {"abroad", "buitenland", "overseas", "overzee"},
    {"hybrid", "hybride"},
    {"europe", "europa"},
    {"asia", "azie"},
    {"africa", "afrika"},
    {"north", "noord"},
    {"south", "zuid"},
    {"america", "amerika"},
    {"middle", "midden"},
    {"east", "oosten"},
    {"united", "verenigde"},
    {"states", "staten"},
    {"kingdom", "koninkrijk"},
    {"country", "land"},
    {"countries", "landen"},
    {"mobile", "mobiel"},
    {"mobility", "mobiliteit"},
    {"region", "regio"},
]

_BILINGUAL_PHRASE_GROUPS = [
    {"data center", "data centre", "datacenter"},
    {"supply chain", "toeleveringsketen"},
    {"theme park", "pretpark"},
    {"guest experience", "gastervaring"},
    {"critical facilities", "kritieke faciliteiten"},
    {"facility operations", "facilitaire operaties"},
    {"work from home", "thuiswerk"},
    {"work from abroad", "werken vanuit buitenland"},
    {"international travel", "internationaal reizen"},
    {"client site", "klantlocatie"},
    {"site visit", "sitebezoek"},
    {"site visits", "sitebezoeken"},
    {"remote within europe", "op afstand binnen europa"},
    {"on site", "op locatie"},
    {"european union", "europese unie"},
    {"north america", "noord amerika"},
    {"south america", "zuid amerika"},
    {"middle east", "midden oosten"},
    {"asia pacific", "azie pacific"},
    {"united states", "verenigde staten"},
    {"united kingdom", "verenigd koninkrijk"},
]


def _build_bilingual_lookup(groups):
    return sleeves._build_bilingual_lookup(groups)


_BILINGUAL_TOKEN_LOOKUP = _build_bilingual_lookup(_BILINGUAL_TOKEN_GROUPS)
_BILINGUAL_PHRASE_LOOKUP = _build_bilingual_lookup(_BILINGUAL_PHRASE_GROUPS)


def _bilingual_query_variants(term, max_variants=24):
    normalized_term = sleeves.normalize_for_match(term)
    if not normalized_term:
        return []

    limit = max(1, int(max_variants))
    variants = {normalized_term}
    tokens = normalized_term.split()
    if tokens:
        per_token_options = []
        for token in tokens:
            token_variants = _BILINGUAL_TOKEN_LOOKUP.get(token, [token])
            per_token_options.append(list(token_variants)[:4])
        for option_tuple in itertools.product(*per_token_options):
            candidate = sleeves.normalize_for_match(" ".join(option_tuple))
            if candidate:
                variants.add(candidate)
            if len(variants) >= limit:
                break

    for phrase, phrase_group in _BILINGUAL_PHRASE_LOOKUP.items():
        if phrase not in normalized_term:
            continue
        for phrase_variant in phrase_group:
            if phrase_variant == phrase:
                continue
            candidate = sleeves.normalize_for_match(normalized_term.replace(phrase, phrase_variant))
            if candidate:
                variants.add(candidate)
            if len(variants) >= limit:
                break
        if len(variants) >= limit:
            break
    return sorted(variants)


def _expand_terms_with_bilingual_variants(queries):
    expanded = []
    for query in queries or []:
        normalized_term = sleeves.normalize_for_match(query)
        if not normalized_term:
            continue
        expanded.append(normalized_term)
        for variant in _bilingual_query_variants(normalized_term):
            if variant == normalized_term:
                continue
            expanded.append(variant)
    return _dedupe_queries(expanded)


def _parse_search_queries(raw_value):
    raw_text = str(raw_value or "")
    if not raw_text.strip():
        return []
    parts = re.split(r"[,;\n\r|]+", raw_text)
    parsed_queries = []
    seen = set()
    for part in parts:
        cleaned = _clean_value(part, "")
        if len(cleaned) < 2:
            continue
        normalized = sleeves.normalize_for_match(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parsed_queries.append(cleaned)
    return parsed_queries


def _parse_extra_queries(raw_value):
    return _parse_search_queries(raw_value)


def _dedupe_queries(queries):
    ordered = []
    seen = set()
    for query in queries or []:
        cleaned = _clean_value(query, "")
        if len(cleaned) < 2:
            continue
        normalized = sleeves.normalize_for_match(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(cleaned)
    return ordered


def _search_query_bundle_for_career_sleeve(career_sleeve_key, search_queries=None, extra_queries=None):
    career_sleeve = (career_sleeve_key or "").upper()
    overrides = (RUNTIME_CONFIG.get("query_overrides") or {}).get(career_sleeve, [])
    base_queries = (
        overrides
        if isinstance(overrides, list) and overrides
        else sleeves.CAREER_SLEEVE_SEARCH_QUERIES.get(career_sleeve, [])
    )
    ordered_queries = _dedupe_queries(search_queries if search_queries else base_queries)
    ordered_queries = _expand_terms_with_bilingual_variants(ordered_queries)
    if extra_queries:
        ordered_queries = _dedupe_queries(ordered_queries + list(extra_queries))
        ordered_queries = _expand_terms_with_bilingual_variants(ordered_queries)
    return _prioritize_queries(career_sleeve, ordered_queries)


def _source_headers(source_name, location_mode=MVP_LOCATION_MODE, user_agent=""):
    mode = _normalized_location_mode(location_mode)
    source_name_text = _clean_value(source_name, "").lower()
    if mode == "nl_vn":
        accept_language = "en-US,en;q=0.9,nl-NL;q=0.8,nl;q=0.7,vi;q=0.6"
        referer = "https://www.google.com/"
    else:
        accept_language = "en-US,en;q=0.9"
        referer = "https://www.google.com/"
    if "linkedin" in source_name_text:
        referer = "https://www.linkedin.com/jobs/"
    agent = _clean_value(user_agent, "")
    if not agent:
        agent = REQUEST_USER_AGENTS[0]
    return {
        "User-Agent": agent,
        "Accept-Language": accept_language,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer,
    }


def _configure_session_for_scrape(session):
    http_proxy = _clean_value(os.getenv("SCRAPE_HTTP_PROXY"), "")
    https_proxy = _clean_value(os.getenv("SCRAPE_HTTPS_PROXY"), "")
    if not https_proxy and http_proxy:
        https_proxy = http_proxy
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    if proxies:
        session.proxies.update(proxies)


def _indeed_rss_url_for_mode(location_mode):
    mode = _normalized_location_mode(location_mode)
    if mode == "nl_vn":
        return "https://www.indeed.com/rss"
    return "https://www.indeed.com/rss"


def _compact_whitespace(values):
    if isinstance(values, str):
        values = [values]
    return _clean_value(" ".join(str(value or "") for value in values), "")


def _looks_like_salary_text(value):
    text = _clean_value(value, "")
    if not text:
        return False
    pattern = (
        r"(\u20ac|\$|\u00a3|\u00a5|\beur\b|\busd\b|\bgbp\b|"
        r"\bper\s*(uur|hour|maand|month|jaar|year)\b|"
        r"/\s*(uur|h|maand|month|jaar|yr)|"
        r"\d+\s*-\s*\d+|\d+[.,]?\d*\s*[kK])"
    )
    return bool(re.search(pattern, text, flags=re.IGNORECASE))


def _extract_salary_from_chunks(chunks):
    if not chunks:
        return ""
    seen = set()
    for raw in chunks:
        cleaned = _clean_value(raw, "")
        if not cleaned:
            continue
        normalized = cleaned.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        if _looks_like_salary_text(cleaned):
            return cleaned
    return ""


def _normalize_indeed_location_and_mode(location_text):
    location = _clean_value(location_text, "")
    if not location:
        return "", ""
    normalized = location.lower()

    patterns = [
        (r"^\s*hybride?\s+werken\s+in\s+", "Hybrid"),
        (r"^\s*remote\s+in\s+", "Remote"),
        (r"^\s*op\s+afstand\s+in\s+", "Remote"),
        (r"^\s*thuiswerk\s+in\s+", "Remote"),
        (r"^\s*werk\s+vanuit\s+huis\s+in\s+", "Remote"),
        (r"^\s*on[\s-]?site\s+in\s+", "On-site"),
        (r"^\s*op\s+locatie\s+in\s+", "On-site"),
        (r"^\s*op\s+kantoor\s+in\s+", "On-site"),
    ]
    for pattern, mode in patterns:
        if re.search(pattern, normalized):
            cleaned_location = re.sub(pattern, "", location, flags=re.IGNORECASE).strip(" -:,")
            return (_clean_value(cleaned_location, location), mode)
    return location, ""


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
        metadata_parts = card.css(
            "[data-testid='attribute_snippet_testid'] *::text, "
            "[data-testid='attribute_snippet'] *::text, "
            "div.metadata *::text, "
            "div.jobMetaDataGroup *::text, "
            "div.jobsearch-JobMetadataHeader-item *::text"
        ).getall()
        salary_parts = card.css(
            "div.salary-snippet-container *::text, "
            "div.metadata.salary-snippet-container *::text, "
            "span.estimated-salary *::text, "
            "span.salaryText *::text, "
            "[data-testid='attribute_snippet_testid'] *::text, "
            "[data-testid='attribute_snippet_compensation'] *::text"
        ).getall()
        raw_location = (
            card.css("[data-testid='text-location']::text").get()
            or card.css("div.companyLocation::text").get()
        )
        location_value, location_mode_hint = _normalize_indeed_location_and_mode(raw_location)
        salary_value = _extract_salary_from_chunks(salary_parts) or _extract_salary_from_chunks(metadata_parts)
        metadata_text = _compact_whitespace(metadata_parts)
        work_mode_hint = _compact_whitespace([location_mode_hint, metadata_text])
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
            "location": _clean_value(location_value, ""),
            "link": requests.compat.urljoin(response_url, link) if link else "",
            "snippet": _compact_whitespace(snippet_parts),
            "salary": _clean_value(salary_value, "Not listed"),
            "work_mode_hint": work_mode_hint,
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


def _parse_indeed_rss_items(xml_text, response_url):
    text = _clean_value(xml_text, "")
    if not text:
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    items = []
    for node in root.findall(".//item"):
        raw_title = _clean_value(node.findtext("title"), "")
        raw_link = _clean_value(node.findtext("link"), "")
        raw_description = _clean_value(node.findtext("description"), "")
        raw_date = _clean_value(node.findtext("pubDate"), "Unknown")

        title = raw_title
        company = ""
        location = ""
        parts = [part.strip() for part in raw_title.split(" - ") if part.strip()]
        if len(parts) >= 3:
            title, company, location = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            title, company = parts[0], parts[1]

        snippet = _strip_html(raw_description)
        salary = _extract_salary_from_chunks([raw_description])
        link = requests.compat.urljoin(response_url, raw_link) if raw_link else ""

        parsed_item = {
            "title": _clean_value(title, ""),
            "company": _clean_value(company, ""),
            "location": _clean_value(location, ""),
            "link": _clean_value(link, ""),
            "snippet": _clean_value(snippet, ""),
            "salary": _clean_value(salary, "Not listed"),
            "work_mode_hint": _normalize_text(snippet, location),
            "date": raw_date,
            "source": "Indeed",
        }
        if parsed_item["title"] or parsed_item["link"]:
            items.append(parsed_item)
    return items


def _warmup_indeed_session(
    session,
    request_headers,
    domain_state,
    requests_per_second,
    location_mode,
):
    home_url = "https://www.indeed.com/" if _normalized_location_mode(location_mode) == "nl_vn" else "https://www.indeed.com/"
    response, _ = _rate_limited_get(
        session,
        home_url,
        params=None,
        headers=request_headers,
        domain_state=domain_state,
        requests_per_second=requests_per_second,
        timeout_seconds=DEFAULT_HTTP_TIMEOUT,
        max_retries=1,
    )
    if response is None:
        return False
    if response.status_code in {401, 403, 429}:
        return False
    return not sleeves.detect_blocked_html(response.text)


def _fetch_indeed_rss_fallback(
    session,
    query,
    location,
    diagnostics,
    domain_state,
    requests_per_second,
    location_mode,
    request_headers,
):
    rss_url = _indeed_rss_url_for_mode(location_mode)
    response, error = _rate_limited_get(
        session,
        rss_url,
        params={"q": query, "l": location},
        headers=request_headers,
        domain_state=domain_state,
        requests_per_second=requests_per_second,
        timeout_seconds=DEFAULT_HTTP_TIMEOUT,
        max_retries=DEFAULT_HTTP_RETRIES,
    )
    status = response.status_code if response is not None else 0
    body = response.text if response is not None else ""
    blocked = bool(
        status in {401, 403, 429}
        or sleeves.detect_blocked_html(body)
    )
    if blocked:
        _record_blocked(diagnostics, "Indeed")
        if body:
            _save_html_snapshot(
                "Indeed",
                query,
                0,
                body,
                "rss-blocked",
                diagnostics,
            )
        return [], error or "rss_blocked", blocked

    if response is None or not response.ok:
        return [], error or f"rss_status_{status}", False

    items = _parse_indeed_rss_items(body, response.url or rss_url)
    return items, "", False


def _extract_linkedin_job_id(value):
    text = _clean_value(value, "")
    if not text:
        return ""
    match = re.search(r"(?:jobPosting:|jobs/view/)(\d+)", text)
    if match:
        return match.group(1)
    return ""


def _detect_linkedin_blocked(body_text, cards_found=0, parsed_count=0):
    text = _clean_value(body_text, "").lower()
    if not text:
        return False
    hard_markers = [
        "captcha",
        "access denied",
        "security verification",
        "verify you are human",
        "unusual traffic",
    ]
    if any(marker in text for marker in hard_markers):
        return True
    # LinkedIn often serves signin wall HTML to guest scraping; only treat as block
    # when no useful cards were parsed.
    soft_markers = [
        "sign in to linkedin",
        "join linkedin",
        "login",
        "challenge",
    ]
    if cards_found == 0 and parsed_count == 0 and any(marker in text for marker in soft_markers):
        return True
    return False


def _parse_linkedin_cards(selector, response_url):
    cards = selector.css("li")
    parsed = []
    for card in cards:
        link = (
            card.css("a.base-card__full-link::attr(href)").get()
            or card.css("a.base-card-link::attr(href)").get()
            or card.css("a::attr(href)").get()
        )
        title = _clean_value(
            card.css("h3.base-search-card__title::text").get()
            or card.css("h3::text").get(),
            "",
        )
        company = _clean_value(
            card.css("h4.base-search-card__subtitle a::text").get()
            or card.css("h4.base-search-card__subtitle::text").get(),
            "",
        )
        location = _clean_value(
            card.css("span.job-search-card__location::text").get()
            or card.css("span.base-search-card__metadata::text").get(),
            "",
        )
        date_value = _clean_value(
            card.css("time::attr(datetime)").get()
            or card.css("time::text").get(),
            "Unknown",
        )
        snippet = _compact_whitespace(
            card.css(
                "p.job-search-card__snippet *::text, "
                "div.base-search-card__metadata *::text"
            ).getall()
        )
        full_link = requests.compat.urljoin(response_url, link) if link else ""
        job_id = (
            _extract_linkedin_job_id(card.attrib.get("data-entity-urn"))
            or _extract_linkedin_job_id(full_link)
        )
        parsed_item = {
            "title": title,
            "company": company,
            "location": location,
            "link": _clean_value(full_link, ""),
            "snippet": _clean_value(snippet, ""),
            "salary": "Not listed",
            "work_mode_hint": _normalize_text(location, snippet),
            "date": date_value,
            "source": "LinkedIn",
            "job_id": job_id,
        }
        if parsed_item["title"] or parsed_item["link"]:
            parsed.append(parsed_item)
    return cards, parsed


def _extract_linkedin_links_from_detail(html_text, response_url):
    selector = Selector(text=html_text or "")
    response_url = _clean_value(response_url, "")
    linkedin_url = response_url if "linkedin.com" in _host_for_url(response_url) else ""
    company_candidates = []

    for anchor in selector.css("a"):
        href_raw = anchor.attrib.get("href", "")
        if not href_raw:
            continue
        href = requests.compat.urljoin(response_url, _decode_embedded_url(href_raw))
        if not _is_absolute_http_url(href):
            continue
        external_destination = _extract_external_destination_from_url(href)
        if external_destination and not _is_platform_job_host(external_destination):
            company_candidates.append(external_destination)
        if not _is_platform_job_host(href):
            text_blob = _normalize_text(
                anchor.attrib.get("id"),
                anchor.attrib.get("class"),
                anchor.attrib.get("data-tracking-control-name"),
                anchor.attrib.get("aria-label"),
                _compact_whitespace(anchor.css("*::text").getall()),
            )
            if any(marker in text_blob for marker in ["apply", "external", "company website", "website"]):
                company_candidates.append(href)
        elif not linkedin_url:
            linkedin_url = href

    company_url = ""
    seen = set()
    for candidate in company_candidates:
        normalized = _clean_value(candidate, "")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        if not _is_platform_job_host(normalized):
            company_url = normalized
            break

    return {
        "linkedin_url": linkedin_url if _is_absolute_http_url(linkedin_url) else "",
        "company_url": company_url if _is_absolute_http_url(company_url) else "",
    }


def _decode_embedded_url(value):
    text = _clean_value(value, "")
    if not text:
        return ""
    text = (
        text.replace("\\u002F", "/")
        .replace("\\u003A", ":")
        .replace("\\u0026", "&")
        .replace("\\/", "/")
        .replace("&amp;", "&")
    )
    if text.startswith("//"):
        text = f"https:{text}"
    return text


def _decode_url_repeatedly(value, rounds=4):
    text = _clean_value(value, "")
    if not text:
        return ""
    for _ in range(max(1, int(rounds))):
        decoded = _decode_embedded_url(unquote(text))
        if decoded == text:
            break
        text = decoded
    return text


def _extract_external_destination_from_url(url):
    parsed = urlparse(_clean_value(url, ""))
    if not parsed.netloc:
        return ""

    redirect_keys = {
        "adurl",
        "dest",
        "destination",
        "destinationurl",
        "desturl",
        "dest_url",
        "redirect",
        "redirecturl",
        "redirect_uri",
        "next",
        "rurl",
        "clickurl",
        "ad_url",
        "url",
        "u",
        "uddg",
        "target",
    }
    query_pairs = list(parse_qsl(parsed.query, keep_blank_values=False))
    if parsed.fragment and "=" in parsed.fragment:
        query_pairs.extend(parse_qsl(parsed.fragment, keep_blank_values=False))

    for key, value in query_pairs:
        if key.lower() not in redirect_keys:
            continue
        candidate = _decode_url_repeatedly(value, rounds=5)
        if candidate.startswith("/"):
            candidate = requests.compat.urljoin(
                f"{parsed.scheme or 'https'}://{parsed.netloc}",
                candidate,
            )
        if _is_public_destination_url(candidate) and not _is_indeed_host(_host_for_url(candidate)):
            return candidate
    return ""


def _resolve_external_from_indeed_redirect(url, timeout_seconds=8, max_hops=4, headers=None):
    current = _clean_value(url, "")
    if not _is_allowed_platform_lookup_url(current) or not _is_indeed_host(_host_for_url(current)):
        return ""

    for _ in range(max(1, int(max_hops))):
        direct = _extract_external_destination_from_url(current)
        if _is_public_destination_url(direct) and not _is_indeed_host(_host_for_url(direct)):
            return direct

        if not _is_indeed_host(_host_for_url(current)):
            return current if _is_public_destination_url(current) else ""

        try:
            response = requests.get(
                current,
                headers=headers,
                timeout=timeout_seconds,
                allow_redirects=False,
            )
        except requests.RequestException:
            return ""

        location = _decode_url_repeatedly(response.headers.get("Location", ""), rounds=5)
        if not location:
            alt = _extract_external_destination_from_url(response.url or current)
            if _is_absolute_http_url(alt) and "indeed." not in _host_for_url(alt):
                return alt
            return ""

        next_url = requests.compat.urljoin(response.url or current, location)
        if _is_public_destination_url(next_url) and not _is_indeed_host(_host_for_url(next_url)):
            return next_url
        current = next_url

    return ""


def _extract_indeed_links_from_detail(html_text, response_url):
    selector = Selector(text=html_text or "")
    response_url = _clean_value(response_url, "")
    indeed_url = response_url if "indeed." in _host_for_url(response_url) else ""
    company_candidates = []

    for anchor in selector.css("a"):
        href_raw = anchor.attrib.get("href", "")
        if not href_raw:
            continue
        href = requests.compat.urljoin(response_url, _decode_embedded_url(href_raw))
        if not _is_absolute_http_url(href):
            continue

        external_destination = _extract_external_destination_from_url(href)
        if external_destination:
            company_candidates.append(external_destination)

        for attr_key, attr_value in (anchor.attrib or {}).items():
            key_text = _clean_value(attr_key, "").lower()
            if not key_text or ("url" not in key_text and "apply" not in key_text):
                continue
            candidate = requests.compat.urljoin(response_url, _decode_embedded_url(attr_value))
            if not _is_absolute_http_url(candidate):
                continue
            external_candidate = _extract_external_destination_from_url(candidate)
            if external_candidate:
                company_candidates.append(external_candidate)
                continue
            if "indeed." not in _host_for_url(candidate):
                company_candidates.append(candidate)

        host = _host_for_url(href)
        text_blob = _normalize_text(
            anchor.attrib.get("id"),
            anchor.attrib.get("class"),
            anchor.attrib.get("data-testid"),
            anchor.attrib.get("aria-label"),
            _compact_whitespace(anchor.css("*::text").getall()),
        )
        has_apply_marker = any(
            marker in text_blob
            for marker in ["apply", "sollic", "bewerb", "company", "website", "extern"]
        )

        if "indeed." in host:
            if not indeed_url:
                indeed_url = href
            continue
        if has_apply_marker:
            company_candidates.append(href)

    json_patterns = [
        r'"(?:applyUrl|companyApplyUrl|externalApplyUrl|externalUrl|companyPageUrl)"\s*:\s*"([^"]+)"',
        r'"(?:applyLink|jobUrl|job_url|thirdPartyApplyUrl|redirectUrl|companyJobUrl|externalApplyLink)"\s*:\s*"([^"]+)"',
    ]
    for pattern in json_patterns:
        for match in re.finditer(pattern, html_text or "", flags=re.IGNORECASE):
            candidate = requests.compat.urljoin(response_url, _decode_embedded_url(match.group(1)))
            if not _is_absolute_http_url(candidate):
                continue
            external_destination = _extract_external_destination_from_url(candidate)
            if external_destination:
                company_candidates.append(external_destination)
            host = _host_for_url(candidate)
            if "indeed." in host:
                if not indeed_url:
                    indeed_url = candidate
            else:
                company_candidates.append(candidate)

    for encoded in re.findall(r"https?%3A%2F%2F[^\s\"'<>]+", html_text or "", flags=re.IGNORECASE):
        candidate = _decode_embedded_url(unquote(unquote(encoded)))
        if not _is_absolute_http_url(candidate):
            continue
        external_candidate = _extract_external_destination_from_url(candidate)
        if external_candidate:
            company_candidates.append(external_candidate)
            continue
        if "indeed." not in _host_for_url(candidate):
            company_candidates.append(candidate)

    company_url = ""
    deduped_candidates = []
    seen = set()
    for candidate in company_candidates:
        if not _is_absolute_http_url(candidate):
            continue
        normalized = candidate.strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped_candidates.append(normalized)

    for candidate in deduped_candidates:
        if "indeed." not in _host_for_url(candidate):
            company_url = candidate
            break

    return {
        "indeed_url": indeed_url if _is_absolute_http_url(indeed_url) else "",
        "company_url": company_url if _is_absolute_http_url(company_url) else "",
    }


def _fetch_detail_page_text(
    session,
    url,
    source_name,
    diagnostics,
    domain_state,
    detail_rps,
    location_mode=MVP_LOCATION_MODE,
    request_headers=None,
):
    link = _clean_value(url, "")
    if not link:
        return "", True, 0, {"indeed_url": "", "linkedin_url": "", "company_url": ""}

    response, error = _rate_limited_get(
        session,
        link,
        params=None,
        headers=request_headers or _source_headers(source_name, location_mode),
        domain_state=domain_state,
        requests_per_second=detail_rps,
        timeout_seconds=DEFAULT_HTTP_TIMEOUT,
        max_retries=DEFAULT_HTTP_RETRIES,
    )
    error_count = 1 if error else 0
    if error or response is None:
        return "", True, error_count, {"indeed_url": "", "linkedin_url": "", "company_url": ""}

    detail_links = {"indeed_url": "", "linkedin_url": "", "company_url": ""}
    if source_name == "Indeed":
        detail_links = _extract_indeed_links_from_detail(response.text, response.url or link)
        if not detail_links.get("indeed_url") and _is_absolute_http_url(link):
            detail_links["indeed_url"] = link
    elif source_name == "LinkedIn":
        detail_links = _extract_linkedin_links_from_detail(response.text, response.url or link)
        if not detail_links.get("linkedin_url") and _is_absolute_http_url(link):
            detail_links["linkedin_url"] = link

    if source_name == "LinkedIn":
        blocked = response.status_code in {401, 403, 429, 999} or _detect_linkedin_blocked(response.text)
    else:
        blocked = response.status_code in {401, 403, 429} or sleeves.detect_blocked_html(response.text)
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
        return "", True, error_count + 1, detail_links

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
        return "", True, error_count, detail_links
    return description, False, error_count, detail_links


def _fetch_indeed_jobs_direct(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    diagnostics=None,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
):
    diagnostics = diagnostics or _new_diagnostics()
    search_url = _indeed_search_url_for_mode(location_mode)
    jobs = []
    seen_unique = set()
    domain_state = {}
    queries = _search_query_bundle_for_career_sleeve(
        career_sleeve_key,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )
    locations = _location_passes_for_mode(location_mode)
    session = requests.Session()
    _configure_session_for_scrape(session)
    session_user_agent = random.choice(REQUEST_USER_AGENTS)
    request_headers = _source_headers("Indeed", location_mode, user_agent=session_user_agent)
    anti_block_cfg = RUNTIME_CONFIG.get("anti_block") or {}
    prefer_rss_first = bool(anti_block_cfg.get("prefer_rss_first", True))
    skip_html_if_rss_has_items = bool(anti_block_cfg.get("skip_html_if_rss_has_items", True))
    rss_skip_threshold = max(1, int(anti_block_cfg.get("rss_skip_threshold_per_query", 5)))
    disable_detail_fetch = bool(anti_block_cfg.get("disable_detail_fetch", True))
    warmup_gate_to_rss_only = bool(anti_block_cfg.get("warmup_gate_to_rss_only", True))
    detail_base_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("base_budget_per_page", DEFAULT_DETAIL_FETCH_BASE_BUDGET))
    detail_reduced_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("reduced_budget_per_page", DEFAULT_DETAIL_FETCH_REDUCED_BUDGET))
    detail_failures = 0
    detail_attempts = 0
    warmup_ok = _warmup_indeed_session(
        session,
        request_headers,
        domain_state,
        requests_per_second,
        location_mode,
    )

    def _ingest_rss_items(rss_items, query, location):
        rss_new_unique = 0
        for item in rss_items:
            dedupe_key, _, _ = _build_dedupe_key(item)
            if dedupe_key in seen_unique:
                continue
            seen_unique.add(dedupe_key)
            rss_new_unique += 1
            item["full_description"] = ""
            item["detail_fetch_failed"] = True
            item["indeed_url"] = _clean_value(item.get("link"), "")
            item["linkedin_url"] = ""
            item["company_url"] = _extract_external_destination_from_url(item["indeed_url"])
            item["query"] = query
            item["query_location"] = location
            item["source"] = "Indeed"
            jobs.append(item)
        return rss_new_unique

    for query in queries:
        for location in locations:
            previous_jobs = len(jobs)
            no_new_unique_streak = 0
            blocked_in_query = False
            last_response_body = ""
            rss_attempted = False
            _progress_from_diagnostics(
                diagnostics,
                "query-start",
                f"Indeed: started query '{query}' in {location}",
                source="Indeed",
                query=query,
                location=location,
                max_pages=int(max_pages),
            )

            if prefer_rss_first:
                rss_attempted = True
                rss_items, rss_error, rss_blocked = _fetch_indeed_rss_fallback(
                    session,
                    query,
                    location,
                    diagnostics,
                    domain_state,
                    requests_per_second,
                    location_mode,
                    request_headers,
                )
                rss_new_unique = _ingest_rss_items(rss_items, query, location)
                _log_page_metrics(
                    diagnostics,
                    source="Indeed",
                    query=query,
                    location=location,
                    page=0,
                    url=_indeed_rss_url_for_mode(location_mode),
                    status=200 if not rss_error and not rss_blocked else 0,
                    cards_found=len(rss_items),
                    parsed_count=len(rss_items),
                    new_unique_count=rss_new_unique,
                    detailpages_fetched=0,
                    full_description_count=0,
                    error_count=1 if rss_error else 0,
                    blocked_detected=rss_blocked,
                )
                if len(seen_unique) >= target_raw:
                    return jobs, diagnostics
                if skip_html_if_rss_has_items and rss_new_unique >= rss_skip_threshold:
                    _save_debug_event(
                        "Indeed",
                        query,
                        0,
                        "skip-html-after-rss",
                        diagnostics,
                        location=location,
                        rss_new_unique=rss_new_unique,
                        threshold=rss_skip_threshold,
                    )
                    continue

            if warmup_gate_to_rss_only and not warmup_ok:
                _save_debug_event(
                    "Indeed",
                    query,
                    0,
                    "warmup-blocked-rss-only",
                    diagnostics,
                    location=location,
                )
                continue

            for page_idx in range(max_pages):
                start = page_idx * 10
                params = {"q": query, "l": location, "start": start}
                _progress_from_diagnostics(
                    diagnostics,
                    "page-start",
                    f"Indeed: requesting page {page_idx + 1} for '{query}' in {location}",
                    source="Indeed",
                    query=query,
                    location=location,
                    page=int(page_idx + 1),
                )
                response, error = _rate_limited_get(
                    session,
                    search_url,
                    params=params,
                    headers=request_headers,
                    domain_state=domain_state,
                    requests_per_second=requests_per_second,
                    timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                    max_retries=DEFAULT_HTTP_RETRIES,
                )
                status = response.status_code if response is not None else 0
                body = response.text if response is not None else ""
                if body:
                    last_response_body = body
                blocked = False
                cards_found = 0
                parsed_count = 0
                new_unique_count = 0
                detailpages_fetched = 0
                full_description_count = 0
                error_count = 0
                if error:
                    error_count += 1
                if response is None or status >= 400:
                    error_count += 1
                if blocked:
                    error_count += 1
                parsed_items = []
                request_url = response.url if response is not None else search_url

                if response is not None and response.ok:
                    selector = Selector(text=body)
                    cards, parsed_items = _parse_indeed_cards(selector, request_url)
                    cards_found = len(cards)
                    parsed_count = len(parsed_items)
                    blocked = bool(
                        status in {401, 403, 429}
                        or (
                            sleeves.detect_blocked_html(body)
                            and cards_found == 0
                            and parsed_count == 0
                        )
                    )
                    if blocked:
                        time.sleep(random.uniform(1.1, 2.3))
                        retry_response, retry_error = _rate_limited_get(
                            session,
                            search_url,
                            params=params,
                            headers=request_headers,
                            domain_state=domain_state,
                            requests_per_second=max(0.2, requests_per_second * 0.7),
                            timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                            max_retries=1,
                        )
                        retry_status = retry_response.status_code if retry_response is not None else 0
                        retry_body = retry_response.text if retry_response is not None else ""
                        retry_cards_found = 0
                        retry_parsed_count = 0
                        retry_parsed_items = []
                        retry_blocked = bool(
                            retry_status in {401, 403, 429}
                            or sleeves.detect_blocked_html(retry_body)
                        )
                        if retry_response is not None and retry_response.ok:
                            retry_selector = Selector(text=retry_body)
                            retry_cards, retry_parsed_items = _parse_indeed_cards(
                                retry_selector,
                                retry_response.url or request_url,
                            )
                            retry_cards_found = len(retry_cards)
                            retry_parsed_count = len(retry_parsed_items)
                            retry_blocked = bool(
                                retry_status in {401, 403, 429}
                                or (
                                    sleeves.detect_blocked_html(retry_body)
                                    and retry_cards_found == 0
                                    and retry_parsed_count == 0
                                )
                            )
                        should_use_retry = bool(
                            retry_response is not None
                            and (
                                not retry_blocked
                                or (retry_response.ok and len(retry_body) > len(body))
                            )
                        )
                        if should_use_retry:
                            response, error = retry_response, retry_error
                            status, body = retry_status, retry_body
                            request_url = retry_response.url or request_url
                            blocked = retry_blocked
                            cards_found = retry_cards_found
                            parsed_count = retry_parsed_count
                            parsed_items = retry_parsed_items

                    if cards_found == 0 or parsed_count == 0:
                        error_count += 1
                        _save_html_snapshot(
                            "Indeed",
                            query,
                            page_idx + 1,
                            body,
                            "parse-empty",
                            diagnostics,
                        )
                    if blocked:
                        blocked_in_query = True
                        _record_blocked(diagnostics, "Indeed")
                        error_count += 1

                    fail_rate = (detail_failures / detail_attempts) if detail_attempts else 0.0
                    detail_budget = 0 if disable_detail_fetch else min(detail_base_budget, len(parsed_items))
                    if not disable_detail_fetch and (blocked or fail_rate > 0.5):
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
                        detail_links = {"indeed_url": "", "linkedin_url": "", "company_url": ""}
                        base_indeed_url = _clean_value(item.get("link"), "")
                        base_company_url = _extract_external_destination_from_url(base_indeed_url)
                        if idx < detail_budget:
                            full_description, detail_failed, detail_errors, detail_links = _fetch_detail_page_text(
                                session,
                                item.get("link"),
                                "Indeed",
                                diagnostics,
                                domain_state,
                                detail_rps,
                                location_mode,
                                request_headers=request_headers,
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
                        item["indeed_url"] = _clean_value(
                            detail_links.get("indeed_url") or base_indeed_url,
                            "",
                        )
                        item["linkedin_url"] = ""
                        item["company_url"] = _clean_value(
                            detail_links.get("company_url") or base_company_url,
                            "",
                        )
                        item["query"] = query
                        item["query_location"] = location
                        item["source"] = "Indeed"
                        jobs.append(item)
                else:
                    blocked = bool(
                        status in {401, 403, 429}
                        or sleeves.detect_blocked_html(body)
                    )
                    if blocked:
                        blocked_in_query = True
                        _record_blocked(diagnostics, "Indeed")
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
                    _progress_from_diagnostics(
                        diagnostics,
                        "query-finish",
                        f"Indeed: target reached for '{query}' in {location}",
                        source="Indeed",
                        query=query,
                        location=location,
                        new_items=int(len(jobs) - previous_jobs),
                        blocked=bool(blocked_in_query),
                    )
                    return jobs, diagnostics
            if len(jobs) == previous_jobs and not rss_attempted:
                rss_items, rss_error, rss_blocked = _fetch_indeed_rss_fallback(
                    session,
                    query,
                    location,
                    diagnostics,
                    domain_state,
                    requests_per_second,
                    location_mode,
                    request_headers,
                )
                rss_new_unique = _ingest_rss_items(rss_items, query, location)
                _log_page_metrics(
                    diagnostics,
                    source="Indeed",
                    query=query,
                    location=location,
                    page=0,
                    url=_indeed_rss_url_for_mode(location_mode),
                    status=200 if not rss_error and not rss_blocked else 0,
                    cards_found=len(rss_items),
                    parsed_count=len(rss_items),
                    new_unique_count=rss_new_unique,
                    detailpages_fetched=0,
                    full_description_count=0,
                    error_count=1 if rss_error else 0,
                    blocked_detected=rss_blocked,
                )
            elif len(jobs) == previous_jobs and blocked_in_query and rss_attempted:
                _save_debug_event(
                    "Indeed",
                    query,
                    0,
                    "rss-and-html-blocked",
                    diagnostics,
                    location=location,
                    pages_attempted=max_pages,
                )
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
            _progress_from_diagnostics(
                diagnostics,
                "query-finish",
                f"Indeed: finished query '{query}' in {location} with {len(jobs) - previous_jobs} new items",
                source="Indeed",
                query=query,
                location=location,
                new_items=int(len(jobs) - previous_jobs),
                blocked=bool(blocked_in_query),
            )
    return jobs, diagnostics


def _fetch_linkedin_jobs_direct(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    diagnostics=None,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
):
    diagnostics = diagnostics or _new_diagnostics()
    jobs = []
    seen_unique = set()
    domain_state = {}
    queries = _search_query_bundle_for_career_sleeve(
        career_sleeve_key,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )
    locations = _location_passes_for_mode(location_mode)
    session = requests.Session()
    _configure_session_for_scrape(session)
    session_user_agent = random.choice(REQUEST_USER_AGENTS)
    request_headers = _source_headers("LinkedIn", location_mode, user_agent=session_user_agent)
    anti_block_cfg = RUNTIME_CONFIG.get("anti_block") or {}
    disable_detail_fetch = bool(anti_block_cfg.get("disable_detail_fetch", True))
    detail_base_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("base_budget_per_page", DEFAULT_DETAIL_FETCH_BASE_BUDGET))
    detail_reduced_budget = int((RUNTIME_CONFIG.get("detail_fetch") or {}).get("reduced_budget_per_page", DEFAULT_DETAIL_FETCH_REDUCED_BUDGET))
    detail_failures = 0
    detail_attempts = 0

    for query in queries:
        for location in locations:
            previous_jobs = len(jobs)
            no_new_unique_streak = 0
            last_response_body = ""
            blocked_in_query = False
            _progress_from_diagnostics(
                diagnostics,
                "query-start",
                f"LinkedIn: started query '{query}' in {location}",
                source="LinkedIn",
                query=query,
                location=location,
                max_pages=int(max_pages),
            )
            for page_idx in range(max_pages):
                start = page_idx * 25
                params = {"keywords": query, "location": location, "start": start}
                _progress_from_diagnostics(
                    diagnostics,
                    "page-start",
                    f"LinkedIn: requesting page {page_idx + 1} for '{query}' in {location}",
                    source="LinkedIn",
                    query=query,
                    location=location,
                    page=int(page_idx + 1),
                )
                response, error = _rate_limited_get(
                    session,
                    LINKEDIN_SEARCH_URL,
                    params=params,
                    headers=request_headers,
                    domain_state=domain_state,
                    requests_per_second=requests_per_second,
                    timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                    max_retries=DEFAULT_HTTP_RETRIES,
                )
                status = response.status_code if response is not None else 0
                body = response.text if response is not None else ""
                if body:
                    last_response_body = body
                cards_found = 0
                parsed_count = 0
                new_unique_count = 0
                detailpages_fetched = 0
                full_description_count = 0
                error_count = 0
                if error:
                    error_count += 1
                if response is None or status >= 400:
                    error_count += 1
                parsed_items = []
                request_url = response.url if response is not None else LINKEDIN_SEARCH_URL
                blocked = False

                if response is not None and response.ok:
                    selector = Selector(text=body)
                    cards, parsed_items = _parse_linkedin_cards(selector, request_url)
                    cards_found = len(cards)
                    parsed_count = len(parsed_items)
                    blocked = bool(
                        status in {401, 403, 429, 999}
                        or _detect_linkedin_blocked(body, cards_found=cards_found, parsed_count=parsed_count)
                    )
                    if blocked:
                        time.sleep(random.uniform(1.2, 2.6))
                        retry_response, retry_error = _rate_limited_get(
                            session,
                            LINKEDIN_SEARCH_URL,
                            params=params,
                            headers=request_headers,
                            domain_state=domain_state,
                            requests_per_second=max(0.2, requests_per_second * 0.7),
                            timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                            max_retries=1,
                        )
                        retry_status = retry_response.status_code if retry_response is not None else 0
                        retry_body = retry_response.text if retry_response is not None else ""
                        retry_cards_found = 0
                        retry_parsed_count = 0
                        retry_parsed_items = []
                        retry_blocked = bool(
                            retry_status in {401, 403, 429, 999}
                            or _detect_linkedin_blocked(retry_body)
                        )
                        if retry_response is not None and retry_response.ok:
                            retry_selector = Selector(text=retry_body)
                            retry_cards, retry_parsed_items = _parse_linkedin_cards(
                                retry_selector,
                                retry_response.url or request_url,
                            )
                            retry_cards_found = len(retry_cards)
                            retry_parsed_count = len(retry_parsed_items)
                            retry_blocked = bool(
                                retry_status in {401, 403, 429, 999}
                                or _detect_linkedin_blocked(
                                    retry_body,
                                    cards_found=retry_cards_found,
                                    parsed_count=retry_parsed_count,
                                )
                            )
                        should_use_retry = bool(
                            retry_response is not None
                            and (
                                not retry_blocked
                                or (retry_response.ok and len(retry_body) > len(body))
                            )
                        )
                        if should_use_retry:
                            response, error = retry_response, retry_error
                            status, body = retry_status, retry_body
                            request_url = retry_response.url or request_url
                            blocked = retry_blocked
                            cards_found = retry_cards_found
                            parsed_count = retry_parsed_count
                            parsed_items = retry_parsed_items

                    if cards_found == 0 or parsed_count == 0:
                        error_count += 1
                        _save_html_snapshot(
                            "LinkedIn",
                            query,
                            page_idx + 1,
                            body,
                            "parse-empty",
                            diagnostics,
                        )

                    if blocked:
                        _record_blocked(diagnostics, "LinkedIn")
                        blocked_in_query = True
                        error_count += 1

                    fail_rate = (detail_failures / detail_attempts) if detail_attempts else 0.0
                    detail_budget = 0 if disable_detail_fetch else min(detail_base_budget, len(parsed_items))
                    if not disable_detail_fetch and (blocked or fail_rate > 0.5):
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
                        detail_links = {"indeed_url": "", "linkedin_url": "", "company_url": ""}
                        base_linkedin_url = _clean_value(item.get("link"), "")
                        base_company_url = _extract_external_destination_from_url(base_linkedin_url)
                        if idx < detail_budget:
                            full_description, detail_failed, detail_errors, detail_links = _fetch_detail_page_text(
                                session,
                                item.get("link"),
                                "LinkedIn",
                                diagnostics,
                                domain_state,
                                detail_rps,
                                location_mode,
                                request_headers=request_headers,
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
                        item["indeed_url"] = ""
                        item["linkedin_url"] = _clean_value(
                            detail_links.get("linkedin_url") or base_linkedin_url,
                            "",
                        )
                        item["company_url"] = _clean_value(
                            detail_links.get("company_url") or base_company_url,
                            "",
                        )
                        item["query"] = query
                        item["query_location"] = location
                        item["source"] = "LinkedIn"
                        jobs.append(item)
                else:
                    blocked = bool(
                        status in {401, 403, 429, 999}
                        or _detect_linkedin_blocked(body)
                    )
                    if blocked:
                        _record_blocked(diagnostics, "LinkedIn")
                        blocked_in_query = True
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
                    _progress_from_diagnostics(
                        diagnostics,
                        "query-finish",
                        f"LinkedIn: target reached for '{query}' in {location}",
                        source="LinkedIn",
                        query=query,
                        location=location,
                        new_items=int(len(jobs) - previous_jobs),
                        blocked=bool(blocked_in_query),
                    )
                    return jobs, diagnostics

            if len(jobs) == previous_jobs:
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
            _progress_from_diagnostics(
                diagnostics,
                "query-finish",
                f"LinkedIn: finished query '{query}' in {location} with {len(jobs) - previous_jobs} new items",
                source="LinkedIn",
                query=query,
                location=location,
                new_items=int(len(jobs) - previous_jobs),
                blocked=bool(blocked_in_query),
            )
    return jobs, diagnostics


def _fetch_linkedin_web_jobs(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
    diagnostics=None,
    **_kwargs,
):
    return _fetch_linkedin_jobs_direct(
        career_sleeve_key,
        location_mode=location_mode,
        max_pages=max_pages,
        target_raw=target_raw,
        diagnostics=diagnostics,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )


def _company_name_from_host(url):
    host = _host_for_url(url).split(":")[0].strip().lower()
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    parts = [part for part in host.split(".") if part]
    if not parts:
        return ""

    if len(parts) >= 3 and parts[-2] in {"co", "com", "org", "net", "gov"}:
        base = parts[-3]
    elif len(parts) >= 2:
        base = parts[-2]
    else:
        base = parts[0]

    if base in {"jobs", "careers", "werkenbij", "vacatures"} and len(parts) >= 3:
        base = parts[-3]
    cleaned = re.sub(r"[-_]+", " ", base).strip()
    if not cleaned:
        return ""
    return " ".join(token.capitalize() for token in cleaned.split())


def _looks_like_job_opening(title, snippet, url):
    text = sleeves.normalize_for_match(_normalize_text(title, snippet, url))
    if not text:
        return False
    if "openingstijden" in text:
        return False
    return any(hint in text for hint in NL_WEB_OPENING_HINTS)


def _decode_nl_web_result_link(href, response_url):
    raw = _clean_value(href, "")
    if not raw:
        return ""
    absolute = requests.compat.urljoin(response_url or NL_WEB_SEARCH_URL, raw)
    extracted = _extract_external_destination_from_url(absolute)
    if _is_absolute_http_url(extracted):
        return extracted

    decoded = _decode_url_repeatedly(absolute, rounds=5)
    extracted_decoded = _extract_external_destination_from_url(decoded)
    if _is_absolute_http_url(extracted_decoded):
        return extracted_decoded
    if _is_absolute_http_url(decoded):
        return decoded
    return ""


def _parse_nl_web_search_results(selector, response_url):
    parsed = []
    seen_links = set()
    result_nodes = selector.css("div.result")
    if not result_nodes:
        result_nodes = selector.css("article")

    for node in result_nodes:
        anchor_nodes = node.css("a.result__a, h2 a")
        if not anchor_nodes:
            continue
        anchor = anchor_nodes[0]
        href_raw = _clean_value(anchor.attrib.get("href"), "")
        title = (
            _compact_whitespace(anchor.css("*::text").getall())
            or _clean_value(anchor.xpath("string()").get(), "")
        )
        link = _decode_nl_web_result_link(href_raw, response_url)
        if not _is_absolute_http_url(link):
            continue
        host = _host_for_url(link)
        if not host:
            continue
        if "duckduckgo.com" in host or "google." in host or "bing." in host:
            continue
        if _is_platform_job_host(link):
            continue
        if link in seen_links:
            continue

        snippet = _compact_whitespace(
            node.css(
                ".result__snippet::text, "
                ".result__snippet *::text, "
                "a.result__snippet::text, "
                "p::text"
            ).getall()
        )
        if not _looks_like_job_opening(title, snippet, link):
            continue

        seen_links.add(link)
        location = "Netherlands" if _is_netherlands_job(title, snippet, link) else "Unknown"
        parsed.append(
            {
                "title": _clean_value(title, ""),
                "company": _company_name_from_host(link),
                "location": location,
                "link": link,
                "snippet": _clean_value(snippet, ""),
                "salary": _extract_salary_from_chunks([snippet]),
                "work_mode_hint": _normalize_text(title, snippet, location),
                "date": "Unknown",
                "source": "NL Web",
            }
        )
    return parsed


def _build_nl_web_search_query(query, location):
    query_text = _clean_value(query, "")
    location_text = _clean_value(location, "")
    domain_hint = "site:.nl"
    if location_text.lower() in {"netherlands", "nederland"}:
        location_text = "Nederland"
    elif _is_vietnam_job(location_text):
        domain_hint = "(site:.vn OR site:.com.vn)"
    parts = [
        query_text,
        location_text,
        '(vacature OR "job opening" OR "werken bij")',
        domain_hint,
    ]
    return " ".join(part for part in parts if part).strip()


def _fetch_nl_web_openings_direct(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    diagnostics=None,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
):
    diagnostics = diagnostics or _new_diagnostics()
    jobs = []
    seen_unique = set()
    domain_state = {}
    queries = _search_query_bundle_for_career_sleeve(
        career_sleeve_key,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )
    locations = _location_passes_for_mode(location_mode)
    session = requests.Session()
    _configure_session_for_scrape(session)
    request_headers = _source_headers(
        "NL Web",
        location_mode,
        user_agent=random.choice(REQUEST_USER_AGENTS),
    )
    _ = detail_rps  # Reserved for future generic detail-page enrichment.

    for query in queries:
        for location in locations:
            previous_jobs = len(jobs)
            no_new_unique_streak = 0
            blocked_in_query = False
            last_response_body = ""
            _progress_from_diagnostics(
                diagnostics,
                "query-start",
                f"NL Web: started query '{query}' in {location}",
                source="NL Web",
                query=query,
                location=location,
                max_pages=int(max_pages),
            )

            for page_idx in range(max_pages):
                search_query = _build_nl_web_search_query(query, location)
                params = {
                    "q": search_query,
                    "s": str(page_idx * NL_WEB_PAGE_SIZE),
                }
                _progress_from_diagnostics(
                    diagnostics,
                    "page-start",
                    f"NL Web: requesting page {page_idx + 1} for '{query}' in {location}",
                    source="NL Web",
                    query=query,
                    location=location,
                    page=int(page_idx + 1),
                )
                response, error = _rate_limited_get(
                    session,
                    NL_WEB_SEARCH_URL,
                    params=params,
                    headers=request_headers,
                    domain_state=domain_state,
                    requests_per_second=requests_per_second,
                    timeout_seconds=DEFAULT_HTTP_TIMEOUT,
                    max_retries=DEFAULT_HTTP_RETRIES,
                )
                status = response.status_code if response is not None else 0
                body = response.text if response is not None else ""
                if body:
                    last_response_body = body
                blocked = False
                cards_found = 0
                parsed_count = 0
                new_unique_count = 0
                error_count = 0
                if error:
                    error_count += 1
                if response is None or status >= 400:
                    error_count += 1

                parsed_items = []
                request_url = response.url if response is not None else NL_WEB_SEARCH_URL
                if response is not None and response.ok:
                    blocked = bool(
                        status in {401, 403, 429}
                        or sleeves.detect_blocked_html(body)
                    )
                    if blocked:
                        blocked_in_query = True
                        _record_blocked(diagnostics, "NL Web")
                        if body:
                            _save_html_snapshot(
                                "NL Web",
                                query,
                                page_idx + 1,
                                body,
                                "blocked",
                                diagnostics,
                            )
                    else:
                        selector = Selector(text=body)
                        parsed_items = _parse_nl_web_search_results(selector, request_url)
                        cards_found = len(parsed_items)
                        parsed_count = len(parsed_items)

                for item in parsed_items:
                    dedupe_key, _, _ = _build_dedupe_key(item)
                    if dedupe_key in seen_unique:
                        continue
                    seen_unique.add(dedupe_key)
                    new_unique_count += 1
                    item["full_description"] = ""
                    item["detail_fetch_failed"] = True
                    item["query"] = query
                    item["query_location"] = location
                    item["company_url"] = _clean_value(item.get("link"), "")
                    item["indeed_url"] = ""
                    item["linkedin_url"] = ""
                    jobs.append(item)

                _log_page_metrics(
                    diagnostics,
                    source="NL Web",
                    query=query,
                    location=location,
                    page=page_idx + 1,
                    url=request_url,
                    status=status,
                    cards_found=cards_found,
                    parsed_count=parsed_count,
                    new_unique_count=new_unique_count,
                    detailpages_fetched=0,
                    full_description_count=0,
                    error_count=error_count,
                    blocked_detected=blocked,
                )
                if new_unique_count == 0:
                    no_new_unique_streak += 1
                else:
                    no_new_unique_streak = 0
                if parsed_count == 0 or no_new_unique_streak >= max(1, int(no_new_unique_pages)):
                    break
                if len(seen_unique) >= target_raw:
                    _progress_from_diagnostics(
                        diagnostics,
                        "query-finish",
                        f"NL Web: target reached for '{query}' in {location}",
                        source="NL Web",
                        query=query,
                        location=location,
                        new_items=int(len(jobs) - previous_jobs),
                        blocked=bool(blocked_in_query),
                    )
                    return jobs, diagnostics

            if len(jobs) == previous_jobs:
                _save_debug_event(
                    "NL Web",
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
                        "NL Web",
                        query,
                        0,
                        last_response_body,
                        "no-new-items",
                        diagnostics,
                    )
            _progress_from_diagnostics(
                diagnostics,
                "query-finish",
                f"NL Web: finished query '{query}' in {location} with {len(jobs) - previous_jobs} new items",
                source="NL Web",
                query=query,
                location=location,
                new_items=int(len(jobs) - previous_jobs),
                blocked=bool(blocked_in_query),
            )
    return jobs, diagnostics


def _fetch_nl_web_openings_jobs(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
    diagnostics=None,
    **_kwargs,
):
    return _fetch_nl_web_openings_direct(
        career_sleeve_key,
        location_mode=location_mode,
        max_pages=max_pages,
        target_raw=target_raw,
        diagnostics=diagnostics,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )


def _clamp01(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, numeric))


def _confidence_band(value):
    numeric = _clamp01(value)
    if numeric >= 0.8:
        return "high"
    if numeric >= 0.55:
        return "medium"
    return "low"


def _text_evidence_confidence(full_description, raw_text):
    full_length = len(_clean_value(full_description, ""))
    raw_length = len(_clean_value(raw_text, ""))
    if full_length >= 900:
        return 1.0
    if full_length >= 420:
        return 0.9
    if full_length >= 180:
        return 0.78
    if raw_length >= 280:
        return 0.66
    if raw_length >= 120:
        return 0.56
    return 0.46


def _career_sleeve_fit_confidence(
    primary_score,
    career_sleeve_scores,
    total_positive_hits,
    full_description,
    raw_text,
    custom_mode=False,
    custom_coverage_ratio=0.0,
    custom_title_hits_count=0,
):
    fit_norm = _clamp01(float(primary_score or 0) / 5.0)
    hits_norm = _clamp01(float(total_positive_hits or 0) / 8.0)
    evidence_conf = _text_evidence_confidence(full_description, raw_text)

    if custom_mode:
        coverage_norm = _clamp01(custom_coverage_ratio)
        title_hit_norm = 1.0 if int(custom_title_hits_count or 0) > 0 else 0.0
        confidence = (
            (0.46 * fit_norm)
            + (0.29 * coverage_norm)
            + (0.10 * title_hit_norm)
            + (0.15 * evidence_conf)
        )
        return _clamp01(confidence)

    score_values = sorted(
        [float(value or 0) for value in (career_sleeve_scores or {}).values()],
        reverse=True,
    )
    top_score = score_values[0] if score_values else 0.0
    second_score = score_values[1] if len(score_values) > 1 else 0.0
    margin_norm = _clamp01((top_score - second_score) / 3.0)
    confidence = (
        (0.40 * fit_norm)
        + (0.27 * margin_norm)
        + (0.18 * hits_norm)
        + (0.15 * evidence_conf)
    )
    return _clamp01(confidence)


def _abroad_range_fit(min_percent, max_percent, observed_percent):
    if observed_percent is None:
        return 0.45
    observed = int(observed_percent)
    if int(min_percent) <= observed <= int(max_percent):
        return 1.0
    if observed < int(min_percent):
        delta = int(min_percent) - observed
    else:
        delta = observed - int(max_percent)
    return _clamp01(1.0 - (float(delta) / 60.0))


def _abroad_preferences_fit_profile(
    custom_mode,
    normalized_custom_geo_queries,
    custom_geo_matches,
    custom_abroad_range_active,
    custom_abroad_min_percent,
    custom_abroad_max_percent,
    custom_abroad_percent,
    abroad_score,
    abroad_identifiers,
    abroad_meta,
):
    signal_norm = _clamp01(float(abroad_score or 0) / 4.0)
    geo_pref_count = len(normalized_custom_geo_queries or [])
    geo_match_count = len(custom_geo_matches or [])
    geo_requested = geo_pref_count > 0
    range_requested = bool(custom_abroad_range_active)
    geo_match_ratio = (
        _clamp01(float(geo_match_count) / float(max(1, geo_pref_count)))
        if geo_requested
        else 1.0
    )
    range_fit = (
        _abroad_range_fit(custom_abroad_min_percent, custom_abroad_max_percent, custom_abroad_percent)
        if range_requested
        else 1.0
    )

    requested_components = []
    if geo_requested:
        requested_components.append(geo_match_ratio)
    if range_requested:
        requested_components.append(range_fit)
    has_explicit_preferences = bool(requested_components) and bool(custom_mode)
    if has_explicit_preferences:
        requested_fit = sum(requested_components) / float(len(requested_components))
        fit_norm = _clamp01((0.68 * requested_fit) + (0.32 * signal_norm))
        fit_mode = "custom_preferences"
    else:
        fit_norm = signal_norm
        fit_mode = "general_signal"

    identifier_conf = _clamp01(float(len(abroad_identifiers or [])) / 4.0)
    geo_conf = _clamp01(float(len((abroad_meta or {}).get("locations") or [])) / 4.0)
    percent_conf = 1.0 if (abroad_meta or {}).get("percentage") is not None else 0.45
    if has_explicit_preferences:
        specificity_conf = 0.85 if (geo_requested and range_requested) else 0.72
    else:
        specificity_conf = 0.58
    base_confidence = (
        (0.42 * identifier_conf)
        + (0.24 * percent_conf)
        + (0.18 * geo_conf)
        + (0.16 * specificity_conf)
    )
    if has_explicit_preferences:
        request_evidence = sum(requested_components) / float(len(requested_components))
        confidence = _clamp01((0.72 * base_confidence) + (0.28 * request_evidence))
    else:
        confidence = _clamp01(base_confidence)

    return {
        "mode": fit_mode,
        "fit_score": round(float(fit_norm) * 5.0, 2),
        "fit_confidence": round(float(confidence), 4),
        "fit_confidence_pct": int(round(float(confidence) * 100)),
        "fit_confidence_band": _confidence_band(confidence),
        "geo_match_ratio": round(float(geo_match_ratio), 4),
        "geo_match_count": int(geo_match_count),
        "geo_pref_count": int(geo_pref_count),
        "range_fit": round(float(range_fit), 4),
    }


def rank_and_filter_jobs(
    items,
    target_career_sleeve=None,
    min_target_score=4,
    location_mode=MVP_LOCATION_MODE,
    strict_career_sleeve=True,
    include_fail=False,
    return_diagnostics=False,
    diagnostics=None,
    custom_mode=False,
    custom_search_queries=None,
    custom_location_preferences=None,
):
    diagnostics = diagnostics or _new_diagnostics()
    normalized_custom_queries = []
    custom_term_variant_map = {}
    normalized_custom_location_preferences = _default_custom_location_preferences()
    normalized_custom_geo_queries = []
    custom_abroad_min_percent = 0
    custom_abroad_max_percent = 100
    custom_abroad_range_active = False
    if custom_mode:
        normalized_custom_queries = _dedupe_queries(
            [
                sleeves.normalize_for_match(term)
                for term in (custom_search_queries or [])
                if term
            ]
        )
        custom_term_variant_map = {
            term: _bilingual_query_variants(term)
            for term in normalized_custom_queries
        }
        normalized_custom_location_preferences = _parse_custom_location_preferences(
            custom_location_preferences
        )
        normalized_custom_geo_queries = _dedupe_queries(
            [
                sleeves.normalize_for_match(term)
                for term in (
                    list(normalized_custom_location_preferences.get("countries") or [])
                    + list(normalized_custom_location_preferences.get("regions") or [])
                )
                if term
            ]
        )
        custom_abroad_min_percent = int(
            normalized_custom_location_preferences.get("abroad_min_percent", 0) or 0
        )
        custom_abroad_max_percent = int(
            normalized_custom_location_preferences.get("abroad_max_percent", 100) or 100
        )
        custom_abroad_range_active = (
            custom_abroad_min_percent > 0 or custom_abroad_max_percent < 100
        )
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
        if not _is_absolute_http_url(link) and canonical_url:
            link = canonical_url
        if not _is_absolute_http_url(link):
            link = ""
        date_posted = _clean_value(job.get("date") or job.get("date_posted"), "Unknown")
        salary = _clean_value(job.get("salary"), "Not listed")
        source_text = source.lower()
        link_host = _host_for_url(link)
        company_url = _clean_value(job.get("company_url") or job.get("external_url"), "")
        if not _is_absolute_http_url(company_url) or _is_platform_job_host(company_url):
            company_url = ""

        indeed_url = _clean_value(job.get("indeed_url"), "")
        if not _is_absolute_http_url(indeed_url):
            if "indeed" in source_text and _is_absolute_http_url(link) and "indeed." in link_host:
                indeed_url = link
            else:
                indeed_url = ""

        linkedin_url = _clean_value(job.get("linkedin_url"), "")
        if not _is_absolute_http_url(linkedin_url):
            if "linkedin" in source_text and _is_absolute_http_url(link):
                if "linkedin.com" in link_host or link_host.endswith("lnkd.in"):
                    linkedin_url = link
                else:
                    linkedin_url = ""
            else:
                linkedin_url = ""

        if not _is_absolute_http_url(link):
            if _is_absolute_http_url(indeed_url):
                link = indeed_url
            elif _is_absolute_http_url(linkedin_url):
                link = linkedin_url

        if not company_url:
            external_candidates = [
                _clean_value(job.get("external_url"), ""),
                _extract_external_destination_from_url(indeed_url),
                _extract_external_destination_from_url(linkedin_url),
                _extract_external_destination_from_url(link),
            ]
            for candidate in external_candidates:
                if _is_absolute_http_url(candidate) and not _is_platform_job_host(candidate):
                    company_url = candidate
                    break

        if (
            not company_url
            and _is_absolute_http_url(link)
            and not _is_platform_job_host(link)
            and "indeed" not in source_text
            and "linkedin" not in source_text
        ):
            company_url = link

        if company_url:
            canonical_company = _canonicalize_url(company_url) or company_url
            if indeed_url:
                canonical_indeed = _canonicalize_url(indeed_url) or indeed_url
                if canonical_company == canonical_indeed:
                    company_url = ""
            if company_url and linkedin_url:
                canonical_linkedin = _canonicalize_url(linkedin_url) or linkedin_url
                if canonical_company == canonical_linkedin:
                    company_url = ""
            if company_url and _is_platform_job_host(company_url):
                company_url = ""

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
        prepared_text = sleeves.prepare_text(raw_text)
        title_text = _normalize_text(title)
        work_mode = _infer_work_mode(
            _normalize_text(
                location,
                snippet,
                full_description,
                job.get("work_mode_hint"),
            )
        )

        language_flags, language_notes = sleeves.detect_language_flags(raw_text)
        career_sleeve_scores, career_sleeve_details = sleeves.score_all_career_sleeves(raw_text, title_text)
        primary_career_sleeve, natural_primary_score = max(
            career_sleeve_scores.items(),
            key=lambda pair: pair[1],
        )

        scoring_career_sleeve = (
            target_career_sleeve
            if target_career_sleeve in sleeves.VALID_CAREER_SLEEVES
            else primary_career_sleeve
        )
        primary_score = career_sleeve_scores.get(scoring_career_sleeve, natural_primary_score)
        primary_career_sleeve_details = career_sleeve_details.get(scoring_career_sleeve, {})
        total_positive_hits = int(primary_career_sleeve_details.get("total_positive_hits", 0))
        missing_domain_anchors = (
            _clean_value(primary_career_sleeve_details.get("reason"), "") == "missing_domain_anchors"
        )
        custom_title_hits = []
        custom_text_hits = []
        custom_coverage_ratio = 0.0
        custom_missing_queries = []
        custom_geo_matches = []
        custom_pref_bonus = 0
        custom_abroad_percent = None
        custom_abroad_percent_in_range = None
        if custom_mode and normalized_custom_queries:
            prepared_title = sleeves.prepare_text(title_text)
            found_queries = set()
            for term in normalized_custom_queries:
                query_variants = custom_term_variant_map.get(term) or [term]
                text_variant_hits = sleeves.find_hits(prepared_text, query_variants)
                title_variant_hits = sleeves.find_hits(prepared_title, query_variants)
                if text_variant_hits:
                    custom_text_hits.append(term)
                    found_queries.add(term)
                if title_variant_hits:
                    custom_title_hits.append(term)
                    found_queries.add(term)
            custom_text_hits = sorted(set(custom_text_hits))
            custom_title_hits = sorted(set(custom_title_hits))
            custom_hit_count = len(found_queries)
            custom_missing_queries = sorted(
                [term for term in normalized_custom_queries if term not in found_queries]
            )
            custom_coverage_ratio = (
                custom_hit_count / len(normalized_custom_queries)
                if normalized_custom_queries
                else 0.0
            )
            coverage_bonus = 1 if custom_hit_count >= 2 and custom_coverage_ratio >= 0.6 else 0
            custom_score = min(
                5,
                custom_hit_count
                + (1 if custom_title_hits else 0)
                + coverage_bonus,
            )
            primary_score = custom_score
            total_positive_hits = custom_hit_count

        hard_reject_reason = sleeves.detect_hard_reject(title, raw_text)
        abroad_components, abroad_badges, _ = sleeves.score_abroad_components(raw_text)
        remote_flex_score = float(abroad_components.get("remote_flex_score", 0.0))
        mobility_score = float(abroad_components.get("mobility_score", 0.0))
        visa_score = float(abroad_components.get("visa_score", 0.0))
        abroad_base_score = float(abroad_components.get("abroad_score", 0.0))
        abroad_meta = _extract_abroad_metadata(raw_text)
        custom_abroad_percent = abroad_meta.get("percentage")
        if custom_mode and normalized_custom_queries:
            if normalized_custom_geo_queries:
                geo_candidates = _expand_terms_with_bilingual_variants(normalized_custom_geo_queries)
                custom_geo_matches = sorted(
                    {
                        sleeves.normalize_for_match(hit)
                        for hit in sleeves.find_hits(prepared_text, geo_candidates)
                        if sleeves.normalize_for_match(hit)
                    }
                )
                if custom_geo_matches:
                    custom_pref_bonus += 1
            if custom_abroad_range_active:
                if (
                    custom_abroad_percent is not None
                    and custom_abroad_min_percent <= int(custom_abroad_percent) <= custom_abroad_max_percent
                ):
                    custom_abroad_percent_in_range = True
                    custom_pref_bonus += 1
                else:
                    custom_abroad_percent_in_range = False
            if custom_pref_bonus:
                primary_score = min(5, float(primary_score) + custom_pref_bonus)
        abroad_score, abroad_badges = _enhance_abroad_score(
            abroad_base_score,
            abroad_badges,
            abroad_meta,
            raw_text,
        )
        mobility_score, _ = _enhance_abroad_score(
            mobility_score,
            [],
            abroad_meta,
            raw_text,
        )
        abroad_identifiers = _derive_abroad_identifiers(
            abroad_meta.get("percentage"),
            abroad_meta.get("locations") or [],
            raw_text,
            badges=abroad_badges,
        )
        career_sleeve_fit_confidence = _career_sleeve_fit_confidence(
            primary_score=primary_score,
            career_sleeve_scores=career_sleeve_scores,
            total_positive_hits=total_positive_hits,
            full_description=full_description,
            raw_text=raw_text,
            custom_mode=bool(custom_mode),
            custom_coverage_ratio=custom_coverage_ratio,
            custom_title_hits_count=len(custom_title_hits),
        )
        career_sleeve_fit_score = round(float(primary_score or 0), 2)
        career_sleeve_fit_confidence = round(float(career_sleeve_fit_confidence), 4)
        career_sleeve_fit_confidence_pct = int(round(career_sleeve_fit_confidence * 100))
        career_sleeve_fit_confidence_band = _confidence_band(career_sleeve_fit_confidence)
        abroad_preferences_profile = _abroad_preferences_fit_profile(
            custom_mode=bool(custom_mode),
            normalized_custom_geo_queries=normalized_custom_geo_queries,
            custom_geo_matches=custom_geo_matches,
            custom_abroad_range_active=custom_abroad_range_active,
            custom_abroad_min_percent=custom_abroad_min_percent,
            custom_abroad_max_percent=custom_abroad_max_percent,
            custom_abroad_percent=custom_abroad_percent,
            abroad_score=abroad_score,
            abroad_identifiers=abroad_identifiers,
            abroad_meta=abroad_meta,
        )
        location_profile = _score_location_proximity(location, raw_text, work_mode)
        location_proximity_score = float(location_profile.get("score", 0))
        synergy_score, synergy_hits = sleeves.score_synergy(raw_text)
        penalty_points, penalty_reasons = sleeves.evaluate_soft_penalties(raw_text)
        penalty_reasons = list(penalty_reasons or [])
        required_languages = {
            sleeves.normalize_for_match(lang)
            for lang in (language_flags.get("extra_languages") or [])
        }
        if language_flags.get("extra_language_required") and {"vietnamese", "vietnamees"} & required_languages:
            penalty_points += 6
            penalty_reasons.append(
                "Vietnamese language required; lower priority for visa-friendly international profile."
            )

        weights = sleeves.ranking_weights_for_career_sleeve(scoring_career_sleeve)
        weighted_score = (
            (visa_score * weights.get("visa_score", 0.30))
            + (mobility_score * weights.get("mobility_score", 0.16))
            + (remote_flex_score * weights.get("remote_flex_score", 0.06))
            + (primary_score * weights.get("primary_career_sleeve_score", 0.38))
            + (synergy_score * weights.get("synergy_score", 0.05))
            + (location_proximity_score * weights.get("location_proximity_score", 0.05))
        )
        location_gate_text = _build_location_gate_text(
            location,
            job.get("query_location"),
            job.get("work_mode_hint"),
            raw_text,
        )
        location_gate_match = _passes_location_gate(location_gate_text, location_mode)
        location_penalty = 0 if location_gate_match else 4
        rank_score = (weighted_score * 20) - penalty_points - location_penalty

        primary_career_sleeve_config = sleeves.CAREER_SLEEVE_CONFIG[scoring_career_sleeve]
        distance_km = location_profile.get("distance_km")
        if distance_km is None:
            proximity_reason = (
                f"Main location {location_profile.get('main_location', 'Unknown')} "
                f"(distance to {location_profile.get('anchor', HOME_LOCATION_LABEL)} unknown; "
                f"proximity {location_proximity_score}/4)"
            )
        else:
            proximity_reason = (
                f"Main location {location_profile.get('main_location', 'Unknown')} "
                f"({distance_km} km to {location_profile.get('anchor', HOME_LOCATION_LABEL)}; "
                f"proximity {location_proximity_score}/4)"
            )
        travel_share_text = abroad_meta.get("percentage_text") or "n/a"
        geo_scope_text = ", ".join((abroad_meta.get("locations") or [])[:4]) or "none"
        abroad_identifier_text = ", ".join(abroad_identifiers) or "no explicit signal"
        abroad_summary = (
            f"Abroad score {abroad_score}/4 via {abroad_identifier_text} "
            f"(visa: {visa_score}/4; mobility: {mobility_score}/4; remote flexibility: {remote_flex_score}/4; "
            f"travel share: {travel_share_text}; geo: {geo_scope_text})"
        )
        reasons = [
            (
                f"Career Sleeve {scoring_career_sleeve} fit {primary_score}/5 "
                f"(A:{career_sleeve_scores['A']} B:{career_sleeve_scores['B']} "
                f"C:{career_sleeve_scores['C']} D:{career_sleeve_scores['D']} E:{career_sleeve_scores['E']})"
            ),
            proximity_reason,
            abroad_summary,
            f"Keyword coverage {total_positive_hits} hits for Career Sleeve {scoring_career_sleeve}",
        ]
        if custom_mode:
            coverage_pct = int(round(custom_coverage_ratio * 100))
            reasons[0] = (
                f"Custom Career Sleeve relevance {primary_score}/5 "
                f"(matched {total_positive_hits} of {len(normalized_custom_queries)} custom queries; "
                f"coverage {coverage_pct}%)"
            )
            custom_pref_parts = []
            if normalized_custom_geo_queries:
                custom_pref_parts.append(
                    f"geo matches {len(custom_geo_matches)}/{len(normalized_custom_geo_queries)}"
                )
            if custom_abroad_range_active:
                if custom_abroad_percent is None:
                    custom_pref_parts.append(
                        f"abroad % n/a (requested {custom_abroad_min_percent}-{custom_abroad_max_percent}%)"
                    )
                elif custom_abroad_percent_in_range:
                    custom_pref_parts.append(
                        f"abroad % {custom_abroad_percent}% within {custom_abroad_min_percent}-{custom_abroad_max_percent}%"
                    )
                else:
                    custom_pref_parts.append(
                        f"abroad % {custom_abroad_percent}% outside {custom_abroad_min_percent}-{custom_abroad_max_percent}%"
                    )
            if custom_pref_parts:
                reasons.append(f"Custom location preferences: {'; '.join(custom_pref_parts)}")
            if custom_title_hits:
                reasons.append(
                    f"Custom title hits: {', '.join(custom_title_hits[:4])}"
                )
            elif custom_text_hits:
                reasons.append(
                    f"Custom text hits: {', '.join(custom_text_hits[:4])}"
                )
        if language_notes:
            reasons.append(language_notes[0])
        if penalty_reasons:
            reasons.append(penalty_reasons[0])
        if missing_domain_anchors:
            reasons.append("Domain anchors missing for this Career Sleeve; likely low relevance.")
        if not location_gate_match:
            reasons.append("Location outside preferred scope; ranked as lower priority.")
        reasons = reasons[:MAX_REASON_COUNT]

        scored_jobs.append(
            {
                "title": title or "Unknown role",
                "company": company or "Unknown company",
                "location": location or "Unknown",
                "url": link,
                "source": source,
                "date_posted": date_posted,
                "work_mode": work_mode,
                "snippet": snippet,
                "full_description": full_description,
                "company_url": company_url,
                "indeed_url": indeed_url,
                "linkedin_url": linkedin_url,
                "raw_text": raw_text,
                "prepared_text": prepared_text.strip(),
                "primary_career_sleeve_id": scoring_career_sleeve,
                "primary_career_sleeve_name": primary_career_sleeve_config.get("name", ""),
                "primary_career_sleeve_tagline": primary_career_sleeve_config.get("tagline", ""),
                "career_sleeve_id": scoring_career_sleeve,
                "career_sleeve_name": primary_career_sleeve_config.get("name", ""),
                "career_sleeve_tagline": primary_career_sleeve_config.get("tagline", ""),
                "career_sleeve_scores": career_sleeve_scores,
                "primary_career_sleeve_score": primary_score,
                "career_sleeve_fit_score": career_sleeve_fit_score,
                "career_sleeve_fit_confidence": career_sleeve_fit_confidence,
                "career_sleeve_fit_confidence_pct": career_sleeve_fit_confidence_pct,
                "career_sleeve_fit_confidence_band": career_sleeve_fit_confidence_band,
                "abroad_score": abroad_score,
                "remote_flex_score": remote_flex_score,
                "mobility_score": mobility_score,
                "visa_score": visa_score,
                "abroad_badges": abroad_badges,
                "abroad_identifiers": abroad_identifiers,
                "abroad_summary": abroad_summary,
                "abroad_preferences_fit_score": abroad_preferences_profile.get("fit_score", 0),
                "abroad_preferences_fit_confidence": abroad_preferences_profile.get("fit_confidence", 0),
                "abroad_preferences_fit_confidence_pct": abroad_preferences_profile.get(
                    "fit_confidence_pct",
                    0,
                ),
                "abroad_preferences_fit_confidence_band": abroad_preferences_profile.get(
                    "fit_confidence_band",
                    "low",
                ),
                "abroad_preferences_fit_mode": abroad_preferences_profile.get("mode", "general_signal"),
                "abroad_preferences_geo_match_ratio": abroad_preferences_profile.get("geo_match_ratio", 0),
                "abroad_preferences_geo_match_count": abroad_preferences_profile.get("geo_match_count", 0),
                "abroad_preferences_geo_pref_count": abroad_preferences_profile.get("geo_pref_count", 0),
                "abroad_preferences_range_fit": abroad_preferences_profile.get("range_fit", 0),
                "abroad_percentage": abroad_meta["percentage"],
                "abroad_percentage_text": abroad_meta["percentage_text"],
                "abroad_countries": abroad_meta["countries"],
                "abroad_regions": abroad_meta["regions"],
                "abroad_continents": abroad_meta["continents"],
                "abroad_locations": abroad_meta["locations"],
                "main_location": location_profile.get("main_location", location or "Unknown"),
                "distance_from_home_km": location_profile.get("distance_km"),
                "distance_anchor": location_profile.get("anchor", HOME_LOCATION_LABEL),
                "distance_anchor_full": HOME_LOCATION_FULL_LABEL,
                "distance_match_city": location_profile.get("matched_city"),
                "proximity_score": location_proximity_score,
                "proximity_tier": location_profile.get("tier"),
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
                "primary_career_sleeve": scoring_career_sleeve,
                "why_relevant": reasons,
                "custom_location_preferences": (
                    normalized_custom_location_preferences if custom_mode else _default_custom_location_preferences()
                ),
                "custom_geo_matches": custom_geo_matches,
                "custom_abroad_percent_in_range": custom_abroad_percent_in_range,
                "_base_reasons": list(reasons),
                "_score_components": {
                    "synergy_score": synergy_score,
                    "penalty_points": penalty_points,
                    "total_positive_hits": total_positive_hits,
                    "location_gate_match": location_gate_match,
                    "location_proximity_score": location_proximity_score,
                    "abroad_score": abroad_score,
                    "remote_flex_score": remote_flex_score,
                    "mobility_score": mobility_score,
                    "visa_score": visa_score,
                    "custom_mode": bool(custom_mode),
                    "custom_query_count": len(normalized_custom_queries),
                    "custom_text_hits": custom_text_hits,
                    "custom_title_hits": custom_title_hits,
                    "custom_missing_queries": custom_missing_queries,
                    "custom_coverage_ratio": round(custom_coverage_ratio, 4),
                    "custom_geo_queries": normalized_custom_geo_queries,
                    "custom_geo_matches": custom_geo_matches,
                    "custom_pref_bonus": custom_pref_bonus,
                    "custom_abroad_range_active": custom_abroad_range_active,
                    "custom_abroad_min_percent": custom_abroad_min_percent,
                    "custom_abroad_max_percent": custom_abroad_max_percent,
                    "custom_abroad_percent": custom_abroad_percent,
                    "custom_abroad_percent_in_range": custom_abroad_percent_in_range,
                    "career_sleeve_fit_score": career_sleeve_fit_score,
                    "career_sleeve_fit_confidence": career_sleeve_fit_confidence,
                    "career_sleeve_fit_confidence_pct": career_sleeve_fit_confidence_pct,
                    "career_sleeve_fit_confidence_band": career_sleeve_fit_confidence_band,
                    "abroad_preferences_fit_profile": abroad_preferences_profile,
                    "strict_target_mismatch": bool(
                        strict_career_sleeve and target_career_sleeve and primary_career_sleeve != target_career_sleeve
                    ),
                    "missing_domain_anchors": bool(missing_domain_anchors),
                },
                "_fail_reason": "",
                "_rank": (
                    round(rank_score, 4),
                    weighted_score,
                    primary_score,
                    visa_score,
                    mobility_score,
                    remote_flex_score,
                    location_proximity_score,
                    synergy_score,
                ),
            }
        )

    threshold_cfg = RUNTIME_CONFIG.get("threshold_overrides", {})
    threshold_career_sleeve = (
        target_career_sleeve
        if target_career_sleeve in sleeves.VALID_CAREER_SLEEVES
        else ""
    )
    if not threshold_career_sleeve and scored_jobs:
        threshold_career_sleeve = _clean_value(scored_jobs[0].get("primary_career_sleeve_id"), "").upper()
    career_sleeve_threshold_defaults = sleeves.decision_thresholds_for_career_sleeve(
        threshold_career_sleeve or "E"
    )

    base_min_total_hits = int(
        threshold_cfg.get(
            "min_total_hits",
            career_sleeve_threshold_defaults.get("min_total_hits", sleeves.MIN_TOTAL_HITS_TO_SHOW),
        )
    )
    base_min_primary = int(
        threshold_cfg.get(
            "min_primary_score",
            career_sleeve_threshold_defaults.get("min_primary_score", min_target_score),
        )
    )
    base_min_maybe_total = int(
        threshold_cfg.get(
            "min_maybe_total_hits",
            career_sleeve_threshold_defaults.get("min_maybe_total_hits", sleeves.MIN_TOTAL_HITS_TO_MAYBE),
        )
    )
    base_min_maybe_primary = int(
        threshold_cfg.get(
            "min_maybe_primary_score",
            career_sleeve_threshold_defaults.get(
                "min_maybe_primary_score",
                sleeves.MIN_PRIMARY_CAREER_SLEEVE_SCORE_TO_MAYBE,
            ),
        )
    )
    custom_pass_score = max(
        1,
        int(threshold_cfg.get("custom_pass_score", career_sleeve_threshold_defaults.get("custom_pass_score", 2))),
    )
    custom_pass_hits = max(
        1,
        int(threshold_cfg.get("custom_pass_hits", career_sleeve_threshold_defaults.get("custom_pass_hits", 1))),
    )
    custom_maybe_score = max(
        1,
        int(threshold_cfg.get("custom_maybe_score", career_sleeve_threshold_defaults.get("custom_maybe_score", 1))),
    )
    custom_maybe_hits = max(
        1,
        int(threshold_cfg.get("custom_maybe_hits", career_sleeve_threshold_defaults.get("custom_maybe_hits", 1))),
    )

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
            "name": "fallback:min_primary_career_sleeve_score-1",
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
            primary_score = float(scored.get("primary_career_sleeve_score", 0))
            total_hits = int(scored.get("_score_components", {}).get("total_positive_hits", 0))
            location_gate_match = bool(scored.get("_score_components", {}).get("location_gate_match", True))
            mismatch = scored.get("_score_components", {}).get("strict_target_mismatch", False)
            missing_domain_anchors = bool(
                scored.get("_score_components", {}).get("missing_domain_anchors", False)
            )
            fail_reason = ""

            if hard_reject_reason:
                decision = "FAIL"
                fail_reason = hard_reject_reason
            elif not location_gate_match:
                decision = "FAIL"
                fail_reason = "location_out_of_scope"
            elif mismatch:
                decision = "FAIL"
                fail_reason = "target_career_sleeve_mismatch"
            elif missing_domain_anchors:
                decision = "FAIL"
                fail_reason = "missing_domain_anchors"
            elif custom_mode and not normalized_custom_queries:
                decision = "FAIL"
                fail_reason = "custom_queries_missing"
            elif custom_mode:
                if total_hits >= custom_pass_hits and primary_score >= custom_pass_score:
                    decision = "PASS"
                elif total_hits >= custom_maybe_hits and primary_score >= custom_maybe_score:
                    decision = "MAYBE"
                else:
                    decision = "FAIL"
                    fail_reason = "custom_queries_no_match"
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
                    fail_reason = "primary_career_sleeve_score_too_low"
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

    # Always return PASS first, then MAYBE, so the UI can paginate without losing MAYBE visibility.
    selected_jobs = pass_jobs + maybe_jobs
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


def _fetch_indeed_web_jobs(
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
    diagnostics=None,
    **_kwargs,
):
    return _fetch_indeed_jobs_direct(
        career_sleeve_key,
        location_mode=location_mode,
        max_pages=max_pages,
        target_raw=target_raw,
        diagnostics=diagnostics,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        search_queries=search_queries,
        extra_queries=extra_queries,
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
    "nl_web_openings": {
        "label": "NL Web (job openings discovery)",
        "default_enabled": True,
        "requires_env": [],
        "query_based": True,
        "fetcher": _fetch_nl_web_openings_jobs,
    },
}


def _source_env_missing(source_key):
    config = SOURCE_REGISTRY[source_key]
    missing = []
    for env_name in config["requires_env"]:
        if not os.getenv(env_name, "").strip():
            missing.append(env_name)
    return missing


def _source_health_settings():
    cfg = RUNTIME_CONFIG.get("source_health") or {}
    block_threshold = int(cfg.get("block_threshold", SOURCE_HEALTH_DEFAULT_BLOCK_THRESHOLD))
    blocked_cooldown = int(cfg.get("blocked_cooldown_seconds", SOURCE_HEALTH_DEFAULT_BLOCK_COOLDOWN_SECONDS))
    error_cooldown = int(cfg.get("error_cooldown_seconds", SOURCE_HEALTH_DEFAULT_ERROR_COOLDOWN_SECONDS))
    return {
        "block_threshold": max(1, block_threshold),
        "blocked_cooldown_seconds": max(60, blocked_cooldown),
        "error_cooldown_seconds": max(30, error_cooldown),
    }


def _classify_source_error_kind(error):
    text = _clean_value(error, "").lower()
    if not text:
        return ""
    blocked_markers = [
        "blocked_detected",
        "captcha",
        "access denied",
        "security check",
        "unusual traffic",
        "verify you are human",
        "status_403",
        "status_429",
        " 403",
        " 429",
    ]
    if any(marker in text for marker in blocked_markers):
        return "blocked"
    return "error"


def _source_health_status(source_key):
    settings = _source_health_settings()
    now = time.time()
    default_payload = {
        "state": "ok",
        "failure_streak": 0,
        "last_error": "",
        "last_error_kind": "",
        "last_failure_at": 0,
        "next_retry_at": 0,
        "cooldown_seconds_remaining": 0,
    }
    with source_health_lock:
        health_raw = source_health.get(source_key)
        health = dict(health_raw) if isinstance(health_raw, dict) else None
    if not isinstance(health, dict):
        return default_payload

    failure_streak = int(health.get("failure_streak", 0) or 0)
    last_error = _clean_value(health.get("last_error"), "")
    last_error_kind = _clean_value(health.get("last_error_kind"), "").lower()
    last_failure_at = float(health.get("last_failure_at", 0) or 0)
    is_blocked = (
        last_error_kind == "blocked"
        and failure_streak >= settings["block_threshold"]
    )
    cooldown_seconds = (
        settings["blocked_cooldown_seconds"]
        if is_blocked
        else settings["error_cooldown_seconds"]
    )
    next_retry_at = last_failure_at + cooldown_seconds if last_failure_at and last_error else 0
    cooldown_remaining = max(0, int(next_retry_at - now)) if next_retry_at else 0

    if not last_error:
        state = "ok"
    elif is_blocked and cooldown_remaining > 0:
        state = "blocked"
    elif cooldown_remaining > 0:
        state = "degraded"
    elif is_blocked:
        state = "degraded"
    else:
        state = "degraded"

    return {
        "state": state,
        "failure_streak": failure_streak,
        "last_error": last_error,
        "last_error_kind": last_error_kind,
        "last_failure_at": int(last_failure_at) if last_failure_at else 0,
        "next_retry_at": int(next_retry_at) if next_retry_at else 0,
        "cooldown_seconds_remaining": int(cooldown_remaining),
    }


def _source_is_cooled_down(source_key):
    status = _source_health_status(source_key)
    return status.get("cooldown_seconds_remaining", 0) > 0


def _source_available(source_key, force_retry=False):
    if source_key not in MVP_SOURCE_IDS:
        return False
    if _source_env_missing(source_key):
        return False
    if not force_retry and _source_is_cooled_down(source_key):
        return False
    return True


def _source_availability_reason(source_key):
    if source_key not in MVP_SOURCE_IDS:
        return "Disabled in MVP source bundle"
    missing_env = _source_env_missing(source_key)
    if missing_env:
        return f"Missing env: {', '.join(missing_env)}"
    status = _source_health_status(source_key)
    if status.get("cooldown_seconds_remaining", 0) > 0:
        state = status.get("state", "degraded")
        if state == "blocked":
            return (
                f"Blocked cooldown active ({status.get('cooldown_seconds_remaining')}s left): "
                f"{status.get('last_error') or 'blocked_detected'}"
            )
        return (
            f"Source cooldown active ({status.get('cooldown_seconds_remaining')}s left): "
            f"{status.get('last_error') or 'recent errors'}"
        )
    return ""


def _record_source_health(source_key, error):
    with source_health_lock:
        health = dict(
            source_health.get(
                source_key,
                {
                    "failure_streak": 0,
                    "last_failure_at": 0,
                    "last_error": "",
                    "last_error_kind": "",
                },
            )
        )
        if error:
            health["failure_streak"] = health.get("failure_streak", 0) + 1
            health["last_failure_at"] = time.time()
            health["last_error"] = _clean_value(error, "Unknown source error")
            health["last_error_kind"] = _classify_source_error_kind(error)
        else:
            health["failure_streak"] = 0
            health["last_error"] = ""
            health["last_error_kind"] = ""
        source_health[source_key] = health


def _default_sources():
    return [
        source_key
        for source_key in MVP_SOURCE_IDS
        if _source_available(source_key, force_retry=False)
    ]


def _normalize_scrape_variant(value):
    normalized = _clean_value(value, SCRAPE_VARIANT_DEFAULT).lower().strip()
    if normalized in {"ultra_fast", "ultrafast", "ultra"}:
        return SCRAPE_VARIANT_ULTRA_FAST
    return SCRAPE_VARIANT_DEFAULT


def _cache_key_for(
    source_key,
    career_sleeve_key,
    location_mode,
    max_pages,
    target_raw,
    no_new_unique_pages,
    search_queries=None,
    extra_queries=None,
):
    config = SOURCE_REGISTRY[source_key]
    query_key = ""
    if search_queries:
        normalized_search_queries = [sleeves.normalize_for_match(term) for term in search_queries if term]
        normalized_search_queries = [term for term in normalized_search_queries if term]
        if normalized_search_queries:
            query_key = f":q{','.join(sorted(set(normalized_search_queries)))}"
    extra_key = ""
    if extra_queries:
        normalized_extra_queries = [sleeves.normalize_for_match(query) for query in extra_queries if query]
        normalized_extra_queries = [query for query in normalized_extra_queries if query]
        if normalized_extra_queries:
            extra_key = f":x{','.join(sorted(set(normalized_extra_queries)))}"
    if config["query_based"] and config.get("cache_by_sleeve", True):
        return f"{source_key}:{career_sleeve_key}:{location_mode}:p{max_pages}:t{target_raw}:n{no_new_unique_pages}{query_key}{extra_key}"
    if config["query_based"]:
        return f"{source_key}:{location_mode}:p{max_pages}:t{target_raw}:n{no_new_unique_pages}{query_key}{extra_key}"
    return source_key


def _derive_source_fetch_error(items, diagnostics):
    summaries = list((diagnostics.get("source_query_summary") or {}).values())
    if not summaries:
        return "no_pages_attempted"
    total_parsed = sum(int(entry.get("parsed_count", 0)) for entry in summaries)
    total_errors = sum(int(entry.get("error_count", 0)) for entry in summaries)
    blocked_any = any(bool(entry.get("blocked_detected")) for entry in summaries)
    if blocked_any and not items:
        return "blocked_detected"
    if not items and total_parsed == 0 and total_errors > 0:
        return f"zero_parsed_with_errors:{total_errors}"
    return ""


def _fetch_source_with_cache(
    source_key,
    career_sleeve_key,
    location_mode,
    force_refresh=False,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
    run_id="",
):
    cache_key = _cache_key_for(
        source_key,
        career_sleeve_key,
        location_mode,
        max_pages,
        target_raw,
        no_new_unique_pages,
        search_queries=search_queries,
        extra_queries=extra_queries,
    )
    now = time.time()
    with source_cache_lock:
        cache_entry = source_cache.get(cache_key)
    if cache_entry and not force_refresh:
        has_no_data = not cache_entry.get("items")
        has_error = bool(cache_entry.get("error"))
        ttl = JOBS_CACHE_EMPTY_TTL_SECONDS if (has_no_data or has_error) else JOBS_CACHE_TTL_SECONDS
        cache_age = now - float(cache_entry.get("fetched_at", 0) or 0)
        if cache_age < ttl:
            _progress_update(
                run_id,
                "source-cache",
                (
                    f"{source_key}: using cached snapshot "
                    f"({len(cache_entry.get('items') or [])} items, ttl {int(ttl)}s)"
                ),
                source=source_key,
                cache_hit=True,
                item_count=len(cache_entry.get("items") or []),
            )
            return (
                cache_entry["items"],
                cache_entry["error"],
                cache_entry.get("diagnostics") or _new_diagnostics(),
            )

    fetcher = SOURCE_REGISTRY[source_key]["fetcher"]
    query_based = SOURCE_REGISTRY[source_key]["query_based"]
    source_diag = _new_diagnostics()
    try:
        if query_based:
            result = fetcher(
                career_sleeve_key,
                location_mode=location_mode,
                max_pages=max_pages,
                target_raw=target_raw,
                requests_per_second=requests_per_second,
                detail_rps=detail_rps,
                no_new_unique_pages=no_new_unique_pages,
                search_queries=search_queries,
                extra_queries=extra_queries,
                diagnostics=source_diag,
            )
        else:
            result = fetcher()
        if isinstance(result, tuple) and len(result) == 2:
            items, source_diag = result
        else:
            items = result
        error = _derive_source_fetch_error(items, source_diag)
        if not error:
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
    cache_entry_age = now - float((cache_entry or {}).get("fetched_at", 0) or 0)
    stale_cache_usable = bool(
        cache_entry
        and cache_entry.get("items")
        and cache_entry_age <= MAX_STALE_CACHE_FALLBACK_SECONDS
    )
    # If a live refresh/source call fails but we still have previous usable cache,
    # prefer stale data over a full hard-fail in the UI.
    if error and not items and stale_cache_usable:
        _record_source_health(source_key, error)
        fallback_diag = cache_entry.get("diagnostics") or _new_diagnostics()
        fallback_diag.setdefault("auto_failover", []).append(
            {
                "source_activated": source_key,
                "reason": "stale_cache_fallback_after_error",
                "new_items": len(cache_entry.get("items") or []),
            }
        )
        _progress_update(
            run_id,
            "source-fallback",
            f"{source_key}: live fetch failed, reused stale cached items ({len(cache_entry.get('items') or [])})",
            source=source_key,
            fallback="stale_cache",
            item_count=len(cache_entry.get("items") or []),
            error=_clean_value(error, ""),
            cache_age_seconds=int(cache_entry_age),
        )
        return cache_entry["items"], None, fallback_diag

    _record_source_health(source_key, error)

    with source_cache_lock:
        source_cache[cache_key] = {
            "items": items,
            "error": error,
            "diagnostics": source_diag,
            "fetched_at": now,
        }
    return items, error, source_diag


def fetch_jobs_from_sources(
    selected_sources,
    career_sleeve_key,
    location_mode=MVP_LOCATION_MODE,
    force_refresh=False,
    max_pages=DEFAULT_MAX_PAGES,
    target_raw=DEFAULT_TARGET_RAW_PER_SLEEVE,
    requests_per_second=DEFAULT_RATE_LIMIT_RPS,
    detail_rps=DEFAULT_DETAIL_RATE_LIMIT_RPS,
    no_new_unique_pages=DEFAULT_NO_NEW_UNIQUE_PAGES,
    search_queries=None,
    extra_queries=None,
    allow_failover=True,
    run_id="",
    force_source_retry=False,
    enforce_mvp_bundle=True,
    parallel_fetch=False,
):
    profile = SCRAPE_MODE
    requested = [source for source in selected_sources if source in SOURCE_REGISTRY]
    if enforce_mvp_bundle:
        candidate_sources = [source for source in MVP_SOURCE_IDS if source in SOURCE_REGISTRY]
    else:
        candidate_sources = requested or [source for source in _default_sources() if source in SOURCE_REGISTRY]
    unavailable_reasons = {}
    usable_sources = []
    for source in candidate_sources:
        if _source_available(source, force_retry=force_source_retry):
            usable_sources.append(source)
            continue
        reason = _source_availability_reason(source)
        unavailable_reasons[source] = reason or "Source unavailable"

    source_policy_note = (
        "backend enforces full MVP source bundle each run"
        if enforce_mvp_bundle
        else "using requested source subset"
    )
    _progress_update(
        run_id,
        "source-plan",
        (
            f"Using sources: {', '.join(usable_sources) if usable_sources else 'none'}"
            + (
                " (MVP lock: source bundle only)"
                if enforce_mvp_bundle and requested and set(requested) - set(MVP_SOURCE_IDS)
                else ""
            )
            + (f" ({source_policy_note})")
        ),
        requested_sources=requested,
        candidate_sources=candidate_sources,
        usable_sources=usable_sources,
        profile=profile,
        location_mode=location_mode,
        force_source_retry=bool(force_source_retry),
        enforce_mvp_bundle=bool(enforce_mvp_bundle),
        parallel_fetch=bool(parallel_fetch and len(usable_sources) > 1),
        unavailable_reasons=unavailable_reasons,
    )
    if not usable_sources:
        errors = [f"{source}: {reason}" for source, reason in unavailable_reasons.items()] or [
            "No available sources for the selected options."
        ]
        return [], errors, [], _new_diagnostics()

    items = []
    errors = []
    diagnostics = _new_diagnostics()
    diagnostics["run_id"] = run_id

    def _merge_source_diagnostics(source_diag):
        source_diag = source_diag or {}
        diagnostics["source_query_pages"].extend(source_diag.get("source_query_pages", []))
        diagnostics["snapshots"].extend(source_diag.get("snapshots", []))
        for source, blocked in (source_diag.get("blocked_detected") or {}).items():
            diagnostics["blocked_detected"][source] = bool(
                diagnostics["blocked_detected"].get(source, False) or blocked
            )
        for key, value in (source_diag.get("source_query_summary") or {}).items():
            diagnostics["source_query_summary"][key] = value

    def _consume_source_result(source_key, source_label, source_items, source_error, source_diag):
        items.extend(source_items or [])
        if source_error:
            errors.append(f"{source_key}: {source_error}")
        _progress_update(
            run_id,
            "source-finish",
            (
                f"Completed {source_label}: {len(source_items or [])} items"
                + (f", error: {source_error}" if source_error else "")
            ),
            source=source_key,
            item_count=len(source_items or []),
            error=source_error or "",
        )
        _merge_source_diagnostics(source_diag)

    if parallel_fetch and len(usable_sources) > 1:
        worker_count = min(3, len(usable_sources))
        _progress_update(
            run_id,
            "source-parallel",
            f"Running {len(usable_sources)} sources in parallel ({worker_count} workers)",
            worker_count=worker_count,
        )
        future_map = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for source_key in usable_sources:
                source_label = SOURCE_REGISTRY.get(source_key, {}).get("label", source_key)
                _progress_update(
                    run_id,
                    "source-start",
                    f"Now scraping {source_label}",
                    source=source_key,
                    label=source_label,
                    mode="parallel",
                )
                future = executor.submit(
                    _fetch_source_with_cache,
                    source_key,
                    career_sleeve_key,
                    location_mode,
                    force_refresh=force_refresh,
                    max_pages=max_pages,
                    target_raw=target_raw,
                    requests_per_second=requests_per_second,
                    detail_rps=detail_rps,
                    no_new_unique_pages=no_new_unique_pages,
                    search_queries=search_queries,
                    extra_queries=extra_queries,
                    run_id=run_id,
                )
                future_map[future] = (source_key, source_label)
            for future in as_completed(future_map):
                source_key, source_label = future_map[future]
                try:
                    source_items, source_error, source_diag = future.result()
                except Exception as exc:  # pragma: no cover
                    source_items = []
                    source_error = str(exc)
                    source_diag = _new_diagnostics()
                _consume_source_result(
                    source_key,
                    source_label,
                    source_items,
                    source_error,
                    source_diag,
                )
    else:
        for source_key in usable_sources:
            source_label = SOURCE_REGISTRY.get(source_key, {}).get("label", source_key)
            _progress_update(
                run_id,
                "source-start",
                f"Now scraping {source_label}",
                source=source_key,
                label=source_label,
                mode="sequential",
            )
            source_items, source_error, source_diag = _fetch_source_with_cache(
                source_key,
                career_sleeve_key,
                location_mode,
                force_refresh=force_refresh,
                max_pages=max_pages,
                target_raw=target_raw,
                requests_per_second=requests_per_second,
                detail_rps=detail_rps,
                no_new_unique_pages=no_new_unique_pages,
                search_queries=search_queries,
                extra_queries=extra_queries,
                run_id=run_id,
            )
            _consume_source_result(
                source_key,
                source_label,
                source_items,
                source_error,
                source_diag,
            )

    _update_query_performance_from_diagnostics(diagnostics, career_sleeve_key)

    return items, errors, usable_sources, diagnostics


def _public_scrape_config():
    profile = SCRAPE_MODE
    sources = []
    source_health_payload = {}
    for source_key in MVP_SOURCE_IDS:
        config = SOURCE_REGISTRY.get(source_key)
        if not config:
            continue
        available = _source_available(source_key, force_retry=False)
        health_status = _source_health_status(source_key)
        source_health_payload[source_key] = health_status
        reason = _source_availability_reason(source_key)
        sources.append(
            {
                "id": source_key,
                "label": config["label"],
                "available": available,
                "default_enabled": bool(config["default_enabled"] and available),
                "enabled_in_profile": True,
                "reason": reason,
                "state": health_status.get("state", "ok"),
                "cooldown_seconds_remaining": int(health_status.get("cooldown_seconds_remaining", 0) or 0),
                "next_retry_at": int(health_status.get("next_retry_at", 0) or 0),
                "last_error": health_status.get("last_error", ""),
                "last_error_kind": health_status.get("last_error_kind", ""),
                "failure_streak": int(health_status.get("failure_streak", 0) or 0),
            }
        )
    location_modes = [
        {"id": MVP_LOCATION_MODE, "label": sleeves.LOCATION_MODE_LABELS[MVP_LOCATION_MODE]},
    ]

    return {
        "profile": profile,
        "sources": sources,
        "source_health": source_health_payload,
        "config_version": RUNTIME_CONFIG.get("config_version", "1.0"),
        "career_sleeve_search_queries_defaults": {
            key: list(value)
            for key, value in (sleeves.CAREER_SLEEVE_SEARCH_QUERIES or {}).items()
            if key in sleeves.VALID_CAREER_SLEEVES
        },
        "defaults": {
            "sources": [source.get("id") for source in sources if source.get("available")],
            "location_mode": MVP_LOCATION_MODE,
            "strict": False,
            "max_results": 200,
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
        "location_modes": location_modes,
    }


# Root URL maps to this function
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/healthz')
def healthz():
    return jsonify({"status": "ok"})


# Redirect '/index.html' to '/'
@app.route('/index.html')
def redirect_to_index():
    return redirect('/')


# Routes for different decades
@app.route('/genesis')
def show_genesis():
    return render_template('genesis.html')


@app.route('/aspiration')
def show_aspiration():
    return render_template('aspiration.html')


@app.route('/enlightenment')
def show_enlightenment():
    return render_template('enlightenment.html')


@app.route('/synergy')
def show_synergy():
    return render_template('synergy.html')


@app.route('/synergy-sleeves', methods=['GET'])
def synergy_sleeves():
    with custom_sleeves_lock:
        payload = _career_sleeve_catalog()
    return jsonify(payload)


@app.route('/synergy-sleeves', methods=['POST'])
def save_synergy_sleeve():
    payload = request.get_json(silent=True) or {}
    requested_letter = _normalize_career_sleeve_letter(payload.get("letter"))
    if requested_letter and _is_fixed_career_sleeve_letter(requested_letter):
        return jsonify({"error": "Career Sleeves A-D are fixed and cannot be overwritten."}), 409
    if requested_letter and not _is_custom_career_sleeve_letter(requested_letter):
        return jsonify({"error": "Only letters E-Z can be saved as custom Career Sleeves."}), 400

    title = _clean_value(payload.get("title"), "")[:120]
    if not title:
        return jsonify({"error": "Title is required."}), 400

    queries = _parse_queries_for_storage(payload.get("queries"))
    if not queries:
        return jsonify({"error": "At least one search query is required."}), 400
    location_preferences = _parse_custom_location_preferences(payload.get("location_preferences"))
    allow_overwrite = bool(payload.get("allow_overwrite"))

    with custom_sleeves_lock:
        existing = _load_custom_career_sleeves()
        existing_map = {
            _normalize_career_sleeve_letter(entry.get("letter")): entry
            for entry in existing
            if isinstance(entry, dict)
        }
        if requested_letter:
            letter = requested_letter
        else:
            letter = _next_available_custom_career_sleeve_letter(existing)
            if not letter:
                return jsonify({"error": "No custom Career Sleeve letters available (E-Z are all in use)."}), 409

        if letter in existing_map and not allow_overwrite:
            return jsonify({"error": f"Custom Career Sleeve {letter} already exists. Use another letter."}), 409

        record = {
            "letter": letter,
            "title": title,
            "queries": queries,
            "location_preferences": location_preferences,
            "locked": False,
            "scope": "custom",
            "updated_at": _now_utc_stamp(),
        }
        filtered = [entry for entry in existing if _normalize_career_sleeve_letter(entry.get("letter")) != letter]
        filtered.append(record)
        filtered.sort(key=lambda entry: entry.get("letter", ""))
        if not _save_custom_career_sleeves(filtered):
            return jsonify({"error": "Could not persist custom Career Sleeve state."}), 500
        catalog = _career_sleeve_catalog()

    return jsonify({"ok": True, "saved": record, "catalog": catalog})


@app.route('/synergy-sleeves/<letter>', methods=['DELETE'])
def delete_synergy_sleeve(letter):
    normalized_letter = _normalize_career_sleeve_letter(letter)
    if not normalized_letter:
        return jsonify({"error": "Invalid Career Sleeve letter."}), 400
    if _is_fixed_career_sleeve_letter(normalized_letter):
        return jsonify({"error": "Career Sleeves A-D are fixed and cannot be deleted."}), 409
    if not _is_custom_career_sleeve_letter(normalized_letter):
        return jsonify({"error": "Only letters E-Z can be deleted as custom Career Sleeves."}), 400

    with custom_sleeves_lock:
        existing = _load_custom_career_sleeves()
        filtered = [
            entry for entry in existing
            if _normalize_career_sleeve_letter(entry.get("letter")) != normalized_letter
        ]
        if len(filtered) == len(existing):
            return jsonify({"error": f"Custom Career Sleeve {normalized_letter} was not found."}), 404
        if not _save_custom_career_sleeves(filtered):
            return jsonify({"error": "Could not persist custom Career Sleeve state."}), 500
        catalog = _career_sleeve_catalog()

    return jsonify({"ok": True, "deleted": normalized_letter, "catalog": catalog})


@app.route('/immersion')
def show_immersion():
    return render_template('immersion.html')


@app.route('/transcendence')
def show_transcendence():
    return render_template('transcendence.html')


@app.route('/toadstools')
def toadstools():
    return render_template('toadstools.html')


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


@app.route('/scrape-progress/<run_id>')
def scrape_progress(run_id):
    tail_raw = request.args.get("tail", "30")
    try:
        tail = int(tail_raw)
    except ValueError:
        tail = 30
    snapshot = _progress_snapshot(run_id, tail=tail)
    if not snapshot:
        return jsonify({"error": "run_not_found", "run_id": run_id}), 404
    return jsonify(snapshot)


@app.route('/company-opening')
def company_opening():
    def _is_external_company_url(url):
        return _is_public_destination_url(url) and not _is_platform_job_host(url)

    def _resolve_external_from_candidate(url, allow_network=False):
        candidate = _clean_value(url, "")
        if _is_external_company_url(candidate):
            return candidate
        extracted = _extract_external_destination_from_url(candidate)
        if _is_external_company_url(extracted):
            return extracted
        if not allow_network or not _is_allowed_platform_lookup_url(candidate):
            return ""
        host = _host_for_url(candidate)
        if _is_indeed_host(host):
            resolved = _resolve_external_from_indeed_redirect(
                candidate,
                timeout_seconds=10,
                max_hops=5,
                headers=_source_headers("Indeed", MVP_LOCATION_MODE),
            )
            if _is_external_company_url(resolved):
                return resolved
        if _is_linkedin_host(host):
            try:
                response = requests.get(
                    candidate,
                    headers=_source_headers("LinkedIn", MVP_LOCATION_MODE),
                    timeout=10,
                )
                if response.ok:
                    detail_links = _extract_linkedin_links_from_detail(response.text, response.url or candidate)
                    extracted_company = _clean_value(detail_links.get("company_url"), "")
                    if _is_external_company_url(extracted_company):
                        return extracted_company
                    resolved_from_response = _extract_external_destination_from_url(response.url or "")
                    if _is_external_company_url(resolved_from_response):
                        return resolved_from_response
            except requests.RequestException:
                return ""
        return ""

    def _error_response(message, status=424):
        payload = {"error": message, "code": "company_opening_unresolved"}
        accepts_json = (
            request.args.get("format", "").lower() == "json"
            or request.accept_mimetypes.best == "application/json"
        )
        if accepts_json:
            return jsonify(payload), status
        safe_message = html.escape(_clean_value(message, "Unknown error"), quote=True)
        html_body = (
            "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<title>Company Opening Error</title>"
            "<style>body{font-family:Manrope,Arial,sans-serif;background:#f7f7f7;color:#111;"
            "margin:0;padding:24px} .card{max-width:620px;margin:40px auto;padding:20px;"
            "border-radius:12px;background:#fff;box-shadow:0 8px 24px rgba(0,0,0,.08)}"
            "h1{margin:0 0 8px;font-size:1.2rem}p{margin:0 0 10px;line-height:1.4}"
            "code{background:#f0f0f0;padding:2px 6px;border-radius:6px}</style></head>"
            "<body><main class='card'><h1>Company opening URL not found</h1>"
            f"<p>{safe_message}</p>"
            "<p>The scraper tried redirect parsing and detail-page extraction, but no external company URL was resolved.</p>"
            "<p>Tip: open <code>Indeed URL</code> or <code>LinkedIn URL</code> and apply from there if needed.</p>"
            "</main></body></html>"
        )
        return html_body, status, {"Content-Type": "text/html; charset=utf-8"}

    company_url = _clean_value(request.args.get("company_url"), "")
    indeed_url = _clean_value(request.args.get("indeed_url"), "")
    linkedin_url = _clean_value(request.args.get("linkedin_url"), "")
    job_url = _clean_value(request.args.get("job_url"), "")

    resolved_company = _resolve_external_from_candidate(company_url, allow_network=False)
    if resolved_company:
        return redirect(resolved_company, code=302)

    resolved_from_job = _resolve_external_from_candidate(job_url, allow_network=True)
    if resolved_from_job:
        return redirect(resolved_from_job, code=302)

    attempted_sources = []
    if _is_allowed_platform_lookup_url(indeed_url):
        attempted_sources.append("Indeed")
        extracted = _resolve_external_from_candidate(indeed_url, allow_network=True)
        if extracted:
            return redirect(extracted, code=302)

    if _is_allowed_platform_lookup_url(linkedin_url):
        attempted_sources.append("LinkedIn")
        extracted = _resolve_external_from_candidate(linkedin_url, allow_network=True)
        if extracted:
            return redirect(extracted, code=302)

    if attempted_sources:
        return _error_response(
            (
                "Could not resolve company URL from "
                f"{' / '.join(attempted_sources)} page after all extraction steps."
            ),
            status=424,
        )

    return _error_response(
        "Missing usable URL inputs. Provide company_url, indeed_url, linkedin_url, or job_url.",
        status=400,
    )


# Route to trigger scraping
@app.route('/scrape')
def scrape():
    run_id = _clean_value(request.args.get("run_id"), "") or uuid.uuid4().hex[:12]
    profile = SCRAPE_MODE
    career_sleeve_key = request.args.get("career_sleeve", "").upper().strip()
    if career_sleeve_key not in sleeves.VALID_CAREER_SLEEVES:
        allowed = ", ".join(sorted(sleeves.VALID_CAREER_SLEEVES))
        return jsonify({"error": f"Invalid Career Sleeve. Use one of: {allowed}."}), 400

    location_mode = MVP_LOCATION_MODE

    strict_career_sleeve = request.args.get("strict", "0") == "1"
    force_refresh = request.args.get("refresh", "0") == "1"
    include_fail = request.args.get("include_fail", "0") == "1"

    max_results_raw = request.args.get("max_results", "200")
    try:
        max_results = int(max_results_raw)
    except ValueError:
        max_results = 200
    max_results = max(10, min(max_results, 500))

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
    requested_sources = [source.strip().lower() for source in sources_param.split(",") if source.strip()]
    scrape_variant = _normalize_scrape_variant(
        request.args.get("scrape_variant", SCRAPE_VARIANT_DEFAULT)
    )
    selected_sources = [source for source in MVP_SOURCE_IDS if source in SOURCE_REGISTRY]
    enforce_mvp_bundle = True
    parallel_fetch = len(selected_sources) > 1
    if scrape_variant == SCRAPE_VARIANT_ULTRA_FAST:
        selected_sources = [source for source in ULTRA_FAST_SOURCE_IDS if source in SOURCE_REGISTRY]
        enforce_mvp_bundle = False
        parallel_fetch = False
        max_pages = min(max_pages, ULTRA_FAST_MAX_PAGES)
        target_raw = min(target_raw, ULTRA_FAST_TARGET_RAW)
        requests_per_second = max(requests_per_second, ULTRA_FAST_MIN_RPS)
        detail_rps = max(detail_rps, ULTRA_FAST_MIN_DETAIL_RPS)
        no_new_unique_pages = min(no_new_unique_pages, ULTRA_FAST_NO_NEW_UNIQUE_PAGES)
    search_queries_param = request.args.get("search_queries", "")
    search_queries = _parse_search_queries(search_queries_param)
    extra_queries_param = request.args.get("extra_queries", "")
    extra_queries = _parse_extra_queries(extra_queries_param)
    custom_mode = request.args.get("custom_mode", "0") == "1"
    custom_letter = _normalize_career_sleeve_letter(request.args.get("custom_letter", ""))
    custom_geo_countries = _parse_geo_preferences_for_storage(
        request.args.get("custom_geo_countries", "")
    )
    custom_geo_regions = _parse_geo_preferences_for_storage(
        request.args.get("custom_geo_regions", "")
    )
    custom_abroad_min_percent = _parse_abroad_percent_for_storage(
        request.args.get("custom_abroad_min_percent", 0),
        fallback=0,
    )
    custom_abroad_max_percent = _parse_abroad_percent_for_storage(
        request.args.get("custom_abroad_max_percent", 100),
        fallback=100,
    )
    if custom_abroad_max_percent < custom_abroad_min_percent:
        custom_abroad_max_percent = custom_abroad_min_percent
    custom_location_preferences = _parse_custom_location_preferences(
        {
            "countries": custom_geo_countries,
            "regions": custom_geo_regions,
            "abroad_min_percent": custom_abroad_min_percent,
            "abroad_max_percent": custom_abroad_max_percent,
        }
    )
    custom_pref_requested = bool(
        custom_geo_countries
        or custom_geo_regions
        or request.args.get("custom_abroad_min_percent") is not None
        or request.args.get("custom_abroad_max_percent") is not None
    )
    if custom_mode and custom_letter and not custom_pref_requested:
        with custom_sleeves_lock:
            for record in _load_custom_career_sleeves():
                if _normalize_career_sleeve_letter(record.get("letter")) != custom_letter:
                    continue
                custom_location_preferences = _parse_custom_location_preferences(
                    record.get("location_preferences")
                )
                break
    if custom_mode and not search_queries:
        return jsonify({"error": "Custom Career Sleeve scraping requires at least one search query."}), 400
    allow_failover = False
    force_source_retry = request.args.get("retry_source", "0") == "1"

    _progress_start(
        run_id,
        profile=profile,
        career_sleeve=career_sleeve_key,
        location_mode=location_mode,
    )
    _progress_update(
        run_id,
        "start",
        (
            f"Scrape started for Career Sleeve {career_sleeve_key} "
            f"({location_mode}, profile {profile}, variant {scrape_variant})"
        ),
        career_sleeve=career_sleeve_key,
        location_mode=location_mode,
        scrape_variant=scrape_variant,
        selected_sources=selected_sources,
        requested_sources=requested_sources,
        enforce_mvp_bundle=bool(enforce_mvp_bundle),
        parallel_fetch=bool(parallel_fetch),
        strict_career_sleeve=strict_career_sleeve,
        force_refresh=force_refresh,
        max_pages=max_pages,
        target_raw=target_raw,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        max_results=max_results,
    )

    items, fetch_errors, used_sources, fetch_diagnostics = fetch_jobs_from_sources(
        selected_sources,
        career_sleeve_key,
        location_mode=location_mode,
        force_refresh=force_refresh,
        max_pages=max_pages,
        target_raw=target_raw,
        requests_per_second=requests_per_second,
        detail_rps=detail_rps,
        no_new_unique_pages=no_new_unique_pages,
        search_queries=search_queries,
        extra_queries=extra_queries,
        allow_failover=allow_failover,
        run_id=run_id,
        force_source_retry=force_source_retry,
        enforce_mvp_bundle=enforce_mvp_bundle,
        parallel_fetch=parallel_fetch,
    )
    _progress_update(
        run_id,
        "fetch-finished",
        f"Fetch finished: {len(items)} raw items from {', '.join(used_sources) if used_sources else 'no sources'}",
        raw_items=len(items),
        used_sources=used_sources,
        errors=fetch_errors,
    )
    if not items and fetch_errors:
        _progress_update(
            run_id,
            "fetch-warning",
            "All selected sources returned errors; returning empty result set with diagnostics.",
            errors=fetch_errors,
        )

    _progress_update(run_id, "ranking-start", "Ranking and filtering started")
    ranking_result = rank_and_filter_jobs(
        items,
        target_career_sleeve=career_sleeve_key,
        min_target_score=sleeves.MIN_PRIMARY_CAREER_SLEEVE_SCORE_TO_SHOW,
        location_mode=location_mode,
        strict_career_sleeve=strict_career_sleeve,
        include_fail=include_fail,
        return_diagnostics=True,
        diagnostics=fetch_diagnostics,
        custom_mode=custom_mode,
        custom_search_queries=search_queries,
        custom_location_preferences=custom_location_preferences,
    )
    candidate_items = ranking_result.get("jobs") or []
    incremental_skipped = 0
    if incremental_mode:
        candidate_items, incremental_skipped = _apply_incremental_filter(candidate_items, state_window_days)
    response_items = candidate_items[:max_results]
    funnel = ranking_result.get("funnel") or {}
    summary = {
        "run_id": run_id,
        "profile": profile,
        "config_version": RUNTIME_CONFIG.get("config_version", "1.0"),
        "career_sleeve": career_sleeve_key,
        "location_mode": location_mode,
        "scrape_variant": scrape_variant,
        "requested_sources": requested_sources,
        "enforced_sources": selected_sources,
        "enforce_mvp_bundle": bool(enforce_mvp_bundle),
        "fetch_parallel": bool(parallel_fetch),
        "effective_fetch_limits": {
            "max_pages": max_pages,
            "target_raw": target_raw,
            "requests_per_second": requests_per_second,
            "detail_requests_per_second": detail_rps,
            "no_new_unique_pages": no_new_unique_pages,
        },
        "search_queries": search_queries,
        "custom_mode": bool(custom_mode),
        "custom_letter": custom_letter,
        "custom_location_preferences": custom_location_preferences,
        "extra_queries": extra_queries,
        "sources_used": used_sources,
        "sources_used_labels": [
            SOURCE_REGISTRY.get(source_key, {}).get("label", source_key)
            for source_key in used_sources
        ],
        "source_errors": fetch_errors,
        "source_health": {
            source_id: _source_health_status(source_id)
            for source_id in SOURCE_REGISTRY
        },
        "force_source_retry": bool(force_source_retry),
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
        career_sleeve=career_sleeve_key,
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

    _progress_finish(
        run_id,
        status="done",
        summary=summary,
    )
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
    port = int(os.getenv("PORT", "8080"))
    app.run(host='0.0.0.0', port=port, threaded=True)









