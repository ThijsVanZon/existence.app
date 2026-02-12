from flask import Flask, request, redirect, render_template, url_for, jsonify
import math
import os
import random
import requests
import time
import re

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

SLEEVE_KEYWORDS = {
    "A": [
        "av technician", "audiovisual technician", "audiovisueel",
        "event technician", "show technician", "showtechniek",
        "stage crew", "stagehand", "showcrew", "foh", "boh",
        "monitor engineer", "live sound", "sound technician",
        "lighting technician", "lichttechnicus", "geluidstechnicus",
        "podiumtechniek", "licht", "geluid", "rigging", "led wall",
        "projection", "video technician", "production technician",
        "venue technician", "theater techniek", "festival production",
        "festival", "venue", "touring", "concert",
        "audio", "video", "broadcast", "streaming", "live production",
    ],
    "B": [
        "operations", "operations specialist", "ops manager",
        "revenue ops", "product ops", "customer success", "solutions engineer",
        "implementation consultant", "enablement", "workflow",
        "automation", "ai tooling", "ai workflow", "process improvement",
        "no-code", "low-code", "rpa", "integrations", "zapier", "make",
        "make.com", "airtable", "notion", "data operations",
        "analytics ops", "tooling", "systems", "jira", "confluence",
        "business analyst", "implementation",
    ],
    "C": [
        "experience design", "experience designer", "immersive",
        "creative producer", "producer", "project producer",
        "content producer", "production coordinator", "project manager",
        "project manager creative", "brand experience", "activation",
        "event producer", "festival production", "exhibition", "museum",
        "theme park creative", "scenography", "interactive installation",
        "creative technologist", "installation", "storytelling",
    ],
    "D": [
        "field service engineer", "service engineer", "service technician",
        "commissioning", "inbedrijfstelling", "installation", "installateur",
        "maintenance", "troubleshooting", "support engineer",
        "technical support", "onsite support", "on-site support",
        "systems engineer", "service coordinator", "werkvoorbereider",
        "technical operations", "field engineer", "site visits",
        "klantlocatie",
    ],
    "E": [
        "partnership manager", "partnerships manager", "community manager",
        "event marketer", "event marketing", "brand partnerships",
        "alliances", "bookings", "promoter", "venue relations",
        "program coordinator", "talent relations", "artist relations",
        "sponsorship", "festival partnerships", "event manager",
    ],
}
VALID_SLEEVES = set(SLEEVE_KEYWORDS.keys())

SLEEVE_CONTEXT_KEYWORDS = {
    "A": ["festival", "venue", "theater", "concert", "tour", "touring", "events", "livemuziek"],
    "B": ["saas", "platform", "b2b software", "integrations", "tooling", "enablement"],
    "C": ["experience", "immersive", "storytelling", "concept-to-delivery", "themapark", "activation"],
    "D": ["site visits", "travel", "op locatie", "klantlocatie", "field"],
    "E": ["music", "festival", "nightlife", "culture", "events", "creator economy"],
}

SLEEVE_B_TOOLING_KEYWORDS = [
    "automation", "ai tooling", "ai workflow", "no-code", "low-code", "rpa",
    "integrations", "zapier", "make", "make.com", "airtable", "notion",
    "jira", "confluence", "tooling", "systems",
]

SLEEVE_D_FIELD_KEYWORDS = [
    "field service", "service engineer", "service technician", "commissioning",
    "installation", "inbedrijfstelling", "maintenance", "troubleshooting",
    "onsite support", "on-site support", "technical operations", "field engineer",
]

SLEEVE_C_STRONG_KEYWORDS = [
    "creative producer", "content producer", "event producer", "project producer",
    "experience design", "experience designer", "immersive", "brand experience",
    "exhibition", "museum", "scenography", "interactive installation",
    "creative technologist", "production coordinator",
]

SLEEVE_E_MUST_HAVE_KEYWORDS = [
    "community manager", "partnership manager", "partnerships manager",
    "brand partnerships", "event marketing", "event marketer",
    "sponsorship", "festival partnerships", "event manager",
    "promoter", "bookings", "artist relations",
]

NEGATIVE_KEYWORDS = [
    "cashier", "kassa", "vakkenvuller", "orderpicker", "telemarketing",
    "callcenter", "geen reizen mogelijk", "alleen op locatie in nl",
    "must live within", "commission only", "commission-only",
]
GLOBAL_NEGATIVE_KEYWORDS = [
    "technical account manager", "account executive", "inside sales",
    "sales development", "b2c sales", "recruiter", "callcenter",
    "verzekering", "hypotheek", "sales only", "commission-only",
    "door to door", "cold calling", "tele-sales",
]
GERMAN_EXCLUSION_KEYWORDS = [
    "german required", "must speak german", "german speaker",
    "deutsch", "deutschland", "dach", "dach-only",
]

FOREIGN_KEYWORDS = [
    "remote", "work from abroad", "work-from-abroad", "anywhere",
    "hybrid", "international travel", "travel", "reizen", "site visit",
    "site visits", "on-site at client", "onsite at client", "rotatie",
    "rotation", "workation", "global", "international", "emea",
    "worldwide", "remote-first", "remote only", "work from home",
]

TRAVEL_ROLETYPE_KEYWORDS = [
    "field service", "service engineer", "technical operations",
    "production technician", "event technician", "venue technician",
    "installation", "commissioning", "festival", "touring",
]

GROWTH_KEYWORDS = [
    "owner", "ownership", "lead", "coordinate", "coordinator",
    "project", "delivery", "stakeholder", "process improvement",
    "automation", "technical depth", "implementation", "client impact",
    "verantwoordelijkheid", "coordinatie",
]

SYNERGY_KEYWORDS = [
    "event", "festival", "live", "music", "creative", "experience",
    "immersive", "av", "audio", "video", "automation", "ai", "workflow",
    "tooling", "systems", "production",
]

SLEEVE_SEARCH_TERMS = {
    "A": ["av technician", "event technician", "stagehand", "live sound"],
    "B": ["solutions engineer", "workflow automation", "product operations", "ai workflow"],
    "C": ["creative producer", "experience design", "event producer", "immersive installation"],
    "D": ["field service engineer", "technical operations", "commissioning engineer", "service technician"],
    "E": ["community manager events", "partnerships manager", "event marketing", "festival partnerships"],
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
COUNTRY_BLOCKLIST_KEYWORDS = [
    "germany", "deutschland", "berlin", "munich", "frankfurt", "dach",
]

REMOTIVE_API = "https://remotive.com/api/remote-jobs"
REMOTEOK_API = "https://remoteok.com/api"
THEMUSE_API = "https://www.themuse.com/api/public/jobs"
THEMUSE_PAGES = 3
ARBEITNOW_API = "https://www.arbeitnow.com/api/job-board-api"
JOBICY_API = "https://jobicy.com/api/v2/remote-jobs"
HIMALAYAS_API = "https://himalayas.app/jobs/api"
ADZUNA_API_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/nl/search/{page}"
SERPAPI_URL = "https://serpapi.com/search.json"


def _normalize_text(*parts):
    return " ".join(str(part or "") for part in parts).lower()


def _find_hits(text, keywords):
    return [keyword for keyword in keywords if keyword in text]


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


def _strip_html(value):
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return _clean_value(text, "")


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


def _passes_language_gate(text):
    return not bool(_find_hits(text, GERMAN_EXCLUSION_KEYWORDS))


def _passes_location_gate(text, location_mode):
    if location_mode == "global":
        return True

    has_non_eu_hint = any(keyword in text for keyword in NON_EU_COUNTRY_KEYWORDS)
    has_blocked_country = any(keyword in text for keyword in COUNTRY_BLOCKLIST_KEYWORDS)
    is_netherlands = _is_netherlands_job(text)
    if location_mode == "nl_only":
        return is_netherlands and not has_non_eu_hint and not has_blocked_country

    if is_netherlands:
        return True

    is_remote_or_hybrid = "remote" in text or "hybrid" in text
    has_eu_hint = any(keyword in text for keyword in EU_REMOTE_HINTS)
    has_global_remote_hint = any(keyword in text for keyword in GLOBAL_REMOTE_HINTS)

    if not is_remote_or_hybrid:
        return False

    if location_mode == "nl_eu":
        return (
            (has_eu_hint or has_global_remote_hint)
            and not has_blocked_country
            and not has_non_eu_hint
        )

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


def _score_sleeve(text, title_text, sleeve_keywords):
    text_hits = _find_hits(text, sleeve_keywords)
    title_hits = _find_hits(title_text, sleeve_keywords)
    raw_hits = len(set(text_hits)) + len(set(title_hits))

    if raw_hits >= 4:
        return 5
    if raw_hits == 3:
        return 4
    if raw_hits == 2:
        return 3
    if raw_hits == 1:
        return 2
    return 0


def _count_unique_hits(text, keywords):
    return len(set(_find_hits(text, keywords)))


def _passes_sleeve_must_have(sleeve_key, text, title_text):
    sleeve_keywords = SLEEVE_KEYWORDS.get(sleeve_key, [])
    sleeve_hits = len(
        set(_find_hits(text, sleeve_keywords)).union(_find_hits(title_text, sleeve_keywords))
    )
    context_hits = _count_unique_hits(text, SLEEVE_CONTEXT_KEYWORDS.get(sleeve_key, []))

    if sleeve_key == "A":
        return sleeve_hits >= 2 or (sleeve_hits >= 1 and context_hits >= 1)

    if sleeve_key == "B":
        tooling_hits = _count_unique_hits(text, SLEEVE_B_TOOLING_KEYWORDS)
        remote_hits = _count_unique_hits(text, ["remote", "hybrid", "work from home"])
        return tooling_hits >= 1 and remote_hits >= 1

    if sleeve_key == "C":
        title_strong_hits = _count_unique_hits(title_text, SLEEVE_C_STRONG_KEYWORDS)
        text_strong_hits = _count_unique_hits(text, SLEEVE_C_STRONG_KEYWORDS)
        return title_strong_hits >= 1 or text_strong_hits >= 2

    if sleeve_key == "D":
        field_hits = _count_unique_hits(text, SLEEVE_D_FIELD_KEYWORDS)
        travel_hits = _count_unique_hits(
            text,
            ["travel", "reizen", "site visit", "site visits", "on-site", "onsite", "klantlocatie"],
        )
        return field_hits >= 1 and travel_hits >= 1

    if sleeve_key == "E":
        title_hits = _count_unique_hits(title_text, SLEEVE_E_MUST_HAVE_KEYWORDS)
        text_hits = _count_unique_hits(text, SLEEVE_E_MUST_HAVE_KEYWORDS)
        return title_hits >= 1 or text_hits >= 2

    return True


def rank_and_filter_jobs(
    items,
    target_sleeve=None,
    min_target_score=4,
    location_mode="nl_only",
    strict_sleeve=True,
):
    ranked = []
    seen = set()

    for job in items:
        title = _clean_value(job.get("title"), "")
        company = _clean_value(job.get("company"), "")
        location = _clean_value(job.get("location"), "")
        snippet = _clean_value(job.get("snippet"), "")
        link = _clean_value(job.get("link"), "")
        source = _clean_value(job.get("source"), "unknown")
        date_posted = _clean_value(job.get("date"), "Unknown")
        salary = _clean_value(job.get("salary"), "Not listed")

        dedupe_key = (
            title.lower(),
            company.lower(),
            link.lower() if link and link != "Unknown" else "",
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        full_text = _normalize_text(
            title, company, location, snippet,
            job.get("work_mode_hint"), job.get("employment_type"), source,
        )
        title_text = _normalize_text(title)

        if _find_hits(full_text, NEGATIVE_KEYWORDS) or _find_hits(full_text, GLOBAL_NEGATIVE_KEYWORDS):
            continue
        if not _passes_language_gate(full_text):
            continue
        if not _passes_location_gate(full_text, location_mode):
            continue

        sleeve_scores = {
            sleeve: _score_sleeve(full_text, title_text, keywords)
            for sleeve, keywords in SLEEVE_KEYWORDS.items()
        }
        primary_sleeve, primary_score = max(
            sleeve_scores.items(),
            key=lambda pair: pair[1],
        )
        if target_sleeve:
            target_score = sleeve_scores.get(target_sleeve, 0)
            required_score = min_target_score if strict_sleeve else max(2, min_target_score - 1)
            if strict_sleeve and primary_sleeve != target_sleeve:
                continue
            if target_score < required_score:
                continue
            if not _passes_sleeve_must_have(target_sleeve, full_text, title_text):
                continue
            primary_sleeve = target_sleeve
            primary_score = target_score

        foreign_hits = _find_hits(full_text, FOREIGN_KEYWORDS)
        explicit_foreign = bool(foreign_hits)
        inferred_foreign = bool(_find_hits(full_text, TRAVEL_ROLETYPE_KEYWORDS))
        has_foreign_mechanism = explicit_foreign or inferred_foreign

        growth_hits = _find_hits(full_text, GROWTH_KEYWORDS)
        has_growth = bool(growth_hits)
        has_strong_sleeve = primary_score >= min_target_score

        if not has_foreign_mechanism:
            continue
        if strict_sleeve and sum([has_foreign_mechanism, has_strong_sleeve, has_growth]) < 2:
            continue
        if strict_sleeve and primary_score <= 2:
            continue
        if not strict_sleeve and primary_score <= 1:
            continue

        synergy_hits = _find_hits(full_text, SYNERGY_KEYWORDS)
        synergy_score = min(5, len(set(synergy_hits)))
        ownership_score = min(5, len(set(growth_hits)))
        abroad_clarity = 2 if explicit_foreign else 1
        distance_km, nearest_city = _estimate_distance_km(location)
        if distance_km is None:
            distance_bonus = 0
        elif distance_km <= 30:
            distance_bonus = 3
        elif distance_km <= 60:
            distance_bonus = 2
        elif distance_km <= 120:
            distance_bonus = 1
        else:
            distance_bonus = 0

        work_mode = _infer_work_mode(full_text)
        if explicit_foreign:
            foreign_label = "Explicit remote/hybrid/travel signal"
        else:
            foreign_label = "Inferred travel-compatible role type"

        why = [
            f"Sleeve {primary_sleeve} fit {primary_score}/5 "
            f"(A:{sleeve_scores['A']} B:{sleeve_scores['B']} C:{sleeve_scores['C']} "
            f"D:{sleeve_scores['D']} E:{sleeve_scores['E']})",
            f"International-work mechanism: {foreign_label}",
            (
                "Growth path: ownership and impact signals found"
                if has_growth
                else "Growth path: limited ownership signals"
            ),
        ]
        if distance_km is not None and nearest_city:
            why.append(f"Approx. {distance_km} km from Den Bosch (matched on {nearest_city})")

        ranked.append(
            {
                "title": title or "Unknown role",
                "company": company or "Unknown company",
                "location": location or "Unknown",
                "work_mode": work_mode,
                "foreign_mechanism": foreign_label,
                "sleeve_scores": sleeve_scores,
                "primary_sleeve": primary_sleeve,
                "primary_sleeve_score": primary_score,
                "why_relevant": why,
                "link": link,
                "date": date_posted,
                "salary": salary,
                "source": source,
                "distance_km": distance_km,
                "_rank": (
                    abroad_clarity,
                    primary_score,
                    distance_bonus,
                    synergy_score,
                    ownership_score,
                ),
            }
        )

    ranked.sort(key=lambda job: job["_rank"], reverse=True)
    for job in ranked:
        job.pop("_rank", None)
    return ranked


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
    terms = SLEEVE_SEARCH_TERMS.get(sleeve_key, [])
    return " OR ".join(terms[:4]) if terms else ""


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


def _fetch_jobicy_jobs(_sleeve_key=None, location_mode="nl_only"):
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


def _fetch_adzuna_jobs(sleeve_key, location_mode="nl_only"):
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


def _fetch_serpapi_jobs(sleeve_key, location_mode="nl_only", provider_filter=None):
    api_key = os.getenv("SERPAPI_API_KEY", "").strip()
    if not api_key:
        raise ValueError("SerpApi key is missing")
    query = _sleeve_query_string(sleeve_key)
    if location_mode in {"nl_only", "nl_eu"}:
        query = f"({query}) (remote OR hybrid OR travel) Netherlands"

    search_location = "Netherlands" if location_mode in {"nl_only", "nl_eu"} else "Europe"
    response = requests.get(
        SERPAPI_URL,
        timeout=12,
        params={
            "engine": "google_jobs",
            "q": query,
            "location": search_location,
            "hl": "en",
            "api_key": api_key,
        },
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


def _fetch_serpapi_indeed_jobs(sleeve_key, location_mode="nl_only"):
    return _fetch_serpapi_jobs(
        sleeve_key,
        location_mode=location_mode,
        provider_filter="indeed",
    )


def _fetch_serpapi_linkedin_jobs(sleeve_key, location_mode="nl_only"):
    return _fetch_serpapi_jobs(
        sleeve_key,
        location_mode=location_mode,
        provider_filter="linkedin",
    )


SOURCE_REGISTRY = {
    "remotive": {
        "label": "Remotive",
        "default_enabled": True,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_remotive_jobs,
    },
    "remoteok": {
        "label": "RemoteOK",
        "default_enabled": True,
        "requires_env": [],
        "query_based": False,
        "fetcher": _fetch_remoteok_jobs,
    },
    "jobicy": {
        "label": "Jobicy",
        "default_enabled": True,
        "requires_env": [],
        "query_based": True,
        "cache_by_sleeve": False,
        "fetcher": _fetch_jobicy_jobs,
    },
    "himalayas": {
        "label": "Himalayas",
        "default_enabled": True,
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


def _cache_key_for(source_key, sleeve_key, location_mode):
    config = SOURCE_REGISTRY[source_key]
    if config["query_based"] and config.get("cache_by_sleeve", True):
        return f"{source_key}:{sleeve_key}:{location_mode}"
    if config["query_based"]:
        return f"{source_key}:{location_mode}"
    return source_key


def _fetch_source_with_cache(source_key, sleeve_key, location_mode, force_refresh=False):
    cache_key = _cache_key_for(source_key, sleeve_key, location_mode)
    now = time.time()
    cache_entry = source_cache.get(cache_key)
    if (
        cache_entry
        and not force_refresh
        and now - cache_entry["fetched_at"] < JOBS_CACHE_TTL_SECONDS
    ):
        return cache_entry["items"], cache_entry["error"]

    fetcher = SOURCE_REGISTRY[source_key]["fetcher"]
    query_based = SOURCE_REGISTRY[source_key]["query_based"]
    try:
        items = fetcher(sleeve_key, location_mode=location_mode) if query_based else fetcher()
        error = None
    except Exception as exc:  # pragma: no cover
        items = []
        error = str(exc)
    _record_source_health(source_key, error)

    source_cache[cache_key] = {
        "items": items,
        "error": error,
        "fetched_at": now,
    }
    return items, error


def fetch_jobs_from_sources(selected_sources, sleeve_key, location_mode="nl_only", force_refresh=False):
    requested = [source for source in selected_sources if source in SOURCE_REGISTRY]
    if not requested:
        requested = _default_sources()
    usable_sources = [source for source in requested if _source_available(source)]
    if not usable_sources:
        return [], ["No available sources for the selected options."], []

    items = []
    errors = []
    for source_key in usable_sources:
        source_items, source_error = _fetch_source_with_cache(
            source_key,
            sleeve_key,
            location_mode,
            force_refresh=force_refresh,
        )
        items.extend(source_items)
        if source_error:
            errors.append(f"{source_key}: {source_error}")

    deduped = []
    seen_links = set()
    for item in items:
        link = _clean_value(item.get("link"), "")
        if link and link.lower() in seen_links:
            continue
        if link:
            seen_links.add(link.lower())
        deduped.append(item)

    return deduped, errors, usable_sources


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
        "defaults": {
            "sources": _default_sources(),
            "location_mode": "nl_only",
            "strict": False,
            "max_results": 30,
            "use_cache": True,
        },
        "location_modes": [
            {"id": "nl_only", "label": "Netherlands focus"},
            {"id": "nl_eu", "label": "Netherlands + EU remote"},
            {"id": "global", "label": "Global"},
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
    sleeve_key = request.args.get("sleeve", "").upper().strip()
    if sleeve_key not in VALID_SLEEVES:
        return jsonify({"error": "Invalid sleeve. Use one of: A, B, C, D, E."}), 400

    location_mode = request.args.get("location_mode", "nl_only").strip()
    if location_mode not in {"nl_only", "nl_eu", "global"}:
        location_mode = "nl_only"

    strict_sleeve = request.args.get("strict", "1") == "1"
    force_refresh = request.args.get("refresh", "0") == "1"

    max_results_raw = request.args.get("max_results", "20")
    try:
        max_results = int(max_results_raw)
    except ValueError:
        max_results = 20
    max_results = max(5, min(max_results, 100))

    sources_param = request.args.get("sources", "")
    selected_sources = [source.strip().lower() for source in sources_param.split(",") if source.strip()]

    items, fetch_errors, used_sources = fetch_jobs_from_sources(
        selected_sources,
        sleeve_key,
        location_mode=location_mode,
        force_refresh=force_refresh,
    )
    if not items and fetch_errors:
        return jsonify({"error": "All selected sources failed.", "details": fetch_errors}), 502

    ranked_items = rank_and_filter_jobs(
        items,
        target_sleeve=sleeve_key,
        min_target_score=3,
        location_mode=location_mode,
        strict_sleeve=strict_sleeve,
    )
    response_items = ranked_items[:max_results]
    response = jsonify(response_items)
    response.headers["X-Sources-Used"] = ",".join(used_sources)
    return response


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
