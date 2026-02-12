from flask import Flask, request, redirect, render_template, url_for, jsonify
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
SCRAPE_TIMEOUT_SECONDS = 45

SLEEVE_KEYWORDS = {
    "A": [
        "av technician", "audiovisueel", "event technician", "showtechniek",
        "stage crew", "foh", "boh", "monitor engineer", "lighting technician",
        "licht", "geluid", "rigging", "led wall", "projection", "stagehand",
        "production technician", "venue technician", "theater techniek",
        "festival production", "audio", "video", "broadcast", "streaming",
        "media production", "sound", "live production",
    ],
    "B": [
        "operations", "revenue ops", "product ops", "customer success",
        "solutions engineer", "implementation consultant", "enablement",
        "workflow", "automation", "ai tooling", "process improvement",
        "no-code", "low-code", "integrations", "zapier", "make",
        "data operations", "analytics ops", "tooling", "systems",
        "jira", "confluence",
    ],
    "C": [
        "experience design", "immersive", "creative producer",
        "content producer", "production coordinator", "project manager",
        "brand experience", "activation", "exhibition", "museum",
        "theme park creative", "scenography", "interactive installation",
    ],
    "D": [
        "field service engineer", "service engineer", "commissioning",
        "installation", "maintenance", "troubleshooting",
        "technical support", "onsite support", "systems engineer",
        "service coordinator", "werkvoorbereider", "technical operations",
    ],
    "E": [
        "partnership manager", "community manager", "event marketer",
        "brand partnerships", "alliances", "bookings", "promoter",
        "venue relations", "program coordinator", "talent relations",
        "account manager", "festival partnerships",
    ],
}
VALID_SLEEVES = set(SLEEVE_KEYWORDS.keys())

NEGATIVE_KEYWORDS = [
    "cashier", "kassa", "vakkenvuller", "orderpicker", "telemarketing",
    "callcenter", "geen reizen mogelijk", "alleen op locatie in nl",
    "must live within", "commission only", "commission-only",
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
    "A": ["av technician", "event technician", "production technician", "live sound"],
    "B": ["solutions engineer", "workflow automation", "product operations", "ai tooling"],
    "C": ["creative producer", "experience design", "immersive design", "brand activation"],
    "D": ["field service engineer", "technical operations", "commissioning engineer", "onsite support"],
    "E": ["community manager events", "partnership manager", "event marketing", "festival partnerships"],
}

JOB_SOURCE_NAME = "Remotive"
JOB_SOURCE_API = "https://remotive.com/api/remote-jobs"
ARBEITNOW_SOURCE_NAME = "Arbeitnow"
ARBEITNOW_API = "https://www.arbeitnow.com/api/job-board-api"


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


def rank_and_filter_jobs(items, target_sleeve=None, min_target_score=4):
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

        if _find_hits(full_text, NEGATIVE_KEYWORDS):
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
            if target_score < min_target_score:
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
        if sum([has_foreign_mechanism, has_strong_sleeve, has_growth]) < 2:
            continue
        if primary_score <= 2:
            continue

        synergy_hits = _find_hits(full_text, SYNERGY_KEYWORDS)
        synergy_score = min(5, len(set(synergy_hits)))
        ownership_score = min(5, len(set(growth_hits)))
        abroad_clarity = 2 if explicit_foreign else 1

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
                "_rank": (
                    abroad_clarity,
                    primary_score,
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


def fetch_jobs_for_sleeve(sleeve_key):
    seen = set()
    items = []
    remotive = requests.get(JOB_SOURCE_API, timeout=12)
    remotive.raise_for_status()
    for job in remotive.json().get("jobs", []):
        link = _clean_value(job.get("url"), "")
        if not link or link in seen:
            continue
        seen.add(link)
        location = _clean_value(job.get("candidate_required_location"), "")
        category = _clean_value(job.get("category"), "")
        tags = " ".join(job.get("tags") or [])
        items.append(
            {
                "title": _clean_value(job.get("title"), ""),
                "company": _clean_value(job.get("company_name"), ""),
                "location": location,
                "link": link,
                "snippet": _strip_html(job.get("description")),
                "salary": _clean_value(job.get("salary"), "Not listed"),
                "date": _clean_value(job.get("publication_date"), "Unknown"),
                "work_mode_hint": _normalize_text(job.get("job_type"), location, category, tags),
                "source": JOB_SOURCE_NAME,
            }
        )

    arbeitnow = requests.get(ARBEITNOW_API, timeout=12)
    arbeitnow.raise_for_status()
    for job in arbeitnow.json().get("data", []):
        link = _clean_value(job.get("url"), "")
        if not link or link in seen:
            continue
        seen.add(link)
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
                "link": link,
                "snippet": _strip_html(job.get("description")),
                "salary": "Not listed",
                "date": date_value,
                "work_mode_hint": _normalize_text(remote_flag, location, tags, job_types),
                "source": ARBEITNOW_SOURCE_NAME,
            }
        )

    return items


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


# Route to trigger scraping
@app.route('/scrape')
def scrape():
    sleeve_key = request.args.get("sleeve", "").upper().strip()
    if sleeve_key not in VALID_SLEEVES:
        return jsonify({"error": "Invalid sleeve. Use one of: A, B, C, D, E."}), 400

    try:
        items = fetch_jobs_for_sleeve(sleeve_key)
    except requests.Timeout:
        return jsonify({"error": "Job fetch timed out. Please try again."}), 504
    except requests.RequestException as exc:
        return jsonify({"error": f"Failed to fetch jobs from source: {exc}"}), 502

    ranked_items = rank_and_filter_jobs(items, target_sleeve=sleeve_key, min_target_score=4)
    if not ranked_items:
        ranked_items = rank_and_filter_jobs(items, target_sleeve=sleeve_key, min_target_score=3)
    if not ranked_items:
        ranked_items = rank_and_filter_jobs(items, target_sleeve=sleeve_key, min_target_score=2)
    if ranked_items:
        return jsonify(ranked_items)
    return jsonify([])


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
