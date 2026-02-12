from flask import Flask, request, redirect, render_template, url_for, jsonify
from scrapy.crawler import CrawlerProcess
from scrapy import signals
from scrapy.signalmanager import dispatcher
from scrapy_career.career_spiders.spiders.career_spiders import (
    CareerSpiderIndeed,
    CareerSpiderLinkedIn,
)
import multiprocessing
from queue import Empty
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
        "festival production",
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


def rank_and_filter_jobs(items, target_sleeve=None):
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
            if target_score < 4:
                continue
            primary_sleeve = target_sleeve
            primary_score = target_score

        foreign_hits = _find_hits(full_text, FOREIGN_KEYWORDS)
        explicit_foreign = bool(foreign_hits)
        inferred_foreign = bool(_find_hits(full_text, TRAVEL_ROLETYPE_KEYWORDS))
        has_foreign_mechanism = explicit_foreign or inferred_foreign

        growth_hits = _find_hits(full_text, GROWTH_KEYWORDS)
        has_growth = bool(growth_hits)
        has_strong_sleeve = primary_score >= 4

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


def run_scrapers(result_queue, sleeve_key):
    """Run Scrapy spiders in a child process so each request has a fresh reactor."""
    items = []
    crawl_errors = []
    spider_stats = {}

    def crawler_results(item, response, spider):
        items.append(dict(item))

    def crawler_error(failure, response, spider):
        crawl_errors.append(f"{spider.name}: {failure.getErrorMessage()}")

    def spider_closed(spider, reason):
        spider_stats[spider.name] = {"reason": reason}

    process = CrawlerProcess(
        settings={
            "LOG_ENABLED": False,
            "ROBOTSTXT_OBEY": False,
            "USER_AGENT": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "DOWNLOAD_TIMEOUT": 12,
            "RETRY_ENABLED": False,
            "CONCURRENT_REQUESTS": 8,
            "CLOSESPIDER_PAGECOUNT": 40,
            "CLOSESPIDER_TIMEOUT": 20,
        }
    )
    dispatcher.connect(crawler_results, signal=signals.item_scraped)
    dispatcher.connect(crawler_error, signal=signals.spider_error)
    dispatcher.connect(spider_closed, signal=signals.spider_closed)

    try:
        process.crawl(CareerSpiderIndeed, sleeve_key=sleeve_key)
        process.crawl(CareerSpiderLinkedIn, sleeve_key=sleeve_key)
        process.start(stop_after_crawl=True)
    except Exception as exc:  # pragma: no cover - defensive fallback
        result_queue.put({"error": f"Failed to scrape jobs: {exc}"})
        return
    finally:
        dispatcher.disconnect(crawler_results, signal=signals.item_scraped)
        dispatcher.disconnect(crawler_error, signal=signals.spider_error)
        dispatcher.disconnect(spider_closed, signal=signals.spider_closed)

    result_queue.put(
        {
            "items": items,
            "errors": crawl_errors,
            "spider_stats": spider_stats,
        }
    )


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

    result_queue = multiprocessing.Queue()
    scrape_process = multiprocessing.Process(
        target=run_scrapers,
        args=(result_queue, sleeve_key),
    )
    scrape_process.start()
    scrape_process.join(timeout=SCRAPE_TIMEOUT_SECONDS)

    if scrape_process.is_alive():
        scrape_process.terminate()
        scrape_process.join(timeout=3)
        if scrape_process.is_alive():
            scrape_process.kill()
            scrape_process.join(timeout=2)
        return jsonify({"error": "Scraping timed out. Please try again."}), 504

    try:
        result = result_queue.get_nowait()
    except Empty:
        if scrape_process.exitcode and scrape_process.exitcode != 0:
            return jsonify({"error": "Scraping process crashed before returning results."}), 500
        return jsonify({"error": "Scraping failed to return any results."}), 500

    if "error" in result:
        return jsonify(result), 500

    items = result.get("items", [])
    ranked_items = rank_and_filter_jobs(items, target_sleeve=sleeve_key)
    if ranked_items:
        return jsonify(ranked_items)

    details = []
    crawl_errors = result.get("errors", [])
    if crawl_errors:
        details.extend(crawl_errors)

    spider_stats = result.get("spider_stats", {})
    if spider_stats:
        reasons = ", ".join(
            f"{name} ({data.get('reason', 'unknown')})"
            for name, data in spider_stats.items()
        )
        details.append(f"Spider close reasons: {reasons}")

    message = (
        "Scraping completed but no jobs passed your filter "
        "(foreign mechanism + sleeve fit + growth)."
    )
    if details:
        message = f"{message} " + " | ".join(details)
    return jsonify({"error": message}), 502


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
