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

# Create an instance of the Flask class
app = Flask(__name__)

# Cache the latest XKCD id to avoid hardcoded limits and repeated API calls
latest_comic_cache = {"id": None, "fetched_at": 0}
CACHE_TTL_SECONDS = 3600
DEFAULT_COMIC_ID = 3000
SCRAPE_TIMEOUT_SECONDS = 60


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


def run_scrapers(result_queue):
    """Run Scrapy spiders in a child process so each request has a fresh reactor."""
    items = []

    def crawler_results(item, response, spider):
        items.append(dict(item))

    process = CrawlerProcess(settings={"LOG_ENABLED": False})
    dispatcher.connect(crawler_results, signal=signals.item_scraped)

    try:
        process.crawl(CareerSpiderIndeed)
        process.crawl(CareerSpiderLinkedIn)
        process.start(stop_after_crawl=True)
    except Exception as exc:  # pragma: no cover - defensive fallback
        result_queue.put({"error": f"Failed to scrape jobs: {exc}"})
        return
    finally:
        dispatcher.disconnect(crawler_results, signal=signals.item_scraped)

    result_queue.put({"items": items})


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
    result_queue = multiprocessing.Queue()
    scrape_process = multiprocessing.Process(target=run_scrapers, args=(result_queue,))
    scrape_process.start()
    scrape_process.join(timeout=SCRAPE_TIMEOUT_SECONDS)

    if scrape_process.is_alive():
        scrape_process.terminate()
        scrape_process.join()
        return jsonify({"error": "Scraping timed out. Please try again."}), 504

    try:
        result = result_queue.get_nowait()
    except Empty:
        if scrape_process.exitcode and scrape_process.exitcode != 0:
            return jsonify({"error": "Scraping process crashed before returning results."}), 500
        return jsonify({"error": "Scraping failed to return any results."}), 500

    if "error" in result:
        return jsonify(result), 500

    return jsonify(result.get("items", []))


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)
