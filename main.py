from flask import Flask, request, redirect, render_template, url_for, jsonify
from scrapy.crawler import CrawlerRunner
from scrapy.signalmanager import dispatcher
from scrapy import signals
from twisted.internet import reactor, defer
from scrapy_career.career_spiders.spiders.career_spiders import CareerSpiderIndeed, CareerSpiderLinkedIn
import requests
import random
import threading

# Create an instance of the Flask class
app = Flask(__name__)

# Global variable to handle invalid comic IDs
invalid_id = None

# Global list to store scraped items
scraped_items = []


# Root URL maps to this function
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')  # Render index.html


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
    global scraped_items
    error = None
    if not scraped_items:
        error = "No jobs found or an error occurred."
    return render_template('2020.html', jobs=scraped_items, error=error)


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
    global invalid_id
    data = request.form
    comic_id = data.get('comic_id')
    print("User requested comic_id: " + str(comic_id))
    if comic_id:
        try:
            comic_id = int(comic_id)
            if not 1 <= comic_id <= 2940:
                invalid_id = "True"
                raise ValueError(
                    "The requested comic_id doesn't exist (is out of range).")
            else:
                invalid_id = None
        except ValueError:
            print("Invalid input detected for comic_id")
            invalid_id = "True"
            comic_id = random.randint(1, 2940)
    else:
        print("No input detected for comic_id")
        invalid_id = "True"
        comic_id = random.randint(1, 2940)
    return redirect(url_for('show_comic', comic_id=comic_id))


# Show the XKCD comic based on comic_id
@app.route('/comic/<int:comic_id>')
def show_comic(comic_id):
    global invalid_id
    response = requests.get(f'https://xkcd.com/{comic_id}/info.0.json')
    data = response.json()
    print("Received comic_id: " + str(comic_id))
    return render_template('comic.html', data=data, invalid_id=invalid_id)


# Route to trigger scraping
@app.route('/scrape')
def scrape():
    global scraped_items
    scraped_items = []

    # Function to collect items from the Scrapy spiders
    def crawler_results(signal, sender, item, response, spider):
        scraped_items.append(item)
        print(f"Scraped item: {item}")

    # Connect the item_passed signal to the crawler_results function
    dispatcher.connect(crawler_results, signal=signals.item_passed)

    runner = CrawlerRunner()

    # Define the crawling process using inline callbacks
    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(CareerSpiderIndeed)
        yield runner.crawl(CareerSpiderLinkedIn)
        reactor.stop()

    # Function to start the crawling process
    def start_crawling():
        d = crawl()
        d.addBoth(lambda _: reactor.stop())

    # Ensure the reactor is running only once
    if not reactor.running:
        threading.Thread(
            target=lambda: reactor.run(installSignalHandlers=False)).start()
        threading.Thread(target=start_crawling).start()
    else:
        reactor.callFromThread(lambda: defer.ensureDeferred(crawl()))

    return jsonify(scraped_items)


# Main driver function
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, threaded=True)  # Run the Flask app
