from flask import Flask, request, redirect, render_template, url_for, jsonify
from scrapy.crawler import CrawlerRunner
from scrapy.signalmanager import dispatcher
from twisted.internet import reactor, defer
from scrapy_career.career_spiders.spiders.career_spiders import CareerSpiderIndeed, CareerSpiderLinkedIn
import scrapy
import requests
import random

# Create an instance of the Flask class, which represents the Flask application
app = Flask(__name__)

# Initiate global variable invalid_id with no value
invalid_id = None

# Map the root url to this main.py file and execute the index function
@app.route('/', methods=['GET', 'POST'])
def index():
    # Render a webpage where the user is shown the contents of index.html with a timeline-menu to navigate to the listed decades and with a form where the user can input a comic_id to fetch a specific XKCD comic
    return render_template('index.html')

# Redirect '/index.html' to '/' so that '/index.html' is not displayed in the url
@app.route('/index.html')
def redirect_to_index():
    return redirect('/')

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

# Map the /comic url to this main.py file and execute the read_form function
@app.route('/comic', methods=['GET', 'POST'])
def read_form():
    # Make variable invalid_id available in this function
    global invalid_id
    # Get the comic_id from the form data and print it in the console
    data = request.form
    comic_id = data.get('comic_id')
    print("User requested comic_id: " + str(comic_id))
    # If comic_id was assigned a value (by the user), try to convert it to an integer and check if the corresponding comic exists
    if comic_id:
        try:
            # Convert the comic_id to an integer
            comic_id = int(comic_id)
            # Check if the comic_id is in the range 1 to 2877
            if not 1 <= comic_id <= 2877:
                # If not, assign value "True" to variable invalid_id and raise a ValueError
                invalid_id = "True"
                raise ValueError(
                    "The requested comic_id doesn't exist (is out of range).")
            else:
                # If a valid comic_id input was given, variable invalid_id should have no assigned value
                invalid_id = None
        except ValueError:
            # If an invalid or out-of-range comid_id was given, raise a ValueError that is printed in the console, assign value "True" to variable invalid_id and generate a random comic_id
            print("Invalid input detected for comic_id")
            invalid_id = "True"
            comic_id = random.randint(1, 2877)
    else:
        # If no input was given for comic_id, print that in the console, assign value "True" to invalid_id and generate a random comic_id
        print("No input detected for comic_id")
        invalid_id = "True"
        comic_id = random.randint(1, 2877)
    # Rendering of the following webpage is handled by the show_comic function, so send the comic_id to that function
    # The user is immediately redirected to the comic/comic_id url via the /comic url, so the /comic url never displays a webpage
    return redirect(url_for('show_comic', comic_id=comic_id))

# Map /comic/comic_id url to this main.py file and execute the show_comic function
@app.route('/comic/<int:comic_id>')
def show_comic(comic_id):
    global invalid_id
    # Get the JSON data for the requested comic from the XKCD API
    response = requests.get(f'https://xkcd.com/{comic_id}/info.0.json')
    data = response.json()
    # Print the received comic_id in the console
    print("Received comic_id: " + str(comic_id))
    # Render the comic.html template with the data
    return render_template('comic.html', data=data, invalid_id=invalid_id)

scraped_items = []

@app.route('/scrape')
def scrape():
    global scraped_items
    scraped_items = []

    def crawler_results(signal, sender, item, response, spider):
        scraped_items.append(item)

    dispatcher.connect(crawler_results, signal=scrapy.signals.item_passed)

    runner = CrawlerRunner()
    
    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(CareerSpiderIndeed)
        yield runner.crawl(CareerSpiderLinkedIn)
        reactor.stop()
    
    crawl()
    reactor.run()

    return jsonify(scraped_items)

# Main Driver Function
if __name__ == '__main__':
    # Run the application on the local development server
    app.run(host='0.0.0.0', port=8080)
