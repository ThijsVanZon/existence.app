from flask import Flask, render_template, redirect

# Create an instance of the Flask class, which represents the Flask application
app = Flask(__name__)

# Map the root url to this main.py file and execute the index function
@app.route('/', methods=['GET', 'POST'])
def index():
  # Render a webpage where the user is shown the contents of index.html with a timeline-menu to navigate to the listed decades
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

# Main Driver Function
if __name__ == '__main__':
  # Run the application on the local development server
  app.run(host='0.0.0.0', port=8080)
