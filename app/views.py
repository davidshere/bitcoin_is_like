from flask import jsonify, render_template, request

from app import app
from series_fetcher import SeriesFetcher

@app.route('/')
@app.route('/index')
def index():
	return render_template('index.html')

@app.route('/_btc_history')
def bitcoin_endpoint():
	bitcoin_series = SeriesFetcher().fetch_bitcoin_series()
	return jsonify(bitcoin_series)


@app.route('/_fetch_match_series', methods=['GET', 'POST'])
def match_endpoint():
	''' We haven't implemented the end date yet, so for now we'll only utilize the start_date '''
	start_date, end_date = request.form.values()
	match_dict = SeriesFetcher().fetch_match(start_date=start_date, end_date=end_date)
	return jsonify(match_dict)

@app.route('/_match_range')
def match_range():
	dates = SeriesFetcher().fetch_last_match()
	return jsonify(dates)

@app.route('/about-btc')
def about_btc():
	return render_template('base.html')


@app.route('/about-bil')
def about_bil():
	return render_template('about-bil.html')
