from flask import jsonify, render_template

from app import app
from connection_manager import DBConnect
from models import Match, EconomicSeries, EconomicMetadata

SAMPLE_START_DATE = '2014-01-01'

@app.route('/')
@app.route('/index')
def index():
	return render_template('bootstrap.html')


@app.route('/_btc_history')
def bitcoin_endpoint():
	session = DBConnect().create_session()
	btc_id = session.query(EconomicMetadata.id).filter(EconomicMetadata.quandl_code==None).one()[0]
	query = session.query(EconomicSeries.date, EconomicSeries.value)
	filtered_query = query.filter(EconomicSeries.series_id==btc_id)
	result = filtered_query.all()
	processed_result = [(a[0].isoformat(), float(a[1])) for a in result] # datetime into string, Decimal into float
	return jsonify(processed_result)