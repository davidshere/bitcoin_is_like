import os

def get_db_config():
	config_vars = {
		'database': 'postgres',
		'username': os.environ['USERNAME'],
		'password': os.environ['PASSWORD'],
		'port': os.environ['DB_PORT'],
		'hostname': os.environ['HOSTNAME']
	}
	return config_vars

QUANDL_API_KEY = os.environ['QUANDL_KEY']
SECRET_KEY = os.environ['FLASK_WTF_SECRET_KEY']
WTF_CSRF_ENABLED = True
