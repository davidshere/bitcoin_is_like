import os

def get_db_config():
	config_vars = {
		'database': os.environ['DB_NAME'],
		'username': os.environ['DB_USERNAME'],
		'password': os.environ['DB_PASSWORD'],
		'port': os.environ['DB_PORT'],
		'hostname': os.environ['DB_HOSTNAME']
	}
	return config_vars

QUANDL_API_KEY = os.environ['QUANDL_KEY']
SECRET_KEY = os.environ['FLASK_WTF_SECRET_KEY']
WTF_CSRF_ENABLED = True
