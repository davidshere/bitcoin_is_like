import os

ENV = 'dev'

CONFIG_VARIABLES = {
	'database': 'postgres',
	'username': os.environ['USERNAME'],
	'password': os.environ['PASSWORD'],
	'port': os.environ['DB_PORT'],
	'hostname': os.environ['HOSTNAME']
}

if ENV == 'prod':
	CONFIG_VARIABLES['username'] = 'dshere_master'
	CONFIG_VARIABLES['hostname'] = 'postgresql-shere.cayimxhn4zl7.us-west-2.rds.amazonaws.com' 


QUANDL_API_KEY = os.environ['QUANDL_KEY']
SECRET_KEY = os.environ['FLASK_WTF_SECRET_KEY']
WTF_CSRF_ENABLED = True
