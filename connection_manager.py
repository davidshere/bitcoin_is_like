import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

YAML_NAME = 'config.yaml'
YAML_ADDRESS = '{root}/{config}'.format(root=ROOT_DIR, config=YAML_NAME)
DB_URI = os.environ['DB_URI']
ENV = os.environ['BIL_ENV']

class DBConnect(object):

	def __init__(self, env=ENV):
		self.environment = env
		self.engine = None

	def get_credentials(self):
		with open(YAML_ADDRESS, 'r') as f:
			credentials = f.read()
			config = yaml.load(credentials)[self.environment]
		return DB_URI.format(database='postgres', 
							 username=config['username'], 
							 password=config['password'], 
							 hostname=config['hostname'], 
							 port=config['port'])

	def create_engine(self):
		pg_address = self.get_credentials()
		self.engine = create_engine((pg_address), echo=True)
		return self.engine

	def create_session(self):
		if not self.engine:
			self.engine = self.create_engine()
		Session = sessionmaker(bind=self.engine)
		return Session()