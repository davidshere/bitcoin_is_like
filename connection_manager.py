import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import get_db_config

DB_URI = os.environ['DB_URI']
ENV = os.environ['BIL_ENV']

class DBConnect(object):

	def __init__(self):
		self.engine = None
		self.config = get_db_config()

	def get_credentials(self):
		return DB_URI.format(database='postgres', 
							 username=self.config['username'], 
							 password=self.config['password'], 
							 hostname=self.config['hostname'], 
							 port=self.config['port'])

	def create_engine(self):
		pg_address = self.get_credentials()
		self.engine = create_engine((pg_address), echo=True)
		return self.engine

	def create_session(self):
		if not self.engine:
			self.engine = self.create_engine()
		Session = sessionmaker(bind=self.engine)
		return Session()