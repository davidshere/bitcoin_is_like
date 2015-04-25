from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Match(Base):
	__tablename__ = 'fact_match'

	id = Column(Integer, primary_key=True)
	start_date = Column(Integer)
	end_date = Column(Integer)
	match_id = Column(String) # maps to Economic Metadata

class EconomicMetadata(Base):
	__tablename__ = 'dim_series'

	id = Column(Integer, primary_key=True)
	code_for_fetcher = Column(String)
	series_name = Column(String)
	source_code = Column(String)
	series_source_name = Column(String)
	series_source_code = Column(String)
	description = Column(String)

class EconomicSeries(Base):
	__tablename__ = 'cust_series'

	id = Column(Integer, primary_key=True)
	series_id = Column(Integer) #maps to Economic Metadata
	date = Column(String)
	value = Column(Integer) # should be a floating number, probably. 

if __name__ == '__main__':
	engine = create_engine('sqlite:///:memory:', echo=True)

