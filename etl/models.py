from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref

from connection_manager import DBConnect

Base = declarative_base()

class EconomicMetadata(Base):
	__tablename__ = 'dim_series'

	id = Column(Integer, primary_key=True)
	quandl_code = Column(String) # this is the code that gets fed into the Quandl API
	source_code = Column(String) # The code for the source (i.e. GOOG)
	code = Column(String) # the code for the particular series, (i.e. NASDAQ_FB)
	series_name = Column(String) # The name of the series (i.e. NASDAQ - Apple)
	description = Column(String)
	last_updated = Column(Date)

	children = relationship('EconomicSeries')

class EconomicSeries(Base):
	__tablename__ = 'cust_series'

	id = Column(Integer, primary_key=True)
	series_id = Column(Integer, ForeignKey('dim_series.id')) #maps to Economic Metadata
	date = Column(Date)
	value = Column(Float(asdecimal=True)) # should be a floating number, probably. 

class Match(Base):
	__tablename__ = 'fact_match'

	id = Column(Integer, primary_key=True)
	start_date = Column(Date)
	end_date = Column(Date)
	series_id = Column(Integer, ForeignKey('dim_series.id')) # maps to Economic Metadata

	match = relationship(EconomicMetadata, backref=backref('fact_match', order_by=id))


if __name__ == '__main__':
	engine = DBConnect().create_engine()
	Base.metadata.create_all(engine)

