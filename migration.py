from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import os 
from dotenv import load_dotenv

load_dotenv()
Base = declarative_base()

# Dimension Tables
class Province(Base):
    __tablename__ = 'dim_province'
    province_id = Column(Integer, primary_key=True)
    province_name = Column(String)

class District(Base):
    __tablename__ = 'dim_district'
    district_id = Column(Integer, primary_key=True)
    province_id = Column(Integer)
    district_name = Column(String)

class Case(Base):
    __tablename__ = 'dim_case'
    id = Column(Integer, primary_key=True)
    status_name = Column(String)  # suspect, closecontact, probable, confirmation
    status_detail = Column(String)

# Fact Tables
class ProvinceDaily(Base):
    __tablename__ = 'province_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(Integer)
    case_id = Column(Integer)
    date = Column(String)
    total = Column(Integer)

class ProvinceMonthly(Base):
    __tablename__ = 'province_monthly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(Integer)
    case_id = Column(Integer)
    month = Column(String)
    total = Column(Integer)
    
class ProvinceYearly(Base):
    __tablename__ = 'province_yearly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(Integer)
    case_id = Column(Integer)
    year = Column(String)
    total = Column(Integer)
    
class DistrictMonthly(Base):
    __tablename__ = 'district_monthly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(Integer)
    case_id = Column(Integer)
    month = Column(String)
    total = Column(Integer)
    
class DistrictYearly(Base):
    __tablename__ = 'district_yearly'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(Integer)
    case_id = Column(Integer)
    year = Column(String)
    total = Column(Integer)


username = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
port = os.environ['POSTGRES_PORT']
db = os.environ['POSTGRES_DB']

engine = create_engine(f'postgresql://{username}:{password}@localhost:{port}/{db}')
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()
session.commit()

print("success migrate table")
session.close()
