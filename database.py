from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func,Boolean,Float
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()
engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')


class FSSubject(Base):
    __tablename__ = 'fssubject'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class FirstClassSubject(Base):
    __tablename__ = 'firstclasssubject'
    id = Column(Integer, primary_key=True)
    code = Column(String)
    name = Column(String)

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    legal_representative=Column(String)
    establish_date = Column(DateTime)
    address = Column(String)
    code = Column(String,unique=True)
    registered_capital = Column(String)
    business_scope = Column(String)
    holders = relationship('Holder', backref='company')


class Holder(Base):
    __tablename__ = 'holder'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ratio = Column(Float)
    company_code = Column(String, ForeignKey('company.code'))



Base.metadata.create_all(engine)