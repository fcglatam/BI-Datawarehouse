import os
from urllib.parse import quote_plus
import sqlalchemy as alq
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.serializer import loads, dumps
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv 
import pickle as pk
load_dotenv(".env")


def local_engine(which_base): 
  if which_base == "mssql":
    quote_at = quote_plus("@")
    the_engine = create_engine(  
       'mssql://{user}:{pass_}@{host}/{name}?driver={driver}'.format(
          user   = os.getenv("MS_USER"), 
          pass_  = os.getenv("MS_PASS"), 
          host   = os.getenv("MS_SERVER"), 
          name   = os.getenv("MS_NAME"),  
          driver = os.getenv("MS_DRIVER").replace(" ", "+") ))      
    
  elif which_base == "postgresql":
    the_engine = create_engine(
      'postgresql://{user}:{pass_}@{host}/{name}'.format(
          user   = os.getenv("PG_USER"), 
          pass_  = os.getenv("PG_PASS"), 
          host   = os.getenv("PG_HOST"), 
          name   = os.getenv("PG_NAME") ))  
  the_engine.logging_name = which_base
  return the_engine


def begin_session(engine):
  session = sessionmaker(bind=engine)
  return session()
  

def reflect_engine(engine, update = True, file = None):
  if file is None: 
    file = f"../data/config/meta_{ engine.logging_name }.pkl"
  
  if update | ~os.path.isfile(file):
    meta = alq.MetaData()
    meta.reflect(engine)
    with open(file, "wb") as opened:
      pk.dump(meta, opened)
  else: 
    meta = pk.load(file)

  return meta


Base = declarative_base()










