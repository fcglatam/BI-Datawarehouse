import os
import sqlalchemy as alq
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.serializer import loads, dumps
from dotenv import load_dotenv 
import pickle as pkl
load_dotenv("../data/config/.env")


def local_engine(which_base): 
  if which_base == "mssql":
    engine = alq.create_engine(  
       'mssql://{user}:{pass_}@{host}/{name}?driver={driver}'.format(
          user   = os.getenv("MS_USER"), 
          pass_  = os.getenv("MS_PASS"), 
          host   = os.getenv("MS_SERVER"), 
          name   = os.getenv("MS_NAME"),  
          driver = os.getenv("MS_DRIVER").replace(" ", "+") ))      
    
  elif which_base == "postgresql":
    engine = alq.create_engine(
      'postgresql://{user}:{pass_}@{host}/{name}'.format(
          user   = os.getenv("PG_USER"), 
          pass_  = os.getenv("PG_PASS"), 
          host   = os.getenv("PG_HOST"), 
          name   = os.getenv("PG_NAME") ))  
  engine.logging_name = which_base
  return engine


def begin_session(engine):
  session = alq.orm.sessionmaker(bind=engine)
  return session()
  

def reflect_engine(engine, update=True, store=None):
  if store is None: 
    store = f"../data/config/meta_{ engine.logging_name }.pkl"
  
  if update | ~os.path.isfile(store):
    meta = alq.MetaData()
    meta.reflect(engine)
    with open(store, "wb") as opened:
      pkl.dump(meta, opened)
  else: 
    meta = pkl.load(store)

  return meta


Base = declarative_base()










