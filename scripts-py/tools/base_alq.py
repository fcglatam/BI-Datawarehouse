import os
from os import path, pardir
import sqlalchemy as alq
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.serializer import loads, dumps
from sqlalchemy.dialects import postgresql as psql 
from sqlalchemy.inspection import inspect

from dotenv import load_dotenv 
import pickle as pkl
load_dotenv("../data/config/.env")


Base = declarative_base()


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
      'postgresql://{user}:{pswd}@{host}/{name}'.format(
          user = os.getenv("PG_USER"), 
          pswd = os.getenv("PG_PASS"), 
          host = os.getenv("PG_HOST"), 
          name = os.getenv("PG_NAME")))  
  engine.logging_name = which_base
  return engine


def begin_session(engine):
  session = alq.orm.sessionmaker(bind=engine)
  return session()
  

def reflect_engine(engine, update=True, store=None):
  if store is None: 
    store = f'../data/config/cache/meta_{engine.logging_name}.pkl'
  
  if update or not os.path.isfile(store):
    meta = alq.MetaData()
    meta.reflect(bind=engine)
    with open(store, "wb") as opened:
      pkl.dump(meta, opened)
  else: 
    with open(store, "rb") as opened:
      meta = pkl.load(opened)
  return meta



def upsert_psql(df, table, engine, update_meta=True): 
  metadata = reflect_engine(engine, update=update_meta)
  
  table_obj = metadata.tables[table]
  keys = [key.name for key in inspect(table_obj).primary_key]
  
  records = df.to_dict(orient='records')
  insert_stmt = psql.insert(table_obj).values(records)
  update_dict = {c.name: c for c in insert_stmt.excluded}
  on_conflict = insert_stmt.\
    on_conflict_do_update(index_elements = keys, set_ = update_dict)
  result = engine.execute(on_conflict)
  return result
  
  
  
#  '''According to this blog: 
#  https://www.codementor.io/bruce3557/graceful-data-ingestion-with-sqlalchemy-and-pandas-pft7ddcy6
#  
#  4: Needs  connection/engine, MentorInformation, session
#  '''
#  
  # if method == 1:
  #   df.to_sql(table, engine, schema, index=False)

  # elif method == 2:
  #   logging.info("Start ingesting data into postgres ...")
  #   logging.info("Table name: {table}".format(table=table))
  #   logging.info("CSV schema: {schema}".format(schema=columns))
  #   conn = engine.connect().connection
  #   cursor = conn.cursor()
  #   cursor.copy_from(data, tablename, columns=columns, sep=sep, null='null')
  #   conn.commit()
  #   conn.close()
  #   logging.info("Finish ingesting")

  #   df.to_csv(csv_path, index=False, header=False)
  #   buldload_csv_data_to_database(engine, tablename, columns, data) 

  # elif method == 3: 
  #   Session = sessionmaker(bind=conn)
  #   session = Session()
  #   for _, row in df.iterrows():
  #     user = User(name=row["name"])
  #     session.add(user)
  #   session.commit()
  #   session.close()

  # elif method == 4: 
#     Session = sessionmaker(bind=dest_db_con)
#     session = Session()
#     session.bulk_insert_mappings(MentorInformation, df.to_dict(orient="records"))
#     session.close()






