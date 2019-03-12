from os import path, pardir
import pandas as pd, numpy as np
import sqlalchemy as alq
from sqlalchemy.dialects import mssql
from datetime import datetime as dt, timedelta as delta
from tools.base_alq import begin_session

# from tools.models_alq import (Cars, Makes, Models, Allowances, 
#   Auctions, Inspections, Changes, Dealers)


  


 
def get_inventory(engine, update_meta): 
  session  = begin_session(engine)  
  metadata = reflect_engine(engine, update=update_meta)

  Cars   = metadata.tables['Cars']
  Makes  = metadata.tables['CarManufacturers']
  Models = metadata.tables['CarModel']
  Allowances = metadata.tables['CarAllowances']
  
  allowances_sub = session.query(Allowances.c.car_id, 
        alq.func.sum(Allowances.c.allowance_sum).label('allowance_sum')).\
      group_by(Allowances.c.car_id).\
      subquery()
  
  cars_cols = [ getattr(Cars.c, cada_una) for cada_una in [
      'car_id',                   'internal_car_id',
      'car_selling_status',       'car_physical_status',      
      'car_legal_status',         'car_vin', 
      'purchase_channel',         'car_purchased_date', 
      'car_purchase_price_car',   'car_purchase_price_other', 
      'car_purchase_price_total', 'car_purchase_location', 
      'car_handedover_from_seller', 'car_current_location', 
      'client_subtype',           'year_manufactured',
      'car_trim' ,                'car_color']] + [
      Makes.c.car_manufacturer_name, 
      Models.c.car_model_name, 
      allowances_sub.c.allowance_sum ]
  
  statuses = {
      'selling'  : ['AVAILABLE','RESERVED','NOTAVAILABLE',
                    'PENDINGCLEARANCE','CONFIRMED','CONSIGNED'], 
      'physical' : ['INTRANSIT', 'ATOURLOCATION'] }
  
  inventory_conditions = alq.or_( 
      Cars.c.car_selling_status == 'INTERNALUSE', 
      alq.and_( Cars.c.car_legal_status != None,
                Cars.c.purchase_channel != None, 
                Cars.c.car_selling_status.in_( statuses['selling' ]),
                Cars.c.car_physical_status.in_(statuses['physical']), 
                ~ Cars.c.car_current_location.like('%Buyer%') ,
                ~ Cars.c.car_current_location.like('%B2B%'  ) ) )

  the_query = ( session.query(*cars_cols).
      join(Makes,  Cars.c.car_manufacturer_id == Makes.c.car_manufacturer_id).
      join(Models, Cars.c.car_model_id == Models.c.car_model_id).
      outerjoin(allowances_sub, Cars.c.car_id == allowances_sub.c.car_id).
      filter(inventory_conditions).
      statement )

  the_inventory = pd.read_sql(the_query, engine)
  return the_inventory
  


def get_cars(engine, project_dir, days_past=0): 
  if days_past: 
    changes_from = dt.today() - delta(days_past)
  else:
    changes_from = dt(2017, 1, 1, 0, 0)
    
  cars_file = path.join(project_dir, pardir, 
      'sql-queries', 'queries-repo', 'Tables', 'std_Cars.sql')
  with open(cars_file, encoding='utf-8-sig') as opened:
    the_script = opened.read() 
    
  date_cols = ["car_purchased_date", "car_handedover_from_seller",
     "car_handedover_to_buyer", "reserved_at_date", "original_reserved_at_date",
     "car_created", "invoice_date", "car_sold_date", 
     "latest_outgoing_payment_date", "latest_incoming_payment_date", 
     "auction_last_date", "deleted_at", "updated_at"]
  
  
  the_query = alq.text(the_script).bindparams(from_when=changes_from)    
  the_cars = pd.read_sql(the_query, engine, parse_dates=date_cols).\
    replace({col : {np.nan:None} for col in date_cols})
  
  return the_cars



def convert_inventory(inventory_in, for_day): 
  
  cols_compute = {
    'inventory_date'  : lambda df: for_day.date(),
    'car_id'          : lambda df: df.car_id,
    'selling_status'  : lambda df: df.car_selling_status, 
    'physical_status' : lambda df: df.car_physical_status, 
    'legal_status'    : lambda df: df.car_legal_status,
    'internal_id'     : lambda df: 'MX-' + df.internal_car_id.astype(str),
    'vehicle_id'      : lambda df: df.car_vin.str.replace(' .*', ''), 
    'car_location'    : lambda df: df.car_purchase_location, 
    'car_name'        : lambda df: df.car_manufacturer_name.str.\
        cat(sep=' - ', others = df.car_model_name.str.\
        cat(sep=' - ', others = df.year_manufactured.astype(str))), 
    'car_cost'        : lambda df: np.where(df.client_subtype == 'person', 
        df.car_purchase_price_car, df.car_purchase_price_car/1.16),
    'allowance_cost'  : lambda df: df.allowance_sum.fillna(0),
    'total_cost'      : lambda df: np.where(df.client_subtype == 'person', 
        df.car_purchase_price_total, df.car_purchase_price_total) ,
    'incoming_date'   : lambda df: df.car_handedover_from_seller.dt.date, 
    'inventory_days'  : lambda df: (for_day - df.car_handedover_from_seller).dt.days,
    'status_days'     : lambda df: (for_day - df.car_handedover_from_seller).dt.days, 
    'created_at'      : lambda df: dt.now(),
    'updated_at'      : lambda df: dt.now(),
    }

  inventory_out = inventory_in.assign(**cols_compute).\
      loc[:, tuple(cols_compute.keys())]
  return inventory_out


  
def get_some_cars(engine, metadata): 
  # Toy Example. 
  session = begin_session(engine)  

  Cars  = metadata.tables['Cars']
  Makes = metadata.tables['CarManufacturers']
  
  cars_cols = [ getattr(Cars.c, cada_una) for cada_una in [
      'car_id',                   
      'car_selling_status',       
      'car_purchased_date', 
      'car_purchase_price_car']] + [
      Makes.c.car_manufacturer_name]
  
  statuses = {
      'selling'  : ['AVAILABLE','RESERVED'], 
      'physical' : ['ATOURLOCATION'] }
  
  inventory_conditions = alq.and_( 
      Cars.c.purchase_channel == 'Inspection', 
      Cars.c.car_selling_status.in_( statuses['selling' ]),
      Cars.c.car_physical_status.in_(statuses['physical']),)

  the_query = ( session.query(*cars_cols).
      join(Makes, Cars.c.car_manufacturer_id == Makes.c.car_manufacturer_id).
      filter(inventory_conditions).
      statement )

  the_inventory = pd.read_sql(the_query, engine)
  return the_inventory


