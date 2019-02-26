import pandas as pd, numpy as np
import sqlalchemy as alq
from sqlalchemy.dialects import mssql
from datetime import datetime as dt 
from tools.base import begin_session



 
def get_inventory(engine, metadata): 
  session = begin_session(engine)  

  Cars   = metadata.tables["Cars"]
  Makes  = metadata.tables["CarManufacturers"]
  Models = metadata.tables["CarModel"]
  Allows = metadata.tables["CarAllowances"]
  
  allows_sub = session.query(Allows.c.car_id, 
        alq.func.sum(Allows.c.allowance_sum).label("allowance_sum")).\
      group_by(Allows.c.car_id).\
      subquery()
  
  cars_cols = [ getattr(Cars.c, cada_una) for cada_una in [
      "car_id",                   "internal_car_id",
      "car_selling_status", 
      "car_physical_status",      "car_legal_status",
      "car_vin", 
      "purchase_channel",         "car_purchased_date", 
      "car_purchase_price_car",   "car_purchase_price_other", 
      "car_purchase_price_total", "car_purchase_location", 
      "car_handedover_from_seller", "car_current_location", 
      "client_subtype",           "year_manufactured",
      "car_trim" ,                "car_color"]] + [
      Makes.c.car_manufacturer_name, 
      Models.c.car_model_name, 
      allows_sub.c.allowance_sum ]
  
  statuses = {
      "selling"  :   ['AVAILABLE','RESERVED','NOTAVAILABLE',
                    'PENDINGCLEARANCE','CONFIRMED','CONSIGNED'], 
      "physical" : ["INTRANSIT", "ATOURLOCATION"] }
  
  inventory_conditions = alq.or_( 
      Cars.c.car_selling_status == "INTERNALUSE", 
      alq.and_( Cars.c.car_legal_status != None,
                Cars.c.purchase_channel != None, 
                Cars.c.car_selling_status.in_( statuses["selling" ]),
                Cars.c.car_physical_status.in_(statuses["physical"]), 
                ~ Cars.c.car_current_location.like("%Buyer%") ,
                ~ Cars.c.car_current_location.like("%B2B%") ) )

  the_query = ( session.query(*cars_cols).
      join(Makes, Cars.c.car_manufacturer_id == Makes.c.car_manufacturer_id).
      join(Models, Cars.c.car_model_id == Models.c.car_model_id).
      outerjoin(allows_sub, Cars.c.car_id == allows_sub.c.car_id).
      filter(inventory_conditions).
      statement )

  the_inventory = pd.read_sql(the_query, engine)
  return the_inventory
  


def convert_inventory(inventory_in, for_day): 
  
  cols_compute = {
    "inventory_date"  : lambda df: for_day.date(),
    "car_id"          : lambda df: df.car_id,
    "selling_status"  : lambda df: df.car_selling_status, 
    "physical_status" : lambda df: df.car_physical_status, 
    "legal_status"    : lambda df: df.car_legal_status,
    "internal_id"     : lambda df: "MX-" + df.internal_car_id.astype(str),
    "vehicle_id"      : lambda df: df.car_vin.str.replace(" .*", ""), 
    "car_location"    : lambda df: df.car_purchase_location, 
    "car_name"        : lambda df: df.car_manufacturer_name.str.\
        cat(sep = " - ", others = df.car_model_name.str.\
        cat(sep = " - ", others = df.year_manufactured.astype(str) ) ), 
    "car_cost"        : lambda df: np.where(df.client_subtype == "person", 
        df.car_purchase_price_car, df.car_purchase_price_car/1.16),
    "allowance_cost"  : lambda df: df.allowance_sum.fillna(0),
    "total_cost"      : lambda df: np.where(df.client_subtype == "person", 
        df.car_purchase_price_total, df.car_purchase_price_total) ,
    "incoming_date"   : lambda df: df.car_handedover_from_seller.dt.date, 
    "inventory_days"  : lambda df: (for_day - df.car_handedover_from_seller).dt.days,
    "status_days"     : lambda df: (for_day - df.car_handedover_from_seller).dt.days, 
    "created_at"      : lambda df: dt.now(),
    "updated_at"      : lambda df: dt.now(),
    }

  inventory_out = inventory_in.assign(**cols_compute).\
    loc[:, tuple(cols_compute.keys()) ].\
    fillna({"allowance_cost" : 0})
  return inventory_out


    
    
    
    
    
  