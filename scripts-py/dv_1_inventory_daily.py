# -*- coding: utf-8 -*-
"""
Created on Thu Feb  7 17:23:26 2019
@author: Diego, basado en script de Quique. 
"""

# 0. Preparación
# 1. Procedimiento digerido.

%load_ext autoreload
%autoreload 2


#%% Paquetes y configuración. 

from tools import mssql_api as ms
from datetime import datetime as dt
from tools.base import local_engine, reflect_engine


#%% 1. Procedimiento digerido. 

engine_ms = local_engine("mssql")
meta_ms = reflect_engine(engine_ms, update = False)

inventory_ms = ms.get_inventory(engine_ms, meta_ms)
inventory_pg = ms.convert_inventory(inventory_ms, when = dt.today())

engine_pg = local_engine("postgresql")
inventory_pg.to_sql("xinventoryDaily", con = engine_pg, 
    if_exists = "append", index = False)









