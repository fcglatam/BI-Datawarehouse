# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 18:28:10 2019
@author: Diego
"""

#%% 0. Preparar THIS_DIR y seudo-activar-venv3 / preparar Autoreload.

import __main__ as main
from os import getcwd, path, pardir
from IPython import get_ipython

if hasattr(main, "__file__"):
    this_dir = path.dirname(path.realpath(__file__))
    activate_this = path.join(
        this_dir, pardir, "venv3", "bin", "activate_this.py")
    exec(open(activate_this).read(), {'__file__': activate_this})
else:
    this_dir = getcwd()
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')


#%% 1. Paquetes. 

import pandas as pd
from tools.base_alq import local_engine
from tools.google_api import (build_service, 
    drive_archivos_en_carpeta, sheets_leer_inventario)
from tools.base_alq import local_engine



#%% 2. Archivos de inventario     

# Se agrega columna de FECHA, a partir de los nombres en formato "yymmdd"

servicio_drive = build_service('drive',  'v3', this_dir)

archivos_df = drive_archivos_en_carpeta(servicio_drive, "Inventario").\
    assign(fecha = lambda df : pd.to_datetime(
        df.name.str.extract(r"(\d{6})$", expand = False), 
        format = "%y%m%d") ) 


#%% 3. Preparar tablas de inventario independientes y unir. 

servicio_sheet = build_service('sheets', 'v4', this_dir)

tipos_columnas = [("inventory_date", "datetime64"),  
    ("car_id", object), 
    ("selling_status", object), ("physical_status", object), 
    ("legal_status",   object), ("internal_id", object),  ("vehicle_id", object),
    ("car_location",   object), ("car_name",    object),  ("car_cost", float), 
    ("allowance_cost", float),  ("total_cost",  float), 
    ("incoming_date", "datetime64"), ("inventory_days", float),
    ("status_days", float), ("created_at", "datetime64"), 
    ("updated_at", "datetime64") ]
inventario_df = pd.DataFrame( 
    columns = [columna[0] for columna in tipos_columnas ]).\
    astype(dict(tipos_columnas))

for i, cada_fila in archivos_df.iterrows():
    inventario_1 = sheets_leer_inventario(servicio_sheet, cada_fila.id, cada_fila.fecha)              
    inventario_df = inventario_df.append( inventario_1, sort = False)

csv_file = "../data/history/{}_{}_inventory.csv".format(
        min(inventario_df.inventory_date).strftime("%y%m%d"), 
        max(inventario_df.inventory_date).strftime("%y%m%d"))

inventario_df.to_csv(csv_file, index=False, na_rep="")


#%% 4. Conectar POSTGRES y subir tabla. 

engine_pg = local_engine("postgres")

inventario_df_ok.to_sql("cars_inventory", 
  con=engine_pg, schema="public", if_exists="append", index=False)










