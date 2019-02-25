# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 18:28:10 2019

@author: Diego
"""

# Este archivo:
# 0. Preparación.
# 1. Conectar Google.
# 2. Enlistar archivos de inventarios.
# 3. Descargar datos y revisar. 
# 4. Conectar PSQL y subir datos.   
#    4.1 Cache lista de días subidos. 
#    4.2 Descargar, test de descarga, subir. 

import os 


#%% 0. Preparación: input y paquetes. 

nombre_folder = "Inventario"
creds_folder  = f"../data/config"
google_creds  = {
    "token"       : os.path.join(creds_folder, "_token.json"), 
    "credentials" : os.path.join(creds_folder, "_credentials.json") }

import os 
import pandas as pd
from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import psycopg2 as psql
from sqlalchemy import create_engine
from dotenv import load_dotenv
from itertools import islice
from csv import QUOTE_ALL

load_dotenv(".env")

# Una funcioncita. 
currency_str = lambda str_series: pd.to_numeric(str_series.str.replace("[$,]", ""))


#%% 1. Conectar Google 

# Este código viene del Getting Started de la API. 
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 
          'https://www.googleapis.com/auth/spreadsheets.readonly']

store = file.Storage(google_creds["token"])
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(google_creds["credentials"], SCOPES)
    creds = tools.run_flow(flow, store)
servicio_drive = build('drive',  'v3', http=creds.authorize(Http()))
servicio_sheet = build('sheets', 'v4', http=creds.authorize(Http()))


#%% 3. Archivos de inventario     

busqueda_0 = servicio_drive.files().\
    list(q = f"name = '{nombre_folder}'").execute()
busqueda_1 = busqueda_0.get("files", [])
if len(busqueda_1) != 1: 
    raise(f"No es único el archivo con nombre '{nombre_folder}'.\n")
else: 
    folder_id = busqueda_1[0]["id"]
    
    
archivos_df = pd.DataFrame(columns = ["kind", "id", "name", "mimeType"])

un_request = servicio_drive.files().list(
        q = f"'{folder_id}' in parents and trashed = false")
while un_request is not None: 
    un_response = un_request.execute()    
    archivos_df = archivos_df.append( un_response["files"] )    
    un_request = servicio_drive.files().\
        list_next(un_request, un_response)

archivos_df = archivos_df.assign( 
    fecha = pd.to_datetime(
        archivos_df.name.str.extract(r"(\d{6})$", expand = False), 
        format = "%y%m%d") ) 


#%% 4. Archivos:  descargar, verificar y subir a la base. 
    
tipos_columnas = [("inventory_date", "datetime64"),  ("car_id", object),
    ("selling_status", object), ("physical_status", object), 
    ("legal_status",   object), ("internal_id", object),  ("vehicle_id", object),
    ("car_location",   object), ("car_name",    object),  ("car_cost", float), 
    ("allowance_cost", float),  ("total_cost",  float), 
    ("incoming_date", "datetime64"), ("inventory_days", float),
    ("status_days", float), ("created_at", "datetime64"), 
    ("updated_at", "datetime64") ]

# Columnas en la tabla de SQL. 
inventario_df = pd.DataFrame( 
    columns = [columna[0] for columna in tipos_columnas ]).\
    astype(dict(tipos_columnas))

for i, cada_fila in archivos_df.iterrows():
#for i, cada_fila in islice(archivos_df.iterrows(), 0, 1):
    
    print(f"Leyendo el archivo {i} de {archivos_df.shape[0]}.")
    inventario_req = servicio_sheet.spreadsheets().values().\
        get(spreadsheetId = cada_fila.id, range="Inventario!A:V").execute()
        
    cada_inventario_1 = pd.DataFrame(
            columns = inventario_req["values"][0], 
            data    = inventario_req["values"][1:]).\
        assign(fecha = cada_fila.fecha).\
        rename(columns = { 
            "fecha"            : "inventory_date",
            # "car_id",
            "Estatus de venta" : "selling_status", 
            "Estatus físico"   : "physical_status", 
            "Estatus legal"    : "legal_status",
            "ID" : "internal_id",
            "NIV": "vehicle_id",     
            "Ubicacion Actual" : "car_location", 
            # "car_name", 
            "Precio sin IVA"   : "car_cost"       , 
            "Allowance"        : "allowance_cost"  , 
            # "total_cost"      , 
            "Fecha de ingreso" : "incoming_date"   , 
            "Dias en inventario" : "inventory_days"  , 
            "Dias desde cambio de status" : "status_days",
            # "created_at" , 
            # "updated_at"      
            })
    cada_inventario = cada_inventario_1.assign(
            car_cost       = currency_str(cada_inventario_1.car_cost),
            allowance_cost = currency_str(cada_inventario_1.allowance_cost)).\
        astype(dict( (col, tipo) for (col, tipo) in tipos_columnas 
            if col in cada_inventario_1.columns ))
            
    inventario_df = inventario_df.append( cada_inventario, sort = False)

#%%
    
inventario_df_ok = inventario_df.assign(
        car_id   = inventario_df.internal_id, 
        car_name = inventario_df.Marca.str.cat(  
            inventario_df.Modelo, sep = " - ").str.cat(
            inventario_df.Año, sep = " - "), 
        selling_status = inventario_df.selling_status.str.extract(" - (.*)"),
        total_cost     = inventario_df.car_cost,
        created_at     = pd.Timestamp.now(), 
        updated_at     = pd.Timestamp.now()).drop(
            ["Tipo de compra", "Fecha de compra", "Precio con IVA", 
             "Origen de compra", "Persona compra", "Marca", "Modelo", 
             "Año", "Versión", "Color", "Num. de subastas"], axis = 1).\
    fillna({"allowance_cost":   0,
            "inventory_days":   0, "status_days": 0}).\
    astype({"inventory_days": int, "status_days":int })

csv_file = "../data/history/{}_{}_inventory.csv".format(
        min(inventario_df.inventory_date).strftime("%y%m%d"), 
        max(inventario_df.inventory_date).strftime("%y%m%d"))

inventario_df_ok.to_csv(csv_file, index = False, na_rep = "")


#%% Subir a la base. 

#inventario_df_ok = pd.read_csv("../data/history/inventory_backup.csv")

el_host = os.getenv("DB_HOST")
el_user = os.getenv("DB_USER")
la_base = os.getenv("DB_NAME")
el_pass = os.getenv("DB_PASS")

engine = create_engine(
    f'postgresql+psycopg2://{el_user}:{el_pass}@{el_host}/{la_base}')


#%%

conexion = psql.connect(
    host = el_host, user = el_user, dbname = la_base, password = el_pass)


#%%

conexion = engine.connect().connection
cursor   = conexion.cursor()

#conexion.rollback()
with open(csv_backup) as data_f:
    next(data_f)  # Para brincarse los headers. 
    cursor.copy_from(data_f, '"xinventoryDaily"', 
        columns = [columna for (columna, tipo) in tipos_columnas], 
        sep = ",", null = "")

conexion.close()


#%%




















