from os import path, getenv
import pandas as pd
from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import argparse
from dotenv import load_dotenv
load_dotenv("../data/config/.env")


class ganalytics_service:
    
    def __init__(self, 
                 scopes = 'https://www.googleapis.com/auth/analytics.readonly',
                 discovery_uri = ('https://analyticsreporting.googleapis.com/$discovery/rest'),
                #Dirección de credenciales
                 credentials_dir = 'Ganalytics_token.json',
                 client_secret_dir = 'GoogleAPIsCredentials.json'):
        # Por el proyecto, asume las credenciales en ../data/config/
        # Ver variables de entorno. 
        self.scopes = scopes
        self.discovery_uri = discovery_uri
        self.credentials_dir = credentials_dir
        self.client_secret_dir = client_secret_dir
        self.store = file.Storage(credentials_dir)
        self.creds = self.store.get()
        if not self.creds or self.creds.invalid:
            parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                               parents=[tools.argparser])
            flags = parser.parse_args([])
            flow = client.flow_from_clientsecrets(self.client_secret_dir
                                                  , scope=self.scopes
                                                  , message=tools.message_if_missing(self.client_secret_dir))
      
            self.creds = tools.run_flow(flow, self.store, flags)
    
    def build_service(self):
      return build('analytics', 'v4', http = self.creds.authorize(Http()), discoveryServiceUrl= self.discovery_uri)




def ganalyticsTable(metriclist, dimensionlist, end_date, init_date):
    
    analytics = ganalytics_service().build_service()
    
    view_all_web_data_id = '138397785'
    #Diccionario principal
    body = {}
    #Lista de request a hacer    
    reportRequests = []
    #Request 1
    request = {}
    #view de analytics a usar
    request['viewId'] = view_all_web_data_id
    request['samplingLevel'] = 'LARGE'
    #Cantidad de datos
    request['pageSize'] = 1000
    #Fechas de la request 1
    request['dateRanges'] = []
    dates = {}
    dates['startDate'] = init_date
    dates['endDate'] = end_date
    request['dateRanges'].append(dates)
    
    #Metricas de la request 1
    request['metrics'] = []
    for metric_req in metriclist:
        request['metrics'].append({'expression': metric_req})
    #Dimensiones de la request 1
    request['dimensions'] = []
    for dimension_req in dimensionlist:
        request['dimensions'].append({'name':dimension_req})
    
    #Filtro por si hay dimensión con event label
    if 'ga:eventlabel' in dimensionlist:
        request['dimensionFilterClauses'] = {}
        request['dimensionFilterClauses']['filters'] = []
    
        filters = {}
        filters['dimensionName'] = 'ga:eventLabel'
        filters['expressions'] = []
        filters['expressions'].append('gotQuote')
        request['dimensionFilterClauses']['filters'].append(filters)
    #Agregar los parametros al diccionario principal
    reportRequests.append(request)
    body['reportRequests'] = reportRequests
    
    #Respuesta de analytics  
    response = analytics.reports().batchGet(body = body).execute()  
    
    #Parsear la respuesta a un dataframe
    df = pd.DataFrame()
    for rows in response['reports'][0]['data']['rows']:
        for periodMetric in rows['metrics']:
            for metric in periodMetric['values']:
                rows['dimensions'].append(metric)
        df = df.append([rows['dimensions']])
        
    col = response['reports'][0]['columnHeader']['dimensions']
    for metric in response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']:
        col.append(metric['name'])
        
    for x in enumerate(col):
        col[x[0]]= (x[1])[3:]
        
    df.columns = col
    df = df.reset_index(drop = True)
    return df


## Código repetido debido a desacuerdos del equipo. 

SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 
          'https://www.googleapis.com/auth/spreadsheets.readonly']
          


def build_service(google_api, version, proj_dir): 
    
    creds_dir  = path.join(proj_dir, path.pardir, "data", "config")
    token_file = path.join(creds_dir, getenv("DRIVE_TOKEN"))
    creds_file = path.join(creds_dir, getenv("DRIVE_CREDENTIALS"))

    store = file.Storage(token_file)
    creds = store.get()
    if not creds or creds.invalid:
        flow  = client.flow_from_clientsecrets(creds_file, SCOPES)
        creds = tools.run_flow(flow, store)

    g_service = build(google_api, version, http=creds.authorize(Http()))
    return g_service



def drive_archivos_en_carpeta(servicio, carpeta):
    busqueda_0 = servicio.files().\
        list(q = f"name = '{carpeta}'").execute()
    busqueda_1 = busqueda_0.get("files", [])
    if len(busqueda_1) != 1: 
        raise(f"No es única la carpeta '{carpeta}'.\n")
    else: 
        folder_id = busqueda_1[0]["id"]
        
    archivos_df = pd.DataFrame(columns = ["kind", "id", "name", "mimeType"])

    un_request = servicio.files().list(
            q = f"'{folder_id}' in parents and trashed = false")
    while un_request is not None: 
        un_response = un_request.execute()    
        archivos_df = archivos_df.append( un_response["files"] )    
        un_request = servicio.files().\
            list_next(un_request, un_response)

    return archivos_df




def sheets_leer_inventario(servicio, sheet_id, tag_fecha): 
    currency_str = lambda str_series: pd.to_numeric(str_series.str.replace("[$,]", ""))

    # cols_types = [
    #     ("inventory_date", "datetime64"),  ("car_id", object), 
    #     ("selling_status", object), ("physical_status", object), 
    #     ("legal_status",   object), ("internal_id", object),  ("vehicle_id", object),
    #     ("car_location",   object), ("car_name",    object),  ("car_cost", float), 
    #     ("allowance_cost", float),  ("total_cost",  float), 
    #     ("incoming_date", "datetime64"), ("inventory_days", float),
    #     ("status_days", float), ("created_at", "datetime64"), 
    #     ("updated_at", "datetime64") ]
    
    cols_assign = {
        "inventory_date"    : lambda df: tag_fecha,
        "car_id"            : lambda df: df["ID"], 
        "selling_status"    : lambda df: df["Estatus de venta"].str.extract(" - (.*)"),
        "physical_status"   : lambda df: df["Estatus fisico"],
        "legal_status"      : lambda df: df["Estatus legal"],
        "internal_id"       : lambda df: df["ID"],
        "vehicle_id"        : lambda df: df["NIV"],
        "car_location"      : lambda df: df["Ubicacion Actual"],
        "car_name"          : lambda df: df.Marca.str.
                                cat(sep = " - ", others = df.Modelo.str.
                                cat(sep = " - ", others = df.Año)),
        "car_cost"          : lambda df: currency_str(df["Precio sin IVA"]),
        "allowance_cost"    : lambda df: currency_str(df["Allowance"]).fillna(0),
        "total_cost"        : lambda df: df.car_cost,
        "incoming_date"     : lambda df: df["Fecha de ingreso"],
        "inventory_days"    : lambda df: df["Dias en inventario"].fillna(0),
        "status_days"       : lambda df: df["Dias desde cambio de status"].fillna(0),
        "created_at"        : lambda df: pd.Timestamp.now(),
        "updated_at"        : lambda df: pd.Timestamp.now()
        }
    
    inventario_req = servicio.spreadsheets().values().\
        get(spreadsheetId = sheet_id, range="Inventario!A:V").execute()
    
    inventario_df = pd.DataFrame(
            columns = inventario_req["values"][0], 
            data    = inventario_req["values"][1:]).\
        assign(**cols_assign).\
        loc[:, tuple(cols_assign.keys())]   
            
    return inventario_df
        



