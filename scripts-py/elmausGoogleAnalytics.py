def ganalytics_service(api_name, version, client_secret, credentials):
	from apiclient.discovery import build
	from httplib2 import Http
	from oauth2client import file, client, tools
	import argparse

	scopes = 'https://www.googleapis.com/auth/'
	credentials =  credentials
	client_secret = client_secret
	store = file.Storage(credentials)
	creds = store.get()
	if not creds or creds.invalid:
		parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
								   parents=[tools.argparser])
		flags = parser.parse_args([])
		flow = client.flow_from_clientsecrets(client_secret
										, scopes
										, message=tools.message_if_missing(client_secret))
		creds = tools.run_flow(flow, store, flags)
		
	return build(api_name, version, http = creds.authorize(Http()))

  
def ganalyticsTable(service, metriclist, dimensionlist, end_date, init_date, row_size = 1000):
	import pandas as pd
	
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
	request['pageSize'] = row_size
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
	response = service.reports().batchGet(body = body).execute()
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




