# -*- coding: utf-8 -*-
"""
Created on Thu Feb  7 17:23:26 2019

@author: Diego
"""

# 1. Descargar inventario diario de mssql.
# 2. Modificar inventario para subir a postgres.
# 3. Subir inventario a postgres. 














# -*- coding: utf-8 -*-
#000000000000000000000000000000000000000000000000000000000000000000000000000000
"""
Created on Mon Jun  4 13:43:25 2018
@author: Frontier (Enrique GO) """

import sys
from config_environment import directories
sys.path = directories().path

import os
import pandas as pd
from datetime import datetime
from GoogleSheetsUpdater import update_inventory, update_inventory_history
from OrganizadorDeHorarios import check_run_time, check_log_file, write_in_log_file,check_force

def auto_inventory(date, hr):
    
    todate = datetime.now()
    cars = pd.read_csv(dwh_directory + r'\db_cars.csv')
    auctions = pd.read_csv(dwh_directory + r'\db_auctions.csv')[['car_id',
                          'real_runs']].drop_duplicates()
    
    cars = cars.merge(auctions, how = 'left', on = 'car_id')
    cars.real_runs = cars.real_runs.fillna(0)
    cars.car_created = cars.car_created.apply(pd.to_datetime)
    cars.car_selling_status_last_modified = cars.car_selling_status_last_modified.apply(pd.to_datetime)

    cars.loc[cars.car_selling_status_last_modified.isnull(), 'car_selling_status_last_modified'] =\
    cars.loc[cars.car_selling_status_last_modified.isnull(), 'car_created']
    
    columns = ['internal_car_id'
               , 'car_selling_status'
               , 'car_legal_status'
               , 'car_physical_status'
               , 'car_vin'
                 , 'purchase_channel'
                 , 'car_purchased_date'
                 , 'car_purchase_price_car'
                 , 'car_purchase_price_car_taxless'
                 , 'car_purchase_location'
                 , 'car_handedover_from_seller'
                 , 'car_current_location'
                 , 'client_subtype'
                 , 'car_manufacturer_name'
                 , 'car_model_name'
                 , 'year_manufactured'
                 , 'car_trim'
                 , 'car_color'
                 , 'car_selling_status_last_modified'
                 , 'car_allowance'
                 , 'real_runs']
    
    cars_dwh = cars.copy()
    cars_dwh.client_subtype = cars_dwh.client_subtype.fillna('')
    cars_dwh.client_subtype = cars_dwh.client_subtype.str.lower()
    cars_dwh = cars_dwh[columns]
    
    cars_dwh.car_vin = cars_dwh.car_vin.fillna('Faltante')
    cars_dwh.car_purchase_location = cars_dwh.car_purchase_location.fillna('Faltante')

    cars_dwh.car_vin = cars_dwh.car_vin.str.replace('\(.*\)', '').str.strip()
    cars_dwh.car_color = cars_dwh.car_color.str.replace('.*\(', '').str.replace('\).*', '').str.title()
    
    inventory_cars_dwh = cars_dwh.copy().loc[((cars_dwh.car_selling_status.isin(['AVAILABLE',
                                                                              'RESERVED',
                                                                              'NOTAVAILABLE',
                                                                              'PENDINGCLEARANCE',
                                                                              'CONFIRMED',
                                                                              'CONSIGNED'])) & (-cars_dwh.car_legal_status.isnull()) &
            #                             (-cars_dwh.car_legal_status.isin(['OWNER'])) &
                                         (-cars_dwh.purchase_channel.isnull()) &
                                         (-cars_dwh.car_current_location.isnull()) &
                                         (cars_dwh.car_current_location.str.contains('Buyer') == False) &
                                         (cars_dwh.car_current_location.str.contains('B2B') == False) &
                                         (cars_dwh.car_physical_status.isin(['INTRANSIT',
                                                                             'ATOURLOCATION']))) | 
    (cars_dwh.car_selling_status == 'INTERNALUSE'), :]

    inventory_cars_dwh.loc[:, 'car_handedover_from_seller'] = inventory_cars_dwh['car_handedover_from_seller'].apply(pd.to_datetime)
    inventory_cars_dwh['car_handedover_from_seller'] = inventory_cars_dwh['car_handedover_from_seller'].fillna(pd.NaT)
    
    inventory_cars_dwh.loc[:, 'Dias desde cambio de status'] = inventory_cars_dwh.car_selling_status_last_modified.\
                                                                apply(lambda x: (todate - x).days)
    
    inventory_cars_dwh.loc[inventory_cars_dwh['Dias desde cambio de status'] < 0,
                           'Dias desde cambio de status'] = 0
    inventory_cars_dwh = inventory_cars_dwh.drop(['car_selling_status_last_modified'],
                                                 axis = 1)
    
    inventory_cars_dwh.loc[:, 'Dias en inventario'] = inventory_cars_dwh.car_handedover_from_seller.apply(lambda x: (todate - x).days)
                                            
    inventory_cars_dwh.car_purchased_date = \
                inventory_cars_dwh.car_purchased_date.apply(lambda x: pd.to_datetime(x).date())

    inventory_cars_dwh.car_handedover_from_seller = \
                inventory_cars_dwh.car_handedover_from_seller.apply(lambda x: x.date())
    
    inventory_cars_dwh.car_selling_status = inventory_cars_dwh.car_selling_status.\
    str.replace('PENDINGCLEARANCE', '0 - PENDINGCLEARANCE').\
    str.replace('INTERNALUSE', '1 - INTERNALUSE').\
    str.replace('NOTAVAILABLE', '2 - NOTAVAILABLE').\
    str.replace('RESERVED', '4 - RESERVED').\
    str.replace('CONFIRMED', '4 - CONFIRMED').\
    str.replace('CONSIGNED', '5 - CONSIGNED')

    inventory_cars_dwh.loc[inventory_cars_dwh.car_selling_status == 'AVAILABLE', 'car_selling_status'] = '3 - AVAILABLE'

    inventory_grouped = inventory_cars_dwh.groupby(['car_selling_status'], as_index = False).agg({'internal_car_id' :'count'})
    inventory_grouped['date'] = todate.date()
    inventory_grouped['time'] = todate.time()
    inventory_grouped['month'] = todate.date().replace(day = 1)
    
    inventory_cars_dwh = inventory_cars_dwh.rename(columns = {
            'car_selling_status': 'Estatus de venta',
            'car_physical_status': 'Estatus físico',
            'internal_car_id':'ID', 
            'car_vin':'NIV', 
            'purchase_channel':'Tipo de compra',	
            'car_purchased_date': 'Fecha de compra',
            'car_purchase_price_car':'Precio con IVA',	
            'car_purchase_price_car_taxless':'Precio sin IVA',	                                                              
            'car_purchase_location':'Origen de compra', 
            'car_handedover_from_seller': 'Fecha de ingreso',
            'car_current_location':'Ubicacion Actual',
            'client_subtype': 'Persona compra',
            'car_manufacturer_name': 'Marca',
            'car_model_name': 'Modelo',
            'year_manufactured': 'Año',
            'car_trim': 'Versión',
            'car_color': 'Color',
            'car_allowance': 'Allowance',
            'real_runs': 'Num. de subastas'})
    
    inventory_grouped = inventory_grouped.rename(columns = {
            'car_selling_status': 'Estatus de venta',
            'internal_car_id': 'Cantidad',
            'date': 'Fecha', 
            'time': 'Hora',
            'month': 'Mes'})
    
    inventory_cars_dwh = inventory_cars_dwh.sort_values('ID')
    inventory_grouped = inventory_grouped.sort_values('Estatus de venta')
    
    for i in inventory_cars_dwh:
        inventory_cars_dwh[i] = inventory_cars_dwh[i].fillna('')
        inventory_cars_dwh[i] = inventory_cars_dwh[i].astype(str)

    for i in inventory_grouped:
        inventory_grouped[i] = inventory_grouped[i].fillna('')
        inventory_grouped[i] = inventory_grouped[i].astype(str)

    try:
        update_inventory(inventory_cars_dwh, hr) # Sube el archivo a Orden de Facturación y personal
        update_inventory_history(inventory_grouped, date = date, hr = hr) # Actualiza historial de estatuses

        return 200
                
    except:
        write_in_log_file(filename)
        return 300

    return

#111111111111111111111111111111111111111111111111111111111111111111111111111111

if __name__ == '__main__':    
    
    filename = os.path.basename(__file__)

    if (datetime.now().hour in check_run_time(filename)) | 
        (check_log_file(filename)) | (check_force(filename)):
        
        #0000000000000000000000000000000000000000000000000000000000000000000000

        dwh_directory = directories().dwh_directory

        status = auto_inventory(date = datetime.now().date(), hr = datetime.now().hour)
        
        #1111111111111111111111111111111111111111111111111111111111111111111111
        
        if status == 200:
           print('Estatus {}. El reporte {} se ejecutó con éxito'.format(status, __file__))
        else:
           print('Estatus {}. El reporte {} no se ejecutó con éxito'.format(status, __file__))
        
    else:
        print('Aún no es hora de correr reporte {}'.format(__file__))

