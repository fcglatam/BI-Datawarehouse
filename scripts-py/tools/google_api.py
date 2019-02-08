

def google_leer_inventario(servicio, sheetId, columnas):

  el_request = servicio_sheet.spreadsheets().values().\
        get(spreadsheetId = sheetId, range="Inventario!A:V").execute()
        
    cada_inventario_1 = pd.DataFrame(
            columns = el_request["values"][0], 
            data    = el_request["values"][1:]).\
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

