from tools.google_api import ganalyticsTable
from tools.base_alq import local_engine
from datetime import datetime as dt, timedelta as delta


metricas     = ["", ""]
dimensiones  = ["", ""]
fecha_inicio = dt.today() - delta(30)
fecha_final  = dt.today()

# Las funciones y scripts se escriben con minúsculas_y_guion_bajo.
# Las clases se escriben con CamelCase
quote_df = ganalyticsTable(metricas, dimensiones, fecha_inicio, fecha_final)

engine_pg = local_engine("postgres") 
quote_df.to_sql("ga_quotes", con=engine_pg, 
    schema="public", if_exists="append", index=False)


