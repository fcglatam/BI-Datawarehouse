# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 18:41:35 2019

@author: Diego
"""

# Leer query de Cars, filtrar por fecha de actualización. 
# Escribir a Postgresql.  


#%% Interactive: Set Autoreload, Non-Interactive: pseudo activate env. 

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
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")


#%% Paquetes y configuración. 

from tools import mssql_api as ms
from tools.base_alq import local_engine, upsert_psql


engine_ms = local_engine("mssql")

cars_df = ms.get_cars(engine_ms, this_dir, days_past=1)

engine_pg = local_engine("postgresql")

upsert_sql(cars_df, "x_dwh_cars", engine_pg, update_meta=False)








