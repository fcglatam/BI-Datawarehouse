# -*- coding: utf-8 -*-
"""
Created on Thu Feb  7 17:23:26 2019
@author: Diego, 
"""

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
    get_ipython().run_line_magic('load_ext', 'autorealod')
    get_ipython().run_line_magic('autoreload', '2')


#%% Paquetes y configuración. 

from tools import mssql_api as ms
from datetime import datetime as dt, timedelta as delta
from tools.base_alq import local_engine


#%% 1. Procedimiento digerido. 

which_day = dt.today() - delta(1)

engine_ms = local_engine("mssql")

inventory_ms = ms.get_inventory(engine_ms)
inventory_pg = ms.convert_inventory(inventory_ms, for_day=which_day)

engine_pg = local_engine("postgresql")
inventory_pg.to_sql("cars_inventory", con=engine_pg, 
    schema="public", if_exists="append", index=False)










