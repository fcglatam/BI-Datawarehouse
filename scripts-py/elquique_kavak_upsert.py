# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 11:25:59 2019

@author: Frontier
"""

import __main__ as main
import pandas as pd
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

# Leer query de Cars, filtrar por fecha de actualización. 
# Escribir a Postgresql.  

from tools.base_alq import local_engine

engine_pg = local_engine("postgresql")
kavak_inventory = pd.read_sql('SELECT * FROM kavak_inventory', con = engine_pg)
#kavak_scrap.to_sql("kavak_inventory", con=engine_pg, schema="public", if_exists="append", index=False)

