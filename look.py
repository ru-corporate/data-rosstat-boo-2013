import pandas as pd
df = pd.read_csv("test.csv", delimiter=";")

import sqlite3 as db
con = db.connect(':memory:')
df.to_sql('data', con)
df2 = pd.read_sql_query("SELECT * FROM data", con)
# todo: must assert db equals db2

# g12.py outputs are:
#      bln.csv and half.csv 
#      investment file to look for VEB projects

# Todo - sqlite database: 
#    dict to dataframe
#    dataframe to sql 
#    delete data in sql + load file schema
#    csv dicts to sql
#    select on sqlite 
#    write to csv + to xls (as df)

# Todo - file columns:
#    справочник по кодам из баланса 
#    separate 2013(3) and 2012(4)

# Todo - список для проектов ВЭБ:
#    investment file to look for VEB projects 

# -----------------------------
#   For testing:
# -----------------------------

# Todo - 383 and 385:
#    read only unit == 384

# Todo - file fields:
#    region INN
#    split okved to 3 parts 
#    extract org type

# -----------------------------

