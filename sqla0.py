# -*- coding: utf-8 -*-
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.sql import select

engine = create_engine('sqlite:///:memory:', echo=True)
metadata = MetaData()
companies = Table('nonfin', metadata,
         Column('inn', Integer),
         Column('okpo', Integer, primary_key=True),
         Column('name', String),
)

from sqlalchemy.schema import CreateTable

print (CreateTable(companies))

metadata.create_all(engine)

kv = {'inn':2000, 'name':"ООО Ромашка"}
ins = companies.insert().values(kv)

conn = engine.connect()
result = conn.execute(ins)


###################### DOES NOT WORK ######################

# def find(string):
    # string = string.join(['%']*2).lower()
    # sel = select([companies)]).where(func.lower(companies.c.name).ilike(string))    
    # for row in conn.execute(sel):
         # yield(row)

# for x in ['ООО', 'ооо', 'Ром', 'ром', 'маш', 'ШКА', 'шка']:
     # lst = list(find(x))
     # print (x, list(find(x)))

###########################################################     


# from sqlalchemy.sql import text
# s = text("SELECT UPPER(nonfin.name) as u, inn + 1 FROM nonfin") # WHERE LOWER(nonfin.name) LIKE '%маш%'
# for x in conn.execute(s).fetchall():
    # print(x)