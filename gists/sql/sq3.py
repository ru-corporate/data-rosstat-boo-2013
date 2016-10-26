# -*- coding: utf-8 -*-
from reader import lines_as_dicts

def get_datapoints(d, fields=supported_data_fields):
    c = d['okpo']  
    y = d['year']
    for k in fields:
        yield {'fk_okpo':c, 
               'year'   :y,
               'field'  :int(k), 
               'val' :d[k]
               }

               
from sqlalchemy import create_engine
from sqlalchemy.sql import select


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.schema import CreateTable

metadata = MetaData()
companies = Table('nonfin', metadata,
         Column('inn', Integer),
         Column('okpo', Integer, primary_key=True),
         Column('name', String),
)

balance = Table('data', metadata,
         Column('fk_okpo', Integer, ForeignKey('nonfin.okpo')),
         Column('field', Integer),
         Column('val', Integer),
         Column('year', Integer),
)

#print (CreateTable(companies).__str__())
#print (CreateTable(balance).__str__())

engine = create_engine('sqlite:///:memory:', echo=False)

metadata.create_all(engine)
conn = engine.connect()
gen = lines_as_dicts()

import time
start_time = time.time()

L = 10000
for _ in range(L):
  a = next(gen)
  dv = list(get_datapoints(a))
  ins1 = companies.insert().values({k: a[k] for k in ('inn', 'okpo', 'name')})
  ins2 = balance.insert().values(dv)
  
  conn.execute(ins1)
  #conn.execute(ins2)

t = time.time() - start_time
a = round(t/L * 1.7 * 10 ** 6 / 60, 1) 
print("Execution time, sec:", round(t,2))
print("Estimated for dataset, min:", a)  
  
  
for row in conn.execute(select([companies]) ):
    print(row)

#for row in conn.execute(select([balance]) ):
#    print(row)      


#from sqlalchemy.sql import text
#s = text("SELECT * FROM  nonfin JOIN data ON nonfin.okpo = data.fk_okpo") 
#for x in conn.execute(s):
#    print(x)


      

