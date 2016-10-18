from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.sql import select

engine = create_engine('sqlite:///:memory:', echo=False)
metadata = MetaData()
companies = Table('nonfin', metadata,
         Column('inn', Integer),
         Column('okpo', Integer, primary_key=True),
         Column('name', String),
)


engine = create_engine('sqlite:///:memory:', echo=False)
metadata.create_all(engine)

gen = lines_as_dicts() 
conn = engine.connect()

import time
start_time = time.time()
k = 100
chunk = 200
for i in range(k):
    li = [next(gen) for _ in range(chunk)]
    ins = companies.insert().values(li)
    result = conn.execute(ins)

t = time.time() - start_time
a = round(t/(k*chunk) * 1.7 * 10 ** 6 / 60, 1) 
print("Execution time, sec:", round(t,2))
print("Estimated for dataset, min:", a)

i = 0 
for row in conn.execute(select([companies]) ):
      i = i + 1
      #  print(row)
print(i, "records")