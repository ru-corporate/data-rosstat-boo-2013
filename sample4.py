from reader import Dataset

var = ['title', 'inn', 'ta', 'sales', 
       'cash_flow_oper', 'cash_flow_inv', 'cash_flow_fin',  
       'tp', 'ta_fix', 'ta_nonfix', 'ta_lag', 'tp_lag', 'unit']

def get_df(year):
    return Dataset(year).read_df().nlargest(200, 'ta')[var].sort_values('ta')     
    
df = get_df(2015)
df0 = get_df(2013)


rf=df[['title','inn','ta','ta_lag', 'unit']]
rf.loc[:,'title']=rf['title'].apply(lambda x: x[-20:])
print(rf.iloc[50:100,])   


# bad inns
inns = [str(x) for x in (7707322083,7707089648,7710244903,
        7702844336,7710397699,7708013803)]
        
        
df['cash_flow'] = df.cash_flow_oper + df.cash_flow_inv + df.cash_flow_fin
df0['cash_flow'] = df0.cash_flow_oper + df0.cash_flow_inv + df0.cash_flow_fin

ex = df.cash_flow == 0
print(df[ex])     

#ix = df['inn'].isin(inns)
#print(df[ix][var])   
#
#        
#ix = df0['inn'].isin(inns)
#print(df0[ix][var])   

