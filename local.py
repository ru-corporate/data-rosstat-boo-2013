import pandas as pd
from column_names import colname_to_varname_dict as SUB

# todo: 
# separate to two projects: exstings + private projects  
# inn must be string, not ints, but convertable to ints: change in reader
# (?) save all2013.csv and all2012.csv to local disk  
# read other attributes


INN_CSV = dict(filename='inn.csv', columns=['inn','tag'])
MY_COLUMNS = INN_CSV['columns'] + \
             ['year', 'okved1', 'region', 'title'] + \
             [x for x in sub.keys()]

def get_full_dataset(year):
    """Read INN list and some project attributes, eg *tag*.""" 
    df = pd.read_excel("projects_compact.xlsx")
    df.inn = df.inn.astype(str)
    return df 
    
def read_inn(filename, columns, with_duplicates=False):  
    """Read INN list and some project attributes, eg *tag*.""" 
    inn_df = pd.read_csv(filename, sep="\t", header=0, names=columns, dtype={'inn':str})  
    if with_duplicates:
       return inn_df       
    else:
       return inn_df.drop_duplicates()

def echo_inn_duplicates(filename, columns):    
    inn_df = read_inn(filename, columns, with_duplicates=True)
    inn_dups = inn_df[inn_df.duplicated()]
    print("INN configuration file has %d duplicated rows:" % inn_dups.count().inn)
    for x in inn_dups.inn.tolist():
        print(x)    
    
inn_df = read_inn(**INN_CSV)
assert inn_df.columns.tolist() == INN_CSV['columns']
assert isinstance(inn_df.iloc[1,0], str)
assert '6234028965' in inn_df.inn.tolist() 

def merge_with_inn(df, inn_df):
    # Make 'projects' dataframe
    project_df = pd.merge(inn_df, df, on='inn', how='left')
    project_df['is_found'] = project_df.year.notnull()
    project_df = project_df.sort_values(['is_found','okved1','2110'], ascending=[False, True, True])
    assert not project_df.duplicated().all()  
    return project_df

def echo_not_found(df):
    not_found_count = sum([not x for x in df['is_found']])
    print("%d INN(s) not found in database" % not_found_count)
    print(df[~df['is_found']].inn)


# -------------------------------------------------------------------------------------------
# aggregation
aggr_dict = {"ХОЛДИНГ: Фрештел": [7727560086, 7718571010, 7710646874, 7701641245]}
# -------------------------------------------------------------------------------------------





def shorten(df, slicer, name_mapper):    
    df = df[slicer].rename(columns=name_mapper)
    data_cols = list(name_mapper.values())
    df[data_cols] = df[data_cols].applymap(lambda x: round(x / 10 ** 6, 1))    
    return df
    
def check_balance(df):
    # активы = пассивы  
    flag1 = df.ta-df.tp
    assert abs(flag1).sum() < 15
    # внеоборотные активы + оборотные активы = активы
    flag2 = df.ta_fix + df.ta_nonfix - df.ta
    assert abs(flag2).sum() < 15
    # капитал + долгосрочные обязательства + краткосрочные обязательства = всего пассивы
    flag3 = df.tp_cap+df.tp_short+df.tp_long-df.tp
    assert abs(flag3).sum() < 15   
            
if __name__ == "__main__":
    inn_df = read_inn(**INN_CSV)
    df = get_full_dataset(year)
    project_df = merge_with_inn(df, inn_df)
    echo_not_found(project_df)
    compact_df = shorten(project_df, slicer=MY_COLUMNS, name_mapper=SUB)    
    check_balance(compact_df)  
    project_df, compact_df = get_dfs(2013)
    save(project_df, "projects", folder='projects')
    save(compact_df, "projects_compact", folder='projects')
