# -*- coding: utf-8 -*-
""" Create reduced datasets for end user"""

from reader import Dataset
#from config import make_path_for_user_output
from common import print_elapsed_time

FMT = dict(index=False, encoding="utf-8")

def to_csv(df, file):
    df.to_csv(file, **FMT)
    print("Row count:", len(df))
    print("Saved:", file)

def read_dataset(year):
    df = Dataset(year).read_df()

    @print_elapsed_time
    def _foo(df):
        df = df[df.ta>100]
        tabooed = [1001096 #'Научно-Производственный Финансовый Концерн "ИНТЭКОТЕРРА"'
                 , 1002168 # Газпром?
                 ]
        return df.drop(tabooed)

    return _foo(df)


## userfile wrapper
#def make_path_for_user_output(year, prefix, ext=".csv"):
#    filename = prefix + "_" + str(year) + ext
#    return os.path.join(FOLDERS['user_slices'], filename)

## проверка АвтоВАЗ
#vaz = df[df.inn == '6320002223']
#assert (vaz.sales == 175152000).all()

## Каргилл
## http://kommersant.ru/doc/2735389
#
## Русполимет
#pd = parsed(2015, 25471)
#pprint(pd)


@print_elapsed_time
def adjust_to_mln(df):
    ## все исходные данные в тыс. рублей, преобразрованные - млн руб.
    datacols = list(RENAMER.values())
    df.loc[:,datacols]=(df.loc[:,datacols] / 1000).round(1)
    return df

def get_df(year):
    df = read_dataset(year)
    return adjust_to_mln(df)

def subset1(df):
    year = df.year.loc[1]
    print("Firms with total assets above 30 mln rub or sales above 60 mln")
    #total assets above 30 mln rub or sales above 5 mln rub per month
    ix = (df.ta > 30) | (df.sales> 12*5)
    df2 = df[ix]
    fn = make_path_for_user_output(year, "main")
    to_csv(df2, fn)
    return df2

def subset2(df):
    year = df.year.loc[1]
    print("Firms with sales > 1 bln rub")
    BLN = 10**3 # df already in rub million
    bln = df[df.sales>BLN]
    fn = make_path_for_user_output(year, "bln")
    to_csv(bln, fn)

    fn = make_path_for_user_output(year, 'xl_bln', ext=".xlsx")
    bln.to_excel(fn, *FMT)
    print("Saved:", fn)

if __name__ == '__main__':
     df=Dataset(2015).read_df()

#    import matplotlib.pyplot as plt
#    import matplotlib.ticker as ticker
#    import numpy as np
#    df = get_df(2013)
#    t=200 #mln
#    #z = df[(df.ta<t) & (df.sales<t) & (df.ta>0) & (df.sales>=0)][['ta','sales']]
#    z = df[(df.ta>0) & (df.sales>=0)][['ta','sales']]
#    z['ta_log']=z.ta.apply(lambda x: np.log10(x))
#    df.nlargest(100, 'ta', keep='first')[['inn','title']].to_csv("inn.txt", index=False)
#
#    plt.figure()
#    ax = z.ta_log.hist(bins=10, cumulative=True, edgecolor='none')
#    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: ('%.0f')%(y*1e-3)))
#    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: ('10^%.0f')%x))
#    ax.set_ylabel(" '000")
#    plt.show()

#    import matplotlib.ticker as ticker
#    import numpy as np
#
#    fig = plt.figure()
#    ax = fig.add_subplot(1,1,1)
#
#    mu, sigma=100, 15
#    x=mu + sigma*np.random.randn(1000000)
#    n, bins, patches=ax.hist(x, 50, facecolor='green', alpha=0.75)
#
#    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: ('%.2f')%(y*1e-3)))
#    ax.set_ylabel('Frequency (000s)')
#


    #z.plot(x='ta', y='sales', kind='scatter', s=0.01, xlim=(0,1000), ylim=(0,1000))
    #plt.figure();
    #z.ta.plot(kind='bar'); #plt.axhline(0, color='k')
