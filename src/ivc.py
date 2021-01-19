##########################################
# Inventory Change (Ivc)                 #
# William Fallon                         #
# August 2020                            #
##########################################

import wrds
import math
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *

conn = wrds.Connection()

# compustat
compustat = conn.raw_sql("""
                    select gvkey, datadate, invt, at from comp.funda
                    where indfmt = 'INDL' and datafmt = 'STD' and popsrc = 'D'
                    and consol = 'C' and datadate >= '01/01/2016'
                    """, date_cols = ['datadate'])

compustat['year'] = compustat['datadate'].dt.year
compustat['gvkey'] = compustat['gvkey'].astype(int)

# compute scaled invt change by dividing invt-change by AT assets
compustat['invt-change'] = compustat.groupby('gvkey')['invt'].diff()
compustat['avg-at'] = compustat.groupby('gvkey')['at'].apply(lambda x: (x.shift(1) + x) / 2.0)
compustat['scaled-invt-change'] = compustat['invt-change'] / compustat['avg-at']

compustat.dropna()

# sort into deciles by scaled invt change within each year
compustat['rank'] = compustat.groupby('year')['scaled-invt-change'].rank(method = 'first')
compustat['decile'] = compustat.groupby('year')['rank'].transform(\
    lambda x: pd.qcut(x, 10, labels = False))


# crsp
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, b.ncusip, a.date,
                      b.shrcd, b.exchcd, b.siccd,
                      a.ret, a.vol, a.shrout, a.prc, a.cfacpr, a.cfacshr
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/2016' and '12/31/2019'
                      and b.exchcd between -2 and 2
                      and b.shrcd between 10 and 11
                      """)

crsp_m[['permco','permno','shrcd','exchcd']] = \
    crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

crsp_m['mdate'] = crsp_m['date'] + MonthEnd(0) #set date to end of month
crsp_m['yearend'] = crsp_m['mdate'] + YearEnd(0)

crsp_m.dropna()
# ccm data to link gvkey and permno
ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim,
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """, date_cols = ['linkdt', 'linkenddt'])

ccm['linkenddt']=ccm['linkenddt'].fillna(pd.to_datetime('today'))
ccm[['gvkey', 'permno']] = ccm[['gvkey', 'permno']].astype(int)

ccm1 = pd.merge(compustat[['gvkey', 'year', 'decile', 'datadate']], ccm, how = 'left', on=['gvkey'])
ccm1['yearend'] = ccm1['datadate'] + YearEnd(0)

ccm2 = ccm1[(ccm1['yearend'] >= ccm1['linkdt']) & (ccm1['yearend'] <= ccm1['linkenddt'])]
ccm2 = ccm2[['gvkey','permno','datadate','yearend', 'decile']]


ccm_combined = pd.merge(crsp_m, ccm2, how = 'inner', on=['permno', 'yearend'])
ccm_combined = ccm_combined[['gvkey', 'permno', 'mdate', 'ret', 'decile']]

ccm_long = ccm_combined.loc[ccm_combined['decile'] == 0]
ccm_short = ccm_combined.loc[ccm_combined['decile'] == 9]

#ccm_combined.to_csv('ivc-returns.csv', encoding = 'utf-8', index = False)













"""
#create returns dataframe
returns = {'month': [], 'decile': [], 'permno': [], 'ret': []}
returns_df = pd.DataFrame(data = returns)

crsp_m['gvkey'] = crsp_m.groupby('permno')

#for each line in compustat data, find relevant return data from
#crsp for each month in each year if such data exists
#store and output in returns_df
count = 1
def find_returns(row):
    global count
    print(count)
    count = count + 1
    if (not math.isnan(row['decile'])):
        for i in range (1, 13):
            year = row['year']
            date = pd.to_datetime({'year': [year], 'month': [i], 'day': [1]})
            jdate = date + MonthEnd(0) #set date to end of month
            gvkey = row['gvkey']
            permno_df = ccm.loc[(ccm['gvkey'] == int(gvkey)) & (ccm['linkdt'] <= jdate[0]) & \
                (ccm['linkenddt'] >= jdate[0]), ['permno']]
            permno_df.reset_index(drop = True)
            if not permno_df.empty:
                permno = permno_df['permno'].iloc[0]
                ret_df = crsp_m.loc[(crsp_m['permno'] == int(permno)) & \
                    (crsp_m['jdate'] == jdate[0]), ['ret']]
                if not ret_df.empty:
                    ret = ret_df['ret'].iloc[0]
                    decile = row['decile']
                    global returns_df
                    returns_df = returns_df.append({'month': jdate, 'decile': decile, \
                        'permno': permno, 'ret': ret}, ignore_index = True)


compustat.apply(find_returns, axis = 1)

returns_df.to_csv('ivc-returns.csv', sep = '\t', encoding = 'utf-8', index = False)
"""
