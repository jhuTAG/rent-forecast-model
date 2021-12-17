# -*- coding: utf-8 -*-
"""
Created on Thu Dec 16 14:02:02 2021

@author: jhu

Generate Rent forecast
"""


import pandas as pd
import numpy as np
import pyodbc
import saspy
import math
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from sklearn import datasets, linear_model
import statsmodels.api as sm
from scipy import stats
from scipy.optimize import minimize
import datetime

today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)


""" Historical Index """
stringI = "select year(monthfmt)*100+month(monthfmt) as month, indexcode, monthfmt, [index] \n"
stringI += ", rentg = [index] /lag([index],1) over(partition by indexcode order by monthfmt) -1 \n"
stringI += "from IRSPublish..sf_rentidx_monthly \n"
stringI += "where indexcode <> '90000' \n"
stringI += "order by indexcode, month \n"

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global raw_hist_index
raw_hist_index = pd.read_sql(stringI, cnxn)
cnxn.close()

# change format
raw_hist_index['monthfmt'] = pd.to_datetime(raw_hist_index['monthfmt'])
raw_hist_index.dtypes

# last historical month;
last_month =  raw_hist_index.sort_values("monthfmt").groupby("indexcode").tail(1)
last_month_1 = last_month.copy()
last_month_1['monthfmt'] = np.where((last_month_1['monthfmt'].dt.month%3 ==0), last_month_1['monthfmt'], pd.DatetimeIndex( last_month_1['monthfmt'] ) + pd.DateOffset(months = 1) )
last_month_1['month'] = last_month_1['monthfmt'].dt.year * 100 +  last_month_1['monthfmt'].dt.month

last_month_2 = last_month_1.copy()
last_month_2['monthfmt'] = np.where((last_month_2['monthfmt'].dt.month%3 ==0), last_month_2['monthfmt'], pd.DatetimeIndex( last_month_2['monthfmt'] ) + pd.DateOffset(months = 1) )
last_month_2['month'] = last_month_2['monthfmt'].dt.year * 100 +  last_month_2['monthfmt'].dt.month

last_month = pd.concat([last_month_1, last_month_2], axis=0)
last_month = last_month.drop_duplicates()
last_month = last_month.sort_values(['indexcode', 'monthfmt'])

hist_index = pd.concat([raw_hist_index, last_month], axis=0)
#hist_index = hist_index.drop(['index'], axis=1)
hist_index = hist_index.sort_values(['indexcode', 'monthfmt'])
hist_index['rentg']  = hist_index['rentg'].fillna(0.0) + 1

hist_index['rentidx'] = hist_index.groupby('indexcode')['rentg'].cumprod() *100
hist_index = hist_index[hist_index['monthfmt'].dt.month%3 ==0]

min_month = hist_index.groupby('indexcode')['month'].max().to_frame()
min_month['min_month'] = hist_index.groupby(['indexcode'])['month'].min()
min_month = min_month[(min_month['month'] >= 201901 ) & (min_month['min_month'] < 201001)]
min_month = min_month.reset_index()

min_month = min_month[(min_month['indexcode'].astype(str).map(len) == 5) ]
min_month = list(min_month['indexcode'])

hist_index = hist_index[hist_index['indexcode'].isin(min_month)]
hist_index = hist_index.drop(['index'], axis=1)


""" Need to figure out how to run SA in Python """
hist_index.to_csv(r'E:\Output\Rent Forecast\hist index no sa.csv', index = False)

hist_index_sa= pd.read_csv(r'E:\Output\Rent Forecast\hist index sa.csv')

""" get forecast start qtr """;

fc_start_mon = pd.to_datetime(hist_index_sa['monthfmt']).dt.date.max()
fc_start_qtr = fc_start_mon.year*100+math.floor(int((fc_start_mon.month-1)/3))+1

""" get cbsa list """
fc_cbsa_list = list(hist_index_sa.loc[hist_index_sa['month'] == ( fc_start_mon.year*100 + fc_start_mon.month), 'indexcode'])


""" Ln(rentg) """

hist_index_sa['rn'] = hist_index_sa.groupby('indexcode')["month"].rank()
hist_index_sa['lag_index'] = hist_index_sa.groupby('indexcode')['rentidx_sa'].shift(1)
hist_index_sa['lag_index_nsa'] = hist_index_sa.groupby('indexcode')['rentidx'].shift(1)
hist_index_sa['ln_rentg'] = np.log(hist_index_sa['rentidx_sa']/hist_index_sa['lag_index'])
hist_index_sa['ln_rentg_nsa'] = np.log(hist_index_sa['rentidx']/hist_index_sa['lag_index_nsa'])
hist_index_sa['seasonality'] = hist_index_sa['ln_rentg_nsa'] - hist_index_sa['ln_rentg']
hist_index_sa['lag4_indexcode'] = hist_index_sa.groupby('indexcode')['indexcode'].shift(-4)


hist_input=hist_index_sa[['indexcode','monthfmt','ln_rentg']]
hist_input['rentg_l1']=hist_input.groupby(['indexcode'])['ln_rentg'].shift(1)
hist_input['rentg_l2']=hist_input.groupby(['indexcode'])['rentg_l1'].shift(1)
hist_input['rentg_l3']=hist_input.groupby(['indexcode'])['rentg_l2'].shift(1)
hist_input['rentg_l4']=hist_input.groupby(['indexcode'])['rentg_l3'].shift(1)
hist_input['rentg_l5']=hist_input.groupby(['indexcode'])['rentg_l4'].shift(1)
hist_input['qtr'] =  pd.to_datetime(hist_input['monthfmt'])
hist_input['qtr'] = hist_input['qtr'].dt.year*100+ ((hist_input['qtr'].dt.month-1)/3).apply(np.floor)+1
hist_input =hist_input.drop(['monthfmt'],axis=1)


""" Seasonality """
ln_seasonality = hist_index_sa[hist_index_sa['indexcode'] != hist_index_sa['lag4_indexcode']]
ln_seasonality['qtridx'] = (ln_seasonality['month'] - (ln_seasonality['month']/100).apply(np.floor)*100)/3
sum_seasonality =  ln_seasonality.groupby(['indexcode'])['seasonality'].sum().to_frame()
ln_seasonality = ln_seasonality.rename(columns={"seasonality": "raw_sa"})
sum_seasonality = sum_seasonality.rename(columns={"seasonality": "sum_sa"})
sum_seasonality = sum_seasonality.reset_index()
ln_seasonality = ln_seasonality.merge(sum_seasonality, on=['indexcode'], how='inner')
ln_seasonality['seasonality'] = ln_seasonality['raw_sa'] - ln_seasonality['sum_sa']/4


ln_seasonality = ln_seasonality[['indexcode','qtridx','seasonality']]

""" Historical slope from fred """
from fredapi import Fred
fred = Fred(api_key='6677af73d3a4715a94abafac6552905e')


cmt_2yr = fred.get_series('DGS2').to_frame().reset_index()
cmt_2yr = cmt_2yr.rename(columns={"index":"datefmt",0:"cmt_2yr"})
cmt_2yr['month'] = cmt_2yr['datefmt'].dt.year*100+cmt_2yr['datefmt'].dt.month
cmt_2yr['datefmt'] = cmt_2yr['datefmt'].dt.date
cmt_2yr = cmt_2yr[cmt_2yr['datefmt']<=lastMonth]
cmt_2yr = round(cmt_2yr.groupby('month')['cmt_2yr'].mean().to_frame().reset_index(),2)



cmt_10yr = fred.get_series('DGS10').to_frame().reset_index()
cmt_10yr = cmt_10yr.rename(columns={"index":"datefmt",0:"cmt_10yr"})
cmt_10yr['month'] = cmt_10yr['datefmt'].dt.year*100+cmt_10yr['datefmt'].dt.month
cmt_10yr['datefmt'] = cmt_10yr['datefmt'].dt.date
cmt_10yr = cmt_10yr[cmt_10yr['datefmt']<=lastMonth]
cmt_10yr = round(cmt_10yr.groupby('month')['cmt_10yr'].mean().to_frame().reset_index(),2)

rate_frm_mo = cmt_10yr.merge(cmt_2yr, on='month', how='left')
# rate_frm = rate_frm_mo.copy()
# rate_frm['yr'] = (rate_frm['month']/100).apply(np.floor)
# rate_frm['mon'] = rate_frm['month'] - rate_frm['yr']*100
# rate_frm['qtr'] = ((rate_frm['mon']-1)/3).apply(np.floor) + 1
# rate_frm['qtr'] = rate_frm['yr']*100 + rate_frm['qtr']

# rate_frm = rate_frm.groupby('qtr')['cmt_2yr','cmt_10yr'].mean().reset_index()
# rate_frm['slope'] = rate_frm['cmt_10yr'] - rate_frm['cmt_2yr']
# rate_frm = rate_frm.sort_values(['qtr'])
# rate_frm['slope_l1'] = rate_frm['slope'].shift(1)
# rate_frm['chgslope0_1'] = rate_frm['slope'] - rate_frm['slope_l1']


""" Forecast rate path """
stringIR = "select month = year(dateadd(month, month, rate_timestamp))*100+month(dateadd(month, month, rate_timestamp)) \n"
stringIR += ", path_num, cmt_2yr, cmt_10yr, 0 as priority \n"
stringIR += "from InterestRates..saved_path_values_dt \n"
stringIR += "where path_num between 0 and 1000 and curve_type in ('FWD','OAS') \n"

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global rate2
rate2 = pd.read_sql(stringIR, cnxn)
cnxn.close()
rate2 = rate2.sort_values(['month', 'path_num']).reset_index().drop(['index'], axis=1)

rate_frm_mo['priority'] = 1
rate_frm_mo = rate_frm_mo[rate_frm_mo['month']>=201000]

test = rate2[rate2['path_num']==0]

# rate_last_mo = rate_frm_mo[rate_frm_mo['month'] == lastMonth.year*100+lastMonth.month]

for i in range(0,1001):
    tp = rate_frm_mo.copy()
    tp['path_num'] = i
    try:
        rate_frm_mo2 = pd.concat([rate_frm_mo2, tp], ignore_index=True)
    except:
        rate_frm_mo2 = tp.copy()
        rate_frm_mo2 = pd.concat([rate_frm_mo2, tp], ignore_index=True)
    i+1

    

rate_frm_2 = pd.concat([rate_frm_mo2, rate2], ignore_index=True).reset_index().drop(['index'], axis=1)
rate_frm_2 = rate_frm_2.sort_values(['priority']).groupby(['path_num','month']).last(1).reset_index()
rate_frm_2['yr'] = (rate_frm_2['month']/100).apply(np.floor)
rate_frm_2['mon'] = rate_frm_2['month'] - rate_frm_2['yr']*100
rate_frm_2['qtr'] = ((rate_frm_2['mon']-1)/3).apply(np.floor) + 1
rate_frm_2['qtr'] = rate_frm_2['yr']*100 + rate_frm_2['qtr']

rate_frm_2 = rate_frm_2.drop(['yr','mon'], axis=1)

rate_frm_3 = rate_frm_2.groupby([ 'path_num','qtr']).mean().reset_index()

rate_frm_3['cmt_2yr_0'] = rate_frm_3['cmt_2yr']
rate_frm_3['cmt_10yr_0'] = rate_frm_3['cmt_10yr']

# multiplier in change to make distribution wider;
rate_frm_3['chg_cmt_2yr'] = (rate_frm_3['cmt_2yr'] - rate_frm_3['cmt_2yr'].shift(1))*1.89
rate_frm_3['chg_cmt_10yr'] = (rate_frm_3['cmt_10yr'] - rate_frm_3['cmt_10yr'].shift(1))*1.89
rate_frm_3['rn'] = rate_frm_3.groupby(['path_num'])['month'].rank()

# rate_frm_3 = rate_frm_3[rate_frm_3['path_num']<=101]

# i=1
# import time
# start_time = time.time()

#  3 min;
for i in range(0,1001):
    # print(i)
    rate_onepath= rate_frm_3[rate_frm_3['path_num'] == i].reset_index().drop(['index'],axis=1)
    if i>0:
        for j in  range(0, len(rate_onepath.index)):
            if (rate_onepath.loc[j,'rn']>1) & (rate_onepath.loc[j,'priority']==0):
                rate_onepath.loc[j,'cmt_2yr'] = min(max(0.1,rate_onepath.loc[j-1, 'cmt_2yr']+rate_onepath.loc[j,'chg_cmt_2yr']),60)
                rate_onepath.loc[j,'cmt_10yr'] = min(max(0.1,rate_onepath.loc[j-1, 'cmt_10yr']+rate_onepath.loc[j,'chg_cmt_10yr']),60)
    
    rate_frm_3 = pd.concat([rate_frm_3[rate_frm_3['path_num']!=i], rate_onepath], ignore_index=True)
                

# print("--- %s seconds ---" % (time.time() - start_time))
            
        

# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 )),'cmt_2yr'] = rate_frm_3['cmt_2yr'].shift(1) + rate_frm_3['chg_cmt_2yr']
# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 ) & (rate_frm_3['cmt_2yr'] > 60)),'cmt_2yr'] = 60
# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 ) & (rate_frm_3['cmt_2yr'] < 0.1)),'cmt_2yr'] = 0.1


# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 )),'cmt_10yr'] = rate_frm_3['cmt_10yr'] + rate_frm_3['chg_cmt_10yr']
# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 ) & (rate_frm_3['cmt_10yr'] > 60)),'cmt_10yr'] = 60
# rate_frm_3.loc[((rate_frm_3['rn']>1) & (rate_frm_3['priority'] == 0) & (rate_frm_3['path_num'] > 0 ) & (rate_frm_3['cmt_10yr'] < 0.1)),'cmt_10yr'] = 0.1

# test = rate_frm_3[rate_frm_3['path_num'] == 1]

adj_rate = rate_frm_3[(rate_frm_3['priority']==0) & (rate_frm_3['path_num']>0)].groupby(['qtr'])['cmt_2yr', 'cmt_2yr_0','cmt_10yr','cmt_10yr_0'].mean().reset_index()
adj_rate['cmt_2yr_adj'] = adj_rate['cmt_2yr'] - adj_rate['cmt_2yr_0']
adj_rate['cmt_10yr_adj'] = adj_rate['cmt_10yr'] - adj_rate['cmt_10yr_0']

rate_frm_4 = rate_frm_3.merge(adj_rate[['qtr','cmt_2yr_adj','cmt_10yr_adj']], on='qtr',how='left')

rate_frm_4.loc[rate_frm_4['priority']==0, 'cmt_2yr'] = rate_frm_4['cmt_2yr'] - rate_frm_4['cmt_2yr_adj']
rate_frm_4.loc[rate_frm_4['priority']==0, 'cmt_10yr'] = rate_frm_4['cmt_10yr'] - rate_frm_4['cmt_10yr_adj']

rate_frm_4.loc[((rate_frm_4['priority']==0) & (rate_frm_4['cmt_2yr'] > 60)), 'cmt_2yr'] = 60
rate_frm_4.loc[((rate_frm_4['priority']==0) & (rate_frm_4['cmt_2yr'] <0.1)), 'cmt_2yr'] = 0.1
rate_frm_4.loc[((rate_frm_4['priority']==0) & (rate_frm_4['cmt_10yr'] > 60)), 'cmt_10yr'] = 60
rate_frm_4.loc[((rate_frm_4['priority']==0) & (rate_frm_4['cmt_10yr'] <0.1)), 'cmt_10yr'] = 0.1

rate_frm_4 = rate_frm_4[['qtr','path_num','cmt_2yr','cmt_10yr']]
# test = rate_frm_4.groupby('qtr').mean()
rate_frm_4['slope'] = rate_frm_4['cmt_10yr'] - rate_frm_4['cmt_2yr']
rate_frm_4 = rate_frm_4.sort_values(['path_num','qtr'])
rate_frm_4['slope_l1'] = rate_frm_4.groupby('path_num')['slope'].shift(1)
rate_frm_4['chgslope0_1'] = rate_frm_4['slope'] - rate_frm_4['slope_l1']


""" Random Error """
stringER = "select cast( indexcode as int) as indexcode, simid as path_num, year(dateadd(Q,qtridx-1,'2021-12-01'))*100+datepart(q,dateadd(Q,qtridx-1,'2021-12-01'))  as qtr \n"
stringER += ", qtridx, resid_rent \n"
stringER += "from modeltestbed.dbo.errmat_rentfc \n"


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global errmat
errmat = pd.read_sql(stringER, cnxn)
cnxn.close()
errmat = errmat.drop_duplicates()
errmat = errmat.sort_values(['indexcode', 'path_num','qtr']).reset_index().drop(['index'], axis=1)
# errmat.head(10)

""" HPI Simulation Input File """
stringHistHP = "select cast(cbsa_code as int) as indexcode, qtr, hpg_season, unemp \n"
stringHistHP += ", unemp_g = unemp - lag(unemp,1) over (partition by cbsa_code order by qtr) \n"
stringHistHP += ", hpg_season_last2 = hpg_season + lag(hpg_season,1) over (partition by cbsa_code order by qtr) \n"
stringHistHP += "from IRSPublish..hpi_basefile \n"
stringHistHP += "where qtr>=201600 \n"


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global sim_hist_hpi
sim_hist_hpi = pd.read_sql(stringHistHP, cnxn)
cnxn.close()
# sim_hist_hpi.dtypes
sim_hist_hpi = sim_hist_hpi[sim_hist_hpi['indexcode'].isin(fc_cbsa_list)]

sim_hist_hpi = sim_hist_hpi.drop_duplicates()
sim_hist_hpi = sim_hist_hpi.sort_values(['indexcode', 'qtr']).reset_index().drop(['index'], axis=1)


# import time
# start_time = time.time()
# ~2min
for i in range(0,1001):
    # print(i)
    tp = sim_hist_hpi.copy()
    tp['path_num'] = i
    try:
        sim_hist_hpi_2 = pd.concat([sim_hist_hpi_2,tp], ignore_index=True)
    except:
        sim_hist_hpi_2 = tp.copy()
        
 
# print("--- %s seconds ---" % (time.time() - start_time))       
# sim_hist_hpi_2.head(10)
sim_hist_hpi_2 = sim_hist_hpi_2.sort_values(['path_num','indexcode', 'qtr']).reset_index().drop(['index'], axis=1)


""" HPI Simulation Path """

stringSimHP = "select cast(cbsa_code as int) as indexcode, qtr, path_num, inc_p50  \n"
stringSimHP += ", unemp_g_sim = unemp - lag(unemp,1) over (partition by cbsa_code, path_num order by qtr) \n"
stringSimHP += ", hpg_season_last2_sim = ln_hpi_season - lag(ln_hpi_season,2) over (partition by cbsa_code, path_num order by qtr) \n"
stringSimHP += "from modeltestbed..hpiSimPath \n"



cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global sim_hpi
sim_hpi = pd.read_sql(stringSimHP, cnxn)
cnxn.close()

sim_hpi = sim_hpi[sim_hpi['indexcode'].isin(fc_cbsa_list)]

sim_hpi = sim_hpi.drop_duplicates()
sim_hpi = sim_hpi.sort_values(['path_num','indexcode', 'qtr']).reset_index().drop(['index'], axis=1)
sim_hpi.dtypes
fc_cbsa_list = list(sim_hpi.loc[(sim_hpi['qtr'] == fc_start_qtr) & (sim_hpi['path_num'] == 1),'indexcode'])
# sim_hpi.head(10)


""" Rent Forecast Parameter """
stringP = "select * from modeltestbed.dbo.parm_rentfc where _TYPE_='PARMS' \n"

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global parm
parm = pd.read_sql(stringP, cnxn)
cnxn.close()
parm = parm.fillna(0.0)
parm['Intercept'] = parm['Intercept'] + parm['stage3_int']
parm = parm.drop(['_TYPE_','total_r2','stage3_int'], axis=1)

parm = parm.rename(columns={"rentg_l1":"p_rentg_l1",
                            "rentg_l2":"p_rentg_l2",
                            "rentg_l3":"p_rentg_l3",
                            "rentg_l4":"p_rentg_l4",
                            "rentg_l5":"p_rentg_l5",
                            "afford_Rent":"p_afford_rent",
                            "hpg_season_last2":"p_hpg_season_last2",
                            "unemp_g":"p_unemp_g",
                            "chgslope0_1":"p_chgSlope0_1"})

""" Forecast Input: weight & pSFR_group & historical rent share """
stringW = "select cast(cbsa as int) as indexcode, outlier, pSFR_group, m_rentShare, baserent from modeltestbed.dbo.rentfc_weight \n"

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global hist_share
hist_share = pd.read_sql(stringW, cnxn)
cnxn.close()
# hist_share.dtypes
hist_share = hist_share[hist_share['indexcode'].isin(fc_cbsa_list)]

try:
    del [[qtrlist]]
except:
    pass

for yr in range(2018, fc_start_mon.year+11):
    for qtr in range(1,5):
        q = yr*100+qtr
        if q>=fc_start_qtr-200:
            try:
                qtrlist.append(q)
            except:
                qtrlist=list([q])
            
for qtr in qtrlist:
    
    tp = hist_share.copy()
    tp['qtr'] = qtr
    try:
        qtrlist_2 = pd.concat([qtrlist_2, tp], axis=0, ignore_index=True)
    except:
        qtrlist_2 = tp.copy()
            
            
""" Simulation """

simid = 1
errmat_onepath = errmat[errmat['path_num']==simid]
# errmat.dtypes
simhpi_onepath = sim_hpi[sim_hpi['path_num']==simid]
sim_histhpi_onepath = sim_hist_hpi_2[sim_hist_hpi_2['path_num']==simid].drop(['hpg_season','unemp'],axis=1)
hist_onepath = hist_input.copy()
hist_onepath['path_num'] = simid
rate_onepath = rate_frm_4[rate_frm_4['path_num']==simid]

fc_input = qtrlist_2.copy()
fc_input['path_num']=simid
fc_input =fc_input.merge(errmat_onepath, how='left', on=['indexcode','qtr','path_num'])
fc_input =fc_input.merge(simhpi_onepath, how='left', on=['indexcode','qtr','path_num'])
fc_input =fc_input.merge(sim_histhpi_onepath, how='left', on=['indexcode','qtr','path_num'])
fc_input =fc_input.merge(hist_onepath, how='left', on=['indexcode','qtr','path_num'])
fc_input.loc[fc_input['unemp_g'].isnull(),'unemp_g'] = fc_input['unemp_g_sim']
fc_input.loc[fc_input['hpg_season_last2'].isnull(),'hpg_season_last2'] = fc_input['hpg_season_last2_sim']
fc_input = fc_input.drop(['unemp_g_sim','hpg_season_last2_sim'],axis=1)
fc_input['afford_rent'] = np.log(fc_input['inc_p50']*fc_input['m_rentShare']/12) - np.log(fc_input['baserent'])
fc_input['resid_rent'] = fc_input['resid_rent'].fillna(0.0)
fc_input = fc_input.merge(parm, how='left', on=['pSFR_group'])
fc_input = fc_input.sort_values(['path_num','indexcode','qtr'])
fc_input['qtridx'] = fc_input['qtr']%100
fc_input = fc_input.merge(rate_onepath[['qtr','path_num','chgslope0_1']], how='left', on=['qtr','path_num'])
# fc_input['rn'] = fc_input.groupby(['path_num','indexcode'])["qtr"].rank()
cbsa=12060
i=9


for cbsa in fc_cbsa_list:
    fc_onecbsa = fc_input[fc_input['indexcode']==cbsa].reset_index().drop(['index'], axis=1)
    for i in range(0,len(fc_onecbsa.index)):
        if np.isnan(fc_onecbsa.loc[i,'ln_rentg']):
            fc_onecbsa.loc[i,'rentg_l1'] = fc_onecbsa.loc[i-1,'ln_rentg']
            fc_onecbsa.loc[i,'rentg_l2'] = fc_onecbsa.loc[i-1,'rentg_l1']
            fc_onecbsa.loc[i,'rentg_l3'] = fc_onecbsa.loc[i-1,'rentg_l2']
            fc_onecbsa.loc[i,'rentg_l4'] = fc_onecbsa.loc[i-1,'rentg_l3']
            fc_onecbsa.loc[i,'rentg_l5'] = fc_onecbsa.loc[i-1,'rentg_l4']
            fc_onecbsa.loc[i,'ln_rentg'] = fc_onecbsa.loc[i,'Intercept'] + (fc_onecbsa.loc[i,'rentg_l1'] * fc_onecbsa.loc[i,'p_rentg_l1']) + (fc_onecbsa.loc[i,'rentg_l2'] * fc_onecbsa.loc[i,'p_rentg_l2'])+ (fc_onecbsa.loc[i,'rentg_l3'] * fc_onecbsa.loc[i,'p_rentg_l3']) + fc_onecbsa.loc[i,'rentg_l4'] * fc_onecbsa.loc[i,'p_rentg_l4']+ fc_onecbsa.loc[i,'rentg_l5'] * fc_onecbsa.loc[i,'p_rentg_l5']+ fc_onecbsa.loc[i,'afford_rent'] * fc_onecbsa.loc[i,'p_afford_rent'] + fc_onecbsa.loc[i,'hpg_season_last2'] * fc_onecbsa.loc[i,'p_hpg_season_last2']+ fc_onecbsa.loc[i,'unemp_g'] * fc_onecbsa.loc[i,'p_unemp_g']+ fc_onecbsa.loc[i,'chgslope0_1'] * fc_onecbsa.loc[i,'p_chgSlope0_1']+ fc_onecbsa.loc[i,'resid_rent']
    fc_input = pd.concat([fc_input[fc_input['indexcode']!=cbsa], fc_onecbsa], axis=0, ignore_index=True)       