# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 09:17:16 2021

@author: jhu
"""


import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from sklearn import datasets, linear_model
import statsmodels.api as sm
from scipy import stats
from scipy.optimize import minimize


""" Get Weight for regression """
stringR = "select a.*\n"
stringR += ", coalesce(t.cbsa_div, t.cbsa, z.CBSA_Div, z.CBSA) as cbsa \n"
stringR += ", z.StateFIPS+z.CountyFIPS as County, z.CountyName \n"
stringR += ", price_per_sqft = case when t.cj_living_area>50 then closingRent/t.cj_living_area end \n"
stringR += ", isnull(t.effective_year_built, t.year_built) as year_built \n"
stringR += ", t.cj_living_area \n"
stringR += ", t.bedrooms, t.total_baths, t.census_tract, case when len(t.state) = 2 then t.state else z.state end as State \n"
stringR += "from modeltestbed.dbo.SFR_Rent_CleanUp_new_final a \n"
stringR += "join amhersthpi..hpi_taxroll_vw t \n"
stringR += "on a.asg_propid=t.asg_propid and t.prop_type='SF' \n"
stringR += "left join ThirdPartyData..ZipCodesDotCom_dt z \n"
stringR += "on t.zip = z.ZipCode and z.PrimaryRecord='P' \n"
stringR += "where year(a.lease_enddate)>=2003 \n"
stringR += "and not (source='Altos' and year(lease_enddate)*100+datepart(qq, lease_enddate) in (201601,201801,201802)) \n"
stringR += "order by asg_propid, lease_enddate"

print(stringR)

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global allRent_0, allRent_1, allRent_2, allRent_3, allRent_4
allRent_0 = pd.read_sql(stringR, cnxn)
cnxn.close()

# change var format;
allRent_0['asg_propid'] = allRent_0['asg_propid'].astype('int64')
allRent_0['lease_enddate'] = pd.to_datetime(allRent_0['lease_enddate'])
allRent_0['lease_beginDate'] = pd.to_datetime(allRent_0['lease_beginDate'])
allRent_0['month'] = allRent_0['lease_enddate'].dt.year*100+allRent_0['lease_enddate'].dt.month
allRent_0.dtypes

# previous lease;
allRent_0 = allRent_0.set_index(["asg_propid"])
allRent_0 = allRent_0.sort_values(['asg_propid','lease_enddate'])
allRent_0['rn'] = allRent_0.groupby('asg_propid')["lease_enddate"].rank()
allRent_0['date_l1'] = allRent_0.groupby('asg_propid')['lease_enddate'].shift(1)
allRent_0['daypass'] = (allRent_0['lease_enddate'] - allRent_0['date_l1']).dt.days
allRent_0 = allRent_0.reset_index()

# test = allRent_0.head(10)

#remove records within 183 days of previous lease;
allRent_0.drop(allRent_0[(allRent_0['daypass']<=183) & (allRent_0['rn']>1)].index, inplace=True)

# ACS SFR Units as of 2013;
StringU = "select distinct a.GeographyType, case when a.geographytype='Nation' then 'US' else isnull(cast(s.state as varchar(5)), a.GeographyCode) end as indexcode \n"
StringU += ", a.Value as SFD, b.value as SFA, c.value + isnull(d.value,0) as SFR \n"
StringU += "from ThirdPartyData..DemoEcon_dt a \n"
StringU += "left join ThirdPartyData..DemoEcon_dt b \n"
StringU += "on a.GeographyCode=b.GeographyCode and a.BeginDate=b.BeginDate and b.DataSeries='Housing Units, 1 Unit, Attached, 5-Year Estimate' \n"
StringU += "left join ThirdPartyData..DemoEcon_dt c \n"
StringU += "on cast(a.GeographyCode as bigint)=cast(c.GeographyCode as bigint) and a.BeginDate=c.BeginDate and c.DataSeries='Renter-Occupied Housing Units, 1, Detached Unit, 5-Year Estimate' and c.GeographyType=a.GeographyType \n"
StringU += "left join ThirdPartyData..DemoEcon_dt d \n"
StringU += "on cast(a.GeographyCode as bigint)=cast(d.GeographyCode as bigint) and a.BeginDate=d.BeginDate and d.DataSeries='Renter-Occupied Housing Units, 1, Attached Unit, 5-Year Estimate' and d.GeographyType=a.GeographyType \n"
StringU += "left join ThirdPartyData..State_dt s \n"
StringU += "on cast(a.GeographyCode as bigint)=cast(s.StateFIPS as bigint) \n"
StringU += "where a.DataSeries='Housing Units, 1 Unit, Detached, 5-Year Estimate' and year(a.begindate) =2013  \n"
StringU += "and a.GeographyType not in ('Census Tract') \n"
StringU += "order by a.GeographyType,indexcode"

print(StringU)


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global ACS
ACS = pd.read_sql(StringU, cnxn)
cnxn.close()

ACS = ACS.fillna(0.0)
ACS = ACS.reset_index()
ACS['SF'] = ACS['SFD'] + ACS['SFA']
ACS['pct_SFR'] = ACS['SFR']/ACS['SF']
ACS['pct_SFR'] = ACS['pct_SFR'] .fillna(0.0)

# udpate old CBSA code
ACS.loc[ACS.indexcode == '16974', 'indexcode'] = '16984'
ACS.loc[ACS.indexcode == '19380', 'indexcode'] = '19430'
ACS.loc[ACS.indexcode == '43524', 'indexcode'] = '23224'

# avg Rent for all listing in 2013 in our data;
listing_2013 = allRent_0[allRent_0['lease_enddate'].dt.year == 2013].groupby(["cbsa"])['closingRent'].count().to_frame()
listing_2013 = listing_2013.rename(columns={"closingRent":"n_closed_listing_2013"})
listing_2013['avg_rent_2013'] = allRent_0[allRent_0['lease_enddate'].dt.year == 2013].groupby(["cbsa"])['closingRent'].mean()


ACS = ACS.rename(columns={"indexcode":"cbsa"})
listing_2013 = listing_2013.reset_index()
dfs = [df.set_index('cbsa') for df in [listing_2013, ACS]]
cbsa_weight = pd.concat(dfs, axis=1, join="inner").reset_index()
cbsa_weight['geoW'] = cbsa_weight['avg_rent_2013'] * cbsa_weight['SFR']

"""  HP """
stringH = "select * from irspublish..hpi_basefile where qtr>0"

cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=DFILSQL07.insightlabs.amherst.com;"
                     "Trusted_Connection=yes;")
global homeprice
homeprice = pd.read_sql(stringH, cnxn)
cnxn.close()

homeprice['cbsa_code'] = homeprice['cbsa_code'].astype(int)
homeprice['indexcode'] = homeprice['cbsa_code'].astype(str)
homeprice.dtypes

homeprice = homeprice[['indexcode', 'qtr','hpi_sa_medhp','Inc','us_unemp','unemp','incG','ln_inc',
                       'Avginc_G', 'hpi_sa','hpg_season','housing','chgslope0_1','refi_rate','cmt_2yr','cmt_10yr','slope']]

homeprice = homeprice.rename(columns = {'qtr': 'month'})

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
min_month = min_month[(min_month['month'] >= 202109) & (min_month['min_month'] <= 201001)]
min_month = min_month.reset_index()

min_month = min_month[(min_month['indexcode'] == 'US') | (min_month['indexcode'].astype(str).map(len) == 5) ]
min_month = list(min_month['indexcode'])

hist_index = hist_index[hist_index['indexcode'].isin(min_month)]
hist_index = hist_index.drop(['index'], axis=1)

""" Need to figure out how to run SA in Python """
hist_index.to_csv(r'E:\Output\Rent Forecast\hist index no sa.csv', index = False)

hist_index_sa= pd.read_csv(r'E:\Output\Rent Forecast\hist index sa.csv')


""" Ln(rentg) """

hist_index_sa['rn'] = hist_index_sa.groupby('indexcode')["month"].rank()
hist_index_sa['lag_index'] = hist_index_sa.groupby('indexcode')['rentidx_sa'].shift(1)
hist_index_sa['ln_rentg'] = np.log(hist_index_sa['rentidx_sa']/hist_index_sa['lag_index'])

geoW = cbsa_weight[['cbsa', 'geoW']]
geoW = geoW.rename(columns={'cbsa':'indexcode'})
modelinp = hist_index_sa.merge(geoW, on='indexcode', how='inner')
modelinp = modelinp[modelinp['rn']>1]
modelinp = modelinp.drop(['rn', 'lag_index'], axis=1)

modelinp['ln_rentg'] = np.where( (( (modelinp['month']<202001) & (abs(modelinp['ln_rentg'])>0.1) )| (abs(modelinp['ln_rentg'])>0.25)) , np.nan ,modelinp['ln_rentg'])
modelinp['l1_rentg'] =  modelinp.groupby('indexcode')['ln_rentg'].shift(1)
modelinp['l2_rentg'] =  modelinp.groupby('indexcode')['ln_rentg'].shift(2)
modelinp['l3_rentg'] =  modelinp.groupby('indexcode')['ln_rentg'].shift(3)
modelinp['l4_rentg'] =  modelinp.groupby('indexcode')['ln_rentg'].shift(4)
modelinp['l5_rentg'] =  modelinp.groupby('indexcode')['ln_rentg'].shift(5)
modelinp = modelinp[(modelinp['month']<202109) & (modelinp['month']>201100)]

modelinp = modelinp.dropna()
      
modelinp['monthfmt'] = pd.to_datetime(modelinp['monthfmt'])   
modelinp['qtr'] = modelinp['monthfmt'].dt.year*100 + modelinp['monthfmt'].dt.month/3

modelinp1 = modelinp.merge(homeprice, on=['indexcode','qtr'], how='inner')

cbsalist = modelinp1['indexcode'].to_frame()
cbsalist = cbsalist.drop_duplicates()
cbsalist = cbsalist.reset_index(drop=True)
cbsalist['rn'] = cbsalist.index+1



for x in list(range(1, len(cbsalist)+1)):
    x_1 = 'z_' + str(x)
    try:
        z_list += [x_1]
    except:
        z_list = [x_1]

cbsalist.dtypes
modelinp1 = modelinp1.merge(cbsalist, on='indexcode', how='inner')
   


tmp = pd.DataFrame(0, index=modelinp1.index, columns=z_list)
# tmp_3 = tmp_3.fillna(0)

modelinp1 = modelinp1.join(tmp, how='inner')

for p in list(range(1, len(cbsalist)+1)):
# print ('period_'+str(p))
    modelinp1.loc[(modelinp1['rn'] == p), 'z_'+str(p)] = 1

modelinp1.dtypes


""" Simple Regression: all CBSA together no CBSA dummy """
x_list = ['l1_rentg','l2_rentg','l3_rentg','l4_rentg']
# x_list = x_list + z_list
lr = linear_model.LinearRegression()
lr_x = modelinp1.loc[:, x_list]
lr_y = modelinp1.loc[:,'ln_rentg']
sample_weight = modelinp1.loc[:,'geoW']
lr.fit(lr_x, lr_y, sample_weight)

modelinp2 = modelinp1[modelinp1['month']<202001]
lr_x = modelinp2.loc[:,x_list]
lr_y = modelinp2.loc[:,'ln_rentg']
sample_weight = modelinp2.loc[:,'geoW']
sample_weight2 = modelinp2.loc[:,'housing']

# Check T stats
X2 = sm.add_constant(lr_x)
est = sm.WLS(lr_y, X2, sample_weight)
est2 = est.fit()
print(est2.summary())


norist_coef = pd.DataFrame(lr.coef_.reshape(-1, len(lr.coef_)),columns=x_list)
norist_coef['intercept'] = lr.intercept_
norist_coef['R2'] = lr.score(lr_x, lr_y, sample_weight)

x_init = np.array([lr.intercept_,lr.coef_[0], lr.coef_[1], lr.coef_[2], lr.coef_[3]])

""" Constrained: sum of AR(4)>=0.6 """
model_constrain = lambda b, x: b[0] + b[1] * x.iloc[:,0] + b[2] * x.iloc[:,1] + b[3] * x.iloc[:,2] + b[4] * x.iloc[:,3]
obj_constrain = lambda b, y, x: np.sum(np.abs(y-model_constrain(b, x))**2)
bnds_constrain = [(None, None),(-1, 1),(-1, 1),(-1, 1),(-1, 1)]
# hess = lambda x,v : numpy.zeros(len(X_repeat),2)
cons = [{"type": "ineq", "fun": lambda x: x[1]+x[2]+x[3]+x[4] - 0.6},
        {"type": "ineq", "fun": lambda x: 1.05- (x[1]+x[2]+x[3]+x[4]) }]
# initial guess: unrestricted linear regression;
res_constrain = minimize(obj_constrain, args=(lr_y, lr_x), x0=x_init, bounds=bnds_constrain,
                                constraints=cons, method='trust-constr', hess=None)       
res_constrain.x


constrain_coef = pd.DataFrame(res_constrain.x.reshape(-1, len(res_constrain.x)),columns=['intercept']+x_list)