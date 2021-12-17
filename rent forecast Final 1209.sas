
option compress=yes error=10;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
libname IR ODBC DSN='InterestRates' schema=dbo;

%let lt_out=\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1;
libname RentParm "E:\Output\Rent Forecast\Param";
LIBNAME simHPI "\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1\parameters";
%let amherstmarket = ('12060','16740','28140','38060','36740','45300','27260','34980','23104','19124','32820','31140','13820','26900','36420','41180','39580','41700','29460','15980','35840','26420','49180'
,'37340','46060','24660','18140','19660','19740','29820','48424','22744','20500','38940','44380','45104','33124','15500','45640','23580','42680','46220','39460','36260');


/* geoW */
data _null_;set thirdp.cbsa_dt;
call symput(compress("cbsaname"||trim(cbsa)), name);
run;
%put &cbsaname19124.;


data _null_; set thirdp.ZipCodesDotCom_dt(where=(PrimaryRecord='P'));
call symput(compress("fips_to_state"||statefips),state);
if cbsa_div ne '' then call symput(compress("div2cbsa"||cbsa_div),cbsa);
if cbsa_div='' then do; cbsa_div=cbsa;end;
call symput(compress("cbsa_to_name"||cbsa_div),name);
call symput(compress("ziptocbsa"||trim(zipcode)),cbsa_div);
call symput(compress("ziptocbsa1"||trim(zipcode)),cbsa);
call symput(compress("ziptofips"||trim(zipcode)),fips);
run;
%put &ziptocbsa00501.;

proc SQL; connect to odbc(DSN='modeltestbed'); create table allRent00 as select 
* from connection to odbc(
select a.*
, coalesce(t.cbsa_div, t.cbsa, z.CBSA_Div, z.CBSA) as cbsa 
, z.StateFIPS+z.CountyFIPS as County, z.CountyName 
, price_per_sqft = case when t.cj_living_area>50 then closingRent/t.cj_living_area end 
, isnull(t.effective_year_built, t.year_built) as year_built 
, t.cj_living_area 
, t.bedrooms, t.total_baths, t.census_tract, case when len(t.state) = 2 then t.state else z.state end as State 
from modeltestbed.dbo.SFR_Rent_CleanUp_new_final a 
join amhersthpi..hpi_taxroll_vw t 
on a.asg_propid=t.asg_propid and t.prop_type='SF' 
left join ThirdPartyData..ZipCodesDotCom_dt z 
on t.zip = z.ZipCode and z.PrimaryRecord='P' 
where year(a.lease_enddate)>=2003 
and not (source='Altos' and year(lease_enddate)*100+datepart(qq, lease_enddate) in (201601,201801,201802)) 
order by asg_propid, lease_enddate
); 
disconnect from odbc;quit;
proc sort nodup; by asg_Propid lease_enddate; run;

data repeatedSale; set allrent00; by asg_propid lease_enddate ; 
date_l1=lag(lease_enddate); format date_l1 date9.;

if not first.asg_propid and intck('day',date_l1,lease_enddate)<=183 then delete; 
qtr = year(lease_enddate)*100+qtr(lease_enddate);
length us $5.;
us='US';
run; 
proc sort data=repeatedSale nodup; by asg_propid lease_enddate; run;


** Total SF units & Renter Occ Unit;
proc sql; connect to odbc(DSN='thirdpartydata');
create table ACS as select distinct * from connection to odbc(
  select distinct a.GeographyType, case when a.geographytype='Nation' then 'US' else isnull(cast(s.state as varchar(5)), a.GeographyCode) end as indexcode,year(a.begindate) as year,
a.Value as SFD, b.value as SFA, c.value + isnull(d.value,0) as SFR
, isnull(oc.value,0)+isnull(oc1.value,0) as SF_OwnerOcc
, e.value as RenterOcc, tc.value as total_Occ, m.value as MedRent_ACS
from ThirdPartyData..DemoEcon_dt a
left join ThirdPartyData..DemoEcon_dt b
on a.GeographyCode=b.GeographyCode and a.BeginDate=b.BeginDate and b.DataSeries='Housing Units, 1 Unit, Attached, 1-Year Estimate'
left join ThirdPartyData..DemoEcon_dt c
on cast(a.GeographyCode as bigint)=cast(c.GeographyCode as bigint) and a.BeginDate=c.BeginDate and c.DataSeries='Renter-Occupied Housing Units, 1, Detached Unit, 1-Year Estimate' and c.GeographyType=a.GeographyType
left join ThirdPartyData..DemoEcon_dt d
on cast(a.GeographyCode as bigint)=cast(d.GeographyCode as bigint) and a.BeginDate=d.BeginDate and d.DataSeries='Renter-Occupied Housing Units, 1, Attached Unit, 1-Year Estimate' and d.GeographyType=a.GeographyType
left join ThirdPartyData..DemoEcon_dt e
on cast(a.GeographyCode as bigint)=cast(e.GeographyCode as bigint) and a.BeginDate=e.BeginDate and e.DataSeries='Renter-Occupied Housing Units, 1-Year Estimate' and e.GeographyType=a.GeographyType
left join ThirdPartyData..DemoEcon_dt m
on cast(a.GeographyCode as bigint)=cast(m.GeographyCode as bigint) and a.BeginDate=m.BeginDate and m.DataSeries='Median Gross Rent, 1-Year Estimate' and m.GeographyType=a.GeographyType
left join ThirdPartyData..DemoEcon_dt tc
on cast(a.GeographyCode as bigint)=cast(tc.GeographyCode as bigint) and a.BeginDate=tc.BeginDate and tc.DataSeries='Total Occupied Housing Units, 1-Year Estimate' and tc.GeographyType=a.GeographyType

left join ThirdPartyData..DemoEcon_dt oc
on cast(a.GeographyCode as bigint)=cast(oc.GeographyCode as bigint) and a.BeginDate=oc.BeginDate and oc.DataSeries='Owner-Occupied Housing Units, 1, Detached Unit, 1-Year Estimate' and oc.GeographyType=a.GeographyType
left join ThirdPartyData..DemoEcon_dt oc1
on cast(a.GeographyCode as bigint)=cast(oc1.GeographyCode as bigint) and a.BeginDate=oc1.BeginDate and oc1.DataSeries='Owner-Occupied Housing Units, 1, Attached Unit, 1-Year Estimate' and oc1.GeographyType=a.GeographyType
left join ThirdPartyData..State_dt s
on cast(a.GeographyCode as bigint)=cast(s.StateFIPS as bigint)
where a.DataSeries='Housing Units, 1 Unit, Detached, 1-Year Estimate'
and a.GeographyType not in ('Census Tract')
order by a.GeographyType,indexcode, year

); disconnect from odbc; quit;

proc sort data=ACS nodup; by indexcode year;run;

data rentunits; set ACS; by indexcode year; 
SF=sfd+sfa; 
if sfr=. then sfr=0;
if renterOcc=. then renterocc=0;
if total_occ=. then total_occ=0;

if sfr+sf_ownerOcc>0 then p_sfr = sfr/(sfr+sf_ownerOcc);
if total_occ>0 then p_rentocc = renterocc/total_occ;

if indexcode='16974' then indexcode='16984';
if indexcode='19380' then indexcode='19430';
if indexcode='43524' then indexcode='23224';
run; 
proc sort; by indexcode year;run;


proc sql  noprint; create table Listing2013 as 
select distinct cbsa,count( asg_propid) as closedListing,median(closingRent) as medPrice,avg(closingRent) as avgPrice 
from repeatedSale 
where year(lease_enddate)=2013 and cbsa ne ''
group by cbsa 
order by cbsa;
quit;



data geoW; merge listing2013(in=f1) rentunits(in=f2 rename=indexcode=cbsa where=(year=2013))  ;  by cbsa;  if f1 and f2 ; 
where cbsa ne ''; 
geoW=SFR*avgPrice;
geoW_allRentUnit = RenterOcc*medRent_ACS;
p_sfr_allrent = sfr/renterocc;
*p_SFR = sfr/sf;
keep cbsa  geoW SFR RenterOcc SFD MedRent_ACS SF p_SFR geoW_allRentUnit p_rentocc p_sfr_allrent   avgPrice;
run;


proc sql; create table geoW as
select distinct c.Name as Market
,  case when a.cbsa in  &amherstmarket. then 1 else 0 end as MSRMarket
,  a.*
from geoW a
left join thirdp.cbsa_dt c
on a.cbsa = c.cbsa
where a.cbsa in (select distinct indexcode from irs.sf_rentidx_monthly)
order by cbsa;
quit;

proc univariate data=geoW noprint;class msrMarket ; var  p_sfr ;  output out=P_momPop2 pctlpre=  sfr_  pctlpts= 33,67; run;

proc sql; create table geoW as
select a.*
, case when A.msrMarket=1 then
	case when p_SFR<=p2.sfr_33 then 1 when p_SFR<=p2.sfr_67 then 2 else 3 end 
  ELSE 
  	case when p_SFR<=p2.sfr_33 then 4 when p_SFR<=p2.sfr_67 then 5 else 6 end
  END 
as pSFR_group
from geoW a
join P_momPop2 p2
on p2.msrMarket=a.msrMarket
order by cbsa;
quit;

/* Historical Index */

data rawRentidxOrg; set irs.sf_rentidx_monthly ; date=year(monthfmt)*100+month(monthfmt);*substr(monthfmt,1,4)*100+substr(monthfmt,6,2); 
drop monthfmt; *if pricetier='agg'; *drop index_: cbsa;* if city20=0 or city20=.; where indexcode not in ('90000','91000');
keep indexcode index date;
proc sort nodup; by indexcode date; run;
data rawRentidxOrg; set rawRentidxOrg; by indexcode date; rentg= index/lag(index)-1; if first.indexcode then rentg=.; run;
data rawrentidx0; set rawRentidxOrg; by indexcode date;  if last.indexcode; keep indexcode pricetier  date rentg; run;
data rawrentIdx1; set rawrentIdx0; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;
data rawrentIdx2; set rawrentIdx1; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;

data rawrentIdx3; set rawrentIdx0 rawrentIdx1 rawrentIdx2; proc sort nodup; by indexcode date; run;
data test; set rawrentidx3; by indexcode date; if last.date; run;
data _null_; set rawrentidx3; by indexcode date; if last.date; call symput("fcqtrStart", int(date/100)*100+mod(date,100)/3);  call symput("fcqtrmo", int(date/100)*100+mod(date,100)); run;
%let fcqtrStart=%eval(&fcqtrStart*1);
%let fcqtrmo=%eval(&fcqtrmo*1);
%put &fcqtrStart &fcqtrmo;

data rawrentIdx; merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; 
by indexcode date; 
retain index; 
if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0; else index=index*(1+rentg);
drop rentidx0 rentg0 ; if mod(date,100)  in (3,6,9,12);
run;

data allrentIdx; set rawrentIdx; *drop geographytype; *if length(geographycode)=5; 
indexmonth=input(put(date*100+1,8.),YYMMDD10.);
format indexmonth  MMDDYYD10.; 
rename date=qtr index=rentidx;
keep indexcode date index indexmonth; run;
proc sql; create table allrentIdx as select distinct * from allrentIdx group by indexcode having min(qtr)<201001 and max(qtr)>=201901 order by indexcode ,qtr;quit;
proc x12 data=allrentIdx date=indexmonth noprint interval=QTR; by indexcode ; var Rentidx;   
x11;    output out=sa d11;    ods select d11; run;

data allrentIdx; merge  allrentIdx(in=f1) sa(in=f2 rename=Rentidx_D11=Rentidx_sa); 
by indexcode  indexmonth;  if f1 and f2;
LnRentg=log(Rentidx_sa/lag(Rentidx_sa)); 
if first.indexcode then do; LnRentg=.; end;
qtr=year(indexmonth)*100+month(indexmonth); 
if lnrentg=. then delete;
qtr=int(qtr/100)*100+mod(qtr,100)/3;run;


data _null_; set thirdp.county_dt;
if cbsadiv='' then cbsadiv=cbsa;
call symput(compress("fipscbsa"||trim(fips)), cbsadiv);
run;
%put &fipscbsa01001;
** Employment;

proc sql; connect to odbc(DSN='thirdpartydata');
create table Cntyemp as select distinct * from connection to odbc(
select  geographycode = substring(area_code, 3, 5)
, month=year*100+right(period,2)
, value as totemp

  from thirdpartydata..bls_la_current
where series_title like '%Employment%' and measure_name='employment'
and area_type_name = 'Counties and equivalents'
and seasonal_name='Not Seasonally Adjusted'
and period <> 'M13'
 order by geographycode, month
);disconnect from odbc; quit;

proc sql; connect to odbc(DSN='thirdpartydata');
create table CntyLabor as select distinct * from connection to odbc(
select  geographycode = substring(area_code, 3, 5)
, month=year*100+right(period,2)
, value as laborforce

  from thirdpartydata..bls_la_current
where series_title like '%Labor Force%'
and area_type_name = 'Counties and equivalents'
and seasonal_name='Not Seasonally Adjusted'
and period <> 'M13'
and value>0
 order by geographycode, month
);disconnect from odbc; quit;

proc sort data=cntylabor; by geographycode month;
proc sort data=Cntyemp; by geographycode month; run;


data cntyUnemp0; merge cntyEmp(in=f1) cntyLabor(in=f2); by geographycode month; if f1 and f2;
if mod(month,100)<=3 then tp=1;
else if mod(month,100)<=6 then tp=2;
else if mod(month,100)<=9 then tp=3;
else if mod(month,100)<=12 then tp=4;

qtr=input(put(int(month/100)*10000+tp*300+1,8.),yymmdd10.);
format qtr  monyy.;
run;
proc means data=cntyUnemp0 noprint nway; class geographycode qtr; var totemp laborforce; output out=countyemp mean=;run;


data countyemp; set countyemp; where geographycode ne '' and qtr ne .;  
cbsa_code=symget(compress("fipscbsa"||geographycode));
keep geographycode qtr cbsa_code totemp laborforce;; run;

proc means data=countyemp noprint; class cbsa_code qtr; where cbsa_code ne '';  var totemp laborforce; output out=cbsaemp sum=;run;
data cbsaemp; set cbsaemp; if cbsa_code ne '' and qtr ne .;  *if cbsa_code ='16984'  then cbsa_code='16974';
unemp=1-totemp/laborforce;
ln_emp = log(totemp);
ln_laborforce = log(laborforce);
keep ln_emp ln_laborforce totemp laborforce cbsa_code date qtr unemp; run;


proc x12 data=cbsaemp date=qtr noprint interval=QTR; by cbsa_code; var unemp;    x11;    output out=unemp_sa d11;    ods select d11; run;
data unemp_sa; set unemp_sa(rename=(qtr=date unemp_d11=unemp)); qtr=year(date)*100+qtr(date); keep qtr cbsa_code unemp; run;


/* Home Price */
data HP; set irs.hpi_basefile; indexcode=put(cbsa_Code,$5.); drop cbsa_code; if qtr>0; 
run;

** Base Rent as of 2013Q1;
proc SQL; connect to odbc(DSN='thirdpartydata'); create table baseRent as select * from connection to odbc(
select a.*,b.state,isnull(cbsa_div,cbsa) as cbsa 
from modeltestbed.dbo.SFR_Rent_CleanUp_new_final a 
join amhersthpi..hpi_taxroll_vw b
on a.asg_propid=b.asg_propid and b.prop_type ='SF' 
and 2013=year(lease_enddate) and month(lease_enddate)<=3;
); 
disconnect from odbc;quit;

proc means data=baseRent noprint; class state; var closingRent; where closingRent between 100 and 10000; output out=baseRent_state p50=baseRent;run;
proc means data=baseRent noprint; class cbsa; var closingRent; where closingRent between 100 and 10000; output out=baseRent_cbsa p50=baseRent;run;

data baseRent_med; set baseRent_cbsa(rename=cbsa=indexcode) baseRent_state(rename=state=indexcode) ; keep indexcode baseRent; where indexcode ne '';run;

proc sql; create table modelinp0b as select distinct a.*, baseRent, c1.hpi_sa_medhP as baseHP,c.*,
baseRent/e.rentidx_sa*a.rentidx_sa as medRent, em.ln_emp, em.ln_laborforce, geoW, geoW_allRentUnit
, pSFR_group
, case when a.indexcode in &amherstmarket. then 1 else 0 end as MSRMarket
from allrentIdx a
join rawRentidx b
on a.indexcode=b.indexcode and b.date=201303
join baseRent_med m 
on a.indexcode=m.indexcode
join HP c 
on a.indexcode=c.indexcode 
and a.qtr=c.qtr 
join HP c1
on a.indexcode=c1.indexcode 
and  c1.qtr=201301
join allrentIdx e
on a.indexcode=e.indexcode 
and e.qtr=201301
left join cbsaemp em
on a.indexcode=em.cbsa_code
and year(a.indexmonth)*100+qtr(a.indexmonth) = year(em.qtr)*100+qtr(em.qtr)
join geoW w
on a.indexcode=w.cbsa

where a.qtr>=200001
and a.indexcode in (select indexcode from HP); ;quit;
proc sort nodup; by indexcode indexmonth; run;

data hp1; set hp; by indexcode qtr;
hpg_season_l1=lag(hpg_season);
if indexcode ne lag(indexcode) then hpg_season_l1=.;
hpg_season_last2 = hpg_season+hpg_season_l1;
run;


data modelinp1; set modelinp0b; by indexcode  indexmonth; retain  Rentidx1 ;  
if first.indexcode  then Rentidx1=1;
else Rentidx1=Rentidx1*(exp(LnRentg)); 
ln_Rentidx=log(Rentidx1);

month = intck('qtr','01JAN2000'D, indexmonth)+1;

if abs(LnRentg)>.1 and qtr<202001 then LnRentg=.;
if abs(lnRentg)>.25 then lnrentg=.;


rentyield=log(medRent*12/hpi_sa_medhp);

unemp_l1=lag(unemp);
ln_inc_l1=lag(ln_inc);
hpg_season_l1=lag(hpg_season);
medHP_l1 = lag(hpi_sa_medhp);

rentyield_l1=lag(rentyield);
medRent_l1 = lag(medRent);

ln_rentIdx_l1 = lag(ln_rentidx);
if first.indexcode then do; ln_inc_l1=.; hpg_season_l1=.; medHP_l1=.;unemp_l1=.; rentyield_l1=.;medRent_l1=.;ln_rentIdx_l1=.; end;

inc_g=ln_inc - ln_inc_l1;
unemp_g=unemp-unemp_l1;
unemp_perc = unemp*100;

rentg_l1=lag(LnRentg); if first.indexcode then rentg_l1=.;
rentg_l2=lag(rentg_l1); if indexcode ne lag2(indexcode) then rentg_l2=.;
rentg_l3=lag(rentg_l2); if indexcode ne lag3(indexcode) then rentg_l3=.;
rentg_l4=lag(rentg_l3); if indexcode  ne lag4(indexcode) then rentg_l4=.;
rentg_l5=lag(rentg_l4); if indexcode  ne lag5(indexcode) then rentg_l5=.;


hpg_season_l2 = lag(hpg_season_l1); if indexcode ne lag2(indexcode) then hpg_season_l2=.;
hpg_season_l3 = lag(hpg_season_l2); if indexcode ne lag3(indexcode) then hpg_season_l3=.;
hpg_season_l4 = lag(hpg_season_l3); if indexcode ne lag4(indexcode) then hpg_season_l4=.;

hpg_season_last2 = hpg_season+hpg_season_l1;

rent_share = medRent_l1*12/inc;

insurerate=0.5;
year=min(year(indexmonth),2018);
taxrate= symget(compress("cbsatax"||trim(indexcode)||trim(year)))*1;
if taxRate>0.06 or taxRate<0.005 then taxRate=0.015;

factor= (refi_l1/1200+1)**360*(refi_l1/1200) /((refi_l1/1200+1)**360 -1)*0.8 +(taxrate+insurerate/100)/12;
mort_share = medHP_l1*factor/(Inc/12);
rent2Own = max(min(medRent_l1/(medHP_l1*factor),2),0.5);
ln_rent2Own = log(rent2Own);

diff_mort_rent = (mort_share-rent_share);
*diff_mort_rent_l1 = lag(diff_mort_rent); *if first.cbsa then diff_mort_rent_l1=.;

inc_gini=log(Inc_mean/inc);
ln_SFDhh = log(sfdhousehold);
run;

* Historical average for fund line;
proc means data=modelinp1(where=(qtr>=201101 and qtr<=202103)) noprint nway; class indexcode;
var mort_share rent_share rent2Own; output out=histmean mean = m_mortshare m_rentShare m_rent2own; run;

proc sql; create table modelinp2 as
select distinct a.*, h.m_mortshare, h.m_rentShare, h.m_rent2own
from modelinp1 a
join histmean h
on a.indexcode= h.indexcode
order by indexcode, indexmonth; quit;


data modelinp2; set modelinp2;  by indexcode indexmonth;

afford_HP = log(Inc*m_mortshare/12/factor)-log(hpi_sa_medhp);
afford_Rent = log(Inc*m_rentshare/12) - log(baseRent);
afford_rent2 = log( (hpi_sa_medhp*factor) * m_rent2Own)- log(baseRent);
run;


** Find Outlier;
 proc sort data=modelinp2; by indexcode; run;
proc reg data=modelinp2(where=(qtr>201100 and qtr<=202103)) outest=parm_test_outlier adjrsq noprint tableout ;  by indexcode; *by MSRmarket;  *weight geoW; 
model lnRentg =  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.1;
output out=insample_outlier r=r_stage2 p=p_rentg; run; quit;

DATA ar_bygeo; set parm_test_outlier(where=(_type_='PARMS'));
if rentg_l1=. then rentg_l1=0;
if rentg_l2=. then rentg_l2=0;
if rentg_l3=. then rentg_l3=0;
if rentg_l4=. then rentg_l4=0;
if rentg_l5=. then rentg_l5=0;
sum_AR = rentg_l1 + rentg_l2 + rentg_l3 + rentg_l4 + rentg_l5;
run;

proc means mean p5 p10 p25 p75 p90 p95; where _rSQ_>0; var sum_AR; run;

data outlier1; set ar_bygeo; 
*where sum_ar<0.25 or sum_ar>1.5 or _RSQ_<0.1;
if (_rsq_<0.4 and (sum_ar<0.25 or sum_ar>1.5)) /* low R square */;
;
keep indexcode; proc sort nodup; by indexcode; run;
proc sql noprint; select distinct "'"||indexcode||"'" into: outlier1 separated by "," from outlier1;quit;
%put &outlier1.;

data parm_test_outlier; set parm_test_outlier;
outlier=0; 
if indexcode in (&outlier1.) then outlier=1;
market =  symget(compress("cbsaname"||trim(indexcode)));
run;

proc sql; create table parm_test_outlier as
select distinct a.*, b.msrMarket, b.pSFR_group, b.p_SFR
from parm_test_outlier a
join geow b
on a.indexcode=b.cbsa 
order by indexcode;
quit;

proc sql; create table geow_output as
select distinct a.*, h.m_mortshare, h.m_rentShare, h.m_rent2own,c.outlier, m.baserent, r.p_SFR as  p_SFR_2019
from geow a
join histmean h
on a.cbsa= h.indexcode
join parm_test_outlier c
on a.cbsa=c.indexcode
join baseRent_med m
on m.indexcode=a.cbsa
left join rentunits r
on r.indexcode=a.cbsa and r.year=2019
order by  cbsa;
quit;


proc delete data=testbed.rentfc_weight;
data testbed.rentfc_weight(insertbuff=32000); set geow_output; run;

proc export data=parm_test_outlier outfile="E:\Output\Rent Forecast\simple AR5 parm 1209.csv" dbms=csv replace; run;
proc export data=geoW_output outfile="E:\Output\Rent Forecast\geoW output.csv" dbms=csv replace; run;


data modelinp3; set modelinp2; where indexcode not in (&outlier1.);
run;

** Try parameter with constraints;
data parmmap;
length parameter $2. parm $25. initial $1. bound $3.;
input parameter $ parm $ initial $ bound $;
datalines;
b0 intercept	0 0
b1 rentg_l1 	0 0
b2 rentg_l2 	0 0
b3 rentg_l3		0 0
b4 rentg_l4		0 0
b5 rentg_l5 	0 0
b6 afford_rent  0 >=0
;
run;

*r_stage2 = b0 + b1* hpg_season_last2 + b2* unemp_g + b3* rentyield_l1 + b4* chgslope0_1 + b5* afford_HP;
*b1 >= 0, b2<=0, b3<=0, b4>=0;;
data parmmap2;
length parameter $2. parm $20. initial $1. bound $3.;
input parameter $ parm $ initial $ bound $;
datalines;
b0 intercept		0 0
b1 hpg_season_last2 0 >=0
b2 unemp_g 			0 <=0
b3 rentyield_l1 	0 <=0
b4 chgslope0_1 		0 >=0
b5 afford_HP 		0 0
;
run;


%macro loop(name=, var=, maxGroup=,  inp =, yvar= );
%do i= 1 %to &maxGroup.;
data test; set &name._v&version.; where &var.=&i.;;
if (probt<=0.05 and probT ne .) or parameter='b0';
if parameter ne 'b0' then text_m = trim(parameter) || "*" || parm; else text_m = trim(parameter);
text_in = trim(parameter) || "=" || input(initial,$1.);
if parameter ne 'b0'  and parm ne 'afford_HP' then text_bound = trim(parameter)||bound;
run;


proc sql noprint; select distinct text_m into: text_m separated by "+" from test ; quit;
proc sql noprint; select distinct trim(text_in) into: text_in separated by " " from test ; quit;
proc sql noprint; select distinct trim(text_bound) into: text_bound separated by "," from test where parameter ne 'b0' and parm ne 'afford_HP' ; quit;
%put &text_m. &text_in. &text_bound.;



proc nlin data=&inp.(where=(qtr>201100 and qtr<=202103 and &var.=&i.)) /*outest=parm_fund_v&version. */  ;  by &byV.; 
   parameters &text_in.;
   bounds &text_bound.;
   model &yvar. = &text_m.;
   ods select ParameterEstimates;
   ods output  ParameterEstimates=tp; *AdditionalEstimates;
run;

data tp; set tp; &var.=&i.;
run;

data Final&name._v&Version.; set Final&name._v&version. tp; if &var. ne . ;run;
%end;
%mend;

%let version=1; %let byV=pSFR_group; %let maxGroup=6;
%macro tryv_wConstraint(version=, byV=, maxgroup=);
proc sort data=modelinp2; by  &byV. indexcode qtr; run;

proc nlin data=modelinp2(where=(qtr>201100 and qtr<=202103 )) /*outest=parm_fund_v&version. */  ;  by &byV.; 
   parameters b0=0 b1=0 b2=0 b3=0 b4=0 b5=0 b6=0;
   bounds b6>=0;
   model lnRentg =  b0 + b1*rentg_l1 + b2* rentg_l2 + b3*rentg_l3 + b4*rentg_l4 + b5*rentg_l5+ b6*afford_rent;
   ods select ParameterEstimates;
   ods output  ParameterEstimates = rawStage1_v&version.; *AdditionalEstimates;
run;

data rawStage1_v&version.; set rawStage1_v&version.(where=(parameter in ('b0','b1','b2','b3','b4','b5','b6'))); drop label;
if probt>0.05 AND parameter ne 'b0' then do; 
estimate=0; stderr=. ;lowerCL=.; upperCL=.; tValue=.;
end;
run;
proc sort data=rawStage1_v&version.; by parameter;
proc sort data=parmmap; by parameter;
data rawStage1_v&version.; merge rawStage1_v&version.(in=f1) parmmap(in=f2); by parameter;if f1; run;
proc sort; by &byv.; run;

data FinalParmStage1_v&Version.;  run;
%loop(name=rawStage1_v,var=&byv., maxGroup=&maxgroup.,  inp =modelinp2, yvar= lnRentg) ;
proc transpose data=FinalParmStage1_v&version. out=FinalParmStage1; by &byv.; var estimate; id parameter; run;

data parmfund; set parmfund;
if b1=. then b1=0;
if b2=. then b2=0;
if b3=. then b3=0;
if b4=. then b4=0;
if b5=. then  b5=0;
if b6=. then b6=0;
run;

data is_fund_v&version.; merge modelinp3(where=(qtr>201100 and qtr<=202103 ) in=f1) parmFund(in=f2 drop=_name_); by &byV.; if f1 and f2;
rentidx_fund = b0 + b1*ln_month + b2* ln_inc +b3*inc_gini + b4*afford_rent + b5*rent2Own;
r_fund = ln_rentidx - rentidx_fund;
drop b0-b5;
run;

** R-square for Fund Line;

proc means data=is_fund_v&version. noprint nway; by &byv.; var ln_rentidx; weight &geoW.; output out=fundr_mean mean=avg_rentidx; run;

data fundr; merge is_fund_v&version.(in=f1) fundr_mean(in=f2); by &byv.;
r_tot = (ln_rentidx - avg_rentidx)**2;
r_res = r_fund**2;
run;


proc means data=fundr(where=(r_res ne .)) noprint nway;by &byv.; weight &geoW.; var r_tot r_res; output out=fundr_sum sum=; run;

data fundr_sum; set fundr_sum;
r2 = 1-r_res/r_tot;
run;

** 1st Stage: AR(5) + afford_rent;
proc sort data=modelinp3; by &byV. indexcode qtr; run;

proc reg data=modelinp3(where=(qtr>201100 and qtr<=202103 ))  outest=parm_stage1_v&version. adjrsq tableout noprint;  by &byv.;*weight housing; 
weight geoW;*where qtr>=201101 and  qtr<202103;
model lnRentg =  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  afford_rent /selection=stepwise sle=0.05 /*sle=0.001 sle=0.05*/;
output out=is_stage1_v&version. r=r_stage1 p=p_stage1; run; quit;

** 2nd stage: HPA and other Var;

** Constrained;

proc nlin data=is_stage1_v&version. /*outest=parm_fund_v&version. */  ;  by &byV.; 
   parameters b0=0 b1=0 b2=0 b3=0 b4=0 b5=0;
   bounds b1 >= 0, b2<=0, b3<=0, b4>=0;
   model r_stage1 = b0 + b1* hpg_season_last2 + b2* unemp_g + b3* rentyield_l1 + b4* chgslope0_1 + b5* afford_HP;
   ods select ParameterEstimates;
   ods output  ParameterEstimates=parm_stage2_v&version.; *AdditionalEstimates;
run;

data parm_stage2_v&version.; set parm_stage2_v&version.(where=(parameter in ('b0','b1','b2','b3','b4','b5'))); drop label;
if probt>0.05 and parameter ne 'b0' then do; 
estimate=0; stderr=. ;lowerCL=.; upperCL=.; tValue=.;
end;
run;
proc sort data=parm_stage2_v&version.; by parameter;
proc sort data=parmmap2; by parameter;
data parm_stage2_v&version.; merge parm_stage2_v&version.(in=f1) parmmap2(in=f2); by parameter;if f1; run;
proc sort; by &byv.; run;

data Finalparm_stage2_v&Version.;  run;
%loop(name=parm_stage2,var=&byv., maxGroup=&maxgroup.,  inp =is_stage1_v&version., yvar= r_stage1) ;

** Total R-square;
proc transpose data=Finalparm_stage2_v&version. out=parm_stage2; var estimate; id parameter; by &byv.;
run;

data parm_stage2; set parm_stage2;
if b0=. then b0=0;
if b1=. then b1=0;
if b2=. then b2=0;
if b3=. then b3=0;
if b4=. then b4=0;
if b5=. then b5=0;
run;

data resid_SFrent_v&version.; merge is_stage1_v&version.(in=f1) parm_stage2(in=f2 drop=_name_); by &byv.; if f1 and f2;
p_rentg = p_stage1 +  b0 + b1* hpg_season_last2 + b2* unemp_g + b3* rentyield_l1 + b4* chgslope0_1 + b5* afford_HP;
r_rentg = lnRentg - p_rentg;
run;

proc means data=resid_SFrent_v&version. noprint nway; by &byv.; var lnRentg; weight geoW; output out=ttr_mean mean=avg_rentidx; run;

data total_r2; merge resid_SFrent_v&version.(in=f1) ttr_mean(in=f2); by &byv.; if f1 and f2;
r_tot = (lnrentg - avg_rentidx)**2;
r_res = r_rentg**2;
run;


proc means data=total_r2(where=(r_res ne .)) noprint nway;by &byv.; weight geoW; var r_tot r_res; output out=ttr_sum sum=; run;

data ttr_sum; set ttr_sum;
r2 = 1-r_res/r_tot;
run;

** Summarize;

proc sql; create table finalparm_stage2 as
select distinct b.parm as var, a.&byv., a.estimate as PARMS, a.stderr as STDERR, a.tValue as T, a.probt as PVALUE
from finalparm_stage2_v&version. a
join parmmap2 b
on a.parameter=b.parameter
order by &byv.;
quit;

proc transpose data=finalparm_stage2 out=finalparm_stage2; var PARMS stderr t pvalue; by &byv.; id var; run;

data finalparm_stage2; retain &byv. _name_ intercept hpg_season_last2 unemp_g rentyield_l1 chgslope0_1 afford_HP; set finalparm_stage2;
drop _label_;
rename _name_=_type_ intercept=stage3_int ;
run;

proc sort data= parm_stage1_v&version.; by &byv. _type_;
proc sort data= finalparm_stage2; by &byv. _type_;

data finalParam_v&version.; merge  parm_stage1_v&version.(in=f2 where=(_type_ in('PARMS','STDERR','T','PVALUE')) 
drop=_DEPVAR_ _RMSE_ _MODEL_ lnrentg _in_ _p_ _EDF_ _RSQ_ _adjRSQ_) finalparm_stage2; by &byv. _type_; run;
data finalParam_v&version.; merge  finalParam_v&version.(in=f1) ttr_sum(in=f2 keep=&byv. r2 rename=(r2=total_r2)); by &byv.; run;
%mend;

%tryv_wConstraint(version=1, byV=psfr_group, maxgroup=6);

data rentParm.finalParam; set finalParam_v1; run;

data allparm; set  rentParm.finalParam(where=(_type_='PARMS'));
if intercept=. then intercept=0;
if stage3_int=. then stage3_int=0;
intercept=intercept+stage3_int;
if rentg_l1=. then rentg_l1=0;
if rentg_l2=. then rentg_l2=0;
if rentg_l3=. then rentg_l3=0;
if rentg_l4=. then rentg_l4=0;
if rentg_l5=. then rentg_l5=0;
if afford_rent=.  then afford_rent=0;
if hpg_season_last2=.  then hpg_season_last2=0;
if unemp_g=.  then unemp_g=0;
if chgslope0_1=.  then chgslope0_1=0;
rename rentg_l1=p_rentg_l1 rentg_l2 = p_rentg_l2 rentg_l3=p_rentg_l3 rentg_l4=p_rentg_l4 rentg_l5 = p_rentg_l5
afford_rent = p_afford_rent
hpg_season_last2 = p_hpg_season_last2
unemp_g = p_unemp_g
chgslope0_1 = p_chgSlope0_1;
drop _type_ total_r2 stage3_int;
run;


data ln_seasonality; set allrentIdx; by indexcode  qtr; seasonality=log(Rentidx/lag(Rentidx))-log(Rentidx_sa/lag(Rentidx_sa));
if first.indexcode  then delete; proc sort; by indexcode  DESCENDING qtr;run;
data ln_seasonality; set ln_seasonality; if indexcode ne lag4(indexcode) ; 
qtridx=qtr-int(qtr/100)*100; 
keep indexcode  qtridx seasonality; proc sort nodup; by indexcode   qtridx;run;
proc sql; create table ln_seasonality as select distinct indexcode ,qtridx,seasonality-sum(seasonality)/4 
as seasonality from ln_seasonality group by indexcode;run;
 


proc sql; create table fc as select distinct a.*, baseRent, h.m_rentshare
, pSFR_group
, case when a.indexcode in &amherstmarket. then 1 else 0 end as MSRMarket
from allrentIdx a
join rawRentidx b
on a.indexcode=b.indexcode and b.date=201303
join baseRent_med m 
on a.indexcode=m.indexcode
join geoW w
on a.indexcode=w.cbsa
join histmean h
on a.indexcode=h.indexcode

where a.qtr>=200001
and a.indexcode in (select indexcode from HP); ;quit;


data fc; set fc; by indexcode  indexmonth; retain  Rentidx1 ;  
if first.indexcode  then Rentidx1=1;
else Rentidx1=Rentidx1*(exp(LnRentg)); 
ln_Rentidx=log(Rentidx1);

if abs(LnRentg)>.1 and qtr<202001 then LnRentg=.;
if abs(lnRentg)>.25 then lnrentg=.;


rentg_l1=lag(LnRentg); if first.indexcode then rentg_l1=.;
rentg_l2=lag(rentg_l1); if indexcode ne lag2(indexcode) then rentg_l2=.;
rentg_l3=lag(rentg_l2); if indexcode ne lag3(indexcode) then rentg_l3=.;
rentg_l4=lag(rentg_l3); if indexcode  ne lag4(indexcode) then rentg_l4=.;
rentg_l5=lag(rentg_l4); if indexcode  ne lag5(indexcode) then rentg_l5=.;



run;
%macro add_fredRent(inp,sm_url, /* URL of text data on FRED */ sm_var, /* name of variable */ 
	sm_firstobs /* line of first data (if you are not sure and don't need the oldest data, ~25 is often safe) */);
filename fredRent url "&sm_url";
data fred_new;  infile fredRent  firstobs=&sm_firstobs;   format date yymmdd10.; input          @1 date yymmdd10.          @13 &sm_var; 
month=year(date)*100+month(date);run; 
proc means data=fred_new noprint; class month;var &sm_var; output out=fred_new mean=;run;
filename fredRent; /* close file reference */
data fred_new; set fred_new; if month ne .; drop _TYPE_ _FREQ_; run;
data &inp; merge &inp(in=f1) fred_new(in=f2); by month; if f1 or f2; if month ne .; drop Date; run;
%mend;

%macro getrates2();
/* initialize empty data set */

%if (%sysfunc(fileexist(rate_frm_mo))) %then %do;%end; %else %do;
data rate_frm_mo; format month BEST12.; run;
%add_fredRent(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/MORTGAGE30US.txt, sm_var=refi_rate, sm_firstobs=16);
%add_fredRent(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/GS2.txt, sm_var=cmt_2yr, sm_firstobs=16);
%add_fredRent(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/GS10.txt, sm_var=cmt_10yr, sm_firstobs=16);
%add_fredRent(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/USD3MTD156N.txt, sm_var=libor_3m, sm_firstobs=33);
proc export data=rate_frm_mo outfile="&lt_out.\monthly frm30 & swap2-10 rate since 199001.csv" replace; run;
%end;

data rate_frm; set rate_frm_mo; qtr=int(month/100)*100+int((month-int(month/100)*100-1)/3)+1;run;
proc means data=rate_frm noprint; class qtr; output out=rate_frm mean=;run;

data rate_frm;	set rate_frm; if qtr ne .; drop _TYPE_ _FREQ_;  
refi_l1=lag(refi_rate); refi_l2=lag2(refi_rate); refi_l3=lag3(refi_rate);
refi_l4=lag4(refi_rate); refi_l5=lag5(refi_rate); refi_l6=lag6(refi_rate); refi_l7=lag7(refi_rate);refi_l8=lag8(refi_rate);
slope=max(cmt_10yr-cmt_2yr);  
cmt_10yr_l1=lag(cmt_10yr);
cmt_10yr_l2=lag2(cmt_10yr);
cmt_10yr_l3=lag3(cmt_10yr);
cmt_10yr_l4=lag4(cmt_10yr);

cmt_10yr_g=cmt_10yr-lag(cmt_10yr); cmt_2yr_g=cmt_2yr-lag(cmt_2yr); slope_g=slope-lag(slope);
chgrefi1_4=lag(refi_rate)-lag4(refi_rate); chgrefi2_6=lag2(refi_rate)-lag6(refi_rate); 
slope_l3=lag3(slope);slope_l4=lag4(slope);slope_l2=lag2(slope);slope_l1=lag(slope); 
slope_l5=lag5(slope); slope_l6=lag6(slope); slope_l7=lag7(slope); slope_l8=lag8(slope); 
chgslope0_1=slope-lag(slope);chgslope0_2=slope-lag2(slope);chgslope0_3=slope-lag3(slope);chgslope0_4=slope-lag4(slope);
chgslope1_2=lag(slope)-lag2(slope); chgslope1_3=lag(slope)-lag3(slope); chgslope2_3=lag2(slope)-lag3(slope);
chgslope4_8=lag4(slope)-lag8(slope); chgslope2_4=lag2(slope)-lag4(slope); chgslope3_6=lag3(slope)-lag6(slope);
chgrefi0_1=refi_rate-lag(refi_rate); chgrefi0_2=refi_rate-lag2(refi_rate);chgrefi0_3=refi_rate-lag3(refi_rate);
chgrefi1_2=lag(refi_rate)-lag2(refi_rate); chgrefi1_3=lag(refi_rate)-lag3(refi_rate);  chgrefi2_4=lag2(refi_rate)-lag4(refi_rate);  
proc sort nodup;	by qtr;	run;
%mend;
%getrates2;

data rate2; set ir.saved_path_values_dt(rename=(PMMS30=refi_rate libor_3mo=libor_3m month=mo)); 
where curve_type in ('FWD','OAS') and 0<=path_num<=1000; ** Start from Oct 2020, we only generate 1000 rate path;
month=year(intnx('month',rate_timestamp,mo))*100+month(intnx('month',rate_timestamp,mo));
if month=. then do;
timestamp=input(put(substr(rate_timestamp,1,4)*10000+substr(rate_timestamp,6,2)*100+1,8.),YYMMDD10.);
month=year(intnx('month',timestamp,mo))*100+month(intnx('month',timestamp,mo));
end;
refi_rate=refi_rate;
 keep path_num  month refi_rate cmt_2yr cmt_10yr libor_3m; proc sort nodup; by month; run;

 

 

data rate_frm_mo2; set rate_frm_mo;  do path_num=0 to 1000; output; end; run;
data rate_frm2_0; set rate_frm_mo2 rate2(in=f2); if f2 then priority=0; else priority=1; run;
proc sort nodup; by path_num month priority;
data rate_frm2_0; set rate_frm2_0; by path_num month priority; if last.priority;run; 

proc means data=rate_frm2_0 noprint; class path_num month; output out=rate_frm2_0 mean=;run;
data  rate_frm2_0; set rate_frm2_0; if month ne . and path_num ne .; qtr=int(month/100)*100+int((month-int(month/100)*100-1)/3)+1;run;
proc means data=rate_frm2_0 noprint; class path_num qtr; output out=rate_frm2_0 mean=;run;

data rate_frm2_0;	set rate_frm2_0(in=f1 rename=(refi_rate=refi_rate0 cmt_2yr=cmt_2yr0
cmt_10yr=cmt_10yr0)); by path_num qtr; where path_num ne . and qtr>0; 
retain refi_rate cmt_2yr cmt_10yr;
chgrefi0=(refi_rate0-lag(refi_rate0));chgcmt_2yr0=(cmt_2yr0-lag(cmt_2yr0));
chgcmt_10yr0=(cmt_10yr0-lag(cmt_10yr0));
seed=12345678;
chgrefi=chgrefi0*1.89; chgcmt_2yr=chgcmt_2yr0*1.89;
chgcmt_10yr=chgcmt_10yr0*1.89;
if first.path_num then do;
chgrefi0=.; chgcmt_2yr0=.;chgcmt_10yr0=.;chgrefi=.;chgcmt_2yr=.;chgcmt_10yr=.;
end;
if first.path_num or path_num=0 or priority>0 then do; refi_rate=refi_rate0; cmt_2yr=cmt_2yr0;
cmt_10yr=cmt_10yr0; end;
else do;
refi_rate=min(max(0.1,refi_rate+chgrefi),60);cmt_2yr=min(max(0.1,cmt_2yr+chgcmt_2yr),60);cmt_10yr=min(max(.1,cmt_10yr+chgcmt_10yr),60);
end;
keep path_num  qtr priority refi_rate cmt_2yr cmt_10yr libor_3m refi_rate0 cmt_2yr0 cmt_10yr0 chg:;
run;
proc means data=rate_frm2_0 noprint; class qtr; where priority=0 and path_num>0; output out=adj(where=(qtr>0)) mean=;run;

data adj; set adj; where qtr>=0; adjrefi_rate=refi_rate-refi_rate0;  adjcmt_2yr=cmt_2yr-cmt_2yr0;  adjcmt_10yr=cmt_10yr-cmt_10yr0;  
keep adj: qtr; run;

proc sort data=rate_frm2_0; by qtr;run;
data rate_frm2_0; merge rate_frm2_0 adj; by qtr; 
if priority=0 then do;  refi_rateADJ=min(max(0.1,refi_rate-adjrefi_rate),60);  cmt_2yrADJ=min(max(0.1,cmt_2yr-adjcmt_2yr),60);  
cmt_10yrADJ=min(max(0.1,cmt_10yr-adjcmt_10yr),60); end;
else do; refi_rateADJ=refi_rate0; cmt_2yrADJ=cmt_2yr0; cmt_10yrADJ=cmt_10yr0;  end;
proc sort data=rate_frm2_0; by path_num qtr;run;

proc means data=rate_frm2_0 noprint; class qtr;var cmt_2yrADJ cmt_10yrADJ; output out=test mean=; run;

data rate_frm2_0; set rate_frm2_0; by path_num qtr; 
chgrefiADJ=(refi_rateADJ-lag(refi_rateADJ)); 
chgcmt_2yrADJ=(log(cmt_2yrADJ)-lag(log(cmt_2yrADJ)));
chgcmt_10yrADJ=(log(cmt_10yrADJ)-lag(log(cmt_10yrADJ)));
if first.path_num then do; chgrefiADJ=.; chgcmt_2yrADJ=.; chgcmt_10yrADJ=.; end;run;  

data rate_frm2_0; set rate_frm2_0(keep=path_num  qtr  refi_rateADJ cmt_2yrADJ cmt_10yrADJ libor_3m rename=(refi_rateADJ=refi_rate cmt_2yrADJ=cmt_2yr cmt_10yrADJ=cmt_10yr));
proc sort nodup; by path_num qtr; run;

data rate_frm2;	set rate_frm2_0; by path_num qtr; if qtr ne . and path_num ne .; drop _TYPE_ _FREQ_;  
refi_l1=lag(refi_rate); refi_l2=lag2(refi_rate); refi_l3=lag3(refi_rate);
refi_l4=lag4(refi_rate); refi_l5=lag5(refi_rate); refi_l6=lag6(refi_rate); refi_l7=lag7(refi_rate);refi_l8=lag8(refi_rate);
slope=max(cmt_10yr-cmt_2yr);  
cmt_10yr_g=cmt_10yr-lag(cmt_10yr); slope_g=slope-lag(slope);
chgrefi1_4=lag(refi_rate)-lag4(refi_rate); chgrefi2_6=lag2(refi_rate)-lag6(refi_rate); 
slope_l3=lag3(slope);slope_l4=lag4(slope);slope_l2=lag2(slope);slope_l1=lag(slope); 
slope_l5=lag5(slope); slope_l6=lag6(slope); slope_l7=lag7(slope); slope_l8=lag8(slope); 
chgslope0_1=slope-lag(slope);chgslope0_2=slope-lag2(slope);chgslope0_3=slope-lag3(slope);chgslope0_4=slope-lag4(slope);
chgslope1_2=lag(slope)-lag2(slope); chgslope1_3=lag(slope)-lag3(slope); chgslope2_3=lag2(slope)-lag3(slope);
chgslope4_8=lag4(slope)-lag8(slope); chgslope2_4=lag2(slope)-lag4(slope); chgslope3_6=lag3(slope)-lag6(slope);
chgrefi0_1=refi_rate-lag(refi_rate); chgrefi0_2=refi_rate-lag2(refi_rate);chgrefi0_3=refi_rate-lag3(refi_rate);
chgrefi1_2=lag(refi_rate)-lag2(refi_rate); chgrefi1_3=lag(refi_rate)-lag3(refi_rate);  
chgrefi2_4=lag2(refi_rate)-lag4(refi_rate);  
if path_num ne lag8(path_num) then delete; run;
proc sort nodup; by path_num qtr; run;


%macro getRandomErrAll();
proc import datafile="&lt_out.\errmat_All11234.csv"      out=errmat11234     dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc import datafile="&lt_out.\errmat_All21234.csv"      out=errmat21234      dbms=csv      replace; datarow=2;  getnames=yes;   run;

proc import datafile="&lt_out.\errmat_All12345.csv"      out=errmat12345     dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc import datafile="&lt_out.\errmat_All22345.csv"      out=errmat22345      dbms=csv      replace; datarow=2;  getnames=yes;   run;

proc import datafile="&lt_out.\errmat_All13456.csv"      out=errmat13456     dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc import datafile="&lt_out.\errmat_All23456.csv"      out=errmat23456      dbms=csv      replace; datarow=2;  getnames=yes;   run;

proc import datafile="&lt_out.\errmat_All14567.csv"      out=errmat14567     dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc import datafile="&lt_out.\errmat_All24567.csv"      out=errmat24567      dbms=csv      replace; datarow=2;  getnames=yes;   run;

proc import datafile="&lt_out.\errmat_All15678.csv"      out=errmat15678     dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc import datafile="&lt_out.\errmat_All25678.csv"      out=errmat25678      dbms=csv      replace; datarow=2;  getnames=yes;   run;

data errmat1; set errmat11234 errmat12345 errmat13456 errmat14567 errmat15678;run;
data errmat2; set errmat21234 errmat22345 errmat23456 errmat24567 errmat25678;run;

proc import datafile="&lt_out.\resid_corr.csv" out=cbsaOrder      dbms=csv      replace; datarow=2;  getnames=yes;   run;
data cbsaorder; set cbsaorder; where _NAME_ not in ('MEAN','STD'); run;
data cbsaorder; set cbsaorder; retain position; position+1;  keep _NAME_ position; run;
proc sql; select count(1) into: ncol from cbsaorder; quit;
%let ncol=%eval(&ncol*1); %let ncolorg=%eval((&ncol-1000)*1);
data errmat2b; set errmat2; array Vold(*) V1-V&ncolorg; array Vnew(*) V1001-V&ncol; 
do i=1 to dim(Vold); Vnew[i]=Vold[i]; end; drop V1-V&ncolorg i;run;
run;

data errmat_All; merge errmat1 errmat2b; run;

%macro geterr(inp=);
data errmat_&inp.1; set errmat_&inp.; retain simid qtridx; if simid=. then simid=1; if qtridx=. then qtridx=0; qtridx+1; if qtridx=123 then do; qtridx=1; simid+1;  end;run;
data errmat_&inp.1; set errmat_&inp.1; array arr(*) v:;
do position=1 to dim(arr); resid_&inp.=arr(position);  output; end; keep resid_&inp. position simid qtridx;run;
proc sort nodup; by position;
data errmat_&inp.1; merge errmat_&inp.1 cbsaorder; by position; drop position;run;
%mend;

%geterr(inp=All);

data errmat_US_Unemp1(keep=resid_us_unemp simid qtridx ) errmat_sradj1(keep=resid_sradj cbsa_code simid qtridx )
errmat_unemp1(keep=resid_unemp cbsa_code simid qtridx ) errmat_house1(keep=resid_house cbsa_code simid qtridx )
errmat_avginc1(keep=resid_avginc cbsa_code simid qtridx ) errmat_medinc1(keep=resid_medinc cbsa_code simid qtridx )
errmat_rhpits1(keep=resid_rhpits cbsa_code simid qtridx ) 
errmat_SFRent1(keep=cbsa_code simid qtridx resid_rent )
errmat_Vacancy1(keep=cbsa_code simid qtridx resid_vacancy )
errmat_Caprate1(keep=cbsa_code simid qtridx resid_caprate)
errmat_sp1(keep= simid qtridx resid_sp)
; set errmat_All1; if _NAME_ = 'r_us_unemp' then do;
resid_us_unemp=resid_all; output errmat_US_Unemp1; end; 
else if _NAME_ = 'r_sp' then do;
resid_sp=resid_all; output errmat_sp1; end;
else do;
cbsa_Code=substr(_NAME_,length(_NAME_)-4,length(_NAME_))*1; 

if substr(_NAME_,1,length(_NAME_)-5)='msa_avginc' then do; resid_avginc=resid_all; output errmat_avginc1;  end;
else if substr(_NAME_,1,length(_NAME_)-5)='msa_inc' then do; resid_medinc=resid_all; output errmat_medinc1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msa_rsradj' then do; resid_sradj=resid_all; output errmat_sradj1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msa_unemp' then do; resid_unemp=resid_all; output errmat_unemp1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msa_houseg' then do; resid_house=resid_all; output errmat_house1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msa_rhpits' then do; resid_rhpits=resid_all; output errmat_rhpits1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msaR_' then do; resid_rent=resid_all; output errmat_SFRent1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msaV_' then do; resid_vacancy=resid_all; output errmat_Vacancy1; end;
else if substr(_NAME_,1,length(_NAME_)-5)='msaC_' then do; resid_caprate=resid_all; output errmat_Caprate1; end;
end;drop _NAME_ resid_all; run;


proc sort data=errmat_sradj1; by cbsa_code simid qtridx; proc sort data=errmat_unemp1; by cbsa_code simid qtridx;
proc sort data=errmat_house1; by cbsa_code simid qtridx; proc sort data=errmat_avginc1; by cbsa_code simid qtridx;
proc sort data=errmat_medinc1; by cbsa_code simid qtridx;  proc sort data=errmat_rhpits1; by cbsa_code simid qtridx;  run;
data errMat; retain cbsa_code simid qtridx; merge errmat_sradj1 errmat_unemp1 errmat_house1 errmat_avginc1 errmat_medinc1 errmat_rhpits1; by cbsa_code simid qtridx; run;
proc means;run;

proc sort data=errMat; by cbsa_code qtridx; run;

proc means data=errMat noprint; by cbsa_Code qtridx; var resid_sradj resid_unemp
resid_house resid_avginc resid_medinc resid_rhpits; output out=mean_byCBSA mean=
resid_sradj_ADJ resid_unemp_ADJ
resid_house_ADJ resid_avginc_ADJ resid_medinc_ADJ resid_rhpits_ADJ;run;

data errMat; merge errMat mean_byCBSA(keep=cbsa_Code qtridx resid:); by cbsa_Code qtridx;
resid_sradj=resid_sradj-resid_sradj_ADJ; resid_unemp=resid_unemp-resid_unemp_ADJ;
resid_house=resid_house-resid_house_ADJ; resid_avginc=resid_avginc-resid_avginc_ADJ;
resid_medinc=resid_medinc-resid_medinc_ADJ; resid_rhpits=resid_rhpits-resid_rhpits_ADJ;
drop resid_sradj_ADJ resid_unemp_ADJ
resid_house_ADJ resid_avginc_ADJ resid_medinc_ADJ resid_rhpits_ADJ;
run;

proc sort data=errMat; by cbsa_code simid qtridx; run;

proc means data=errMat noprint; by cbsa_Code simid; var resid_sradj resid_unemp
resid_house resid_avginc resid_medinc resid_rhpits; output out=mean_byCBSA mean=
resid_sradj_ADJ resid_unemp_ADJ
resid_house_ADJ resid_avginc_ADJ resid_medinc_ADJ resid_rhpits_ADJ;run;

data errMat; merge errMat mean_byCBSA(keep=cbsa_Code simid resid:); by cbsa_Code simid;
resid_sradj=resid_sradj-resid_sradj_ADJ; resid_unemp=resid_unemp-resid_unemp_ADJ;
resid_house=resid_house-resid_house_ADJ; resid_avginc=resid_avginc-resid_avginc_ADJ;
resid_medinc=resid_medinc-resid_medinc_ADJ; resid_rhpits=resid_rhpits-resid_rhpits_ADJ;
drop resid_sradj_ADJ resid_unemp_ADJ
resid_house_ADJ resid_avginc_ADJ resid_medinc_ADJ resid_rhpits_ADJ;
run;

proc sort data=errmat_US_Unemp1; by qtridx; 
proc means noprint; by qtridx; var resid_us_unemp; output out=mean_us_adj mean=resid_us_unemp_ADJ;
data errmat_US_Unemp1; merge errmat_US_Unemp1 mean_us_adj(keep=qtridx resid_us_unemp_ADJ); by qtridx;
resid_us_unemp=resid_us_unemp-resid_us_unemp_ADJ; drop resid_us_unemp_ADJ; run;

proc sort data=errmat_US_Unemp1; by simid; 
proc means noprint; by simid; var resid_us_unemp; output out=mean_us_adj mean=resid_us_unemp_ADJ;
data errmat_US_Unemp1; merge errmat_US_Unemp1 mean_us_adj(keep=simid resid_us_unemp_ADJ); by simid;
resid_us_unemp=resid_us_unemp-resid_us_unemp_ADJ; drop resid_us_unemp_ADJ; run;

proc means; class qtridx;run;


proc sort data=errmat_Vacancy1; by cbsa_code simid qtridx; 
proc sort data=errmat_Caprate1; by cbsa_code simid qtridx; run;
proc sort data=errmat_SFRent1; by cbsa_code simid qtridx;
proc sort data=errmat_sp1; by  simid qtridx;

data errMat_RentFC; retain indexcode simid qtridx; merge errmat_Vacancy1 errmat_SFRent1 errmat_Caprate1; by cbsa_code simid qtridx; 
indexcode=put(cbsa_code,$5.); drop cbsa_code; run;
proc sort data=errMat_RentFC; by indexcode  qtridx; run;

proc means data=errMat_RentFC noprint; by indexcode qtridx; var resid_vacancy resid_rent resid_caprate; 
output out=mean_byCBSA_RentFC mean=resid_vacancy_ADJ resid_rent_ADJ resid_caprate_ADJ;run;

data errMat_RentFC; merge errMat_RentFC mean_byCBSA_RentFC(keep=indexcode qtridx resid:); by indexcode qtridx;
resid_vacancy=resid_vacancy-resid_vacancy_ADJ;
resid_rent=resid_rent-resid_rent_ADJ;
resid_caprate=resid_caprate-resid_caprate_ADJ;
drop resid_vacancy_ADJ resid_rent_ADJ resid_caprate_ADJ; run;

proc sort data=errMat_RentFC; by indexcode simid qtridx; run;

proc means data=errMat_RentFC noprint; by indexcode simid; var resid_vacancy resid_rent resid_caprate; 
output out=mean_byCBSA_RentFC mean=resid_vacancy_ADJ resid_rent_ADJ resid_caprate_ADJ;run;

data errMat_RentFC; merge errMat_RentFC mean_byCBSA_RentFC(keep=indexcode simid resid:); by indexcode simid;
resid_vacancy=resid_vacancy-resid_vacancy_ADJ;
resid_rent=resid_rent-resid_rent_ADJ;
resid_caprate=resid_caprate-resid_caprate_ADJ;
drop resid_vacancy_ADJ resid_rent_ADJ resid_caprate_ADJ; run;


/*
proc corr data=errmat_All out=test noprint cov; run; 
proc means data=test ; where _TYPE_='CORR';run;
*/


%mend;

data cbsa; set modelinp3; keep indexcode pSFR_group baserent m_rentshare; proc sort nodup; by indexcode ;run;

%let startsim=1; %let endsim=10;

%macro loopSim(startsim=,endsim=);
%put &startsim;
data simHistHPA; set HP; if qtr>=201600; do simid=&startsim to &endsim; output; end; 
keep qtr hpg_season indexcode simid unemp; proc sort nodup; by simid indexcode qtr; run;

data simHistHPA; set simHistHPA; ; by simid indexcode qtr;
hpg_season_l1=lag(hpg_season); 
unemp_g= unemp - lag(unemp);
if  first.indexcode then do; hpg_season_l1=.; unemp_g=.; end;
hpg_season_last2 = hpg_season+hpg_season_l1;
run;

data SimHPI; set  simHPI.FIXEDSIM&startsim-simHPI.FIXEDSIM&endsim;*set  simHPI.AllSim_Slope ; *simhpi.hpishock; by path_num cbsa_Code qtr;
hpg_season=ln_hpi_season-lag(ln_hpi_season);
unemp_g = unemp - lag(unemp);
if first.cbsa_code  then do;  hpg_season=.; unemp_g=.; end;
hpg_season_l1=lag(hpg_season);
if first.cbsa_Code then hpg_season_l1=.;
hpg_season_last2 = hpg_season+hpg_season_l1;
indexcode=put(cbsa_Code,$5.); simid=path_num;drop cbsa_Code  path_num; 
run;

data qtr; set cbsa; do simid=&startsim to &endsim; do year=2018 to int(&fcqtrStart/100)+10;
do qidx=1 to 4; if &fcqtrStart-200<=year*100+qidx<=&fcqtrStart+30000 then do; qtr=year*100+qidx; output; end; end; end;
end; keep indexcode  qtr simid; run;

data errMat2; set errMat_RentFC(where=(&startsim<=simid<=&endsim));by indexcode simid qtridx; retain qtr; if first.simid then qtr=&fcqtrStart; 
else do; 
if mod(qtr,100)=4 then qtr=qtr+100-3; else qtr+1;
end; drop qtridx; run;
data qtr; merge qtr cbsa(keep=indexcode baserent m_rentshare pSFR_group); by indexcode ;run;
proc sort data=qtr nodup; by simid qtr; run;
proc sort data=rate_frm2; by path_num qtr; run;

data qtr; merge qtr(in=f1) rate_frm2(where=(&startsim<=simid<=&endsim) in=f2 rename=path_num=simid keep=path_num qtr
cmt_10yr cmt_10yr_g slope slope_l1 refi_rate cmt_2yr cmt_10yr libor_3m chgslope0_1) ; by simid qtr; if f1 and f2; run;

proc sort data=qtr; by simid indexcode qtr ;
proc sort data=errMat2; by simid indexcode qtr ; 
proc sort data=SimHPI; by simid indexcode qtr ; 

*afford_Rent = log(Inc*m_rentshare/12) - log(baseRent);

data qtr1; merge qtr(in=f1) SimHPI(in=f2 keep=simid  unemp  qtr indexcode unemp unemp_g  Inc_p50  hpg_season_last2
 rename=(hpg_season_last2=hpg_season_last2_sim unemp_g= unemp_g_sim)) simHistHPA(drop=hpg_season hpg_season_l1) errMat2; *;
by simid indexcode qtr ; if f1 and f2;
if hpg_season_last2=. then hpg_season_last2=hpg_season_last2_sim; 
if unemp_g=. then unemp_g=unemp_g_sim;
drop hpg_season_last2_sim unemp_g_sim;
run;
proc sort nodup; by indexcode;run;


proc sql; create table qtr2 as
select distinct a.indexcode, a.pSFR_group, a.simid, a.qtr, a.chgslope0_1, unemp, unemp_g, inc_p50, hpg_season_last2, resid_rent, a.baseRent, a.m_rentshare
, b.intercept, b.p_rentg_l1, b.p_rentg_l2, b.p_rentg_l3, b.p_rentg_l4, b.p_rentg_l5, b.p_afford_rent, b.p_hpg_season_last2, b.p_unemp_g, b.p_chgslope0_1
from qtr1 a
join allparm b
on a.pSFR_group = b.pSFR_group ;
quit;


data fc2; set fc(keep=  indexcode qtr psfr_group  LnRentg   ln_rentidx  m_rentshare baseRent 
rename=(LNrentg=LNrentg0  ln_rentidx=ln_rentidx0));
if indexcode not in (&outlier1.) ;
if qtr>=&fcqtrStart-300;do simid=&startsim to &endsim; output; end;
proc sort nodup; by simid indexcode qtr;run;
proc sort data=qtr2; by simid indexcode qtr;run;

data fc3; retain indexcode simid qtr lnRentg ; merge fc2 qtr2; by simid indexcode qtr; retain 
rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  lnRentg ln_rentidx_l1 ln_rentidx  ;
 
if resid_rent=. then resid_rent=0; 
afford_Rent = log(Inc_p50*m_rentshare/12) - log(baseRent);

if first.indexcode  then do;
rentg_l1 =.;rentg_l2=.; rentg_l3=.; rentg_l4=.; rentg_l5=.; lnRentg=.;
ln_rentidx_l1 =.; ln_rentidx=.;
end;
ln_rentidx_l1=ln_rentidx; 
rentg_l5 = rentg_l4;
rentg_l4=rentg_l3;
rentg_l3=rentg_l2;
rentg_l2=rentg_l1; 
rentg_l1=lnRentg;


if qtr<&fcqtrStart  then do;
lnRentg=lnRentg0; ln_rentidx=ln_rentidx0; 

end; 
else do;


lnrentg=intercept+ (rentg_l1)*p_rentg_l1+(rentg_l2)*p_rentg_l2+(rentg_l3)*p_rentg_l3+(rentg_l4)*p_rentg_l4+rentg_l5*p_rentg_l5
+ afford_rent*p_afford_rent
+ hpg_season_last2 * p_hpg_season_last2
+ unemp_g * p_unemp_g
+ chgSlope0_1 * p_chgSlope0_1+resid_rent;

if lnRentg0 ne . then lnRentg=lnRentg0;
if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0;  

ln_rentidx=ln_rentidx_l1+lnrentg;

if lnRentg0 ne . then lnRentg=lnRentg0;
if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0; 
end;
qtridx=mod(qtr,100);

keep qtr qtridx indexcode simid   lnRentg   unemp
Inc_p50 hpg_season_last2;
run;

proc sort data=fc3; by indexcode  qtridx; 
data fc4; merge fc3(in=f1) ln_seasonality ; by indexcode  qtridx; 
rentg=exp(lnrentg+seasonality)-1; 
if f1; if rentg=. then rentg=lnrentg; 
if qtr>=&fcqtrstart; if rentg ne . ;
run;

proc sort nodup; by simid indexcode qtr;
data fc5; set fc4; by simid indexcode qtr; 
retain rentidx ;
if first.indexcode then do; rentidx=1; end;
else do; rentidx=rentidx*(1+rentg); end;
keep qtr  indexcode simid   rentidx Inc_p50 unemp;
run;
data AllSim; set AllSim fc5; if simid ne .; run;
%mend;


data ALlSim;run;
%loopSim(startsim=0, endsim=100);
%loopSim(startsim=101, endsim=200);
%loopSim(startsim=201, endsim=300);
%loopSim(startsim=301, endsim=400);
%loopSim(startsim=401, endsim=500);
%loopSim(startsim=501, endsim=600);
%loopSim(startsim=601, endsim=700);
%loopSim(startsim=701, endsim=800);
%loopSim(startsim=801, endsim=900);
%loopSim(startsim=901, endsim=1000);
proc sort data=allSim; by indexcode;run;


data housing; set simhpi.housing; indexcode=put(cbsa_code,$5.); keep indexcode housing;run;

LIBNAME irs ODBC DSN='irs' schema=dbo;

proc sort data=allsim; by indexcode;
data allsim; merge allsim(in=f1) housing; by indexcode; if f1;run;

proc means data=allsim noprint; class simid qtr; weight housing; var rentidx; output out=US mean= sumwgt=housing;run;
proc means data=US noprint; class qtr;  var rentidx; output out=US_mean mean= ; run;

data allsim_output; set allsim us(in=f1);
if f1 then indexcode='US'; 
if  indexcode ne '' and qtr>0;
drop housing _TYPE_ _FREQ_;
run;
proc sort data=allsim_output; by indexcode simid descending qtr; 

data allsim_output_Monthly0; set allsim_output; by indexcode simid descending qtr;
date=int(qtr/100)*100+mod(qtr,100)*3;
idx=1;
rentidx_l1=lag(rentidx);
drop sp500_chg capr_ust10y_g_l1 capr_ust10y_g qtr;
if first.simid then do; rentidx_l1=.; end;
run;
data allsim_output_Monthly1; set allsim_output_Monthly0; idx+1; if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;
data allsim_output_Monthly2; set allsim_output_Monthly1; idx+1;  if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;

data allsim_output_Monthly; set allsim_output_Monthly0 allsim_output_Monthly1 allsim_output_Monthly2; by indexcode simid descending date;
if rentidx_l1 ne . then do;
rentidx=(rentidx_l1/rentidx)**((idx-1)/3)*rentidx  ;
end;
if rentidx_l1 ne . or idx=1; 
drop  rentidx_l1  ;
run;
data keepCBSA_agg; set allsim_output_Monthly; where date=202502 and rentidx ne .;keep indexcode; 
proc sort nodup; by indexcode; run;
proc sort data=keepCBSA_agg; by indexcode ;
proc sort data=allsim_output_Monthly; by indexcode  date;
data allsim_output_Monthly; merge allsim_output_Monthly(in=f1)  keepCBSA_agg(in=f2); by indexcode; if f1 and f2;run;

proc means data=allsim_output_Monthly(where=(simid>0)) noprint; by indexcode date; output out=meanPath mean=;run;
data MeanPath; set MeanPath; if indexcode ne '' and date>0; drop _TYPE_ _FREQ_ simid path_num ; run;
/*
%put &enddate.;
proc delete data=testbed.bak_RentHPIMeanPath_&enddate.;
data testbed.bak_RentHPIMeanPath_&enddate.(insertbuff=32000); set irs.RentHPIMeanPath_monthly; *_FIXEDHPA;run;

proc delete data=irs.RentHPIMeanPath_monthly; *_FIXEDHPA;
data irs.RentHPIMeanPath_monthly(insertbuff=30000); set MeanPath ;  if indexcode ne ''  and date>0; RateasofDate=20210305; drop simid _TYPE_ _FREQ_ capr_ust10y_g_l1 capr_ust10y_g_l1  capr_ust10y; run;

proc delete data=testbed.bak_SimRentHpi_monthly_&enddate.;
data  testbed.bak_SimRentHpi_monthly_&enddate.(insertbuff=32000); set irs.SimRentHpi_monthly;run;


proc delete data=irs.SimRentHpi_monthly;
data irs.SimRentHpi_monthly(insertbuff=30000); set allsim_output_Monthly; run;

proc delete data=irs.HistMFcaprateVac;
data irs.HistMFcaprateVac(insertbuff=30000); set pprCBSA; run;
*/

data sf_rentIdx_month_dt0 ;  merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index; if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0; else index=index*(1+rentg); drop rentidx0 rentg0 ; 
keep indexcode date index; run;


proc sql; create table cbsa2US as select distinct case when cbsadiv='' then cbsa else cbsadiv end as indexcode, date, avg(rentidx) as rentidx_US
from thirdp.county_dt join meanpath on 'US'=indexcode where cbsa ne ''
group by indexcode,date order by indexcode,date;
run;

data sf_rentIdx_month_dt; merge sf_rentIdx_month_dt0(rename=index=index0) meanpath(keep=indexcode date rentidx)  cbsa2US; by indexcode date;retain index index_last;
if rentidx=. then rentidx=rentidx_us;
if first.indexcode then index=.;
if index0 ne . then do; index=index0; index_last=index0; end; else index=index_last*rentidx;
keep index date indexcode;
run;

proc sql; create table sf_rentIdx_month_dt
as select distinct *
from sf_rentIdx_month_dt a
where index ne .
group by indexcode
having  min(date)<202112
order by indexcode, date;
quit;

 /*
proc delete data=testbed.bak_sf_rentIdx_month_dt_&enddate.;
data testbed.bak_sf_rentIdx_month_dt_&enddate.(insertbuff=30000); set irs.sf_rentIdx_month_dt;run;

proc delete data=irs.sf_rentIdx_month_dt;
data irs.sf_rentIdx_month_dt (insertbuff=30000); set sf_rentIdx_month_dt; cluster='agg'; 
indexmonth=input(put(date*100+1,8.),YYMMDD10.);FORMAT indexmonth date9.; drop date; run;
*/


%macro insampletest(testqtr=);
DATA insample00; set modelinp2(keep=indexcode qtr lnRentg baseRent chgslope0_1 pSFR_group msrmarket afford_rent hpg_season_last2 unemp_g);
outlier=0;
if indexcode in (&outlier1.) then outlier=1;
rename lnRentg = lnRentg0;
run;

proc sql; create table insample00 as
select distinct a.*
, b.intercept, b.p_rentg_l1, b.p_rentg_l2, b.p_rentg_l3, b.p_rentg_l4, b.p_rentg_l5, b.p_afford_rent, b.p_hpg_season_last2, b.p_unemp_g, b.p_chgslope0_1
from insample00 a
join allparm b
on a.pSFR_group = b.pSFR_group ;
quit;
proc sort; by indexcode qtr; run;


data insampletest_&testqtr.; retain indexcode qtr lnRentg;set insample00; by indexcode qtr; 
retain rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  lnRentg ;

if first.indexcode  then do;
rentg_l1 =.;rentg_l2=.; rentg_l3=.; rentg_l4=.; rentg_l5=.; lnRentg=.;
*ln_rentidx_l1 =.; *ln_rentidx=.;
end;
*ln_rentidx_l1=ln_rentidx; 
rentg_l5 = rentg_l4;
rentg_l4=rentg_l3;
rentg_l3=rentg_l2;
rentg_l2=rentg_l1; 
rentg_l1=lnRentg;


if qtr<&testqtr.  then do;
lnRentg=lnRentg0; *ln_rentidx=ln_rentidx0; 

end; 
else do;


lnrentg=intercept+ (rentg_l1)*p_rentg_l1+(rentg_l2)*p_rentg_l2+(rentg_l3)*p_rentg_l3+(rentg_l4)*p_rentg_l4+rentg_l5*p_rentg_l5
+ afford_rent*p_afford_rent
+ hpg_season_last2 * p_hpg_season_last2
+ unemp_g * p_unemp_g
+ chgSlope0_1 * p_chgSlope0_1;
*if lnRentg0 ne . then lnRentg=lnRentg0;
end;
startqtr=&testqtr.;
drop p_: intercept baserent;
run;

data insampletest_&testqtr.; set insampletest_&testqtr.; by indexcode qtr;
retain Rentidx Rentidx0;
if first.indexcode then do;
Rentidx=1;
Rentidx0=1;
end;
else do;
Rentidx = Rentidx*exp(lnRentg);
Rentidx0 = Rentidx0*exp(lnRentg0);
end;
run;
proc sort nodup; by indexcode descending  qtr ;run;

data insampletest_&testqtr.; set insampletest_&testqtr.; by indexcode descending qtr ;
fc_1qtr = lag(rentidx)/rentidx-1;
act_1qtr =  lag(rentidx0)/rentidx0-1;
if indexcode ne lag(indexcode) then do; fc_1qtr=.; act_1qtr=.; end;
fc_1yr = lag4(rentidx)/rentidx-1;
act_1yr =  lag4(rentidx0)/rentidx0-1;
if indexcode ne lag4(indexcode) then do; fc_1yr=.; act_1yr=.; end;
fc_2yr = lag8(rentidx)/rentidx-1;
act_2yr =  lag8(rentidx0)/rentidx0-1;
if indexcode ne lag8(indexcode) then do; fc_2yr=.; act_2yr=.; end;
fc_3yr = lag12(rentidx)/rentidx-1;
act_3yr = lag12(rentidx0)/rentidx0-1;
if indexcode ne lag12(indexcode) then do; fc_3yr=.; act_3yr=.; end;
run;

proc sort nodup; by indexcode  qtr;run;
%mend;

proc sql noprint; select distinct qtr into: qtrlist separated by " " from modelinp3 where qtr>=201101 ; quit;
%put &qtrlist.;

%macro insampletestloop();
%local i qtr ;
  %do i=1 %to %sysfunc(countw(&qtrlist.));

   %let qtr = %scan(&qtrlist., &i, %str( ));
	%put &qtr.;
	%insampletest(testqtr=&qtr.);
  %end;
%mend;
%insampletestloop();

data insamplesummary; set insampletest_:;
if qtr=startqtr;
act_2yr = (1+act_2yr)**(1/2)-1;
act_3yr = (1+act_3yr)**(1/3)-1;
fc_2yr = (1+fc_2yr)**(1/2)-1;
fc_3yr = (1+fc_3yr)**(1/3)-1;
err_1qtr = act_1qtr-fc_1qtr;
err_1yr = act_1yr - fc_1yr;
err_2yr = act_2yr - fc_2yr;
err_3yr = act_3yr - fc_3yr;
abs_err_1qtr = abs(err_1qtr);
abs_err_1yr =abs(err_1yr);
abs_err_2yr = abs(err_2yr);
abs_err_3yr =abs(err_3yr);
keep indexcode outlier pSFR_group msrmarket qtr fc_: act_: err_: abs_err_:;
run;
		
proc means data=insamplesummary noprint nway; where outlier=0; var fc_: act_: err_: abs_err_:; class pSFR_group; output out=nonOutlier_insample mean=; run;
proc means data=insamplesummary noprint nway; where outlier=1; var fc_: act_: err_: abs_err_:; class MSRMarket ; output out=Outlier_insample mean=; run;

proc means data=insamplesummary noprint nway; where  qtr>=202003; var fc_1qtr fc_1yr act_1qtr act_1yr err_1qtr err_1yr abs_err_1qtr abs_err_1yr;
class indexcode; output out=insample_recent mean=; run;

data insample_recent; set insample_recent; 
market = symget(compress("cbsaName"||trim(indexcode)));
outlier=0; msrMarket=0;
if indexcode in (&outlier1.) then outlier=1;
if indexcode in (&amherstmarket.) then msrMarket=1;
run;

data insample_all;retain outlier msrMarket pSFR_group _freq_ fc_1qtr act_1qtr err_1qtr abs_err_1qtr fc_1yr act_1yr err_1yr abs_err_1yr
fc_2yr act_2yr err_2yr abs_err_2yr fc_3yr act_3yr err_3yr abs_err_3yr;

set nonOutlier_insample(in=f1) Outlier_insample(in=f2);
if f1 then outlier=0;
if f2 then outlier=1;
run;

proc export data=inSample_recent outfile="E:\Output\Rent Forecast\Insample Recent Month.csv" dbms=csv replace; run;

proc export data=inSample_all outfile="E:\Output\Rent Forecast\Insample all by market group.csv" dbms=csv replace; run;



%insampletest(testqtr=201101);%insampletest(testqtr=201102);%insampletest(testqtr=201103);%insampletest(testqtr=201104);
