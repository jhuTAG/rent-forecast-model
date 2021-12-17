option compress=yes error=10;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;

LIBNAME Parm "\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1\parameters";
%let amherstmarket = ('12060', '16740', '28140', '45300', '38060', '36740', '34980', '27260', '23104', '31140', '19124', '32820', '13820', '36420', '26900', '39580', '41180', '41700', '29460', '15980', '35840', '49180'
, '26420', '37340', '46060', '24660', '19660', '18140', '29820', '19740', '48424', '22744', '20500', '38940', '45104', '33124', '15500','41620','42644');

/* geoW */

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

data sfr; set rentunits(where=(geographytype in ('CBSA','Metro Division'))); by indexcode year; 
SFR_g = sfr/lag(SFR);
if first.indexcode then do; sfr_g=.; end;
/*
sfr_g_l1 = lag(sfr_g);
if indexcode ne lag2(indexcode) then sfr_g_l1=.;

sfr_g_l2 = lag(sfr_g_l1);
if indexcode ne lag3(indexcode) then sfr_g_l2=.;
*/
run;

** Mom Pop share 2016 - 2021H1;
proc sql; connect to odbc(DSN='thirdpartydata');
create table mompop_share as select distinct * from connection to odbc(
  select isnull(t.cbsa_div, t.cbsa) as indexcode, count(1) as nLease, 1-sum(case when owner='Individual' then 1 else 0 end )*1.0/count(1) as pInst
  from ModelTestBed.dbo.SFR_Rent_CleanUp_new_final a
  join amhersthpi..hpi_taxroll_vw t
  on a.asg_propid= t.asg_propid and t.prop_type='SF'
  where isnull(t.cbsa_div, t.cbsa) is not null and year(lease_enddate)>=2016
  and lease_enddate<'2021-07-01'
  group by isnull(t.cbsa_div, t.cbsa)
  having count(1)>250
  order by isnull(t.cbsa_div, t.cbsa) 

); disconnect from odbc; quit;

proc sql  noprint; create table Listing2013 as 
select distinct cbsa,count( asg_propid) as closedListing,median(closingRent) as medPrice,avg(closingRent) as avgPrice 
from repeatedSale 
where year(lease_enddate)=2013 and cbsa ne ''
group by cbsa 
order by cbsa;
quit;


proc sql  noprint; create table Listing2016 as 
select distinct cbsa,count( asg_propid) as closedListing,median(closingRent) as medPrice,avg(closingRent) as avgPrice 
from repeatedSale 
where year(lease_enddate)=2016 and cbsa ne ''
group by cbsa 
order by cbsa;
quit;


data geoW; merge listing2013(in=f1) rentunits(in=f2 rename=indexcode=cbsa where=(year=2013)) momPop_share(rename=(indexcode=cbsa)) ;  by cbsa;  if f1 and f2 ; 
where cbsa ne ''; 
geoW=SFR*avgPrice;
geoW_allRentUnit = RenterOcc*medRent_ACS;
p_sfr_allrent = sfr/renterocc;
*p_SFR = sfr/sf;
keep cbsa  geoW SFR RenterOcc SFD MedRent_ACS SF p_SFR geoW_allRentUnit p_rentocc p_sfr_allrent nlease pInst avgPrice;
run;


data geoW_2016; merge listing2016(in=f1) rentunits(in=f2 rename=indexcode=cbsa where=(year=2016)) momPop_share(rename=(indexcode=cbsa)) ;  by cbsa;  if f1 and f2 ; 
where cbsa ne ''; 
geoW=SFR*avgPrice;
geoW_allRentUnit = RenterOcc*medRent_ACS;
p_sfr_allrent = sfr/renterocc;
*p_SFR = sfr/sf;
keep cbsa  geoW SFR RenterOcc SFD MedRent_ACS SF p_SFR geoW_allRentUnit p_rentocc p_sfr_allrent nlease pInst avgPrice;
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

proc univariate data=geoW noprint; ; var pInst p_sfr p_rentocc;  output out=P_momPop pctlpre=Inst_  sfr_ rentocc_ pctlpts= 40,50, 70; run;
proc univariate data=geoW noprint;class msrMarket ; var pInst p_sfr p_rentocc;  output out=P_momPop2 pctlpre=Inst_  sfr_ rentocc_ pctlpts= 40,50, 70; run;

proc sql; create table geoW as
select a.*
, case when p_SFR<=p.sfr_40 then 1 when p_sfr<=p.sfr_70 then 2 else 3 end as pSFR_group
, case when a.MSRmarket=1 then 1 when p_SFR<=p2.sfr_40 then 2 when p_SFR<=p2.sfr_70 then 3 else 4 end as pSFR_group2
, case when pInst<=p.Inst_40 then 1 when pInst<=p.Inst_70 then 2 else 3 end as Inst_group
, case when a.MSRmarket=1 then 1 when pInst<=p2.Inst_40 then 2 when pInst<=p2.Inst_70 then 3   else 4 end as Inst_group2
, case when p_rentocc<=p.rentocc_40 then 1 when p_rentOcc<=p.rentocc_70 then 2 else 3 end as rentocc_group
, case when a.MSRmarket=1 then 1 when p_rentocc<=p2.rentocc_40 then 2 when p_rentOcc<=p2.rentocc_70 then 3  else 4 end as rentocc_group2
from geoW a
join p_momPop p
on 1=1
join P_momPop2 p2
on p2.msrMarket=a.msrMarket
order by cbsa;
quit;


proc sql; create table geoW_2016 as
select distinct c.Name as Market
,  case when a.cbsa in &amherstmarket. then 1 else 0 end as MSRMarket
,  a.*
from geoW_2016 a
left join thirdp.cbsa_dt c
on a.cbsa = c.cbsa
where a.cbsa in (select distinct indexcode from irs.sf_rentidx_monthly)
order by cbsa;
quit;

proc univariate data=geoW_2016 noprint; ; var pInst p_sfr p_rentocc;  output out=P_momPop2016 pctlpre=Inst_  sfr_ rentocc_ pctlpts= 40,50, 70; run;

proc sql; create table geoW_2016 as
select a.*
, case when p_SFR<=sfr_40 then 1 when p_sfr<=sfr_70 then 2 else 3 end as pSFR_group
, case when MSRmarket=1 then 1 when p_SFR<=sfr_50 then 2 else 3 end as pSFR_group2
, case when pInst<=Inst_40 then 1 when pInst<=Inst_70 then 2 else 3 end as Inst_group
, case when p_rentocc<=rentocc_40 then 1 when p_rentOcc<=rentocc_70 then 2 else 3 end as rentocc_group
from geoW_2016 a
join p_momPop2016 p
on 1=1
order by cbsa;
quit;

proc univariate data=geoW; var pMomPop; run;
proc export data=geoW outfile="E:\Output\Rent Forecast\weight by cbsa.csv" dbms=csv replace; run;
proc export data=geoW_2016 outfile="E:\Output\Rent Forecast\weight by cbsa 2016.csv" dbms=csv replace; run;

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

data tp; set allrentidx; by indexcode indexmonth;
YOYrentg = log(rentidx/lag4(rentidx));
if indexcode ne lag4(indexcode) then YOYRentg=.;
if mod(qtr,100)=4;
year = int(qtr/100);
run;
 ** employment;

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


data _null_; set thirdp.county_dt;
if cbsadiv='' then cbsadiv=cbsa;
call symput(compress("fipscbsa"||trim(fips)), cbsadiv);
run;
%put &fipscbsa01001;

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
*unemp=1-totemp/laborforce;
ln_emp = log(totemp);
ln_laborforce = log(laborforce);
keep ln_emp ln_laborforce totemp laborforce cbsa_code date qtr; run;


proc x12 data=cbsaemp date=qtr noprint interval=QTR; by cbsa_code; var unemp;    x11;    output out=unemp_sa d11;    ods select d11; run;
data unemp_sa; set unemp_sa(rename=(qtr=date unemp_d11=unemp)); qtr=year(date)*100+qtr(date); keep qtr cbsa_code unemp; run;

/* SFR Unit */
proc sql; create table sfr as 
select a.*
from sfr a
where a.indexcode in (select distinct indexcode from allrentidx)
order by indexcode, year;
quit;

data sfr_last; set sfr(where=(sfr_g ne .)); by indexcode year; if last.indexcode; run;

data year; do i=2010 to 2021; year=i; ; output; end; run;

proc sql; create table sfr_1 as
select a.indexcode, b.year, a1.sfr_g, a1.sfr as sfr0
from year b
join sfr_last a
on 1=1
join sfr_last a1
on a.indexcode=a1.indexcode
and b.year>=a1.year
order by indexcode, year; quit;

data sfr_1; set sfr_1; by indexcode year;
retain sfr;
if first.indexcode then sfr=sfr0;
else sfr = sfr*sfr_g;
run;

data sfrUnit; set sfr(keep=indexcode year sfr) sfr_1(keep=indexcode year sfr); 
ln_SFR = log(sfr);
proc sort nodup; by indexcode year; run;

/* Home Price */
data HP; set irs.hpi_basefile; indexcode=put(cbsa_Code,$5.); drop cbsa_code; if qtr>0; 
run;

* fund HPI;
data fundHPI; set irs.asg_hpi_dt(where=(date>200000) keep=indexcode date fundHPI aggregate monthfmt);
qtr = year(monthfmt)*100+qtr(monthfmt);
ln_fundHPI = log(fundHPI);
ln_hpi = log(aggregate);
if month(monthfmt) in (3,6,9,12);
run;


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
baseRent/e.rentidx_sa*a.rentidx_sa as medRent, em.ln_emp, em.ln_laborforce, sfr.ln_sfr, geoW, geoW_allRentUnit
, pSFR_group, inst_group, rentocc_group, pSFR_group2, inst_group2
, case when a.indexcode in &amherstmarket. then 1 else 0 end as MSRMarket
, f.ln_fundhpi, f.ln_hpi
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
left join sfrUnit sfr
on sfr.indexcode=a.indexcode
 and year(a.indexmonth) = sfr.year
join geoW w
on a.indexcode=w.cbsa

left join fundHPI f
on a.indexcode= f.indexcode
 and a.qtr=f.qtr
where a.qtr>=200001
and a.indexcode in (select indexcode from HP); ;quit;
proc sort nodup; by indexcode indexmonth; run;

proc sql; create table dup as
select * 
from modelinp0b
group by indexcode, indexmonth
having count(1)>1;
quit;



/* Fund Line */;

libname myData '\\czhaodev\D\SAS Output\SFR HPI\Parameter SASdata'; 

data checkTax;set myData.taxhist_stat_2010_2019; by GeographyType GeoCode Year; where GeographyType='CBSA';
taxRate=TAX_AMOUNT_Mean/total_value_mean; 
if taxRate<0.005 or taxRate>0.035 then taxRate=TAX_AMOUNT_p50/total_value_p50; 
if taxRate>0.06 or taxRate<0.005 then taxRate=.;
if GeoCode^=. && Year^=.; 
if geoCode>0;
call symput(compress("cbsaTax"||trim(geocode)||trim(year)), taxRate);

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
proc univariate data=modelinp1(where=(qtr>=201001 and qtr<=202001)); var rent2own; histogram; run;
proc univariate data=modelinp1(where=(qtr>=201001 and qtr<=202001)); var ln_rent2own; histogram; run;

* Historical average for fund line;
proc means data=modelinp1(where=(qtr>=201001 and qtr<=202001)) noprint nway; class indexcode;
var mort_share rent_share; output out=histmean mean = m_mortshare m_rentShare; run;

proc sql; create table modelinp2 as
select distinct a.*, h.m_mortshare, h.m_rentShare
from modelinp1 a
join histmean h
on a.indexcode= h.indexcode
order by indexcode, indexmonth; quit;

data modelinp2; set modelinp2; 

afford_HP = log(Inc*m_mortshare/12/factor)-log(baseHP);
afford_Rent = log(Inc*m_rentshare/12) - log(baseRent);
 run;
 proc  means ; class inst_group; var afford_rent; run;


 data order; set parm_fund( keep =_type_);
 rn+1;
 run;

 * find outlier market: stage 2&3 by msrMarket;
 proc sort data=modelinp2; by msrmarket;
proc reg data=modelinp2(where=(qtr>201100 and qtr<202103)) outest=parm_test_outlier adjrsq noprint tableout ;  by msrMarket; *by MSRmarket;  weight geoW; 
model lnRentg =  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.05;
output out=insample_outlier r=r_stage2 p=p_rentg; run; quit;

proc reg data=insample_outlier(where=(qtr>201100 and qtr<202103)) outest=parm_test_outlier1 adjrsq noprint tableout ;  by msrMarket; *by MSRmarket;  weight geoW; 
model r_stage2 =  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1  /selection=stepwise sle=0.05;
output out=insample_outlier2 r=r_Rentg p=p_rentg2; run; quit;


proc sort data=parm_test_outlier; by msrmarket _model_ _type_;
proc sort data=parm_test_outlier1; by msrmarket _model_ _type_;
data parm_test_outlier2; merge parm_test_outlier(rename=(intercept=int1)) parm_test_outlier1(rename=(intercept=int2)); by msrmarket _model_ _type_;
if int1=. then int1=0;
if int2=. then int2=0;
intercept=int1+int2;
run;


proc score data=insample_outlier2  score=parm_test_outlier2 out=fc_outlier type=parms; by msrmarket;
var   rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 ;
run;

proc sort data=fc_outlier ; by indexcode qtr; run;
PROC REG data=fc_outlier (where=(qtr>201100 and qtr<202109)) outest=reg_act_outlier adjrsq tableout noprint;  by indexcode;   
model lnRentg = model1
;  run; quit; 


data outlier; set reg_act_outlier;
*if indexcode not in &amherstmarket.;
if _type_='PARMS';
drop _type_;
proc sort; by descending model1; run;
proc univariate; var model1; run;

proc sql; create table outlier as
select c.Name, a.*
from outlier a
join thirdp.cbsa_dt c
on a.indexcode=c.cbsa
where a.model1<0.5 or a.model1>1.5;
quit;

proc sql noprint; select distinct "'"||indexcode||"'" into: removelist separated by ',' from outlier; quit;

data modelinp3; set modelinp2; where indexcode not in (&removelist.); run;


 %macro isBlank(param);
%sysevalf(%superq(param)=,boolean)
%mend ;


%let version=1; %let geoW=geoW; %let byMarket=by msrmarket ; %let fundV = month; %let byV =msrmarket ; %let stage3V = inc_g;
%put &bymarket.;
data t; t=%isBlank(&bymarket.); run;

%macro tryModel(version=, geoW=, byMarket=, fundV =, byV=, stage3V=, where=);
proc sort data=modelinp3; by  &byV. indexcode qtr; run;
proc reg data=modelinp3(where=(qtr>201100 and qtr<202103 &where.)) outest=parm_fund_v&version. adjrsq noprint tableout ;  &bymarket.; *by MSRmarket;  weight &geoW.;  *where qtr>201100 and qtr<202001;
model ln_Rentidx = &fundV.   /selection=stepwise sle=0.001 ;* ln_SFR ln_laborforce  ln_laborForce ln_emp;
output out=is_fund_v&version. r=r_fund p=rentidx_fund; run; quit;

** 2nd Stage: AR(5);
proc sort data=is_fund_v&version.; by &byV. indexcode qtr; run;
data is_fund_v&version.; set is_fund_v&version.(where=(rentidx_fund ne .)) ; *intercept=1; by &byV. indexcode qtr;
l1_fundidx=lag(rentidx_fund);
if first.indexcode then l1_fundidx=.;
rhpits = ln_rentidx_l1-l1_fundidx;
rhpits_l1 = lag(rhpits);
if indexcode ne lag(indexcode) then rhpits_l1=.;
run;

proc reg data=is_fund_v&version.(where=(qtr>201100 and qtr<202103)) outest=parm_stage2_v&version. adjrsq tableout noprint;  &bymarket.;*weight housing; 
weight &geoW.;*where qtr>=201101 and  qtr<202103;
model lnRentg = rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.05 /*sle=0.001 sle=0.05*/;
output out=is_stage2_v&version. r=r_stage2 ; run; quit;

** 3rd stage: HPA and other Var;
proc reg data=is_stage2_v&version.(where=(qtr>201100 and qtr<202103)) outest=parm_stage3_v&version. adjrsq tableout noprint;  &bymarket.; *by indexcode;   
weight &geoW.;
model r_stage2=hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 &stage3V. /selection=stepwise sle=0.05;
; output out=resid_SFrent_v&version. r=r_SFRent p=P_rentg; run; quit; 

*** Combined R-Square;

proc means data=resid_SFrent_v&version. noprint nway; &byMarket.; var lnRentg; weight &geoW.; output out=tp_mean mean=avg_rentidx; run;

%if %isBlank(&bymarket.)=1 %then %do;
proc sql; create table test as
select distinct a.*, b.avg_rentidx 
from resid_SFrent_v&version. a
join tp_mean b
on 1=1;
quit;

data test; set test;
r_tot = (lnRentg - avg_rentidx)**2;
r_res = r_SFRent**2;
run;


%end;
%else %do;
data test; merge resid_SFrent_v&version.(in=f1) tp_mean(keep=&byV. avg_rentidx);&bymarket.; if f1;
r_tot = (lnRentg - avg_rentidx)**2;
r_res = r_SFRent**2;
run;
%end;

proc means data=test(where=(r_res ne .)) noprint nway; &bymarket; weight &geoW.; var r_tot r_res; output out=tp_sum sum=; run;

data tp_sum; set tp_sum;
r2 = 1-r_res/r_tot;
run;

proc sort data=parm_fund_v&version.; by &byV. _model_ _type_;
proc sort data=parm_stage2_v&version.; by &byV. _model_ _type_;
proc sort data=parm_stage3_v&version.; by &byV. _model_ _type_;run;

data parm_final_v&version.; merge 
parm_fund_v&version.(in=f0 rename=(intercept=fund_int _rsq_ = fund_r2 _adjRsq_ = fund_adjr2) drop=_depvar_ _rmse_ ln_rentidx _in_ _p_ _edf_ )
parm_stage2_v&version.(in=f1 rename=(intercept=stage2_int _rsq_ = stage2_r2 _adjRsq_ = stage2_adjr2) 
keep = _model_ _type_  rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 intercept &byV. _rsq_ _adjRsq_) 
parm_stage3_v&version.(in=f2 rename=(intercept=stage3_int _rsq_ = stage3_r2 _adjRsq_ = stage3_adjr2) 
keep = _model_ _type_  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 &stage3V. intercept &byV. _rsq_ _adjRsq_);
by &byV. _MODEL_ _type_;
tp_1= rentg_l1; tp_2 = rentg_l2; tp_3=rentg_l3; tp_4=rentg_l4; tp_5=rentg_l5;
if tp_1=. then tp_1=0;
if tp_2=. then tp_2=0;
if tp_3=. then tp_3=0;
if tp_4=. then tp_4=0;
if tp_5=. then tp_5=0;
if _type_='PARMS' then sum_ar = tp_1 + tp_2 + tp_3 + tp_4 + tp_5;
drop tp_1 tp_2 tp_3 tp_4 tp_5;
run;

proc sql; create table parm_final_v&version. as
select a.*, b.rn
from parm_final_v&version. a
left join order b
on a._type_ =b._type_
;
quit;

proc sort ; by &byV. _MODEL_ rn; run;
data parm_final_v&version.;set parm_final_v&version.; drop rn; run;

%if %isBlank(&bymarket.)=1 %then %do;
proc sql; create table parm_final_v&version. as
select distinct a.*, b.r2 as totalR2
from parm_final_v&version. a
join tp_sum b
on 1=1;
quit;

data  parm_final_v&version.; set  parm_final_v&version.;
length version $10.;
version=put("&version.", $10.);
run;


%end;
%else %do;
data parm_final_v&version.; merge parm_final_v&version.(in=f1) tp_sum(keep=&byV. r2 rename=(r2=totalR2)); &byMarket.; if f1;
length version $10.;
version=put("&version.", $10.);

run;
%end;

data finalParm; set parm_final_v&version.(where=(_type_='PARMS'));
intercept= stage2_int + stage3_int;
 drop fund_int &fundV. fund_r2 fund_adjr2 stage2_r2 stage2_adjr2 stage3_r2 stage3_adjr2 stage2_int stage3_int TOTALr2;
 run;

 
proc score data=resid_SFrent_v&version.  score=finalParm out=fc_v&version. type=parms; &bymarket.;
var rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 &stage3V.;
run;

proc sort data=fc_v&version. ; by indexcode qtr; run;
PROC REG data=fc_v&version. (where=(qtr>201100 and qtr<202109)) outest=reg_act_v&version. adjrsq tableout noprint;  by indexcode;   
model lnRentg = model1
;  run; quit; 

DATA reg_act_v&version.; set reg_act_v&version.(keep=indexcode intercept model1 _edf_ _rsq_ _adjRsq_ _type_);
length version $10.;
version=put("&version.", $10.);
run;



%mend;


 /* by MSR Market or not; try use different weight;
 Fundamental line for rent index: ln(rent_index)
on t directly: simple time trend.
on ln(median income) directly
Combine 1 & 2 to see if you only need one
point 3 above, plus ln(Medinc * %historical rent share/median rent in 2013)
Is rent too much relative to income?
5.  point 4 above, plus rent / homeowner payment  (might try log of this ratio).
Is renting expensive relative to owning?
6. You can also throw in ln(household) & income inequality to try...
7. Try to fit the lines for three sets of markets: those with high share of renters, low share of renters and middle ones.
 */
*1. by time by MSRmarket;
%tryModel(version=1, geoW=geoW, byMarket=by Msrmarket, fundV = month, byV= msrmarket, stage3V= inc_g);
*1a. by time all market together;
%tryModel(version=1a, geoW=geoW, byMarket=, fundV = month, byV= , stage3V= inc_g);
*1b. Institution Group;
%tryModel(version=1b, geoW=geoW, byMarket=by inst_group, fundV = month, byV= inst_group, stage3V= inc_g);
*1c. %SFR out of total occ sf unit Group;
%tryModel(version=1c, geoW=geoW, byMarket=by pSFR_group, fundV = month, byV= pSFR_group, stage3V= inc_g);
*1c. %Renter Occ (all Prop type) Group;
%tryModel(version=1d, geoW=geoW, byMarket=by pSFR_group2, fundV = month, byV= pSFR_group2, stage3V= inc_g);
*1c. %Renter Occ (all Prop type) Group;
%tryModel(version=1e, geoW=geoW, byMarket=by inst_group2, fundV = month, byV= inst_group2, stage3V= inc_g);

data summary_1;retain msrmarket inst_group pSFR_group; set parm_final_v1:;drop _model_; run;
proc export data=summary_1 outfile="E:\Output\Rent Forecast\parm idea 1.csv" dbms=csv replace; run;

*2. by Ln(inc) by MSRMarket;
%tryModel(version=2, geoW=geoW, byMarket=by Msrmarket, fundV = ln_inc, byV= msrmarket, stage3V= );
*2. by Ln(inc) all market together;
%tryModel(version=2a, geoW=geoW, byMarket=, fundV = ln_inc, byV= , stage3V= );
*2b. Institution Group;
%tryModel(version=2b, geoW=geoW, byMarket=by inst_group, fundV = ln_inc, byV= inst_group, stage3V= );
*2c. %SFR out of total occ sf unit Group;
%tryModel(version=2c, geoW=geoW, byMarket=by pSFR_group, fundV = ln_inc, byV= pSFR_group, stage3V= );
%tryModel(version=2d, geoW=geoW, byMarket=by pSFR_group2, fundV = ln_inc, byV= pSFR_group2, stage3V= );
%tryModel(version=2e, geoW=geoW, byMarket=by inst_group2, fundV = ln_inc, byV= inst_group2, stage3V= );

data summary_2;retain msrmarket inst_group pSFR_group; set parm_final_v2:;drop _model_; run;
proc export data=summary_2 outfile="E:\Output\Rent Forecast\parm idea 2.csv" dbms=csv replace; run;

*3. time & inc gini, by MSRMarket;
%tryModel(version=3, geoW=geoW, byMarket=by Msrmarket, fundV = inc_gini month, byV= msrmarket, stage3V= );
*3a. time & inc gini;
%tryModel(version=3a, geoW=geoW, byMarket=, fundV = inc_gini month, byV= , stage3V= );
*3b. Institution Group;
%tryModel(version=3b, geoW=geoW, byMarket=by inst_group, fundV = month inc_gini, byV= inst_group, stage3V= );
*3c. %SFR out of total occ sf unit Group;
%tryModel(version=3c, geoW=geoW, byMarket=by pSFR_group, fundV = month inc_gini, byV= pSFR_group, stage3V= );
%tryModel(version=3d, geoW=geoW, byMarket=by pSFR_group2, fundV = month inc_gini, byV= pSFR_group2, stage3V= );
%tryModel(version=3e, geoW=geoW, byMarket=by inst_group2, fundV = month inc_gini, byV= inst_group2, stage3V= );

data summary_3;retain msrmarket inst_group pSFR_group; set parm_final_v3:;drop _model_; run;
proc export data=summary_3 outfile="E:\Output\Rent Forecast\parm idea 3.csv" dbms=csv replace; run;


*r. time & inc , by MSRMarket;
%tryModel(version=4, geoW=geoW, byMarket=by Msrmarket, fundV = ln_inc month, byV= msrmarket, stage3V= );
*3a. time & inc gini;
%tryModel(version=4a, geoW=geoW, byMarket=, fundV = ln_inc month, byV= , stage3V= );
*3b. Institution Group;
%tryModel(version=4b, geoW=geoW, byMarket=by inst_group, fundV = month ln_inc, byV= inst_group, stage3V= );
*3c. %SFR out of total occ sf unit Group;
%tryModel(version=4c, geoW=geoW, byMarket=by pSFR_group, fundV = month ln_inc, byV= pSFR_group, stage3V= );
%tryModel(version=4d, geoW=geoW, byMarket=by pSFR_group2, fundV = month ln_inc, byV= pSFR_group2, stage3V= );
%tryModel(version=4e, geoW=geoW, byMarket=by inst_group2, fundV = month ln_inc, byV= inst_group2, stage3V= );

data summary_4;retain msrmarket inst_group pSFR_group; set parm_final_v4:;drop _model_; run;
proc export data=summary_4 outfile="E:\Output\Rent Forecast\parm idea 4.csv" dbms=csv replace; run;


*4. time & inc, rent affordability, by MSRMarket;
%tryModel(version=5, geoW=geoW, byMarket=by Msrmarket, fundV = ln_inc month afford_rent, byV= msrmarket, stage3V= );
*4a. time & inc, rent affordability all market;
%tryModel(version=5a, geoW=geoW, byMarket=, fundV = ln_inc month afford_rent, byV= , stage3V= );
*4b. Institution Group;
%tryModel(version=5b, geoW=geoW, byMarket=by Inst_group, fundV = ln_inc month afford_rent , byV= Inst_group, stage3V= );
*4c. %SFR out of total occ sf unit Group;
%tryModel(version=5c, geoW=geoW, byMarket=by pSFR_group, fundV = ln_inc month afford_rent , byV= pSFR_group, stage3V= );
*4b. %Renter occ Group;
%tryModel(version=5d, geoW=geoW, byMarket=by psfr_group2, fundV = ln_inc month afford_rent, byV= psfr_group2, stage3V= );
%tryModel(version=5e, geoW=geoW, byMarket=by Inst_group2, fundV = ln_inc month afford_rent, byV= Inst_group2, stage3V= );


data summary_5;retain msrmarket inst_group pSFR_group; set parm_final_v5:;drop _model_; run;
proc export data=summary_5 outfile="E:\Output\Rent Forecast\parm idea 5.csv" dbms=csv replace; run;

*5.  point 4 above, plus rent / homeowner payment  (might try log of this ratio);
%tryModel(version=6, geoW=geoW, byMarket=by Msrmarket, fundV = ln_inc month afford_rent rent2own, byV= msrmarket, stage3V= );
%tryModel(version=6a, geoW=geoW, byMarket=, fundV = ln_inc month afford_rent rent2own, byV= , stage3V= );
*4b. Institution Group;
%tryModel(version=6b, geoW=geoW, byMarket=by Inst_group, fundV =ln_inc month afford_rent rent2own, byV= Inst_group, stage3V= );
*4c. %SFR out of total occ sf unit Group;
%tryModel(version=6c, geoW=geoW, byMarket=by pSFR_group, fundV = ln_inc month afford_rent rent2own, byV= pSFR_group, stage3V= );
%tryModel(version=6d, geoW=geoW, byMarket=by pSFR_group2, fundV = ln_inc month afford_rent rent2own, byV= pSFR_group2, stage3V= );
%tryModel(version=6e, geoW=geoW, byMarket=by Inst_group2, fundV = ln_inc month afford_rent rent2own, byV= Inst_group2, stage3V= );


*6.   You can also throw in ln(household) & income inequality to try...;
%tryModel(version=7, geoW=geoW, byMarket=by Msrmarket, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh , byV= msrmarket, stage3V= );
%tryModel(version=7a, geoW=geoW, byMarket=, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh, byV= , stage3V= );
*4b. Institution Group;
%tryModel(version=7b, geoW=geoW, byMarket=by Inst_group, fundV =inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh , byV= Inst_group, stage3V= );
*4c. %SFR out of total occ sf unit Group;
%tryModel(version=7c, geoW=geoW, byMarket=by pSFR_group, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh, byV= pSFR_group, stage3V= );
%tryModel(version=7d, geoW=geoW, byMarket=by pSFR_group2, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh, byV= pSFR_group2, stage3V= );
%tryModel(version=7e, geoW=geoW, byMarket=by Inst_group2, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh, byV= Inst_group2, stage3V= );



%tryModel(version=8d, geoW=geoW, byMarket=by pSFR_group2, fundV = inc_gini month afford_rent rent2own ln_inc afford_HP ln_sfdhh, byV= pSFR_group2, stage3V=
, where= and indexcode not in (&removelist.) );

data summary_all;retain idea ver msrMarket pSFR_group pSFR_group2 inst_group inst_group2 _type_ fund_int month ln_inc inc_gini afford_rent rent2Own afford_HP ln_SFDhh
fund_r2 fund_adjr2 stage2_int  rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 sum_ar stage2_r2 stage2_adjr2
stage3_int hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 inc_g stage3_r2 stage3_adjr2 totalR2
; set parm_final_v:;
length idea ver $5.;
idea =substr(version,1,1);
ver = substr(version,2,1);
if ver='' then ver='0a';
drop _model_ version;
if psfr_group2=1 or inst_group2=1 then delete;
if _type_ ne 'PARMS' then totalR2=.;
if pSFR_group2 ne . then pSFR_group2 = psfr_group2-1;
if inst_group2 ne . then inst_group2 = inst_group2-1;
run;

proc sort; by   descending msrMarket pSFR_group pSFR_group2 inst_group inst_group2 ver idea; run;
proc export data=summary_all outfile="E:\Output\Rent Forecast\tried version summary.csv" dbms=csv replace; run;
/*
data outlier_output; set outlier; drop _model_ _depVar_ _in_ lnRentg _p_ _edf_;
msrMarket=0;
if indexcode in &amherstmarket. then msrMarket=1;
run;
proc export data=outlier_output outfile="E:\Output\Rent Forecast\outlier market.csv" dbms=csv replace; run;

 proc sgplot data=is_fund_v4c(where=(indexcode='12060')); series x=indexmonth y=rentidx_fund; series x=indexmonth y=ln_rentidx; series x=indexmonth y=rhpits/y2axis; run;
proc print data=param_final_v&version. noobs; run;;

proc score data=resid_SFrent_v&version.  score=param_final_v&version. out=fc_v&version. type=parms; &bymarket.;
var rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 &stage3V.;
run;

data tp; set resid_SFrent_v&version. ;
keep indexcode msrMarket lnRentg rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 &stage3V. qtr;
run;

proc print data=tp(firstobs=1 obs=10) noobs; run;

data fc_v&version.; set fc_v&version.;
res = lnRentg -  model1;
run;

proc means data=fc_v1 noprint nway; class msrmarket; var lnRentg; output out=tp_mean mean=avgRentg; run;
proc sort data=fc_v1; by msrmarket;
data tp; merge fc_v1(in=f1) tp_mean(in=f2); by msrMarket;
r_tot = (lnRentg - avgRentg)**2;
r_res = (lnRentg - model1)**2;
r = (lnRentg - model1);
run;


*/
 /*
data modelinp2; merge modelinp2(in=f1) geoW(in=f2 rename=(cbsa=indexcode)); by indexcode; if f1 and f2; run;
*/
/*
proc reg data=modelinp2(where=(qtr>201100 and qtr<202109)) outest=parm_fund adjrsq noprint tableout ;  *by indexcode;  weight geoW;  *where qtr>201100 and qtr<202001;
model ln_Rentidx = Inc_gini afford_HP afford_rent ln_SFDhh    /selection=stepwise sle=0.001 ;* ln_SFR ln_laborforce  ln_laborForce ln_emp;
output out=insample_fund r=r_fund p=rentidx_fund; run; quit;

 proc print data=parm_fund( drop =_in_ _p_ _model_) noobs; run;
 proc sgplot data=is_fund_v2(where=(indexcode='19124')); series x=indexmonth y=rentidx_fund; series x=indexmonth y=ln_rentidx; run;

 
data insample_LT1; set insample_fund(where=(rentidx_fund ne .)) ; *intercept=1; by indexcode qtr;
l1_fundidx=lag(rentidx_fund);
if first.indexcode then l1_fundidx=.;
rhpits = ln_rentidx_l1-l1_fundidx;
rhpits_l1 = lag(rhpits);
if indexcode ne lag(indexcode) then rhpits_l1=.;
run;

proc reg data=insample_LT1(where=(qtr>201100 and qtr<202109)) outest=parm_stage2 adjrsq tableout noprint; *by indexcode;*weight housing;  weight geoW;*where qtr>=201101 and  qtr<202103;
model lnRentg = rhpits  rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.001;
output out=insample_stage2 r=r_stage2 ; run; quit;

 proc print data=parm_stage2( drop =_in_ _p_ _model_) noobs; run;


proc reg data=insample_stage2(where=(qtr>201100 and qtr<202109)) outest=parm_stage3 adjrsq tableout noprint;  *by indexcode;   weight geoW;* where  indexcode in (&amherst);  where qtr>=201101 and  qtr<202103;
model r_stage2=hpg_season unemp_g  hpg_season_l1-hpg_season_l4 rentyield_l1  chgslope0_1 /selection=stepwise;
; output out=resid_SFrent r=r_SFRent; run; quit; 
 proc print data=parm_stage3( drop =_in_ _p_ _model_) noobs; run;



data test; set modelinp1; where rentg_l5 ne . ; run;
proc sql; create table test as  select indexcode, min(qtr) as qtr from test group by indexcode; quit;
proc sql; select max(qtr) from test; quit;

proc sql; select count(distinct indexcode) from modelinp1; quit;

proc reg data=modelinp2 (where=(qtr>201100 and qtr<202109))  outest=parm_noFund_stage1 adjrsq noprint tableout ;  *by indexcode;  weight geoW; * where qtr>=201101 and  qtr<202103;
 model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.001;
output out=insample_noFund_stage2 r=resid ; run; quit;
 proc print data=parm_noFund_stage1( drop =_in_ _p_ _model_) noobs; run;

 

proc reg data=insample_noFund_stage2 (where=(qtr>201100 and qtr<202109))   outest=parm_noFund_stage2 adjrsq tableout noprint;  *by indexcode;   weight geoW;* where  indexcode in (&amherst);  where qtr>=201101 and  qtr<202103;
model resid=hpg_season unemp_g  hpg_season_l1- hpg_season_l4 rentyield_l1  chgslope0_1 /selection=stepwise ;
; output out=resid_nofund_SFrent r=r_SFRent; run; quit; 
proc print data=parm_noFund_stage2( drop =_in_ _p_ _model_) noobs;run;

proc reg data=modelinp2(where=(qtr>201100 and qtr<202109)) outest=test adjrsq noprint tableout ;  *by indexcode;  weight geoW; *where  indexcode in (&amherst);  ;
 model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4  chgslope0_1
/selection=stepwise sle=0.001 ;  *rentg1_1-rentg1_&Ncbsa. rentg2_1-rentg2_&Ncbsa. rentg3_1-rentg3_&Ncbsa.
rentg4_1-rentg4_&Ncbsa. ;
output out=tp r=resid ; run; quit;

 proc print data=test( drop =_in_ _p_ _model_) noobs; run;


proc reg data=tp outest=test1 adjrsq tableout noprint;  *by indexcode;   weight housing;* where  indexcode in (&amherst); where  qtr<202001;
model resid=hpg_season unemp   rentyield_l1   /selection=stepwise ;
; *output out=resid_SFrent r=r_SFRent; run; quit; 

 proc print data=test1( drop =_in_ _p_ _model_) noobs; run;

proc print data=parm;run;


proc reg data=modelinp1 outest=test adjrsq noprint tableout ;  *by indexcode;  weight geoW;  where qtr>=201101 and  qtr<202103;
 model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5 /selection=stepwise sle=0.001;
*output out=insample_stage2 r=resid ; run; quit;
 proc print data=parm( drop =_in_ _p_ _model_) noobs; run;

 proc sql; create table wrong as
 select a.indexcode, a.qtr, a.rentidx
 from insample_fund a
 left join resid_sfrent b
 on a.indexcode=b.indexcode and a.qtr=b.qtr
 where b.qtr=.;
 quit;

/*


data HP_year; set hp(keep=indexcode qtr hpg_season: sfdhousehold); by indexcode qtr;


hpg_season_l1=lag(hpg_season); if first.indexcode then hpg_season_l1=.;
hpg_season_l2 = lag(hpg_season_l1); if indexcode ne lag2(indexcode) then hpg_season_l2=.;
hpg_season_l3 = lag(hpg_season_l2); if indexcode ne lag3(indexcode) then hpg_season_l3=.;

YOYHPG=hpg_season+hpg_season_l1+hpg_season_l2+hpg_season_l3; 
sfd_hhg = log(sfdhousehold/lag4(sfdhousehold));
if indexcode ne lag4(indexcode) then sfd_hhg=.;
if mod(qtr,100)=4;
year = int(qtr/100);
*if qtr>=200501;
run;

data hp_year; set hp_year; by indexcode year;
yoyHpg_l1 = lag(yoyHPG);
sfd_hhg_l1 = lag(sfd_hhg);
if first.indexcode then do; yoyHPG_l1=.; sfd_hhg_l1=.; end;

YOYHPG_l2 = lag(yoyHPG_l1);
sfd_hhg_l2 =lag(sfd_hhg_l1);
if indexcode ne lag2(indexcode) then do; yoyHPG_l2=.; sfd_hhg_l2=.; end;
run;

proc sql; create table sfr1 as
select a.indexcode, a.year, a.sfr, a.sfr_g, a.sfr_g_l1, a.sfr_g_l2, b.sfd_hh, b.sfd_hhg, b.YOYHpg, b.sfd_hhg_l1, b.yoyHPG_l1, b.sfd_hhg_l2, b.yoyHPG_l2
, a1.sfr as baseSFR, t.YOYRentg as YOYRentg_l1
from sfr a
join HP_year b
on a.indexcode=b.indexcode 
and a.year=b.year 
left join Sfr a1
on a.indexcode=a1.indexcode
and a1.year=2013
join tp t
on a.indexcode=t.indexcode
and a.year = t.year+1
order by indexcode, year;
quit;

proc reg data=sfr1 outest=parm_SFR adjrsq noprint tableout ;  *by indexcode;  weight baseSFR;  *where qtr>=201101 and  qtr<202103;
model SFR_g = SFR_g_l1 SFR_g_l2 SFd_hhg_l1 sfd_hhg yoyHPG YOYHPG_l1 sfd_hhg_l2 yoyHPG_l2 YOyRentg_l1/selection=stepwise sle=0.001 ;
*output out=insample_fund r=r_fund p=rentidx_fund; run; quit;
*/
