
option compress=yes error=10;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
libname IR ODBC DSN='InterestRates' schema=dbo;
LIBname myresult "\\tvodev\T$\Thu Output\HPI\HPI Calculation\v2.0\evenmonth";

%let lt_out=\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1;
libname RentParm "E:\Output\Rent Forecast\Param";
LIBNAME simHPI "\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1\parameters";
%let amherstmarket = ('12060','16740','28140','38060','36740','45300','27260','34980','23104','19124','32820','31140','13820','26900','36420','41180','39580','41700','29460','15980','35840','26420','49180'
,'37340','46060','24660','18140','19660','19740','29820','48424','22744','20500','38940','44380','45104','33124','15500','45640','23580','42680','46220','39460','36260');
%let enddate=202111;

data geoW; set testbed.rentfc_weight; run;


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

proc sql; select count(distinct indexcode) from allrentidx; quit;

proc sql; create table modelinp0b as select distinct a.*
, baseRent, c1.hpi_sa_medhP as baseHP,c.*,
baseRent/e.rentidx_sa*a.rentidx_sa as medRent
, m_rentshare, m_mortShare, m_rent2own
, em.ln_emp, em.ln_laborforce, geoW, geoW_allRentUnit
, pSFR_group
, MSRMarket, outlier
from allrentIdx a
join rawRentidx b
on a.indexcode=b.indexcode and b.date=201303
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
*ln_SFDhh = log(sfdhousehold);

afford_HP = log(Inc*m_mortshare/12/factor)-log(hpi_sa_medhp);
afford_Rent = log(Inc*m_rentshare/12) - log(baseRent);
afford_rent2 = log( (hpi_sa_medhp*factor) * m_rent2Own)- log(baseRent);

run;



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
 


proc sql; create table fc as select distinct a.*, baseRent, m_rentshare
, pSFR_group
, msrmarket, outlier
from allrentIdx a
join rawRentidx b
on a.indexcode=b.indexcode and b.date=201303
join geoW w
on a.indexcode=w.cbsa

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

%getRandomErrAll();


data cbsa; set fc; keep indexcode pSFR_group baserent m_rentshare; proc sort nodup; by indexcode ;run;

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
*if indexcode not in (&outlier1.) ;
if qtr>=&fcqtrStart-300;do simid=&startsim to &endsim; output; end;
proc sort nodup; by simid indexcode qtr;run;
proc sort data=qtr2; by simid indexcode qtr;run;

data fc3; retain indexcode simid qtr lnRentg ; merge fc2 qtr2; by simid indexcode qtr; retain 
rentg_l1 rentg_l2 rentg_l3 rentg_l4 rentg_l5  lnRentg ln_rentidx_l1 ln_rentidx  ;
 
if resid_rent=. then resid_rent=0; 
afford_Rent = log(Inc_p50*m_rentshare/12) - log(baseRent);

*if afford_rent>0.4 then afford_rent=0.4;

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

if lnRentg>=0.06 then lnRentg=0.06;

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
keep qtr  indexcode simid   rentidx Inc_p50 rentg unemp;
run;
data AllSim_v1; set AllSim_v1 fc5; if simid ne .; run;
%mend;


data ALlSim_v1;run;
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
proc sort data=ALlSim_v1; by indexcode;run;
proc means data=ALlSim_v1 noprint nway; class indexcode qtr; var rentidx; output out=meanpath_v1 mean=  /autoname; run;


data allsim_ar5; set allsim; run;
data allsim_noafford; set allsim; run;
data allsim_withcap; set allsim; run;

proc means data=allsim_ar5 noprint nway; class indexcode qtr; var rentidx; output out=meanpath_ar5 mean=rentidx_ar5  /autoname; run;
proc means data=allsim_noafford noprint nway; class indexcode qtr; var rentidx; output out=meanpath_noafford mean=rentidx_noafford  /autoname; run;
proc means data=allsim_withcap noprint nway; class indexcode qtr; var rentidx; output out=meanpath_withcap mean=rentidx_withcap  /autoname; run;


proc means data=fccon0 noprint nway; class cbsa_code qtr; var hpg inc_p50; output out=test mean= p50=/autoname; run;

data test; set fc3; where indexcode='12060';
*drop baserent m_rentshare msrmarket outlier; run;

proc export data=test outfile="E:\Output\Rent Forecast\test.csv" dbms=csv replace; run;
proc print data=allparm noobs; run;
