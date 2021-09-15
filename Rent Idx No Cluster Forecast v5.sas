%macro RentFC();
%let prev_date=19971201;	%let prev_mon=199712; %let Nsim=2000;
LIBNAME cmbs ODBC DSN='Apollo_CMBS' schema=dbo; 
LIBNAME wlres ODBC DSN='Apollo_3Party' schema=dbo;  LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdP ODBC DSN='Apollo_3Party' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
LIBNAME cre ODBC DSN='cre' schema=dbo;
libname CreMacr '\\tvodev\T$\Thu Output\CRE Macro';
LIBNAME krlive ODBC DSN='krlive' schema=dbo; 	
libname output '\\tvodev\T$\Thu Output\CMBS\';
libname parm '\\tvodev\T$\Thu Output\CMBS\Macro Proj\parameters';
libname oldparm '\\tvodev\T$\Thu Output\CMBS\Macro Proj\parameters2';

libname simoutp '\\tvodev\T$\Thu Output\CMBS\Macro Proj\sim output';
libname macro '\\tvodev\T$\Thu Output\CMBS';

libname IR ODBC DSN='InterestRates' schema=dbo;
%let cDrive=\\tvodev\C$\;
%let tDrive=\\tvodev\T$\;
%include "&cDrive.\SAS Codes\cre-macro-projections\CMBS macrovariable model macros v2.12.sas";
%include "&cDrive.\SAS Codes\cre-macro-projections\macros to support CMBS model 1.sas";

%let inputsrc= &tDrive.\Data Source\CMBS\;
%let outputpath= &tDrive.\Thu Output\CMBS\macro proj;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\;
%let thisMon=201512; %let est_startqtr=200001;
%let startSim=1;%let endSim=100;%let nPath=100; 
%let exclude=if not (metrocode='MUNC' and asgproptype='IN') and not (metrocode='YAKI' and asgproptype='IN') and not (metrocode='VINE' and asgproptype='RT')
and not(metrocode ='FOAR' and asgproptype='RT') and not(metrocode='LAFA') and not (metrocode='FOND')  and not (metrocode='DAVE') and not (metrocode='MOBI') ;
%let lt_input=\\tvodev\T$\Thu Output\HPI\HPI Calculation\v2.0\SAS Input\Long term HPI inputs;
%let maxid=100;%let maxqtr=42; %let fcqtr=202101; %let maxfc=42; %let fcqtr_l1=202004;

%let R_EXEC_COMMAND = &cDrive.\Program Files\R\R-3.4.1\bin\x64\Rscript.exe;
%let JAVA_BIN_DIR = &cDrive.\Thu Codes\SAS_Base_OpenSrcIntegration\bin;
%let SAScodedir=&tDrive.\Thu Output\CMBS\Macro Proj;
LIBNAME devVo ODBC DSN='devVo' schema=dbo;

%let reportdir=&cDrive.\Thu Codes\report\;
%let est_endqtr=201404; 

%let lb=-0.035; %let ub=0.35;
%let shock=; %let nametbl=;
%let tdrive=\\tvodev.CORP.amherst.com\T$\;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;


%include "&cDrive.\SAS Codes\rent-forecast-model\Macro For Rent Idx No Cluster Forecast.sas";


%let varlistC=capr_ust10y_l2 capr_ust10y_l3 capr_ust10y_l4    ;
%let varlistC=capr_ust10y_g_l1 capr_ust10y_g_l2     ;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME devvo ODBC DSN='devvo' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME parmSF '\\tvodev\T$\\Thu Output\SF REnt'; 

libname SimHPI "&lt_out.\parameters"; 
%LoadIn_Hist_IR_Format;
%process_ppr_proj; 
 /*
data ppr; set cremacr.ppr;run;
data pprCBSA; set cremacr.pprCBSA;run;
*/
data mapCBSA; set cremacr.mapCBSA;run;

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

proc sql; create table mapCBSA as select distinct a.metrocode,
a.asgproptype,case when b.cbsa_div='' then cbsa
else cbsa_div end as indexcode
from map_sub_metro a
join thirdp.zipcodesdotcom_Dt b
on a.zipcode=b.zipcode and b.primaryrecord='P' and cbsa ne '';

proc sql; create table pprCBSA as select distinct indexcode,qtr,
avg(pprcaprate) as pprcaprate, avg(vacancy) as vacancy from ppr a join mapcbsa b
on a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.asgproptype='MF'
group by indexcode,qtr order by indexcode,qtr;

proc sql; create table pprcbsa1 as select * from pprcbsa a join  rate_frm  b on a.qtr=b.qtr order by indexcode,a.qtr; run;

data pprCBSA2; set pprCBSA1; by indexcode qtr;
vacancy_g=vacancy-lag(vacancy);
if first.indexcode then vacancy_g=.;
if vacancy=0 then vac_xbeta=-7;		else if Vacancy=1 then 
vac_xbeta=7;	else vac_xbeta=max(-7,min(7,log(Vacancy/(1-Vacancy))));

vac_xbeta_l1=lag(vac_xbeta); vac_xbeta_l2=lag2(vac_xbeta); vac_xbeta_l3=lag3(vac_xbeta);  vac_xbeta_l4=lag4(vac_xbeta); 
vac_xbeta_l5=lag5(vac_xbeta);  vac_xbeta_l6=lag6(vac_xbeta);  vac_xbeta_l7=lag7(vac_xbeta);  vac_xbeta_l8=lag8(vac_xbeta); 

vacancy_g_l1=lag(vacancy_g); vacancy_g_l2=lag2(vacancy_g);vacancy_g_l3=lag3(vacancy_g);vacancy_g_l4=lag4(vacancy_g);

pprcaprate_l1=lag(pprcaprate); pprcaprate_l2=lag2(pprcaprate); pprcaprate_l3=lag3(pprcaprate); pprcaprate_l4=lag4(pprcaprate);
pprcaprate_l5=lag5(pprcaprate); pprcaprate_l6=lag6(pprcaprate); pprcaprate_l7=lag7(pprcaprate); pprcaprate_l8=lag8(pprcaprate);

capr_ust10y=pprcaprate-cmt_10yr/100;
caprate_g=pprcaprate-lag(pprcaprate);
caprate_yoy=pprcaprate-lag4(pprcaprate);
caprate_g_l1=lag(caprate_g);
capr_ust10y_l1=lag(capr_ust10y); capr_ust10y_l2=lag2(capr_ust10y); capr_ust10y_l3=lag3(capr_ust10y); capr_ust10y_l4=lag4(capr_ust10y);
capr_ust10y_l5=lag5(capr_ust10y); capr_ust10y_l6=lag6(capr_ust10y); capr_ust10y_l7=lag7(capr_ust10y); capr_ust10y_l8=lag8(capr_ust10y);

qtrtp=int(qtr/100)*4+qtr-int(qtr/100)*100;


if first.indexcode or qtrtp ne lag(qtrtp)+1 then do; 
	vac_xbeta_l1=.;  pprcaprate_l1=.; rentg_l1=.; capr_ust10y_l1=.; rent_g=.; vacancy_g_l1=.; caprate_g=.;
end;
if indexcode ne lag2(indexcode)  or qtrtp ne lag2(qtrtp)+2 then do; 
	vac_xbeta_l2=.; pprcaprate_l2=.; rentg_l2=.;capr_ust10y_l2=.;vacancy_g_l2=.; caprate_g_l1=.;
end;
if indexcode ne lag3(indexcode)  or qtrtp ne lag3(qtrtp)+3   then do;
	vac_xbeta_l3=.; pprcaprate_l3=.; rentg_l3=.;capr_ust10y_l3=.;vacancy_g_l3=.;
end;
if indexcode ne lag4(indexcode)  or qtrtp ne lag4(qtrtp)+4  then do;
	vac_xbeta_l4=.; pprcaprate_l4=.; rentg_l4=.;capr_ust10y_l4=.;vacancy_g_l4=.; caprate_yoy=.;
end;
if indexcode ne lag5(indexcode)  or qtrtp ne lag5(qtrtp)+5  then do;
	vac_xbeta_l5=.; pprcaprate_l5=.;capr_ust10y_l5=.;
end;
if indexcode ne lag6(indexcode) or qtrtp ne lag6(qtrtp)+6  then do;
	vac_xbeta_l6=.; pprcaprate_l6=.;capr_ust10y_l6=.;
end;
if indexcode ne lag7(indexcode)  or qtrtp ne lag7(qtrtp)+7   then do;
	vac_xbeta_l7=.; pprcaprate_l7=.;capr_ust10y_l7=.;
end;
if indexcode ne lag8(indexcode)  or qtrtp ne lag8(qtrtp)+8  then do;
	vac_xbeta_l8=.; pprcaprate_l8=.; capr_ust10y_l8=.;
end;
caprate_chg=pprcaprate-pprcaprate_l1;
slope_g_l1=slope-slope_l1; slope_g_l2=slope-slope_l2; slope_g_l3=slope-slope_l3;   slope_g_l4=slope-slope_l4;  
ust_g_l1=cmt_10yr-cmt_10yr_l1; ust_g_l2=cmt_10yr-cmt_10yr_l2;ust_g_l3=cmt_10yr-cmt_10yr_l3;    ust_g_l4=cmt_10yr-cmt_10yr_l4;  

vac_l1_0=min(max(-2,vac_xbeta_l1+3),1.1);		vac_l1_1=min(max(-1.1,vac_xbeta_l1+1.9),0.8);	
vac_l1_2=min(max(-0.8,vac_xbeta_l1+1.1),0.7);	vac_l1_3=min(max(-0.7,vac_xbeta_l1+0.4),0.6);	
vac_l1_4=min(max(-0.6,vac_xbeta_l1-0.2),1.0);	vac_l1_5=min(1.0, max(0,vac_xbeta_l1-1.2));	
vac_l1_6=min(1.0, max(0,vac_xbeta_l1-2.2));		vac_l1_7=min(1.8, max(0,vac_xbeta_l1-3.2));

capr_ust10y_g=capr_ust10y-capr_ust10y_l1; 
capr_ust10y_g_l1=capr_ust10y_l1-capr_ust10y_l2;
capr_ust10y_g_l2=capr_ust10y_l2-capr_ust10y_l3; 
run;
data _null_; set thirdp.cbsa_dt; call symput(compress("cbsa"||cbsa),name);  run;



proc sql; create table CBSAPOPDist as  select distinct a.cbsa_div,state,pop/sum(pop) as POPPct from 
(select distinct case when cbsa_div ne '' then cbsa_div else cbsa end as cbsa_div,state,sum(population) as pop from wlres.ZipCodesDotCom_dt where cbsa ne ''
group by case when cbsa_div ne '' then cbsa_div else cbsa end,state ) a
group by cbsa_div;run;
%let censusyr=2016;

data ipums_ACS0; set irs.IPUMS_ACS_AllGEO; where geographytype in ('CBSA','State','US');  if year<=&censusyr;
proc sort nodup; by geographytype geographycode  year ;run;

data rentidx0; set ipums_ACS0; where housingType='SFD' and ownership='Rent'; qtr=year*100+3; 
keep geographytype geographycode qtr rent_p50; run;
data qtr&censusYR; do year=1980 to &censusYR; do qtridx=3 to 12 by 3;  qtr=year*100+qtridx; output; end; end; keep qtr; run;


proc sql; create table rentidx as select distinct a.* ,b.*,c.rent_p50 from 
(select distinct geographytype,geographycode from rentidx0) a join qtr&censusYR b on 1=1
left join rentidx0 c on a.geographytype=c.geographytype and a.geographycode=c.geographycode
and b.qtr=c.qtr; proc sort nodup; by geographytype geographycode qtr; run;
data rentidx; set rentidx; qtr2=int(qtr/100)+mod(qtr,100)/12-0.25; run;
proc expand data=rentidx out=rentidx; convert rent_p50=rent_p50_2/ method=join ; by geographytype geographycode; id qtr2;run;

proc sort nodup; by geographytype geographycode descending qtr; run;
data rentidx2; set rentidx; by geographytype geographycode descending qtr; rent_g=rent_p50_2/lag(rent_p50_2)-1;
if first.geographycode then rent_g=.;  drop  qtr2 rent_p50_2; run;

proc sql; create table StateDerivedRent as select distinct a.cbsa_div as geographycode,qtr,
sum(a.PopPct*b.rent_g)/sum(a.PopPct) as Staterent_g from CBSAPOPDist a join rentidx2 b on a.state=b.geographycode 
 group by a.cbsa_div,qtr order by cbsa_div,  qtr desc;run;


/*
 data parmrent; set irs.QtrlyRentIdx ( rename=(index=rentidx0 date=qtr )); geographycode=cbsa_div; keep geographycode qtr rentidx0;  run;
proc sort nodup; by   geographycode descending qtr; run;
 */
data rawRentidxOrg; set  &rentidxTableName ; date=year(monthfmt)*100+month(monthfmt); if date=. then date=substr(monthfmt,1,4)*100+substr(monthfmt,6,2); 
drop monthfmt; if pricetier='agg'; drop index_SF index_TH cbsa; if city20=0 or city20=.;
proc sort nodup; by indexcode date; run;
data rawRentidxOrg; set rawRentidxOrg; by indexcode date; rentg= index/lag(index)-1; if first.indexcode then rentg=.; run;
data rawrentidx0; set rawRentidxOrg; by indexcode date;  if last.indexcode; keep indexcode pricetier  date rentg; run;
data rawrentIdx1; set rawrentIdx0; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;
data rawrentIdx2; set rawrentIdx1; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;

data rawrentIdx3; set rawrentIdx0 rawrentIdx1 rawrentIdx2; proc sort nodup; by indexcode date; run;

%global fcqtrStart;
data _null_; set rawrentidx3; by indexcode date; if last.date; call symput("fcqtrStart", int(date/100)*100+mod(date,100)/3);  call symput("fcqtrmo", int(date/100)*100+mod(date,100)); run;
%let fcqtrStart=%eval(&fcqtrStart*1);
%let fcqtrmo=%eval(&fcqtrmo*1);
%put &fcqtrStart &fcqtrmo;

data rawrentIdx; merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index; if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0; else index=index*(1+rentg); drop rentidx0 rentg0 ; if mod(date,100)  in (3,6,9,12); run;

data parmrent; set rawRentidx ( rename=(index=rentidx0 date=qtr)); 
geographycode=indexcode; if length(geographycode)=5 or geographycode='US'; keep geographycode qtr rentidx0;  run;
proc sort nodup; by   geographycode descending qtr; run;


data ParmRent; set ParmRent; by geographycode descending qtr; ACaM_rent_g=Rentidx0/lag(Rentidx0)-1; if first.geographycode then ACaM_rent_g=.; run;
data AllRentIdx; merge rentidx2(where=(geographytype='CBSA')) ParmRent(in=f1) StateDerivedRent; by geographycode descending qtr;  retain RentIdx; if f1 then Derived=0; else Derived=1;
if first.geographycode then rentidx=1; else do; if ACaM_rent_g ne . then rentIdx=rentIdx*(1+ACaM_rent_g); 
else if rent_g ne . then rentIdx=rentIdx*(1+rent_g); else rentidx=rentidx*(1+staterent_g); end; 
if rentidx ne .;  run;

proc sort data=AllRentIdx; by  geographycode qtr; run;
data allrentIdx; set allRentIdx; drop geographytype; *if length(geographycode)=5; 
indexmonth=input(put(qtr*100+1,8.),YYMMDD10.);format indexmonth  MMDDYYD10.; indexcode=geographycode;  keep indexcode qtr rentidx indexmonth derived; run;
proc sql; create table allrentIdx as select distinct * from allrentIdx group by indexcode having min(qtr)<=201001 and max(qtr)>=201901 order by indexcode ,qtr;run;
proc x12 data=allrentIdx date=indexmonth noprint interval=QTR; by indexcode ; var Rentidx;   
x11;    output out=sa d11;    ods select d11; run;
data allrentIdx; merge  allrentIdx(in=f1) sa(in=f2 rename=Rentidx_D11=Rentidx_sa); 
by indexcode  indexmonth;  if f1 and f2;
LnRentg=log(Rentidx_sa/lag(Rentidx_sa)); 
if first.indexcode then do; LnRentg=.; end; qtr=year(indexmonth)*100+month(indexmonth); 
if lnrentg=. then delete;
qtr=int(qtr/100)*100+mod(qtr,100)/3;run;

/*
proc SQL; connect to odbc(DSN='irspublish'); create table parmSF.Rentavm_p50 as select 
* from connection to odbc(select distinct isnull(cbsa_div,cbsa) as cbsa_code,
PERCENTILE_DISC ( 0.05 ) WITHIN GROUP ( ORDER BY rentavm )   
OVER ( partition by isnull(cbsa_div,cbsa))  as rentavm_p50 from new_rent_AVM a
join amhersthpi..hpi_taxroll_vw b on a.asg_propid=b.asg_propid  where indexmonth='2015-02-01' and  isnull(cbsa_div,cbsa) is not null;);  disconnect from odbc;
*/

data HP; set irs.hpi_basefile; indexcode=put(cbsa_Code,$5.); drop cbsa_code; if qtr>0; run;

proc SQL; connect to odbc(DSN='thirdpartydata'); create table baseRent as select 
* from connection to odbc(select a.*,b.state,isnull(cbsa_div,cbsa) as cbsa 
from modeltestbed..SFR_Rent_CleanUp_new_final a join amhersthpi..hpi_taxroll_vw b
on a.asg_propid=b.asg_propid and b.prop_type ='SF' 
and 2015=year(lease_enddate) and month(lease_enddate)<=3;); 
disconnect from odbc;quit;

proc means data=baseRent noprint; class state; var closingRent; where closingRent between 100 and 10000; output out=baseRent_state p50=baseRent;run;
proc means data=baseRent noprint; class cbsa; var closingRent; where closingRent between 100 and 10000; output out=baseRent_cbsa p50=baseRent;run;

data baseRent_med; set baseRent_cbsa(rename=cbsa=indexcode) baseRent_state(rename=state=indexcode) ; keep indexcode baseRent; where indexcode ne '';run;

proc sql; create table modelinp0b as select distinct a.*, baseRent, c.*,
baseRent/e.rentidx_sa*a.rentidx_sa as medRent,d.*
from allrentIdx a join rawRentidx b
on a.indexcode=b.indexcode and b.date=201503
join baseRent_med m on a.indexcode=m.indexcode
join HP c on a.indexcode=c.indexcode and a.qtr=c.qtr left join pprCBSA2 d
on a.indexcode=d.indexcode and c.qtr=d.qtr join allrentIdx e
on a.indexcode=e.indexcode and e.qtr=201501 where a.qtr>=200001
and a.indexcode in (select indexcode from HP); ;run;
proc sort nodup; by indexcode indexmonth; run;

proc means data=modelinp0b min p1 p5 p95 p99 max; var lnrentg;run;

data modelinp1; set modelinp0b; by indexcode  indexmonth; retain ln_LtLine Rentidx1 t ;  
if first.indexcode  then  t=1;  else  t+1;  ln_LtLine=t*1.0/4.0;
if first.indexcode  then Rentidx1=1; else Rentidx1=Rentidx1*(exp(LnRentg)); ln_Rentidx=log(Rentidx1);
if abs(LnRentg)>.1 and qtr<202001 then LnRentg=.;
if abs(lnRentg)>.25 then lnrentg=.;
unemp_l1=lag(unemp);
unemp_g=unemp-unemp_l1;
CMT_10YR_g_l1=lag(CMT_10YR_g);

us_unemp_g=us_unemp-lag(us_unemp);
rentg_l1=lag(LnRentg);
rentg_l2=lag(rentg_l1);
rentg_l3=lag(rentg_l2);
rentg_l4=lag(rentg_l3);
rentg_l8=lag8(LnRentg);
rentyield=log(medRent/hpi_sa_medhp);
rentyield_l1=lag(rentyield);
if first.indexcode then rentyield_l1=.;

rentyield_g=rentyield-rentyield_l1;
if  indexcode ne lag(indexcode)  then do; unemp_l1 =.;CMT_10YR_g_l1=.; us_unemp_g=.; end;
CMT_10YR_g_l2=lag(CMT_10YR_g_l1);
if  indexcode ne lag(indexcode)  then do; unemp_g =.;CMT_10YR_g_l2=.; end;
CMT_10YR_g_l3=lag(CMT_10YR_g_l2);
if  indexcode ne lag(indexcode)  then do; unemp_g =.;CMT_10YR_g_l3=.; end;
CMT_10YR_g_l4=lag(CMT_10YR_g_l3);
if  indexcode ne lag(indexcode)  then do; unemp_g =.;CMT_10YR_g_l4=.; end;
if  indexcode ne lag4(indexcode) 
or year(indexmonth) ne year(lag4(indexmonth))+1 or month(indexmonth) ne month(lag4(indexmonth))  then do;
rentg_l1=.;  rentg_l2=.; rentg_l2=.;   rentg_l3=.;  rentg_l4=.; rentg_l8=.; end;

if unemp ne . then do; lo_unemp=unemp<0.04; hi_unemp=unemp>0.07; end;

if cmt_10yr ne . then do; hi_ust=cmt_10yr>5.5;lo_ust=cmt_10yr<2.5;end;
if slope ne . then do; hi_slope=slope>2.2;	lo_slope=slope<0.1;  end; 
%let name=vac_xbeta;
lo_unemp_&name._l1=lo_unemp*&name._l1; lo_unemp_&name._l2=lo_unemp*&name._l2; lo_unemp_&name._l3=lo_unemp*&name._l3; lo_unemp_&name._l4=lo_unemp*&name._l4;
hi_unemp_&name._l1=hi_unemp*&name._l1; hi_unemp_&name._l2=hi_unemp*&name._l2; hi_unemp_&name._l3=hi_unemp*&name._l3; hi_unemp_&name._l4=hi_unemp*&name._l4;

lo_ust_&name._l1=lo_ust*&name._l1; lo_ust_&name._l2=lo_ust*&name._l2; lo_ust_&name._l3=lo_ust*&name._l3; lo_ust_&name._l4=lo_ust*&name._l4;
hi_ust_&name._l1=hi_ust*&name._l1; hi_ust_&name._l2=hi_ust*&name._l2; hi_ust_&name._l3=hi_ust*&name._l3; hi_ust_&name._l4=hi_ust*&name._l4;
slope_&name._l1=slope*&name._l1; slope_&name._l2=slope*&name._l2; slope_&name._l3=slope*&name._l3; slope_&name._l4=slope*&name._l4;

lo_slope_&name._l1=lo_slope*&name._l1; lo_slope_&name._l2=lo_slope*&name._l2; lo_slope_&name._l3=lo_slope*&name._l3; lo_slope_&name._l4=lo_slope*&name._l4;
hi_slope_&name._l1=hi_slope*&name._l1; hi_slope_&name._l2=hi_slope*&name._l2; hi_slope_&name._l3=hi_slope*&name._l3; hi_slope_&name._l4=hi_slope*&name._l4;
ust_&name._l1=cmt_10yr*&name._l1; ust_&name._l2=cmt_10yr*&name._l2; ust_&name._l3=cmt_10yr*&name._l3; ust_&name._l4=cmt_10yr*&name._l4;

slope_g_l1_&name._l1=slope_g_l1*&name._l1; slope_g_l2_&name._l2=slope_g_l2*&name._l2;
slope_g_l3_&name._l3=slope_g_l3*&name._l3;  slope_g_l4_&name._l4=slope_g_l4*&name._l4; 

ust_g_l1_&name._l1=ust_g_l1*&name._l1; ust_g_l2_&name._l2=ust_g_l2*&name._l2;
ust_g_l3_&name._l3=ust_g_l3*&name._l3;  ust_g_l4_&name._l4=ust_g_l4*&name._l4; 
hi_ust_capr_ust10y_g_l1=hi_ust*capr_ust10y_g_l1;
lo_ust_capr_ust10y_g_l1=lo_ust*capr_ust10y_g_l1; run;

data Ncbsa; set modelinp0b; where qtr=201702 ; keep indexcode  ; proc sort nodup; by indexcode ; run;
data Ncbsa; set Ncbsa;by indexcode ; retain Ncbsa;  Ncbsa+1; keep indexcode  Ncbsa ; run;

proc sql noprint; select max(Ncbsa) into: Ncbsa from Ncbsa;
%put &Ncbsa; %Let Ncbsa=%eval(&Ncbsa);
data modelinp1; merge modelinp1 Ncbsa; by indexcode ; array Z(*) Z_1-Z_&Ncbsa; 
do i=1 to dim(Z); if i=Ncbsa then Z(i)=1; else Z(i)=0; end; run;


proc import datafile="\\tvodev.CORP.amherst.com\T$\\Thu Output\HPI\HPI Forecast\sp500.csv" out=tv_sp500      dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc sort nodup; by date ;run;
%add_fredRent(inp=sp500,sm_url=http://research.stlouisfed.org/fred2/data/sp500.txt, sm_var=sp500, sm_firstobs=16);
proc sort data =tv_sp500; by date;
proc sort data =sp500; by month; run;
data tv_sp500; merge tv_sp500 sp500(rename=month= date); by date; 
if sp500=. then sp500=close*1; keep date sp500; run;

data tv_sp500; set tv_sp500 (where=(mod(date,100) in (3,6,9,12))); by date; sp500_chg=sp500/lag(sp500)-1; qtr=int(date/100)*100+mod(date,100)/3; sp500_chg_l1=lag(sp500_chg); 
keep sp500_chg qtr sp500_chg_l1 sp500; run;

proc sql; create table modelinp1 as select distinct * from modelinp1 a left join tv_sp500 b on a.qtr=b.qtr;run;
proc sort nodup; by indexcode qtr; run;
data modelinp1; set modelinp1; by indexcode qtr; hpg_season_l1=lag(hpg_season);

if indexcode ne lag(indexcode) then hpg_season_l1=.; run;
data insample; set modelinp1; *if qtr<=201711; run;

/*
proc means data=insample0 noprint; by indexcode ; var rentGDev; output out=Adj mean=adjrentGDev; run;
*/

%let amherst='12060','13820','15980','16740','19124','19660','20500','23104','23580','24660','26420','27260','28140','29460','31140','32820','33124','33460','34940',
'34980','35840','36420','36740','37340','38940','39460','39580','41700','42680','45300','49180';

proc means data=insample noprint; var LnRentg rentyield hpg_season unemp incg; by indexcode; output out=m_LnRentg mean=m_lnRentg m_rentyield m_hpg_season m_unemp m_incg;run;

data insample_LT; merge insample m_LnRentg(keep=indexcode m_lnRentg m_rentyield m_hpg_season m_unemp m_incg); by indexcode; Lnrentg_m=lnRentg-m_lnRentg;
rentg_l1_m=rentg_l1-m_LnRentg;
rentg_l2_m=rentg_l2-m_LnRentg;
rentg_l3_m=rentg_l3-m_LnRentg; 
rentg_l4_m=rentg_l4-m_LnRentg;

hpg_season_l1=lag(hpg_season);
hpg_season_l2=lag2(hpg_season);
hpg_season_l3=lag3(hpg_season);
if indexcode ne lag(indexcode) then hpg_season_l1=.;
if indexcode ne lag2(indexcode) then hpg_season_l2=.;
if indexcode ne lag3(indexcode) then hpg_season_l3=.;
YOYHPG=hpg_season+hpg_season_l1+hpg_season_l2+hpg_season_l3; 

YOYHPG_l4=lag4(YOYHPG);
YOYHPG_l8=lag8(YOYHPG);
if indexcode ne lag4(indexcode) then YOYHPG_l4=.;
if indexcode ne lag8(indexcode) then YOYHPG_l8=.;

YOYHPG_3y=(YOYHPG+YOYHPG_l4+YOYHPG_l8)/3;


YOYLnRentg=LnRentg+rentg_l1+rentg_l2+rentg_l3;
YOYLnRentg_l4=lag4(YOYLnRentg);
YOYLnRentg_l8=lag8(YOYLnRentg);
YOYLnRentg_l12=lag12(YOYLnRentg);
YOYLnRentg_l16=lag16(YOYLnRentg);
YOYLnRentg_l20=lag20(YOYLnRentg);
if indexcode ne lag4(indexcode) then YOYLnRentg_l4=.;
if indexcode ne lag8(indexcode) then YOYLnRentg_l8=.;
if indexcode ne lag12(indexcode) then YOYLnRentg_l12=.;
if indexcode ne lag16(indexcode) then YOYLnRentg_l16=.;
if indexcode ne lag20(indexcode) then YOYLnRentg_l20=.;

YOYLnRentg_3y=(YOYLnRentg+YOYLnRentg_l4+YOYLnRentg_l8)/3; 

YOYLnRentg_l4_m=YOYLnRentg_l4-(m_LnRentg*4);

LnRentg01=LnRentg-rentg_l1;
LnRentg12=rentg_l1-rentg_l2;
LnRentg23=rentg_l2-rentg_l3;
LnRentg34=rentg_l3-rentg_l4;

LNYOY04=YOYLnRentg-YOYLnRentg_l4;
LnYOY48=YOYLnRentg_l4-YOYLnRentg_l8;
rentyield_l1_m=rentyield_l1=m_rentyield;
hpg_season_m=hpg_season-m_hpg_season;
hpg_season_l1_m=hpg_season_l1-m_hpg_season;
unemp_m=unemp-m_unemp;
incg_m=incg-m_incg;
lnrentg4=exp(lnrentg*4)-1;
hpg_season4=exp(hpg_season*4)-1;

array Z(*) Z_1-Z_&Ncbsa.; 
array Z1(*) rentg1_1-rentg1_&Ncbsa.; 
array Z2(*) rentg2_1-rentg2_&Ncbsa.; 
array Z3(*) rentg3_1-rentg3_&Ncbsa.; 
array Z4(*) rentg4_1-rentg4_&Ncbsa.; 


array Z1_m(*)  rentg1_m_1-rentg1_m_&Ncbsa.;
array Z2_m(*)  rentg2_m_1-rentg2_m_&Ncbsa.;
array Z3_m(*)  rentg3_m_1-rentg3_m_&Ncbsa.;
array Z4_m(*)  rentg4_m_1-rentg4_m_&Ncbsa.;

do i=1 to dim(Z1); 
Z1(i)=Z(i)*rentg_l1;
Z2(i)=Z(i)*rentg_l2;
Z3(i)=Z(i)*rentg_l3;
Z4(i)=Z(i)*rentg_l4;

Z1_m(i)=Z(i)*rentg_l1_m;
Z2_m(i)=Z(i)*rentg_l2_m;
Z3_m(i)=Z(i)*rentg_l3_m;
Z4_m(i)=Z(i)*rentg_l4_m;
end;

caprate_yoy=pprcaprate-pprcaprate_l4;
caprate_yoy_l1=lag(caprate_yoy);
sp500_yoy=sp500/lag4(sp500)-1;
unemp_yoy=unemp-lag4(unemp);

if lag4(indexcode) ne indexcode then do; caprate_yoy_l1=.; sp500_yoy=.; unemp_yoy=.;end;
run;

/*
data qtrCPI; set CPI; by month; if mod(month,100) in (2,5,8,11); date=int(month/100)*100+(mod(month,100)+1)/3; drop month; 
cpi_g=cpi/lag(cpi);
cpi_yoy=cpi/lag4(cpi);
run;


proc sql; create table corrdat as select distinct * from insample_lt a join qtrCPI b on a.qtr=b.date order by indexcode, date;run;

data corrdat; set corrdat; cmt_10yr_yoy=cmt_10yr-cmt_10yr_l4; cmt_10yr=cmt_10yr/100; cmt_10yr_g=cmt_10yr_g/100; cmt_10yr_yoy=cmt_10yr_yoy/100;
run;
proc means data=corrdat noprint; class qtr; weight housing; output out=insample_LT_US mean=;run;


proc corr data=insample_LT_US; var  yoylnrentg lnrentg pprcaprate cmt_10yr cmt_10yr_g cpi_G   ;run;
proc corr data=insample_LT_US; var  cpi_yoy YOYLnRentg caprate_yoy  cmt_10yr_yoy;run;


proc corr data=insample_LT; weight housing; var lnRentg  pprcaprate cmt_10yr cmt_2yr_G cmt_10yr_g_l1  ;run;


proc reg data=insample_LT   outest=parmV_1 edf tableout adjrsq;   weight housing; where qtr>=200501 and qtr<202001; 
model vac_xbeta=vac_xbeta_l1 slope_g_l1 ust_g_l1   rentg_l1_m hpg_season_l1_m unemp  hi_unemp hi_ust /selection=stepwise;
output out=r_vacancy p=p_vacancy r=r_vacancy;  run; quit; proc print data=parmV_1;run;


proc reg data=insample_LT  outest=parmC_1 edf tableout adjrsq;  weight housing; where qtr>=200501 and  qtr<202001;
model capr_ust10y_g=  capr_ust10y_g_l1  sp500_chg_l1 unemp_g hpg_season_l1  slope  /selection=stepwise; *chgslope0_1; 
output out=r_caprate p=p_caprate_g r=r_caprate;  run; quit; *lo_ust_capr_ust10y_g_l1; proc print data=parmC_1;run;
proc means data=r_caprate; var r_caprate;run;

proc means data=modelinp1 noprint; weight housing; class qtr; var  slope_l: us_unemp_g unemp_g slope chgslope: slope_l1 
cmt_10yr_l1 ust_g_l1 hpg_season_l1  sp500_chg sp500_chg_l1 ; output out=
tv_sp500_withMacro mean=;run;

proc reg data=tv_sp500_withMacro(where=(qtr>0 and  qtr<202001)) outest=parmSP edf tableout adjrsq;  
model sp500_chg=ust_g_l1 slope_l1 us_unemp_g /selection=stepwise;output out=r_sp p=p_sp r=r_sp;  run; quit;


proc reg data=insample_LT outest=parm adjrsq ;  *by indexcode;  weight housing; *where  indexcode in (&amherst);  where  qtr<202001;
where derived=0; model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4  chgslope0_1
Z_1-Z_&ncbsa.  /selection=stepwise sle=0.001 ;  *rentg1_1-rentg1_&Ncbsa. rentg2_1-rentg2_&Ncbsa. rentg3_1-rentg3_&Ncbsa.
rentg4_1-rentg4_&Ncbsa. ;
output out=insample_stage2 r=resid ; run; quit; proc print data=parm;run;

proc corr data=insample_stage2;  var resid chgslope:;run;

data parmSF.parmRent_FirstStagev2; set parm;run;

proc reg data=insample_stage2 outest=parm_stage2 adjrsq tableout;  *by indexcode;   weight housing;* where  indexcode in (&amherst); where  qtr<202001;
model resid=hpg_season unemp   rentyield_l1   /selection=stepwise ;
; output out=resid_SFrent r=r_SFRent; run; quit; proc print data=parm_stage2;run;

data parmSF.parmV; set parmV_1(where=(_TYPE_='PARMS')  rename=(Intercept=pv_Intercept vac_xbeta_l1=pv_vac_xbeta_l1 slope_g_l1=pv_slope_g_l1 ust_g_l1=pv_ust_g_l1  rentg_l1_m=pv_rentg_l1_m unemp=pv_unemp 
hi_unemp=pv_hi_unemp hi_ust=pv_hi_ust hpg_season_l1_m=pv_hpg_season_l1_m));  keep pv_:;  run;
data parmC_1; set parmC_1; if capr_ust10y_g_l1=. then capr_ust10y_g_l1=0; run;
data parmSF.parmC; set parmC_1(where=(_TYPE_='PARMS') rename=(Intercept=pc_Intercept capr_ust10y_g_l1=pc_capr_ust10y_g_l1
sp500_chg_l1=pc_sp500_chg_l1 unemp_g=pc_unemp_g hpg_season_l1 =pc_hpg_season_l1 slope=pc_slope )); keep pc_:; run;


data parmSFRent; merge parm(rename=(intercept=int rentg_l1=rentg_l1_0 rentg_l2=rentg_l2_0 rentg_l3=rentg_l3_0 rentg_l4=rentg_l4_0 )) parm_stage2(rename=intercept=int2); 
if int=. then int=0; if int2=. then int2=0;
if hpg_season=. then hpg_season=0;  if unemp=. then unemp=0; 
if incg=. then incg=0; if rentyield_l1=. then rentyield_l1=0;
if rentg_l1_0=. then rentg_l1_0=0;
if rentg_l2_0=. then rentg_l2_0=0;
if rentg_l3_0=. then rentg_l3_0=0;
if rentg_l4_0=. then rentg_l4_0=0;
if chgslope0_1=. then chgslope0_1=0;

array Z1(*) rentg1_1-rentg1_&Ncbsa.; 
array Z2(*) rentg2_1-rentg2_&Ncbsa.; 
array Z3(*) rentg3_1-rentg3_&Ncbsa.; 
array Z4(*) rentg4_1-rentg4_&Ncbsa.; 
array Z(*) Z_1-Z_&Ncbsa; 

do Ncbsa=1 to dim(Z);
if Z(NCBSA)=. then Intercept=int+int2; else Intercept=int+int2+Z(NCBSA); 
if Z1(ncbsa)=. then Z1(ncbsa)=0; rentg_l1=rentg_l1_0+Z1(ncbsa);
if Z2(ncbsa)=. then Z2(ncbsa)=0; rentg_l2=rentg_l2_0+Z2(ncbsa);
if Z3(ncbsa)=. then Z3(ncbsa)=0; rentg_l3=rentg_l3_0+Z3(ncbsa);
if Z4(ncbsa)=. then Z4(ncbsa)=0; rentg_l4=rentg_l4_0+Z4(ncbsa);
output;
end; keep Ncbsa Intercept rentg_l1 rentg_l2 rentg_l3 rentg_l4 hpg_season unemp incg rentyield_l1 chgslope0_1;run;

data parmSFRent; merge parmSFRent(in=f2) Ncbsa(in=f1); if f1 and f2;
drop Ncbsa; run;

data sum; set parmSFrent; sum=rentg_l1 +rentg_l2+ rentg_l3+ rentg_l4; name=symget(compress("cbsa"||indexcode));
proc sort nodup; by sum;  run;

proc export data=sum outfile="&lt_out.\Parm SR Rent.csv" replace; run;
*/
/*
data tp; set ncbsa; if ncbsa in (11,16, 21,37,87,103,112,119,121,125);
cbsa_name=symget(compress("CBSA"||indexcode)); run;
proc print;run;
*/

/*
data housing; set simhpi.housing; indexcode=put(cbsa_code,$5.); run;
proc sql; create table lTGrowthCBSA as select distinct * from parm,adj;run;
data lTGrowthCBSA; retain cbsa_name indexcode ltG; set lTGrowthCBSA; LTG=intercept-adjrentGDev*rentGdev;
cbsa_name=symget(compress("CBSA"||indexcode));run;
proc sort nodup; by cbsa_code;
data lTGrowthCBSA; merge lTGrowthCBSA(in=f1) housing(in=f2); by indexcode; if f1;  proc sort nodup; by descending housing  ;run;
data lTGrowthCBSA; set lTGrowthCBSA(obs=50); proc sort nodup; by indexcode;
proc print data=ltgrowthcbsa(obs=50 keep=housing cbsa_name indexcode ltG);run;
*/
/*
data parmSF.parmSFRent; set parmSFRent(rename=(Intercept=psf_Intercept 
rentg_l1=psf_rentg_l1  rentg_l2=psf_rentg_l2   rentg_l3=psf_rentg_l3
rentg_l4=psf_rentg_l4 hpg_season=psf_hpg_season 
unemp=psf_unemp incg=psf_incg rentyield_l1=psf_rentyield_l1  chgslope0_1=psf_chgslope0_1));
keep indexcode psf_:; run; proc print data=parmSF.parmV;run; 

data parmSF.parmsp; set parmSP(keep=_TYPE_ intercept ust_g_l1 us_unemp_g rename=(intercept=psp_intercept ust_g_l1=psp_ust_g_l1 us_unemp_g=psp_us_unemp_g)
where=(_TYPE_='PARMS'));drop _TYPE_; run;
*/
proc sql; create table allparm as select * from  parmSF.parmSFRent, parmSF.parmC, parmSF.parmV, parmSF.parmsp;run;

data allparm;set allparm;  array parm(*) p:;  do i=1 to dim(parm); if parm(i)=. then parm(i)=0; end; drop i;run;

*%genResid;
*%getRandomErrAll;
*%getSimRates;

data ln_seasonality; set allrentIdx; by indexcode  qtr; seasonality=log(Rentidx/lag(Rentidx))-log(Rentidx_sa/lag(Rentidx_sa));
if first.indexcode  then delete; proc sort; by indexcode  DESCENDING qtr;run;
data ln_seasonality; set ln_seasonality; if indexcode ne lag4(indexcode) ;  qtridx=qtr-int(qtr/100)*100; 
keep indexcode  qtridx seasonality; proc sort nodup; by indexcode   qtridx;run;
proc sql; create table ln_seasonality as select distinct indexcode ,qtridx,seasonality-sum(seasonality)/4 
as seasonality from ln_seasonality group by indexcode;run;
 
data fc; set modelinp1;  run;
data cbsa; set NCbsa; keep indexcode ; proc sort nodup; by indexcode ;run;

%let shock=0;
data rate2; set ir.saved_path_values_dt(rename=(PMMS30=refi_rate libor_3mo=libor_3m month=mo)); 
where curve_type in ('FWD','OAS') and 0<=path_num<=1000; ** Start from Oct 2020, we only generate 1000 rate path;
month=year(intnx('month',rate_timestamp,mo))*100+month(intnx('month',rate_timestamp,mo));
if month=. then do;
timestamp=input(put(substr(rate_timestamp,1,4)*10000+substr(rate_timestamp,6,2)*100+1,8.),YYMMDD10.);
month=year(intnx('month',timestamp,mo))*100+month(intnx('month',timestamp,mo));
end;
refi_rate=refi_rate+&shock;
 keep path_num  month refi_rate cmt_2yr cmt_10yr libor_3m; proc sort nodup; by month; run;

 

 

data rate_frm_mo2; set rate_frm_mo;  do path_num=0 to 1000; output; end; run;
data rate_frm2_0; set rate_frm_mo2 rate2(in=f2); if f2 then priority=0; else priority=1; run;
proc sort nodup; by path_num month priority;
data rate_frm2_0; set rate_frm2_0; by path_num month priority; if last.priority;run; 

proc means data=rate_frm2_0 noprint; class path_num month; output out=rate_frm2_0 mean=;run;
data  rate_frm2_0; set rate_frm2_0; if month ne . and path_num ne .; qtr=int(month/100)*100+int((month-int(month/100)*100-1)/3)+1;run;
proc means data=rate_frm2_0 noprint; class path_num qtr; output out=rate_frm2_0 mean=;run;

data rate_frm2_0;	set rate_frm2_0(in=f1 rename=(refi_rate=refi_rate0 cmt_2yr=cmt_2yr0
cmt_10yr=cmt_10yr0)); by path_num qtr; where path_num ne . and qtr>0; retain refi_rate cmt_2yr cmt_10yr;
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
data rate_frm2_0; merge rate_frm2_0 adj; by qtr; if priority=0 then do;  refi_rateADJ=min(max(0.1,refi_rate-adjrefi_rate),60);  cmt_2yrADJ=min(max(0.1,cmt_2yr-adjcmt_2yr),60);  
cmt_10yrADJ=min(max(0.1,cmt_10yr-adjcmt_10yr),60); end;
else do; refi_rateADJ=refi_rate0; cmt_2yrADJ=cmt_2yr0; cmt_10yrADJ=cmt_10yr0;  end;
proc sort data=rate_frm2_0; by path_num qtr;run;


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

%let startsim=1; %let endsim=10;


/*
data rate_frm2_Save; set rate_frm2;  proc means;run;
data rate_frm2; set rate_frm2_save;run;
*/
/*
data base; set rate_frm2_save; if path_num=0; path_num=1;

data shock50Pos; set base; if qtr>&fcqtrStart then  do; cmt_10yr=cmt_10yr+.5;cmt_2yr=cmt_2yr+.5; libor_3m=libor_3m+0.5; refi_rate=refi_rate+.5; end; path_num=2;
data shock100Pos; set base; if qtr>&fcqtrStart then  do; cmt_10yr=cmt_10yr+1;cmt_2yr=cmt_2yr+1; libor_3m=libor_3m+1; refi_rate=refi_rate+1;  end; path_num=3;
data shock50Neg; set base;if qtr>&fcqtrStart then  do;  cmt_10yr=cmt_10yr-.5;cmt_2yr=cmt_2yr-.5; libor_3m=libor_3m-0.5; refi_rate=refi_rate-.5;  end; path_num=4;
data shock100Neg; set base; if qtr>&fcqtrStart then  do; cmt_10yr=cmt_10yr-1;cmt_2yr=cmt_2yr-1; libor_3m=libor_3m-1; refi_rate=refi_rate-1;  end;  path_num=5;
data rate_frm2; set base shock50Pos shock100Pos shock100Neg shock50Neg; proc sort nodup; by path_num qtr; run;
run;



data shock50Pos; set base; if qtr>&fcqtrStart then  do; cmt_2yr=cmt_2yr+1; end; if qtr>&fcqtrStart-100 then do;; slope=cmt_10yr-cmt_2yr; slope_l1=lag(slope);  end; chgslope0_1=slope-slope_l1;  path_num=2;
data shock100Pos; set base; if qtr>&fcqtrStart then  do; cmt_2yr=cmt_2yr+2;end; if qtr>&fcqtrStart-100 then do;; slope=cmt_10yr-cmt_2yr; slope_l1=lag(slope);  end; chgslope0_1=slope-slope_l1;  path_num=3; 
data shock50Neg; set base;if qtr>&fcqtrStart then  do;  cmt_2yr=cmt_2yr-1; end; if qtr>&fcqtrStart-100 then do;; slope=cmt_10yr-cmt_2yr; slope_l1=lag(slope);  end; chgslope0_1=slope-slope_l1;  path_num=4;
data shock100Neg; set base; if qtr>&fcqtrStart then  do; cmt_2yr=cmt_2yr-2;end; if qtr>&fcqtrStart -100 then do;; slope=cmt_10yr-cmt_2yr; slope_l1=lag(slope);  end; chgslope0_1=slope-slope_l1;   path_num=5;
data rate_frm2; set base shock50Pos shock100Pos shock100Neg shock50Neg; proc sort nodup; by path_num qtr; run;
run;

data path1 ; set simHPI.AllSim_Shock; if path_num=1;run;
data path2 ; set path1;  path_num=2;run;
data path3 ; set path1;  path_num=3;run;
data path4 ; set path1;  path_num=4;run;
data path5 ; set path1;  path_num=5;run;

data simHPI.AllSim_Slope; set path1-path5; run;

data tp; set rate_frm2; proc sort nodup; by qtr path_num;
proc transpose data=tp; by qtr; id path_num ; var chgslope0_1;run;

%let startsim=0; %let endsim=10;
data shockhpipos5; set simHPI.AllSim_Slope; where path_num=1; path_num=2; 
data shockhpineg5; set simHPI.AllSim_Slope;  where path_num=1; path_num=3;  run;

data simhpi.hpishock; set simHPI.AllSim_Slope(where=(path_num=1)) shockhpipos5 shockhpineg5 ; by path_num cbsa_code qtr; run;
proc means data=simhpi.hpishock; class path_num; run;
data rate_frm2; set rate_frm2_save; if path_num=0; path_num=1; output; 
 path_num=2; output;  path_num=3; output;   run;

*/
 %macro loopSP;
data spfutures1; set spfutures; qtr=int(date/100)*100+mod(date,100)/3;run;
data _null_; vol=0.1497/2; call symput("vol",vol);run;

/*https://www.alphaquery.com/stock/SPY/volatility-option-statistics/90-day/iv-mean*/


data fcSP ; set tv_sp500(where=(qtr>0 )); do simid=0 to 1000; output; end;
proc sort nodup; by simid qtr; run;

data parmSP_Sim; set parmSF.parmsp; do path_num=0 to 1000; output; end; run; 
data simHPI_US; set simHPI.FIXEDSIM0-simHPI.FIXEDSIM1000; keep qtr path_num us_unemp; if qtr>0 and us_unemp>0; run; proc sort nodup; by path_num qtr us_unemp;run;
proc sql; create table simHPI_US1  as select * from simHPI_US a join parmSP_Sim  b on a.path_num=b.path_num;run;
proc sort nodup; by path_num qtr;run;


data errmat_sp2; set errmat_sp1;by simid qtridx; retain qtr; if first.simid then qtr=&fcqtrStart; 
else do; 
if mod(qtr,100)=4 then qtr=qtr+100-3; else qtr+1;
end; drop qtridx; run;

data fcSP1; merge fcSP(keep=sp500_chg sp500 simid qtr)  rate_frm2( in=f2 rename=path_num=simid keep=path_num qtr
cmt_10yr_g) simHPI_US1(in=f1  rename=path_num=simid) errmat_sp2; by simid qtr;  retain sp500_idx; if f1; 
if resid_sp=. then resid_sp=0;
ust_g_l1=lag(cmt_10yr_g); us_unemp_g=us_unemp-lag(us_unemp); if first.simid then do; ust_g_l1=.;us_unemp_g=.; end;
p_sp500_chg=psp_intercept+ust_g_l1*psp_ust_g_l1+us_unemp_g*psp_us_unemp_g+resid_sp;
if sp500_chg ne . then p_sp500_chg=sp500_chg;
if sp500 ne . then sp500_idx=sp500*1.0;
else sp500_idx=sp500_idx*(1+p_sp500_chg);
keep p_sp500_chg qtr simid resid_sp sp500_idx;  run;

proc means data=fcSP1 noprint; class qtr; var sp500_idx; output out=mean_path_SP mean=sp500_idx ;run;

proc sort data=spfutures1; by qtr;
data mean_path_SP1; merge mean_path_SP spfutures1(keep=qtr idx); by qtr;  where qtr ne .; retain adj  qtridx ; 
if qtridx=. then qtridx=0; qtridx+1;
if idx ne . then do; qtridx=0; adj=idx/(sp500_idx); end; run; proc sort nodup; by descending qtr ; run;
data mean_path_SP1; set mean_path_SP1; by descending qtr ; retain adj1 nextadj idxidx adjadj; if adj ne . then adj1=adj; qtridx_l1=lag(qtridx);
if qtridx_l1=0 then idxidx=qtridx;
if qtridx=0 then nextadj=adj; if nextadj=. or qtridx=0 then adjadj=adj1; else adjadj=(nextadj-adj)*qtridx/(idxidx+1)+adj;
keep adj1 qtr nextadj qtridx adjadj; run;

proc sql; create table fcSP2 as select distinct a.qtr, a.simid, sp500_idx as orgsp500_idx, sp500_idx*case when a.qtr<=maxqtr then 1 else adjadj end
as sp500_idx from fcSP1 a join mean_path_SP1 b on a.qtr=b.qtr
join (select max(qtr) as maxqtr from tv_sp500) on 1=1;run;
proc sort nodup; by simid qtr; run;
data fcSP_FINAl; set fcSP2; by simid qtr; sp500_chg=sp500_idx/lag(sp500_idx)-1; 
sp500_chg_l1=lag(sp500_chg); if simid ne lag2(simid) then sp500_chg_l1=.; if sp500_chg_l1 ne .; keep simid qtr sp500_chg_l1 sp500_chg sp500_idx; run;
proc means; class qtr; var sp500_idx;run; 
%mend;

%loopSP;

%macro loopSim(startsim=,endsim=);
%put &startsim;
data simHistHPA; set HP; if qtr>=201600; do simid=&startsim to &endsim; output; end; 
keep qtr hpg_season indexcode simid; proc sort nodup; by simid indexcode qtr; run;

data simHistHPA; set simHistHPA; ; by simid indexcode qtr; hpg_season_l1=lag(hpg_season);  if  first.indexcode then hpg_season_l1=.;  run;
data SimHPI; set  simHPI.FIXEDSIM&startsim-simHPI.FIXEDSIM&endsim;*set  simHPI.AllSim_Slope ; *simhpi.hpishock; by path_num cbsa_Code qtr;
incg=log(inc_p50/lag(inc_p50));
hpg_season=ln_hpi_season-lag(ln_hpi_season);
if first.cbsa_code  then do; incg=.; hpg_season=.; end;
hpg_season_l1=lag(hpg_season);
if first.cbsa_Code then hpg_season_l1=.; 
indexcode=put(cbsa_Code,$5.); simid=path_num;drop cbsa_Code  path_num; run;

data qtr; set cbsa; do simid=&startsim to &endsim; do year=2018 to int(&fcqtrStart/100)+10;
do qidx=1 to 4; if &fcqtrStart-200<=year*100+qidx<=&fcqtrStart+30000 then do; qtr=year*100+qidx; output; end; end; end;
end; keep indexcode  qtr simid; run;

data errMat2; set errMat_RentFC(where=(&startsim<=simid<=&endsim));by indexcode simid qtridx; retain qtr; if first.simid then qtr=&fcqtrStart; 
else do; 
if mod(qtr,100)=4 then qtr=qtr+100-3; else qtr+1;
end; drop qtridx; run;
data qtr; merge qtr allparm m_LnRentg(keep=indexcode m_LnRentg m_hpg_season); by indexcode ;run;
proc sort data=qtr nodup; by simid qtr; run;
proc sort data=rate_frm2; by path_num qtr; run;

data qtr; merge qtr(in=f1) rate_frm2(where=(&startsim<=simid<=&endsim) in=f2 rename=path_num=simid keep=path_num qtr
cmt_10yr cmt_10yr_g slope slope_l1 refi_rate cmt_2yr cmt_10yr libor_3m chgslope0_1) fcSP_FINAl; by simid qtr; if f1 and f2; run;

proc sort data=qtr; by simid indexcode qtr ;
proc sort data=errMat2; by simid indexcode qtr ; 
proc sort data=SimHPI; by simid indexcode qtr ; 

data qtr1; merge qtr(in=f1) SimHPI(in=f2 keep=simid m_ln_hpi_season hpg_season unemp incg qtr indexcode unemp SFDHousehold Inc_p50 Inc_mean hpg_season_l1
ln_hpi_season rename=(hpg_season=hpg_season_sim hpg_season_l1=hpg_l1_sim)) simHistHPA errMat2; *;
by simid indexcode qtr ; if f1 and f2;
if hpg_season=. then hpg_season=hpg_season_sim; if hpg_season_l1=. then hpg_season_l1=hpg_l1_sim; drop hpg_l1_sim hpg_season_sim; run;

data fc2; set fc(keep=  indexcode qtr  LnRentg ln_LTLine rentyield   ln_rentidx 
vacancy pprcaprate capr_ust10y rename=(LNrentg=LNrentg0 vacancy=vacancy0 pprcaprate=pprcaprate0  ln_LTLine=ln_LTLine0  ln_rentidx=ln_rentidx0
rentyield=rentyield0 capr_ust10y=capr_ust10y0));
if qtr>=&fcqtrStart-300;do simid=&startsim to &endsim; output; end;
proc sort nodup; by simid indexcode qtr;run;

data fc3; retain indexcode simid qtr vacancy pprcaprate lnRentg ; merge fc2 qtr1; by simid indexcode qtr; retain 
vacancy_l1 vac_xbeta_l1 rentyield_l1
vacancy vac_xbeta rentyield
capr_ust10y_l3 capr_ust10y_l2 capr_ust10y_l1 capr_ust10y
rentg_l1 rentg_l2 rentg_l3 rentg_l4  lnRentg
pprcaprate  capr_ust10y_g_l2 capr_ust10y_g_l1 capr_ust10y_g ln_rentidx_l1 ln_rentidx  pprcaprate_l1 capr_g_l1 capr_g   ;
 
if resid_rent=. then resid_rent=0; if resid_vacancy=. then resid_vacancy=0; if resid_caprate=. then resid_caprate=0; if resid_sp=. then resid_sp=0;

if first.indexcode  then do; vacancy_l1 =.;vac_xbeta_l1=.; rentyield_l1=.;  pprcaprate_l1=.; capr_g_l1 =.;capr_g =.;
vacancy=.; vac_xbeta=.; rentyield=.;
capr_ust10y_l3=.; capr_ust10y_l2=.; capr_ust10y_l1 =.;capr_ust10y=.;
rentg_l1 =.;rentg_l2=.; rentg_l3=.; rentg_l4=.;  lnRentg=.;
pprcaprate=.;  capr_ust10y_g_l2=.;capr_ust10y_g_l1=.;capr_ust10y_g=.; ln_rentidx_l1 =.; ln_rentidx=.;
end;
hi_unemp=unemp>0.07;hi_ust=cmt_10yr>5.5;lo_ust=cmt_10yr<2.5;   ln_rentidx_l1=ln_rentidx; 
vacancy_l1=vacancy; vac_xbeta_l1=vac_xbeta;rentyield_l1=rentyield;
capr_ust10y_l3=capr_ust10y_l2; capr_ust10y_l2=capr_ust10y_l1; capr_ust10y_l1=capr_ust10y;
rentg_l4=rentg_l3; rentg_l3=rentg_l2; rentg_l2=rentg_l1; rentg_l1=lnRentg;
capr_ust10y_g_l2=capr_ust10y_g_l1; capr_ust10y_g_l1=capr_ust10y_g; 
pprcaprate_l1=pprcaprate; capr_g_l1=capr_g; unemp_g=unemp-lag(unemp);

if qtr<&fcqtrStart  and pprcaprate0>0 then do;
rentyield=rentyield0;
vacancy=vacancy0;   if vacancy=0 then vac_xbeta=-7;		else if Vacancy=1 then 
vac_xbeta=7;	else vac_xbeta=max(-7,min(7,log(Vacancy/(1-Vacancy))));
pprcaprate=pprcaprate0; capr_ust10y=capr_ust10y0;
capr_ust10y_g=capr_ust10y-capr_ust10y_l1; 
lnRentg=lnRentg0; ln_rentidx=ln_rentidx0; 
capr_g=pprcaprate0-pprcaprate_l1;
end; 
else do;
slope_g_l1=slope-slope_l1; 
ust_g_l1=cmt_10yr_g;
vac_xbeta=pv_Intercept+vac_xbeta_l1*pv_vac_xbeta_l1+slope_g_l1*pv_slope_g_l1+ust_g_l1*pv_ust_g_l1
+hi_unemp*pv_hi_unemp+hi_ust*pv_hi_ust+(rentg_l1-m_LnRentg)*pv_rentg_l1_m+(hpg_season_l1-m_hpg_season)*pv_hpg_season_l1_m+unemp*pv_unemp+resid_vacancy;

if vac_xbeta<=-7 then do; vacancy=0; vac_xbeta=-7; end;
else if vac_xbeta>=7 then do; vacancy=1;vac_xbeta=7; end;
else vacancy=exp(vac_xbeta)/(1+exp(vac_xbeta));

hi_ust_capr_ust10y_g_l1=hi_ust*capr_ust10y_g_l1;
lo_ust_capr_ust10y_g_l1=lo_ust*capr_ust10y_g_l1;

capr_ust10y_g=pc_Intercept+capr_ust10y_g_l1*pc_capr_ust10y_g_l1+ sp500_chg_l1*pc_sp500_chg_l1 +unemp_g*pc_unemp_g 
+hpg_season_l1 *pc_hpg_season_l1 +slope*pc_slope+resid_caprate;
capr_ust10y=capr_ust10y_l1+capr_ust10y_g;
pprcaprate=capr_ust10y+cmt_10yr/100;

if pprcaprate<=0.001 then do; pprcaprate=0.001; capr_ust10y=pprcaprate-cmt_10yr/100; 
capr_ust10y_g=capr_ust10y-capr_ust10y_l1;  end;
lnrentg=(rentg_l1)*psf_rentg_l1+(rentg_l2)*psf_rentg_l2+(rentg_l3)*psf_rentg_l3+(rentg_l4)*psf_rentg_l4
+hpg_season*psf_hpg_season+unemp*psf_unemp+incg*psf_incg
+rentyield_l1*psf_rentyield_l1+psf_Intercept+resid_rent+psf_chgslope0_1*chgslope0_1;
if lnRentg0 ne . then lnRentg=lnRentg0; if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0;  
rentyield=rentyield_l1+lnrentg-hpg_season; 
ln_rentidx=ln_rentidx_l1+lnrentg;

if lnRentg0 ne . then lnRenftg=lnRentg0; if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0; 
end;
fundspread=ln_hpi_season-m_ln_hpi_season;
qtridx=mod(qtr,100);
keep qtr qtridx indexcode simid  vacancy pprcaprate lnRentg hpg_season fundspread  unemp 
SFDHousehold Inc_p50 Inc_mean refi_rate cmt_2yr cmt_10yr libor_3m capr_ust10y_g_l1 capr_ust10y_g capr_ust10y cmt_10yr pprcaprate sp500_chg sp500_idx ;
run;
data hpiseasonality; set  simhpi.ln_seasonality(rename=(qtr=qtridx seasonality=seasonalityHPI));
indexcode=put(cbsa_code,$5.); drop cbsa_code;run;

proc sort data=fc3; by indexcode  qtridx; 
data fc4; merge fc3(in=f1) ln_seasonality hpiseasonality; by indexcode  qtridx; 
rentg=exp(lnrentg+seasonality)-1;  hpg=exp(hpg_season+seasonality)-1; 
if f1; if rentg=. then rentg=lnrentg; if hpg=. then hpg=hpg_season;
if qtr>=&fcqtrstart; if rentg ne . and hpg ne .; run;
proc means;run;
proc sort nodup; by simid indexcode qtr;
data fc5; set fc4; by simid indexcode qtr; 
retain rentidx HPI;
if first.indexcode then do; rentidx=1; hpi=1; end;
else do; rentidx=rentidx*(1+rentg); hpi=hpi*(1+hpg); end;
keep qtr  indexcode simid  vacancy pprcaprate rentidx hpi fundspread  unemp SFDHousehold Inc_p50 Inc_mean refi_rate
cmt_2yr cmt_10yr libor_3m  capr_ust10y_g_l1 capr_ust10y_g capr_ust10y cmt_10yr pprcaprate sp500_chg sp500_idx;
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

proc means data=allsim noprint; class simid qtr; weight housing; var vacancy --hpi cmt_10yr sp500_chg sp500_idx ; output out=US mean= sumwgt=housing;run;
proc means data=US noprint; class qtr;  var vacancy --hpi housing sp500_chg sp500_idx  cmt_10yr; output out=US_mean mean= ; run;

/*
data shock; set us(where=(simid>0)); if  qtr =&fcqtrStart or qtr=&fcqtrStart.+100
 or qtr=&fcqtrStart.+200 or qtr=&fcqtrStart.+300 or qtr=&fcqtrStart.+400 or qtr=&fcqtrStart.+500 or qtr=&fcqtrStart.+700 or qtr= &fcqtrStart.+1000;run;

proc sort data=shock; by simid qtr;
data shock; set shock; by simid qtr; retain fvac fcap frent fHPI;
if first.simid then do; fvac=vacancy; fcap=pprcaprate; frent=rentidx; fHPI=HPI;; end;
vacancy_chg= vacancy-fvac;
caprate_chg= pprcaprate-fcap;
rentidx_g=rentidx/frent-1;
HPA=HPI/fHPI-1; 
if not first.simid;
proc transpose data=shock out=shock2; by simid ; id qtr; var vacancy_chg caprate_chg rentidx_g HPA cmt_10yr;run; 
data shock2; set shock2; 
if simid=1 then scenario='Base';
else if simid=2 then scenario='50Pos';
else if simid=3 then scenario='100Pos';
else if simid=4 then scenario='50Neg';
else scenario='100Neg';
proc print;run;

data hpi; set irs.asg_hpi_dt; if indexcode='US' ; if  date =&fcqtrStart or date=&fcqtrStart.+100
 or date=&fcqtrStart.+200 or date=&fcqtrStart.+300 or date=&fcqtrStart.+400 or date=&fcqtrStart.+500 or date=&fcqtrStart.+700 or date= &fcqtrStart.+1000;
data hpi; set hpi; retain fagg;
if fagg=. then fagg=aggregate;
HPA_prod= aggregate/fagg-1;
keep date agg hpa_prod;
run;
*/

data CBSAstate; merge myresult.CBSAStateAVM(in=f1) myresult.CBSAStateSFD(in=f2); by state cbsa_div; if f1 and f2;
weight=N*priceAVM; keep state cbsa_div weight; if cbsa_div ne ''; run;
proc sort; by cbsa_div; run;
PROC SQL; create table allsim_state as
select distinct a.*, b.state, b.weight
from allsim a
join cbsaState b
on a.indexcode=b.cbsa_div 
order by state, indexcode, simid;
quit;
proc means data= allsim_state noprint nway; class state simid qtr; var vacancy --hpi sp500_chg sp500_idx cmt_10yr; weight weight; output out=state mean=; run;
proc means data=state noprint nway; class state qtr; var vacancy --hpi ; output out=state_mean mean=; run;
data allsim_output; set allsim us(in=f1) state(rename=(state=indexcode) in=f2);
if f1 then indexcode='US'; 
if  indexcode ne '' and qtr>0;
drop housing _TYPE_ _FREQ_;
run;
proc sort data=allsim_output; by indexcode simid descending qtr; 
data allsim_output_Monthly0; set allsim_output; by indexcode simid descending qtr; date=int(qtr/100)*100+mod(qtr,100)*3;
idx=1;
vacancy_l1=lag(vacancy); 
pprcaprate_l1=lag(pprcaprate);
sp500_idx_l1=lag(sp500_idx); 
unemp_l1=lag(unemp);
inc_p50_l1=lag(inc_p50);
inc_mean_l1=lag(inc_mean);
capr_ust10y_l1=lag(capr_ust10y);
rentidx_l1=lag(rentidx);
HPI_l1=lag(HPI);
fundspread_l1= fundspread;
drop sp500_chg capr_ust10y_g_l1 capr_ust10y_g qtr;
if first.simid then do; vacancy_l1=.;pprcaprate_l1=.; sp500_idx_l1=.; unemp_l1=.; inc_p50_l1=.; inc_mean_l1=.; capr_ust10y_l1=.; rentidx_l1=.; hpi_l1=.; fundspread_l1=.; end;
run;
data allsim_output_Monthly1; set allsim_output_Monthly0; idx+1; if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;
data allsim_output_Monthly2; set allsim_output_Monthly1; idx+1;  if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;

data allsim_output_Monthly; set allsim_output_Monthly0 allsim_output_Monthly1 allsim_output_Monthly2; by indexcode simid descending date;
if vacancy_l1 ne . then do;
vacancy=(vacancy_l1-vacancy)*((idx-1)/3)+vacancy;  
pprcaprate=(pprcaprate_l1-pprcaprate)*((idx-1)/3)+pprcaprate;  
sp500_idx=(sp500_idx_l1/sp500_idx)**((idx-1)/3)*sp500_idx;  
unemp=(unemp_l1-unemp)*((idx-1)/3)+unemp  ;
inc_p50=(inc_p50_l1/inc_p50)**((idx-1)/3)*inc_p50  ;
inc_mean=(inc_mean_l1/inc_mean)**((idx-1)/3)*inc_mean  ;
capr_ust10y=(capr_ust10y_l1-capr_ust10y)*((idx-1)/3)+capr_ust10y  ;
rentidx=(rentidx_l1/rentidx)**((idx-1)/3)*rentidx  ;
HPI=(HPI_l1/HPI)**((idx-1)/3)*HPI  ;;
fundspread= (fundspread_l1-fundspread)*((idx-1)/3)+fundspread  ;
end;
if vacancy_l1 ne . or idx=1;  if sp500_idx ne .;
drop  vacancy_l1 pprcaprate_l1  sp500_idx_l1  unemp_l1  inc_p50_l1  inc_mean_l1  capr_ust10y_l1  rentidx_l1  hpi_l1  fundspread_l1 idx;
run;
data keepCBSA_agg; set allsim_output_Monthly; where date=202502 and rentidx ne .;keep indexcode; proc sort nodup; by indexcode; run;
proc sort data=keepCBSA_agg; by indexcode ;
proc sort data=allsim_output_Monthly; by indexcode  date;
data allsim_output_Monthly; merge allsim_output_Monthly(in=f1)  keepCBSA_agg(in=f2); by indexcode; if f1 and f2;run;
proc means data=allsim_output_Monthly(where=(simid>0)) noprint; by indexcode date; output out=meanPath mean=;run;
data MeanPath; set MeanPath; if indexcode ne '' and date>0; drop _TYPE_ _FREQ_ simid path_num capr_ust10y_g_l1 capr_ust10y_g_l1  capr_ust10y; run;

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


data sf_rentIdx_month_dt0 ;  merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index; if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0; else index=index*(1+rentg); drop rentidx0 rentg0 ; 
keep indexcode date index; run;
proc sql; create table cbsa2state as select distinct case when cbsadiv='' then cbsa else cbsadiv end as indexcode, date, avg(rentidx) as rentidx_st 
from thirdp.county_dt join meanpath on state=indexcode where cbsa ne ''
group by indexcode,date order by indexcode,date;
run;


proc sql; create table cbsa2US as select distinct case when cbsadiv='' then cbsa else cbsadiv end as indexcode, date, avg(rentidx) as rentidx_US
from thirdp.county_dt join meanpath on 'US'=indexcode where cbsa ne ''
group by indexcode,date order by indexcode,date;
run;

data sf_rentIdx_month_dt; merge sf_rentIdx_month_dt0(rename=index=index0) meanpath(keep=indexcode date rentidx) cbsa2state cbsa2US; by indexcode date;retain index index_last;
if rentidx=. then rentidx=rentidx_st; if rentidx=. then rentidx=rentidx_us;
if first.indexcode then index=.;
if index0 ne . then do; index=index0; index_last=index0; end; else index=index_last*rentidx;
keep index date indexcode;
run;

proc sql; create table sf_rentIdx_month_dt
as select distinct *
from sf_rentIdx_month_dt a
where index ne .
group by indexcode
having  min(date)<202102
order by indexcode, date;
quit;

proc delete data=testbed.bak_sf_rentIdx_month_dt_&enddate.;
data testbed.bak_sf_rentIdx_month_dt_&enddate.(insertbuff=30000); set irs.sf_rentIdx_month_dt;run;

proc delete data=irs.sf_rentIdx_month_dt;
data irs.sf_rentIdx_month_dt (insertbuff=30000); set sf_rentIdx_month_dt; cluster='agg'; 
indexmonth=input(put(date*100+1,8.),YYMMDD10.);FORMAT indexmonth date9.; drop date; run;

 /*

 *Scenario

%let parm=all;
%let fcqtrStart=201905;
data scen; retain scen;  qtridx=1;
do rentgorg=-0.04 to 0.08 by 0.01;
scen+1;output;end;run;
proc sort nodup; by scen qtridx;
 
data scen2; do qtridx=2 to 80; output; end;run;
proc sql; create table scen2 as select distinct qtridx, scen,rentgorg from scen(drop=qtridx),scen2 order by scen,qtridx;run;

data scenAll; set scen scen2; by scen qtridx; run;
proc sql; create table scenAll as select distinct * from scenall, parmsf.Parmrent_firststagev2(keep=psf:); run;
 proc sort nodup; by scen qtridx; run;

data scenAll&parm; set scenAll;
retain  rentg_l1 rentg_l2 rentg_l3 rentg_l4  rentG rentGDev  YOYrentG LnRentg12 LnRentg23 LnRentg34;
if qtridx=1 then do;
rentg=LOG(1+rentgorg)/4; rentg_l1=LOG(1+rentgorg)/4; rentg_l2=LOG(1+rentgorg)/4; rentg_l3=LOG(1+rentgorg)/4; rentg_l4=LOG(1+rentgorg)/4; 
LnRentg12=rentg_l1-rentg_l2;
LnRentg23=rentg_l2-rentg_l3;
LnRentg34=rentg_l3-rentg_l4;
end; else do;
rentg_l4=rentg_l3; rentg_l3=rentg_l2; rentg_l2=rentg_l1; rentg_l1=rentg; 
LnRentg12=rentg_l1-rentg_l2;
LnRentg23=rentg_l2-rentg_l3;
LnRentg34=rentg_l3-rentg_l4;
rentg=rentg_l1*psf_rentg_l1+(rentg_l2)*psf_rentg_l2+
rentg_l3*psf_rentg_l3+(rentg_l4)*psf_rentg_l4+psf_Intercept;
end;
YOYrentG=exp(rentG+rentG_l1+rentG_l2+rentG_l3)-1;
keep scen qtridx  rentgorg rentG rentGDev YOYrentG; 
qtridx=qtridx/4;
run;
proc sort nodup; by scen qtridx;run;

 
data outputscen; merge  
scenAllALL(rename=(rentg=rentgAll  YOYrentG=YOYrentGAll)) ; * Scenallparmrs(rename=(rentg=rentgRS rentGdev=rentGDevRS))
Scenallparmrs2010(rename=(rentg=rentGRS2010 rentGdev=rentGDevRS2010)) 
scenAllparmAVM(rename=(rentg=rentgRSAVM rentGdev=rentGDevRSAVM)); by scen qtridx; if qtridx in (1,2,3,4,5,7,10);  run;
proc sort nodup; by qtridx rentgorg  ; run;
proc transpose data=outputscen out=outputscen1; by  qtridx  ; id rentgorg; var YOYrentGAll  ;run;
proc print;run;

data fc2; set fc(keep=  indexcode qtr  LnRentg ln_LTLine    ln_rentidx
 rename=(LNrentg=LNrentg0  ln_LTLine=ln_LTLine0  ln_rentidx=ln_rentidx0));
if qtr>=&fcqtrStart-200;proc sort nodup; by  indexcode qtr;run;

data fc3; merge fc2 qtr; by  indexcode qtr; 
data fc3; merge fc3 parm; by indexcode;run;

data fc3; set fc3; by  indexcode qtr; 
retain  ln_LTLineAdj
rentg_l1 rentg_l2 rentg_l3 rentg_l4  lnRentg
  ln_LTLine ln_LTLine_l1 ln_rentidx_l1 ln_rentidx LnRentg12 LnRentg23 LnRentg34; 
 
if resid_rent=. then resid_rent=0;

if first.indexcode  then do;  ln_LTLine=.;
rentg_l1 =.;rentg_l2=.; rentg_l3=.; rentg_l4=.;  lnRentg=.; ln_LTLine_l1=.; ln_rentidx_l1 =.; ln_rentidx=.;
end;

ln_LTLine_l1=ln_LTLine;   ln_rentidx_l1=ln_rentidx; 
rentg_l4=rentg_l3; rentg_l3=rentg_l2; rentg_l2=rentg_l1; rentg_l1=lnRentg;
LnRentg12=rentg_l1-rentg_l2;
LnRentg23=rentg_l2-rentg_l3;
LnRentg34=rentg_l3-rentg_l4;
if qtr<&fcqtrStart  then do;
lnRentg=lnRentg0;ln_LTLine=ln_LTLine0; ln_rentidx=ln_rentidx0; 
end; 
else do;
lnrentg=rentg_l1+(rentg_l1-m_LnRentg)*psf_rentg_l1_m+(LnRentg12)*psf_LnRentg12+
(LnRentg23)*psf_LnRentg23+(LnRentg34)*psf_LnRentg34+psf_Intercept;
ln_rentidx=ln_rentidx_l1+lnrentg;
end;
qtridx=mod(qtr,100);
keep qtr qtridx indexcode lnRentg ln_rentidx ; run;




 *Scenario
data ParmALL; Intercept=-0.00047637; rentg_l1=0.3103065276; rentg_l2=0.14370944;	rentg_l3=0.0531284633; rentg_l4=0;  rentgDev=-0.11653237; run;

%let parm=ParmALL; 

data _null_; set &parm;call symput(compress("Intercept"),Intercept); 
call symput(compress("rentg_l1"),rentg_l1); 
call symput(compress("rentg_l2"),rentg_l2); 
call symput(compress("rentg_l3"),rentg_l3); 
call symput(compress("rentg_l4"),rentg_l4); 
call symput(compress("rentgDev"),rentgDev);  run;

%put &rentg_l1 &rentg_l2 &rentg_l3 &rentg_l4 &rentGdev &Intercept;

proc print data=parmsf.parmrent_firststagev2;run;
data _null_; set &parm; 
%let fcqtrStart=201905;
data scen; retain scen;  qtridx=1;
do rentgorg=-0.04 to 0.08 by 0.01;
do rentGDevorg=-0.2 to 0.2 by 0.02;
scen+1;output;end;end;run;
proc sort nodup; by scen qtridx;
 
data scen2; do qtridx=2 to 80; output; end;run;
proc sql; create table scen2 as select distinct qtridx, scen,rentgorg,rentGDevorg from scen(drop=qtridx),scen2 order by scen,qtridx;run;

data scenAll; set scen scen2; by scen qtridx; run;
data scenAll&parm; set scenAll;
retain  rentg_l1 rentg_l2 rentg_l3 rentg_l4  rentG rentGDev YOYrentG;
if qtridx=1 then do;
rentg=LOG(1+rentgorg)/4-log(1.03)/4; rentg_l1=LOG(1+rentgorg)/4-log(1.03)/4; rentg_l2=LOG(1+rentgorg)/4-log(1.03)/4; rentg_l3=LOG(1+rentgorg)/4-log(1.03)/4; rentg_l4=LOG(1+rentgorg)/4-log(1.03)/4; rentGDev=rentGDevorg;
end; else do;
rentg_l4=rentg_l3; rentg_l3=rentg_l2; rentg_l2=rentg_l1; rentg_l1=rentg; 
rentG=rentg_l1*&rentg_l1+rentg_l2*&rentg_l2+rentg_l3*&rentg_l3+rentg_l4*&rentg_l4+rentGDev*&rentGdev+&Intercept;
rentGDev=rentgDev+rentG+log(1.03)/4-log(1+&ltGrowth);
end;
YOYrentG=exp(rentG+rentG_l1+rentG_l2+rentG_l3+log(1.03))-1;
keep scen qtridx  rentgorg rentGDevorg rentG rentGDev YOYrentG; 
qtridx=qtridx/4;
run;
proc sort nodup; by scen qtridx;run;

data outputscen; merge  
scenAllParmALL(rename=(rentg=rentgAll rentGdev=rentGDevAll YOYrentG=YOYrentGAll)) ; * Scenallparmrs(rename=(rentg=rentgRS rentGdev=rentGDevRS))
Scenallparmrs2010(rename=(rentg=rentGRS2010 rentGdev=rentGDevRS2010)) 
scenAllparmAVM(rename=(rentg=rentgRSAVM rentGdev=rentGDevRSAVM)); by scen qtridx; if qtridx in (1,2,3,4,5,7,10);  run;
proc sort nodup; by qtridx rentgorg  rentGDevorg ; run;
proc transpose data=outputscen out=outputscen1; by  qtridx rentgorg  ; id rentGDevorg; var rentGDevAll  ;run;
proc print;run;

proc transpose data=outputscen out=outputscen2; by  qtridx rentgorg  ; id rentGDevorg; var YOYrentGAll  ;run;
proc print;run;


 */
%mend;
