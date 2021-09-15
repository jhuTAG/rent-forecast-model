options compress=yes errors=10 msglevel=i Threads=YES CPUCOUNT=ACTUAL;	
LIBNAME cmbs ODBC DSN='Apollo_CMBS' schema=dbo; 
LIBNAME wlres ODBC DSN='Apollo_3Party' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdP ODBC DSN='Apollo_3Party' schema=dbo;
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
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;
%let thisMon=201512; %let est_startqtr=200001;
%let startSim=1;%let endSim=100;%let nPath=100; 
%let exclude=if not (metrocode='MUNC' and asgproptype='IN') and not (metrocode='YAKI' and asgproptype='IN') and not (metrocode='VINE' and asgproptype='RT')
and not(metrocode ='FOAR' and asgproptype='RT') and not(metrocode='LAFA') and not (metrocode='FOND')  and not (metrocode='DAVE') and not (metrocode='MOBI') ;
%let lt_input=\\tvodev\T$\Thu Output\HPI\HPI Calculation\v2.0\SAS Input\Long term HPI inputs;
%let maxid=100;%let maxqtr=42; %let fcqtr=202003; %let maxfc=42; %let fcqtr_l1=202002;

%let R_EXEC_COMMAND = &cDrive.\Program Files\R\R-3.4.1\bin\x64\Rscript.exe;
%let JAVA_BIN_DIR = &cDrive.\Thu Codes\SAS_Base_OpenSrcIntegration\bin;
%let SAScodedir=&tDrive.\Thu Output\CMBS\Macro Proj;
LIBNAME devVo ODBC DSN='devVo' schema=dbo;

%let reportdir=&cDrive.\Thu Codes\report\;
%let est_endqtr=201404; 

%let prev_date=19971201;	%let prev_mon=199712; %let Nsim=1000;

%let lb=-0.035; %let ub=0.35;
%let shock=; %let nametbl=;
%let tdrive=\\tvodev.CORP.amherst.com\T$\;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;


%include "C:\SAS Codes\rent-forecast-model\Macro For Rent Idx No Cluster Forecast.sas";


%let varlistC=capr_ust10y_l2 capr_ust10y_l3 capr_ust10y_l4    ;
%let varlistC=capr_ust10y_g_l1 capr_ust10y_g_l2     ;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME devvo ODBC DSN='devvo' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME parmSF '\\tvodev\T$\Thu Output\SF REnt'; 

libname SimHPI "&lt_out.\parameters"; 


options compress=yes errors=10 msglevel=i Threads=YES CPUCOUNT=ACTUAL;	

%let prev_date=19971201;	
%let prev_mon=199712; %let Nsim=1000;

%let lt_out=\\tvodev\T$\Thu Output\HPI\HPI Forecast\v2.1;

LIBNAME cmbs ODBC DSN='Apollo_CMBS' schema=dbo; 
LIBNAME wlres ODBC DSN='wl research' schema=dbo; 
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME thirdP ODBC DSN='Apollo_3Party' schema=dbo;
LIBNAME cre ODBC DSN='cre' schema=dbo;
libname IR ODBC DSN='InterestRates' schema=dbo;

libname CreMacr '\\tvodev\T$\Thu Output\CRE Macro';
LIBNAME krlive ODBC DSN='krlive' schema=dbo; 	
libname output '\\tvodev\T$\Thu Output\CMBS\';
libname parm '\\tvodev\T$\Thu Output\CMBS\Macro Proj\parameters';
libname oldparm '\\tvodev\T$\Thu Output\CMBS\Macro Proj\parameters2';

%let maxid=100;%let maxqtr=42; 
%let fcqtr=202004; 
%let maxfc=42;
%let fcqtr_l1=202003;
%let censusyr=2016;
%let Nsim=1000;


%getrates2;

%process_ppr_proj; 

%let rentidxTableName=irs.SF_rendIdx_new;
%macro add_fred(inp,sm_url, /* URL of text data on FRED */ sm_var, /* name of variable */ 
	sm_firstobs /* line of first data (if you are not sure and don't need the oldest data, ~25 is often safe) */);
filename fred url "&sm_url";
data fred_new;  infile fred  firstobs=&sm_firstobs;   format date yymmdd10.; input          @1 date yymmdd10.          @13 &sm_var; 
month=year(date)*100+month(date);run; 
proc means data=fred_new noprint; class month;var &sm_var; output out=fred_new mean=;run;
filename fred; /* close file reference */
data fred_new; set fred_new; if month ne .; drop _TYPE_ _FREQ_; run;
data &inp; merge &inp(in=f1) fred_new(in=f2); by month; if f1 or f2; if month ne .; drop Date; run;
%mend;

%macro getrates2();
/* initialize empty data set */

%if (%sysfunc(fileexist(rate_frm_mo))) %then %do;%end; %else %do;
data rate_frm_mo; format month BEST12.; run;
%add_fred(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/MORTGAGE30US.txt, sm_var=refi_rate, sm_firstobs=16);
%add_fred(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/GS2.txt, sm_var=cmt_2yr, sm_firstobs=16);
%add_fred(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/GS10.txt, sm_var=cmt_10yr, sm_firstobs=16);
%add_fred(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/USD3MTD156N.txt, sm_var=libor_3m, sm_firstobs=33);
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

cmt_10yr_g=cmt_10yr-lag(cmt_10yr); slope_g=slope-lag(slope);
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

capr_ust10y_l1=lag(capr_ust10y); capr_ust10y_l2=lag2(capr_ust10y); capr_ust10y_l3=lag3(capr_ust10y); capr_ust10y_l4=lag4(capr_ust10y);
capr_ust10y_l5=lag5(capr_ust10y); capr_ust10y_l6=lag6(capr_ust10y); capr_ust10y_l7=lag7(capr_ust10y); capr_ust10y_l8=lag8(capr_ust10y);

qtrtp=int(qtr/100)*4+qtr-int(qtr/100)*100;


if first.indexcode or qtrtp ne lag(qtrtp)+1 then do; 
	vac_xbeta_l1=.;  pprcaprate_l1=.; rent_g_l1=.; capr_ust10y_l1=.; rent_g=.; vacancy_g_l1=.;
end;
if indexcode ne lag2(indexcode)  or qtrtp ne lag2(qtrtp)+2 then do; 
	vac_xbeta_l2=.; pprcaprate_l2=.; rent_g_l2=.;capr_ust10y_l2=.;vacancy_g_l2=.;
end;
if indexcode ne lag3(indexcode)  or qtrtp ne lag3(qtrtp)+3   then do;
	vac_xbeta_l3=.; pprcaprate_l3=.; rent_g_l3=.;capr_ust10y_l3=.;vacancy_g_l3=.;
end;
if indexcode ne lag4(indexcode)  or qtrtp ne lag4(qtrtp)+4  then do;
	vac_xbeta_l4=.; pprcaprate_l4=.; rent_g_l4=.;capr_ust10y_l4=.;vacancy_g_l4=.;
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
proc means; var &varlistV;run;
data _null_; set thirdp.cbsa_dt; call symput(compress("cbsa"||cbsa),name);  run;

%let ltGrowth=0.0073897006;


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

data parmrent; set &rentidxTableName ( rename=(index=rentidx0 date=qtr )); where bytype='All' and subtype='All' and bytype2='All' and subtype2='All'; 
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

data HP; set irs.hpi_basefile; indexcode=put(cbsa_Code,$5.); drop cbsa_code;run;

proc sql; create table modelinp0b as select distinct a.*,b.medRent as baseRent, c.*,
b.medRent /e.rentidx_sa*a.rentidx_sa as medRent,d.*
from allrentIdx a 
join &rentidxTableName b
on a.indexcode=b.indexcode and b.bytype='All' and b.subtype='All' and b.date=201503
join HP c on a.indexcode=c.indexcode and a.qtr=c.qtr join pprCBSA2 d
on a.indexcode=d.indexcode and c.qtr=d.qtr join allrentIdx e
on a.indexcode=e.indexcode and e.qtr=201501 where a.qtr>=200501
and a.indexcode in (select indexcode from HP); ;run;
proc sort nodup; by indexcode indexmonth; run;

proc means data=modelinp0b min p1 p5 p95 p99 max; var lnrentg;run;

data modelinp1; set modelinp0b; by indexcode  indexmonth; retain ln_LtLine Rentidx1 t ;  
if first.indexcode  then  t=1;  else  t+1;  ln_LtLine=t*1.0/4.0;
if first.indexcode  then Rentidx1=1; else Rentidx1=Rentidx1*(exp(LnRentg)); ln_Rentidx=log(Rentidx1);
if abs(LnRentg)>.05 then LnRentg=.;
rentg_l1=lag(LnRentg);
rentg_l2=lag(rentg_l1);
rentg_l3=lag(rentg_l2);
rentg_l4=lag(rentg_l3);
rentg_l8=lag8(LnRentg);
rentyield=log(medRent/hpi_sa_medhp);
rentyield_l1=lag(rentyield);
if first.indexcode then rentyield_l1=.;

rentyield_g=rentyield-rentyield_l1;
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
run;

proc reg data=insample_LT   outest=parmV_1 edf tableout adjrsq;   weight housing; 
model vac_xbeta=vac_xbeta_l1 slope_g_l1 ust_g_l1   rentg_l1_m hpg_season_l1_m unemp  hi_unemp hi_ust /selection=stepwise;
output out=r_vacancy p=p_vacancy r=r_vacancy;  run; quit; proc print data=parmV_1;run;

proc reg data=insample_LT  outest=parmC_1 edf tableout adjrsq;  weight housing;
model capr_ust10y_g= capr_ust10y_g_l1  hi_ust_capr_ust10y_g_l1 lo_ust_capr_ust10y_g_l1/selection=stepwise; output out=r_caprate p=p_caprate r=r_caprate;  run; quit;
proc print data=parmC_1;run;


proc means data=insample_LT noprint; where lnrentg4 ne . and hpg_season4 ne . and derived =0;var lnrentg4 hpg_season4; output out=tp  mean= std=/autoname; run;
proc print;run;
proc means data=insample_LT noprint; where YOYLnRentg ne . and YOYHPG ne . and derived =0;var YOYLnRentg YOYHPG; output out=tp  mean= std=/autoname; run;
proc print;run;
proc means data=insample_LT noprint; where YOYLnRentg_3y ne . and YOYHPG_3y ne . and derived =0;var YOYLnRentg_3y YOYHPG_3y; output out=tp  mean= std=/autoname; run;
proc print;run;


proc means data=insample_LT noprint; class indexcode; where YOYLnRentg ne . and YOYHPG ne . and derived =0;var YOYLnRentg YOYHPG; output out=tp(where=(indexcode ne ''))  mean= std=/autoname; run;
data tp2; merge tp(in=f1) housing(in=f2 ); by indexcode; if f1 and f2; proc sort nodup; by descending housing ; run;
data tp2; retain cbsa_name; set tp2(obs=31); cbsa_name=symget(compress("CBSA"||indexcode)); if indexcode ne '35004'; keep cbsa_name indexcode YOYLnRentg_mean YOYHPG_mean YOYLnRentg_stddev YOYHPG_stddev; run; proc print;run;


proc means data=insample_LT noprint; class indexcode; where YOYLnRentg_3y ne . and YOYHPG_3y ne . and derived =0;var YOYLnRentg_3y YOYHPG_3y; output out=tp(where=(indexcode ne ''))  mean= std=/autoname; run;
data tp2; merge tp(in=f1) housing(in=f2 ); by indexcode; if f1 and f2; proc sort nodup; by descending housing ; run;
data tp2; retain cbsa_name; set tp2(obs=31); cbsa_name=symget(compress("CBSA"||indexcode)); if indexcode ne '35004'; keep cbsa_name indexcode YOYLnRentg_3y_mean YOYHPG_3y_mean YOYLnRentg_3y_stddev YOYHPG_3y_stddev; run; proc print;run;

proc means data=insample_LT noprint; class indexcode; where lnrentg ne . and hpg_season ne . and derived =0;var lnrentg4 hpg_season4; output out=tp(where=(indexcode ne ''))  mean= std=/autoname; run;
data tp2; merge tp(in=f1) housing(in=f2 ); by indexcode; if f1 and f2; proc sort nodup; by descending housing ; run;
data tp2; retain cbsa_name; set tp2(obs=31); cbsa_name=symget(compress("CBSA"||indexcode)); if indexcode ne '35004'; keep cbsa_name indexcode lnrentg4_mean hpg_season4_mean lnrentg4_stddev hpg_season4_stddev; run; proc print;run;

proc means data=insample_LT noprint;  where lnrentg ne . and hpg_season ne . and derived =0;var lnrentg hpg_season; output out=tp  mean= std=/autoname; run;
proc print;run;

proc means data=insample_LT ;  where lnrentg ne . and hpg_season ne .;var qtr; where derived =0; run;
proc print;run;


proc means data=insample_LT ;  where lnrentg ne . and hpg_season ne . and derived =0;var lnrentg hpg_season capr_ust10y_g vac_xbeta;  run;
/*
proc reg data=insample_LT outest=parm adjrsq ;  *by indexcode;  weight housing; *where  indexcode in (&amherst);
model LNYOY04= LnYOY48  YOYLnRentg_l4_m/selection=stepwise ; 
output out=insample_stage2 r=resid ; run; quit; proc print data=parm;run;
*/ 

/*
data tp; set insample_LT; where derived=0;  lnrentgBuck=int(YOYLnRentg_l20/0.02)*0.02;  where YOYLnRentg_l20 ne . and YOYLnRentg_l16 ne . and YOYLnRentg_l12 ne . and
YOYLnRentg_l8 ne . and YOYLnRentg_l4 ne . and YOYLnRentg ne .;
proc means; class lnrentgBuck; var YOYLnRentg_l20 YOYLnRentg_l16 YOYLnRentg_l12 YOYLnRentg_l8 YOYLnRentg_l4 YOYLnRentg; output out=tp2 mean=; run;
proc print;run;
*/

/*


proc reg data=insample_LT outest=parm adjrsq ;  *by indexcode;  weight housing; *where  indexcode in (&amherst); 
where derived=0; model LnRentg_m=rentg_l1_m rentg_l2_m rentg_l3_m rentg_l4_m

rentg1_m_1-rentg1_m_&Ncbsa. rentg2_m_1-rentg2_m_&Ncbsa. rentg3_m_1-rentg3_m_&Ncbsa.
rentg4_m_1-rentg4_m_&Ncbsa. Z_1-Z_&ncbsa.   /selection=stepwise sle=0.001 ; 
output out=insample_stage2 r=resid ; run; quit; proc print data=parm;run;

proc reg data=insample_LT outest=parm adjrsq ;  *by indexcode;  weight housing; *where  indexcode in (&amherst); 
where derived=0; model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4
Z_1-Z_&ncbsa.  /selection=stepwise sle=0.005 ; 
output out=insample_stage2 r=resid ; run; quit; proc print data=parm;run;
*/

data insample_lt; set insample_lt; by indexcode qtr;
unemp_g=unemp-lag(unemp);
if first.indexcode then unemp_g=.;
run;

proc reg data=insample_LT outest=parm adjrsq ;  *by indexcode;  weight housing; *where  indexcode in (&amherst); 
where derived=0; model LnRentg=rentg_l1 rentg_l2 rentg_l3 rentg_l4 
rentg1_1-rentg1_&Ncbsa. rentg2_1-rentg2_&Ncbsa. rentg3_1-rentg3_&Ncbsa.
rentg4_1-rentg4_&Ncbsa. Z_1-Z_&ncbsa./selection=stepwise sle=0.001  ; 
output out=insample_stage2 r=resid ; run; quit; proc print data=parm;run;

data parmSF.parmRent_FirstStagev2; set parm;run;

proc reg data=insample_stage2 outest=parm_stage2 adjrsq ;  *by indexcode;   weight housing;* where  indexcode in (&amherst);
model resid=hpg_season unemp_g  incg rentyield_l1   /selection=stepwise ;
; output out=resid_SFrent r=r_SFRent; run; quit; proc print data=parm_stage2;run;

data parmSF.parmV; set parmV_1(where=(_TYPE_='PARMS')  rename=(Intercept=pv_Intercept vac_xbeta_l1=pv_vac_xbeta_l1 slope_g_l1=pv_slope_g_l1 ust_g_l1=pv_ust_g_l1  rentg_l1_m=pv_rentg_l1_m unemp=pv_unemp 
hi_unemp=pv_hi_unemp hi_ust=pv_hi_ust hpg_season_l1_m=pv_hpg_season_l1_m));  keep pv_:;  run;
data parmSF.parmC; set parmC_1(where=(_TYPE_='PARMS') rename=(Intercept=pc_Intercept capr_ust10y_g_l1=pc_capr_ust10y_g_l1 hi_ust_capr_ust10y_g_l1=pc_hi_ust_capr_ust10y_g_l1 lo_ust_capr_ust10y_g_l1=pc_lo_ust_capr_ust10y_g_l1 )); keep pc_:; run;


data parmSFRent; merge parm(rename=(intercept=int rentg_l1=rentg_l1_0 rentg_l2=rentg_l2_0 rentg_l3=rentg_l3_0 rentg_l4=rentg_l4_0)) parm_stage2(rename=intercept=int2); 
if int=. then int=0; if int2=. then int2=0;
if hpg_season=. then hpg_season=0;  if unemp_G=. then unemp_G=0; 
if incg=. then incg=0; if rentyield_l1=. then rentyield_l1=0;
if rentg_l1_0=. then rentg_l1_0=0;
if rentg_l2_0=. then rentg_l2_0=0;
if rentg_l3_0=. then rentg_l3_0=0;
if rentg_l4_0=. then rentg_l4_0=0;

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
end; keep Ncbsa Intercept rentg_l1 rentg_l2 rentg_l3 rentg_l4 hpg_season unemp_G incg rentyield_l1;run;

data parmSFRent; merge parmSFRent(in=f2) Ncbsa(in=f1); if f1 and f2;
drop Ncbsa; run;

data sum; set parmSFrent; sum=rentg_l1 +rentg_l2+ rentg_l3+ rentg_l4; name=symget(compress("cbsa"||indexcode));
proc sort nodup; by sum;  run;

proc export data=sum outfile="&lt_out.\Parm SR Rent.csv" replace; run;
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
data parmSF.parmSFRent; set parmSFRent(rename=(Intercept=psf_Intercept 
rentg_l1=psf_rentg_l1  rentg_l2=psf_rentg_l2   rentg_l3=psf_rentg_l3
rentg_l4=psf_rentg_l4 hpg_season=psf_hpg_season 
unemp=psf_unemp incg=psf_incg rentyield_l1=psf_rentyield_l1  ));
keep indexcode psf_:; run; proc print data=parmSF.parmV;run; proc print data=parmSF.parmC;run; proc print data=parmSF.parmSFRent;run;


data parmSFRent; set parmSFRent(rename=(Intercept=psf_Intercept 
rentg_l1=psf_rentg_l1  rentg_l2=psf_rentg_l2   rentg_l3=psf_rentg_l3
rentg_l4=psf_rentg_l4 hpg_season=psf_hpg_season 
unemp_g=psf_unemp incg=psf_incg rentyield_l1=psf_rentyield_l1  ));
keep indexcode psf_:; run; 


proc sql; create table allparm as select * from parmSFRent, parmSF.parmC, parmSF.parmV;quit;

data allparm;set allparm;  array parm(*) p:;  do i=1 to dim(parm); if parm(i)=. then parm(i)=0; end; drop i;run;

*%genResid;
%getRandomErrAll;
%getSimRates;
data hist; set irs.hpi_basefile(where=(qtr<=201804 and qtr>201000) keep=cbsa_code qtr us_unemp unemp inc ln_inc  hpi_sa inc_mean hpg_season);
rename inc=inc_p50;
indexcode=put(cbsa_code,$5.);
drop cbsa_code;
run;
proc sort nodup; by  indexcode qtr; run;

data hist; set hist; by indexcode qtr;
hpg_season_l1=lag(hpg_season);
ln_inc_l1=lag(ln_inc);
if first.indexcode then do; hpg_season_l1=.; ln_inc_l1=.; end;
incg=ln_inc - ln_inc_l1;
drop ln_inc_l1;
run;

data hist1; do i=0 to 2000;  simid=i; output; end; keep simid; run;

proc sql; create table hist1 as
select distinct  b.simid, a.*
from hist1 as b
full outer join hist as a
on 1=1;
quit;
proc sort nodup; by indexcode qtr simid; run;


%let fcqtrStart=202004;
data ln_seasonality; set allrentIdx; 
by indexcode  qtr; 
seasonality=log(Rentidx/lag(Rentidx))-log(Rentidx_sa/lag(Rentidx_sa));
if first.indexcode  then delete; 
run;
proc sort; by indexcode  DESCENDING qtr;run;

data ln_seasonality; set ln_seasonality; 
if indexcode ne lag4(indexcode) ;  
qtridx=qtr-int(qtr/100)*100; 
keep indexcode  qtridx seasonality;
run;
proc sort nodup; by indexcode   qtridx;run;
proc sql; create table ln_seasonality1 as select distinct indexcode ,qtridx,seasonality-sum(seasonality)/4 
as seasonality from ln_seasonality group by indexcode;quit;
 
data fc; set modelinp1;  run;
data cbsa; set NCbsa; keep indexcode ; proc sort nodup; by indexcode ;run;
%let startsim=200; %let endsim=202;

%macro loopSim(startsim=,endsim=);
%put &startsim;
data SimHPI; set simHPI.FIXEDsim&startsim-simHPI.FIXEDsim&endsim; by path_num cbsa_Code qtr;
incg=log(inc_p50/lag(inc_p50));
hpg_season=ln_hpi_season-lag(ln_hpi_season);
unemp_g=unemp-lag(unemp);
if first.cbsa_code  then do; incg=.; hpg_season=.;unemp_g=.; end;
hpg_season_l1=lag(hpg_season);
if first.cbsa_Code then hpg_season_l1=.; 
indexcode=put(cbsa_Code,$5.); simid=path_num;drop cbsa_Code  path_num; run;

data qtr; set cbsa; do simid=&startsim to &endsim; do year=2019 to int(&fcqtrStart/100)+10;
do qidx=1 to 4; if &fcqtrStart<=year*100+qidx<=&fcqtrStart+30000 then do; qtr=year*100+qidx; output; end; end; end;
end; keep indexcode  qtr simid; run;

data errMat2; set errMat(where=(&startsim<=simid<=&endsim));by indexcode simid qtridx; retain qtr; if first.simid then qtr=&fcqtrStart; 
else do; 
if mod(qtr,100)=4 then qtr=qtr+100-3; else qtr+1;
end; drop qtridx; run;
data qtr; merge qtr allparm m_LnRentg(keep=indexcode m_LnRentg m_hpg_season); by indexcode ;run;
proc sort data=qtr nodup; by simid qtr; run;
proc sort data=rate_frm2; by path_num qtr; run;

data qtr; merge qtr(in=f1) rate_frm2(where=(&startsim<=simid<=&endsim) in=f2 rename=path_num=simid keep=path_num qtr
cmt_10yr cmt_10yr_g slope slope_l1 refi_rate cmt_2yr cmt_10yr libor_3m); by simid qtr; if f1 and f2; run;

proc sort data=qtr; by simid indexcode qtr ;
proc sort data=errMat2; by simid indexcode qtr ; 
proc sort data=SimHPI; by simid indexcode qtr ; 

data qtr1; merge qtr(in=f1) SimHPI(in=f2 keep=simid m_ln_hpi_season hpg_season unemp incg qtr indexcode unemp unemp_g SFDHousehold Inc_p50 Inc_mean hpg_season_l1
ln_hpi_season ) errMat2; *;
by simid indexcode qtr ; if f1 and f2;run;

data fc2; set fc(keep=  indexcode qtr  LnRentg ln_LTLine rentyield   ln_rentidx
vacancy pprcaprate capr_ust10y rename=(LNrentg=LNrentg0 vacancy=vacancy0 pprcaprate=pprcaprate0  ln_LTLine=ln_LTLine0  ln_rentidx=ln_rentidx0
rentyield=rentyield0 capr_ust10y=capr_ust10y0));
if qtr>=&fcqtrStart-200;do simid=&startsim to &endsim; output; end;
proc sort nodup; by simid indexcode qtr;run;

data fc3; retain indexcode simid qtr vacancy pprcaprate lnRentg ; merge fc2 qtr1; by simid indexcode qtr; retain 
vacancy_l1 vac_xbeta_l1 rentyield_l1
vacancy vac_xbeta rentyield
capr_ust10y_l3 capr_ust10y_l2 capr_ust10y_l1 capr_ust10y
rentg_l1 rentg_l2 rentg_l3 rentg_l4  lnRentg
pprcaprate  capr_ust10y_g_l2 capr_ust10y_g_l1 capr_ust10y_g ln_rentidx_l1 ln_rentidx; 
 
if resid_rent=. then resid_rent=0;
if resid_vacancy=. then resid_vacancy=0;
if resid_caprate=. then resid_caprate=0;

if first.indexcode  then do; vacancy_l1 =.;vac_xbeta_l1=.; rentyield_l1=.; 
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

if qtr<&fcqtrStart  then do;
rentyield=rentyield0;
vacancy=vacancy0;   if vacancy=0 then vac_xbeta=-7;		else if Vacancy=1 then 
vac_xbeta=7;	else vac_xbeta=max(-7,min(7,log(Vacancy/(1-Vacancy))));
pprcaprate=pprcaprate0; capr_ust10y=capr_ust10y0;
capr_ust10y_g=capr_ust10y-capr_ust10y_l1; 
lnRentg=lnRentg0; ln_rentidx=ln_rentidx0; 
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

capr_ust10y_g=pc_Intercept+capr_ust10y_g_l1*pc_capr_ust10y_g_l1+ hi_ust_capr_ust10y_g_l1*pc_hi_ust_capr_ust10y_g_l1+
lo_ust_capr_ust10y_g_l1*pc_lo_ust_capr_ust10y_g_l1+resid_caprate;
capr_ust10y=capr_ust10y_l1+capr_ust10y_g;
pprcaprate=capr_ust10y+cmt_10yr/100;
if pprcaprate<=0.001 then do; pprcaprate=0.001; capr_ust10y=pprcaprate-cmt_10yr/100; 
capr_ust10y_g=capr_ust10y-capr_ust10y_l1;  end;
lnrentg=(rentg_l1)*psf_rentg_l1+(rentg_l2)*psf_rentg_l2+(rentg_l3)*psf_rentg_l3+(rentg_l4)*psf_rentg_l4
+hpg_season*psf_hpg_season+unemp_g*psf_unemp+incg*psf_incg
+rentyield_l1*psf_rentyield_l1+psf_Intercept+resid_rent;
rentyield=rentyield_l1+lnrentg-hpg_season; 
ln_rentidx=ln_rentidx_l1+lnrentg;
end;
fundspread=ln_hpi_season-m_ln_hpi_season;
qtridx=mod(qtr,100);
keep qtr qtridx indexcode simid  vacancy pprcaprate lnRentg hpg_season fundspread  unemp SFDHousehold Inc_p50 Inc_mean refi_rate cmt_2yr cmt_10yr libor_3m;
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
keep qtr  indexcode simid  vacancy pprcaprate rentidx hpi fundspread  unemp SFDHousehold Inc_p50 Inc_mean refi_rate cmt_2yr cmt_10yr libor_3m ;
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
/*
%loopSim(startsim=1001, endsim=1100);
%loopSim(startsim=1101, endsim=1200);
%loopSim(startsim=1201, endsim=1300);
%loopSim(startsim=1301, endsim=1400);
%loopSim(startsim=1401, endsim=1500);
%loopSim(startsim=1501, endsim=1600);
%loopSim(startsim=1601, endsim=1700);
%loopSim(startsim=1701, endsim=1800);
%loopSim(startsim=1801, endsim=1900);
%loopSim(startsim=1901, endsim=2000);
*/
proc sort data=allSim; by indexcode;run;

data housing; set simhpi.housing; indexcode=put(cbsa_code,$5.); keep indexcode housing;run;
LIBNAME irs ODBC DSN='irs' schema=dbo;

proc sort data=allsim; by indexcode;
data allsim; merge allsim(in=f1) housing; by indexcode; if f1;run;

proc means data=allsim noprint; class simid qtr; weight housing; var vacancy --hpi; output out=US mean= sumwgt=housing;run;
proc means data=US noprint; class qtr;  var vacancy --hpi housing; output out=US_mean mean= ; run;

libname myresult "\\tvodev\T$\Thu Output\HPI\HPI Calculation\v2.0\oddmonth";
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

data allsim_state; merge allsim(in=f1) cbsaState(rename=(cbsa_div=indexcode)); by indexcode; if f1; run;
proc means data= allsim_state noprint nway; class state simid qtr; var vacancy --hpi ; weight weight; output out=state mean=; run;

proc means data=state noprint nway; class state qtr; var vacancy --hpi ; output out=state_mean mean=; run;

data allsim_output; set allsim us(in=f1) state(rename=(state=indexcode) in=f2);
if f1 then indexcode='US'; 
if simid>0 and indexcode ne '' and qtr>0;
run;

proc sql; create table allsim_output as
select *
from allsim_output
group by indexcode
having max(qtr)>202104
order by indexcode, qtr;
quit;

proc delete data=irs.bak_SimRentHpi_FIXEDHPA;
data irs.bak_SimRentHpi_FIXEDHPA(insertbuff=30000); set irs.SimRentHpi_FIXEDHPA; run;

proc delete data=irs.SimRentHpi_FIXEDHPA;
data irs.SimRentHpi_FIXEDHPA(insertbuff=30000); set allsim_output;
*if f1 then indexcode='US';  if simid>0 and indexcode ne '' and qtr>0; *RateasofDate=20191206; 
*if qtr<=202902; drop _TYPE_ _FREQ_;  run;

proc means data=allsim noprint; class indexcode qtr; output out=meanPath mean=;run;
data MeanPath; set MeanPath; if indexcode ne '' and qtr>0; drop _TYPE_ _FREQ_ path_num; run;

data meanPath_output; set MeanPath us_mean(in=f1) state_mean(rename=(state=indexcode)); 
if f1 then indexcode='US'; if indexcode ne ''  and qtr>0; *RateasofDate=20191206;
run;

proc sql; create table meanPath_output as
select *
from meanPath_output
group by indexcode
having max(qtr)>202104
order by indexcode, qtr;
quit;

proc delete data=irs.bak_RentHPIMeanPath; *_FIXEDHPA;
data irs.bak_RentHPIMeanPath(insertbuff=30000); set irs.RentHPIMeanPath;run;

proc delete data=irs.RentHPIMeanPath; *_FIXEDHPA;
data irs.RentHPIMeanPath(insertbuff=30000); set meanPath_output;  if indexcode ne ''  and qtr>0; *RateasofDate=20191206; drop simid _TYPE_ _FREQ_; run;

proc delete data=irs.HistMFcaprateVac;
data irs.HistMFcaprateVac(insertbuff=30000); set pprCBSA; run;

data tp; set us_mean; by qtr; if qtr ne .;
yoy=rentidx/lag4(rentidx)-1;




%macro insample(fcqtrStart=);
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
%loopSim(startsim=1001, endsim=1100);
%loopSim(startsim=1101, endsim=1200);
%loopSim(startsim=1201, endsim=1300);
%loopSim(startsim=1301, endsim=1400);
%loopSim(startsim=1401, endsim=1500);
%loopSim(startsim=1501, endsim=1600);
%loopSim(startsim=1601, endsim=1700);
%loopSim(startsim=1701, endsim=1800);
%loopSim(startsim=1801, endsim=1900);
%loopSim(startsim=1901, endsim=2000);
proc sort data=allSim; by indexcode;run;

data allsim&fcqtrStart.; set allsim; run;
%mend;

%insample(fcqtrStart=201603);
%insample(fcqtrStart=201703);
%insample(fcqtrStart=201803);

proc means data=allsim201603 noprint nway; class indexcode qtr; var rentidx; 
where indexcode in ('12060','16740','18140','19124','23104','27260','28140','34980','36740','38060','39580','41700','45300', '19740','41620','42644');
output out=rent201603 mean=; run;

proc sort data=rent201603 nodup; by indexcode qtr; run;

data rent201603; set rent201603; by indexcode qtr;
rentg_q=rentidx/lag(rentidx)-1;
rentg_6m=rentidx/lag2(rentidx)-1;
rentg_1y=rentidx/lag4(rentidx)-1;
rentg_2y=rentidx/lag8(rentidx)-1;

if indexcode ne lag(indexcode) then rentg_q=.;
if indexcode ne lag2(indexcode) then rentg_6m=.;
if indexcode ne lag4(indexcode) then rentg_1y=.;
if indexcode ne lag8(indexcode) then rentg_2y=.;
date=201603;
run;


proc means data=allsim201703 noprint nway; class indexcode qtr; var rentidx; 
where indexcode in ('12060','16740','18140','19124','23104','27260','28140','34980','36740','38060','39580','41700','45300', '19740','41620','42644');
output out=rent201703 mean=; run;

proc sort data=rent201703 nodup; by indexcode qtr; run;

data rent201703; set rent201703; by indexcode qtr;
rentg_q=rentidx/lag(rentidx)-1;
rentg_6m=rentidx/lag2(rentidx)-1;
rentg_1y=rentidx/lag4(rentidx)-1;
rentg_2y=rentidx/lag8(rentidx)-1;

if indexcode ne lag(indexcode) then rentg_q=.;
if indexcode ne lag2(indexcode) then rentg_6m=.;
if indexcode ne lag4(indexcode) then rentg_1y=.;
if indexcode ne lag8(indexcode) then rentg_2y=.;
date=201703;
run;

proc means data=allsim201803 noprint nway; class indexcode qtr; var rentidx; 
where indexcode in ('12060','16740','18140','19124','23104','27260','28140','34980','36740','38060','39580','41700','45300', '19740','41620','42644');
output out=rent201803 mean=; run;

proc sort data=rent201803 nodup; by indexcode qtr; run;

data rent201803; set rent201803; by indexcode qtr;
rentg_q=rentidx/lag(rentidx)-1;
rentg_6m=rentidx/lag2(rentidx)-1;
rentg_1y=rentidx/lag4(rentidx)-1;
rentg_2y=rentidx/lag8(rentidx)-1;

if indexcode ne lag(indexcode) then rentg_q=.;
if indexcode ne lag2(indexcode) then rentg_6m=.;
if indexcode ne lag4(indexcode) then rentg_1y=.;
if indexcode ne lag8(indexcode) then rentg_2y=.;
date=201803;
run;

data allrentfc; set rent201603(where=(qtr=201903)) rent201703(where=(qtr=201903)) rent201803(where=(qtr=201903)); run;
data test; set rent201703(keep=indexcode qtr rentg_1y) ; where qtr=201803; run;
proc print noobs; run;

data housing; set simhpi.housing; indexcode=put(cbsa_code,$5.); keep indexcode housing;run;
LIBNAME irs ODBC DSN='irs' schema=dbo;

proc sort data=allsim; by indexcode;
data allsim; merge allsim(in=f1) housing; by indexcode; if f1;run;

proc means data=allsim noprint; class simid qtr; weight housing; var vacancy --hpi; output out=US mean= sumwgt=housing;run;
proc means data=US noprint; class qtr;  var vacancy --hpi housing; output out=US_mean mean= ; run;


proc delete data=irs.SimRentHpi_FIXEDHPA;
data irs.SimRentHpi_FIXEDHPA(insertbuff=30000); set allsim us(in=f1); if f1 then indexcode='US';  if simid>0 and indexcode ne '' and qtr>0; RateasofDate=20191206; *if qtr<=202902; drop _TYPE_ _FREQ_;  run;

proc means data=allsim noprint; class indexcode qtr; output out=meanPath mean=;run;
data MeanPath; set MeanPath; if indexcode ne '' and qtr>0; drop _TYPE_ _FREQ_ path_num; run;

proc delete data=irs.RentHPIMeanPath; *_FIXEDHPA;
data irs.RentHPIMeanPath(insertbuff=30000); set MeanPath us_mean(in=f1); if f1 then indexcode='US'; if indexcode ne ''  and qtr>0; RateasofDate=20191206; drop simid _TYPE_ _FREQ_; run;

proc delete data=irs.HistMFcaprateVac;
data irs.HistMFcaprateVac(insertbuff=30000); set pprCBSA; run;





data hist; set irs.hpi_basefile(where=(qtr<=201804 and qtr>201200) keep=cbsa_code qtr us_unemp unemp inc ln_inc  hpi_sa inc_mean hpg_season);
rename inc=inc_p50;
indexcode=put(cbsa_code,$5.);
drop cbsa_code;
run;
proc sort nodup; by  indexcode qtr; run;

data hist; set hist; by indexcode qtr;
hpg_season_l1=lag(hpg_season);
ln_inc_l1=lag(ln_inc);
if first.indexcode then do; hpg_season_l1=.; ln_inc_l1=.; end;
incg=ln_inc - ln_inc_l1;
drop ln_inc_l1;
run;

data hist1; do i=0 to 2000;  simid=i; output; end; keep simid; run;

proc sql; create table hist1 as
select distinct  b.simid, a.*
from hist1 as b
full outer join hist as a
on 1=1;
quit;
proc sort nodup; by indexcode qtr simid; run;



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
