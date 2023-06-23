
*%macro RentFC();
/*
Here's how to run the new rent projection
Open the HPI SAS code  "C:\sas codes\repeated-sale-hpa-v2\amherst hpi - main process.sas" and run all the way to "%initsetup"
The rent projection code is in C:\SAS Codes\rent-forecast-model\Test\Rent Idx No Cluster Forecast v2022 FINAL.sas
*/




proc datasets library=work kill nolist;
quit;

options compress=yes errors=10 source notes;
%let cDrive=\\czhaodev\TDrive\C Drive;
%let tdrive=\\czhaodev\TDrive;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;

%let SAScodedir=&cDrive.\SAS Codes\repeated-sale-hpa-v3.0;
%let outputpath=&tDrive.\Thu Output\HPI\HPI Calculation\v2.0;
%let lt_input=&tDrive.\Thu Output\HPI\HPI Calculation\v2.0\SAS Input\Long term HPI inputs;
%let hist_input=&tDrive.\Thu Output\HPI\HPI Calculation\v2.0\SAS Input\Historical HPI inputs;
%let census=&tDrive.\Data Source\Census Bureau;
/*%let R_EXEC_COMMAND = &cDrive.\Program Files\R\R-3.3.1\bin\x64\Rscript.exe;*/
%let R_EXEC_COMMAND = C:\Program Files\R\R-3.4.3\bin\x64\Rscript.exe;
%let JAVA_BIN_DIR = &cDrive.\Thu Codes\SAS_Base_OpenSrcIntegration\bin;
%let reportout=&tDrive.\Thu Output\HPI\Report and Quality Check\v2.0;
LIBNAME Parm "&lt_out.\parameters";
LIBNAME hpi "&outputpath.";
LIBNAME lt_input "&lt_input.";
LIBNAME lt_out "&lt_out.";
LIBNAME devVo ODBC DSN='devVo' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
*LIBNAME com ODBC DSN='asgcommon' schema=dbo;
LIBNAME krlive ODBC DSN='krlive' schema=dbo;
LIBNAME thirdp ODBC DSN='ThirdPartyData' schema=dbo;
LIBNAME ir ODBC DSN='interestrates' schema=dbo;
LIBNAME wlres ODBC DSN='ThirdPartyData' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME CRE ODBC DSN='CRE' schema=dbo;
libname wlres odbc dsn='ThirdPartyData' schema=dbo;
libname devhu odbc dsn='Devhu' schema=dbo;


%include "&SAScodedir.\Amherst HPI Macros.sas";
%include "&SAScodedir.\HPI long term forecast.sas";
%include "&SAScodedir.\adam_mtmltv.sas";
%include "&SAScodedir.\Agency_MtMLTV.sas";

%let report=&cDrive.\Thu Codes\Report\;


%let rentidxTableName=irs.sf_rentIdx_monthly ;
*%let rentidxTableName=testbed.sf_rentIdx_monthly ;
*%let rentidxTableName=devhu.sf_rentidx_monthly2 ;
*
%let rentidxTableName=irs.sf_rentIdx_monthly_v2 ;



data spfutures;/*https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.quotes.html*/ /*https://www.cmegroup.com/trading/equity-index/us-index/sandp-500.html*/
input date idx;
datalines;
202306	4190.50
202309	4232.25
202312	4273.75
202403	4315.75
202406	4352.00
202409  4380.00
202412	4408.00
202503  4430.00
202506  4445.00
202512  4490.00
202612  4575.00
202712  4660.00
;
run;


%include "&SAScodedir.\HPI long term model fit and score macros v2.4.sas";
%include "&SAScodedir.\HPI long term forecast macros v6.2 IPUMS v2.sas";
%include "&SAScodedir.\Historical Index Fit All.sas";
%include "&SAScodedir.\Historical Index Fit Recent Months.sas";

%include "\\czhaodev\TDrive\C Drive\SAS Codes\rent-forecast-model\Rent Idx No Cluster Forecast v5.sas"
/*
%include "&cDrive.\SAS Codes\rent-forecast-model\Macros For Cluster Rent Forecast.sas";
*/
%include "C:\SAS Codes\repeated-rent-index\Macros for Build Monthly Rent Index v2.sas";


%initsetup; %put &lt_endq &Histenddate &lt_curmon &enddate. &curmon &sysyear.;



%let prev_date=19971201;	%let prev_mon=199712; %let Nsim=2000;

LIBNAME cmbs ODBC DSN='Apollo_CMBS' schema=dbo; 
LIBNAME wlres ODBC DSN='thirdpartydata' schema=dbo; 
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME ahpi ODBC DSN='amhersthpi' schema=dbo;
LIBNAME cre ODBC DSN='cre' schema=dbo;
LIBNAME krlive ODBC DSN='krlive' schema=dbo; 

libname CreMacr '\\czhaodev\TDrive\Thu Output\CRE Macro';	
libname output '\\czhaodev\TDrive\Thu Output\CMBS\';
libname parm '\\czhaodev\TDrive\Thu Output\CMBS\Macro Proj\parameters';
libname oldparm '\\czhaodev\TDrive\Thu Output\CMBS\Macro Proj\parameters2';

libname simoutp '\\czhaodev\TDrive\Thu Output\CMBS\Macro Proj\sim output';
libname macro '\\czhaodev\TDrive\Thu Output\CMBS';

libname IR ODBC DSN='InterestRates' schema=dbo;
LIBNAME devVo ODBC DSN='devVo' schema=dbo;

%include "&cDrive.\SAS Codes\cre-macro-projections\CMBS macrovariable model macros v2.12.sas";
%include "&cDrive.\SAS Codes\cre-macro-projections\macros to support CMBS model 1.sas";

%let inputsrc= &tDrive.\Data Source\CMBS\;
%let outputpath= &tDrive.\Thu Output\CMBS\macro proj;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\;
%let thisMon=201512; %let est_startqtr=200001;
%let startSim=1;%let endSim=100;%let nPath=100; 
%let exclude=if not (metrocode='MUNC' and asgproptype='IN') and not (metrocode='YAKI' and asgproptype='IN') and not (metrocode='VINE' and asgproptype='RT')
and not(metrocode ='FOAR' and asgproptype='RT') and not(metrocode='LAFA') and not (metrocode='FOND')  and not (metrocode='DAVE') and not (metrocode='MOBI') ;
%let lt_input=\\tvodevw10\T$\Thu Output\HPI\HPI Calculation\v2.0\SAS Input\Long term HPI inputs;
%let maxid=100;%let maxqtr=42; %let fcqtr=202101; %let maxfc=42; %let fcqtr_l1=202004;

%let R_EXEC_COMMAND = &cDrive.\Program Files\R\R-3.4.1\bin\x64\Rscript.exe;
%let JAVA_BIN_DIR = &cDrive.\Thu Codes\SAS_Base_OpenSrcIntegration\bin;
%let SAScodedir=&tDrive.\Thu Output\CMBS\Macro Proj;

%let reportdir=&cDrive.\Thu Codes\report\;
%let est_endqtr=201404; 

%let lb=-0.035; %let ub=0.35;
%let shock=; %let nametbl=;
%let tdrive=\\tvodevw10.CORP.amherst.com\T$\;
%let lt_out=&tDrive.\Thu Output\HPI\HPI Forecast\v2.1;


%include "&cDrive.\SAS Codes\rent-forecast-model\Macro For Rent Idx No Cluster Forecast.sas";

%include "&cDrive.\SAS Codes\cre-macro-projections\Inflation Macro.sas";

%let varlistC=capr_ust10y_l2 capr_ust10y_l3 capr_ust10y_l4    ;
%let varlistC=capr_ust10y_g_l1 capr_ust10y_g_l2     ;

LIBNAME irs ODBC DSN='irs' schema=dbo;
LIBNAME thirdp ODBC DSN='thirdpartydata' schema=dbo;
LIBNAME devvo ODBC DSN='devvo' schema=dbo;
LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME parmSF '\\czhaodev\TDrive\\Thu Output\SF REnt\Test'; 
/*
libname SimHPI "T:\Thu Output\HPI\HPI Forecast\v3.0\parameters\TEST"; 
*/

%LoadIn_Hist_IR_Format;
%process_ppr_proj; 






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

data infFC; *https://www.imf.org/external/datamapper/PCPIEPCH@WEO/OEMDC/ADVEC/WEOWORLD/USA;
input yr annualInf;
datalines;
2023 3
2024 2.1
2025 2.1
2026 2
2027 2.3
2028 1.6
2029 1.6
2030 1.6
2031 1.6
2032 1.6
2033 1.6
2034 1.6
;run;


proc SQL; connect to odbc(DSN='interestrates'); create table infBE0 as select * from connection to odbc
(

select 
	tenor/12+ year(getdate())-1 AS yr
	, rate_value
	, asofdate=year(rate_date)*100+month(rate_date)
	,baseyr=year(rate_date)
from interestrates..market_rates_info_dt i
join interestrates..market_rates_dt r
	on i.rate_id = r.rate_id
where rate_date = cast((select max(rate_Date) from interestrates..market_rates_dt) as date)
	and i.rate_type = 'Breakeven Inflation Rate'  
	and i.rate_id not in ('USGGBE20','USGGBE30')
order by yr);quit;

proc sort data=infBE0; by yr; run;
data infbe; set infbe0; 
us='US';
run;
proc sort data=infBE; by us yr; run;

data infBE; set infBE; by us yr; 
output;
if last.us then do; ; yr= yr+1; output; end;
run;

data infBE; set infBE; rate_l1=lag(rate_value);
if yr=baseyr then annualInf=rate_value;
else annualInf=((1+rate_value/100)**(yr+1-baseyr)/((1+rate_l1/100)**(yr-baseyr))-1)*100;
drop rate_l1 rate_value; run;

data infMonthlyBE; set infBE; 
do year=yr to yr+1; 
	do monthID=1 to 12; 
		month=year*100+monthID; 
		infBE=.;
		 if (monthID> mod(asofDate,100)-1 and year=yr) or (monthID<mod(asofDate,100) and year=yr+1) or ( mod(asofDate,100)=1 and monthID>=12 and year=yr-1)
		  /*or (monthid = mod(asofDate,100)-1 and year = baseYr)*/ or  ( mod(asofDate,100)=1 and monthID>=12 and year=baseYr-1)
		then infBE=(annualInf/100+1)**(1/12)-1; 
		
		output;
	end; 
end;
run;
data infMonthlyBE; set infMonthlyBE; if infBE ne .; keep month infBE; run;

/*
data infMonthlyBE; set infMonthlyBE; by month; output; month = 202212; infbe = 0.0016490444; output; run;
proc sort nodup; by month; run;
*/

data cpi; month=.; run;
%add_fred(inp=cpi,sm_url=https://fred.stlouisfed.org/data/CPIAUCSL.txt,sm_var=cpi,sm_firstobs=16);run;
data cpi; set cpi; if cpi ne .;run;

proc sort data=cpi; by month;

proc sql; create table YTDCPI as select distinct lastCPI.month, lastCPI.CPI/LastDec.CPI-1 as YTDCPI 
from cpi lastCPI join (select max(month) as maxmonth from cpi) maxmonth
on lastCPI.month=maxmonth
join (select max(month) as LastDecRec from cpi where mod(month,100)=12) LastDecRec
on 1=1 join cpi LastDec on LastDec.month=LastDecRec;run;


data monthId; do monthID=1 to 12; output; end;run;

proc sql; create table infFCMonthly as select distinct 
yr*100+monthID as Month
, case when YTDCPI.YTDCPI ne . then ((1+annualInf/100)/(1+YTDCPI.YTDCPI))**(1/(12-mod(ytdcpi.month,100)))-1 else (annualInf/100+1)**(1/12)-1 end as MonthlyFCInflation
from infFC 
left join YTDCPI
on infFC.yr=int(YTDCPI.month/100)
join monthID
on 1=1
join YTDCPI ytdmo
on yr*100+monthID>ytdcpi.month ; proc sort nodup;  by  month;run;

data infFC_IMFBE; merge infFCMonthly infMonthlyBE; by month; if MonthlyFCInflation ne .; run;
data infFC_imfBE; set infFC_imfBE(rename=(infBE=tp)); by month; 
retain infBE;
if tp ne . /*and month<&sysyear.+10*/ then infBE = tp;
run;
data CPI_withFC; set CPI(rename=cpi=cpi0)  infFC_IMFBE; by month; retain cpi cpi_BE; if cpi0 ne . then do; cpi=cpi0; cpi_BE=cpi0; end;
else do; cpi=cpi*(1+monthlyFCinflation);  cpi_BE=cpi_BE*(1+infBE); end; keep month cpi cpi_BE;run;

data cpiqtr_withFC; set CPI_withFC; if mod(month,100) in (3,6,9,12);qtr=int(month/100)*100+mod(month,100) /3; drop month; 
data cpiqtr_withFC; set cpiqtr_withFC; by qtr; inflation=(CPI)/lag(CPI)-1; inflation_BE=(cpi_BE)/lag(cpi_BE)-1;
if cpi ne . and cpi_BE ne .;
run;




data HP; set irs.hpi_basefile; indexcode=put(cbsa_Code,$5.); drop cbsa_code; if qtr>0;
indexmonth=input(put(int(qtr/100)*10000+mod(qtr,100)*300+1,8.),YYMMDD10.);format indexmonth  MMDDYYD10.; 
run;


** GET ELASTICITY;

data landelastic;infile "\\czhaodev\TDrive\Thu Output\HPI\HPI Calculation\v3.0\SAS Input\Long term HPI inputs\top100cbsa info_with undevelopable.txt" delimiter = '09'x missover dsd lrecl=32767 firstobs=1 ; format moodyname $50.;
input  moodyid $	moodyname $	cbsa_div	cbsa_code	position saizlandelastic
censusdivision	obslandsles	landvalue adjlandvalue	housingcost	adjhousingcost	wages	constonlywages	
regindex	landinelastic 	constcostindex	rawhousing tradables top50 undevelopable rawsaiz; if not missing(position);
landelastic=-saizlandelastic;
if censusdivision=1 then newengland=1; else newengland=0; if censusdivision=2 then midat=1; else midat=0;  if censusdivision=3 then enc=1; else enc=0;
if censusdivision=4 then wnc=1; else wnc=0; if censusdivision=5 then southat=1; else southat=0;  if censusdivision=6 then esc=1; else esc=0;
if censusdivision=7 then wsc=1; else wsc=0; if censusdivision=8 then mountain=1; else mountain=0; if censusdivision=9 then pacific=1; else pacific=0;
insurerate=0.5; 
indexcode=put(cbsa_div,$5.); keep indexcode landelastic; 
proc sort nodup; by indexcode;run;
/*
proc delete data= testbed.landelastic;
data testbed.landelastic; set landelastic; run;
*/


 /*
data ppr; set cremacr.ppr;run;
data pprCBSA; set cremacr.pprCBSA;run;
*/
data mapCBSA; set cremacr.mapCBSA;run;
 /*data testbed.cre_mapcbsa; set mapCBSA; run;*/

data housing; set irs.housing; indexcode=put(cbsa_code,$5.); keep indexcode housing;run;

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
*%add_fredRent(inp=rate_frm_mo,sm_url=http://research.stlouisfed.org/fred2/data/USD3MTD156N.txt, sm_var=libor_3m, sm_firstobs=33);
*proc export data=rate_frm_mo outfile="&lt_out.\monthly frm30 & swap2-10 rate since 199001.csv" replace; run;
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
/*
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
*/

%put &rentidxTableName.;
/*
 data parmrent; set irs.QtrlyRentIdx ( rename=(index=rentidx0 date=qtr )); geographycode=cbsa_div; keep geographycode qtr rentidx0;  run;
proc sort nodup; by   geographycode descending qtr; run;
 */

%getIncomeTS;

data rawRentidxOrg; set  &rentidxTableName ; date=year(monthfmt)*100+month(monthfmt);* if date=. then date=substr(monthfmt,1,4)*100+substr(monthfmt,6,2); 
drop monthfmt; *if pricetier='agg'; *drop index_SF index_TH cbsa;* if city20=0 or city20=.;
keep indexcode date index;
proc sort nodup; by indexcode date; run;
data rawRentidxOrg; set rawRentidxOrg; by indexcode date; rentg= index/lag(index)-1; if first.indexcode then rentg=.; run;
data rawrentidx0; set rawRentidxOrg; by indexcode date;  if last.indexcode; keep indexcode pricetier  date rentg; run;
data rawrentIdx1; set rawrentIdx0; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;
data rawrentIdx2; set rawrentIdx1; if mod(date,100) not in (3,6,9,12) then do; date=date+1; end; keep indexcode pricetier date rentg; run;

data rawrentIdx3; set rawrentIdx0 rawrentIdx1 rawrentIdx2; proc sort nodup; by indexcode date; run;

%global fcqtrStart histEndMon;
data test; set rawrentidx0; by indexcode date; if last.date; 
histEndMon =  date;
if mod(date,100) in (1,4,7,10) then do;
	IF mod(date,100) =1 then date = (int(date/100)-1)*100+12;
	else date = date-1;
end;
else if mod(date,100) in (2,5,8,11) then do;  histEndMon = date+1;date=date+1; end;
call symput("fcqtrStart", int(date/100)*100+mod(date,100)/3); 
call symput("histEndMon", histEndMon);
call symput("fcqtrmo", int(date/100)*100+mod(date,100)); 
run;
%let fcqtrStart=%eval(&fcqtrStart*1);
%let fcqtrmo=%eval(&fcqtrmo*1);
%let histEndMon=%eval(&histEndMon*1);
%put &fcqtrStart &fcqtrmo &histEndMon;

data rawrentIdx; merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index;
if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0; 
else index=index*(1+rentg); drop rentidx0 rentg0 ; if mod(date,100)  in (3,6,9,12);
if date<=&fcqtrmo.;
run;

data parmrent; set rawRentidx ( rename=(index=rentidx0 date=qtr)); 
geographycode=indexcode; if length(geographycode)=5 or geographycode='US'; keep geographycode qtr rentidx0;  run;
proc sort nodup; by   geographycode descending qtr; run;


data ParmRent; set ParmRent; by geographycode descending qtr; ACaM_rent_g=Rentidx0/lag(Rentidx0)-1; if first.geographycode then ACaM_rent_g=.; run;

/*
data AllRentIdx; merge rentidx2(where=(geographytype='CBSA')) ParmRent(in=f1) StateDerivedRent; by geographycode descending qtr;  retain RentIdx; if f1 then Derived=0; else Derived=1;
if first.geographycode then rentidx=1; else do; if ACaM_rent_g ne . then rentIdx=rentIdx*(1+ACaM_rent_g); 
else if rent_g ne . then rentIdx=rentIdx*(1+rent_g); else rentidx=rentidx*(1+staterent_g); end; 
if rentidx ne .;  run;
*/

data AllRentIdx; set ParmRent(in=f1); by geographycode descending qtr;  retain RentIdx;
if f1 then Derived=0; else Derived=1;
if first.geographycode then rentidx=1; else do; if ACaM_rent_g ne . then rentIdx=rentIdx*(1+ACaM_rent_g); 
*else if rent_g ne . then rentIdx=rentIdx*(1+rent_g);
*else rentidx=rentidx*(1+staterent_g); end; 
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

proc SQL; connect to odbc(DSN='thirdpartydata'); create table baseRent as select 
* from connection to odbc(select a.*,b.state,isnull(cbsa_div,cbsa) as cbsa 
from modeltestbed.dbo.SFR_Rent_CleanUp_new_final a join amhersthpi..hpi_taxroll_vw b
on a.asg_propid=b.asg_propid and b.prop_type ='SF' 
and 2015=year(lease_enddate) and month(lease_enddate)<=3;); 
disconnect from odbc;quit;

proc means data=baseRent noprint; class state; var closingRent; where closingRent between 100 and 10000; output out=baseRent_state p50=baseRent;run;
proc means data=baseRent noprint; class cbsa; var closingRent; where closingRent between 100 and 10000; output out=baseRent_cbsa p50=baseRent;run;

data baseRent_med; set baseRent_cbsa(rename=cbsa=indexcode) baseRent_state(rename=state=indexcode) ; keep indexcode baseRent; where indexcode ne '';run;

proc sql; create table modelinp0b as select distinct a.*, baseRent, c.*,
baseRent/e.rentidx_sa*a.rentidx_sa as medRent,d.*
from allrentIdx a 
join rawRentidx b
on a.indexcode=b.indexcode and b.date=201503
join baseRent_med m on a.indexcode=m.indexcode
join HP c on a.indexcode=c.indexcode and a.qtr=c.qtr left join pprCBSA2 d
on a.indexcode=d.indexcode and c.qtr=d.qtr join allrentIdx e
on a.indexcode=e.indexcode and e.qtr=201501 where a.qtr>=200001
and a.indexcode in (select indexcode from HP); ;run;
proc sort nodup; by indexcode indexmonth; run;

data ipums5; set pp_income3; 
rename qtrfmt = indexmonth;
indexcode =put(cbsa_code,$5.);
keep indexcode qtrfmt hhpers_income qtr; run;

proc sort data = ipums5; by indexcode indexmonth; run;

proc sql; select count (distinct indexcode) from ipums5; quit;

data modelinp1; merge modelinp0b(in=f1) ipums5; by indexcode  indexmonth; if f1; run;

data modelinp1; set modelinp1; by indexcode indexmonth;
retain ln_LtLine Rentidx1 t ;  
if first.indexcode  then  t=1;  else  t+1;  ln_LtLine=t*1.0/4.0;
if first.indexcode  then Rentidx1=1; else Rentidx1=Rentidx1*(exp(LnRentg)); ln_Rentidx=log(Rentidx1);
if abs(LnRentg)>.1 and qtr<202001 then LnRentg=.;
if abs(lnRentg)>.25 then lnrentg=.;

factor= (refi_rate/1200+1)**360*(refi_rate/1200) /((refi_rate/1200+1)**360 -1)*0.8 +(taxrate+insurerate/100)/12;
PITI=log(hpi_sa_medhp*factor);

unemp_l1=lag(unemp);
unemp_g=unemp-unemp_l1;
CMT_10YR_g_l1=lag(CMT_10YR_g);

us_unemp_g=us_unemp-lag(us_unemp);
rentg_l1=lag(LnRentg);
rentg_l2=lag(rentg_l1);
rentg_l3=lag(rentg_l2);
rentg_l4=lag(rentg_l3);
rentg_l8=lag8(LnRentg);
RTI=(medRent/(hhpers_income/3));

Rent2Own=(medRent/(hpi_l1_medhp*factor));
Rent2Own_l1=lag(Rent2Own); if first.indexcode then Rent2Own_l1=.; 
Rent2Own_l2=lag2(Rent2Own_l1); if first.indexcode then Rent2Own_l2=.; 
Rent2Own_l3=lag3(Rent2Own_l2); if first.indexcode then Rent2Own_l3=.; 
Rent2Own_l4=lag4(Rent2Own_l3); if first.indexcode then Rent2Own_l4=.; 
Rent2Own_l6=lag2(Rent2Own_l4); if indexcode ne lag2(indexcode) then Rent2Own_l6=.; 
Rent2Own_l8=lag2(Rent2Own_l6); if indexcode ne lag2(indexcode) then Rent2Own_l8=.; 


RTI_l1=lag(RTI); if first.indexcode then RTI_l1=.;
rentyield=(medRent/hpi_sa_medhp);
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
lo_ust_capr_ust10y_g_l1=lo_ust*capr_ust10y_g_l1;

incg=log(hhpers_income/lag(hhpers_income));
if first.indexcode then incg=.;
run;
proc means data=modelinp1 noprint; var LnRentg rentyield hpg_season unemp incg RTI_l1 Rent2Own ; by indexcode; output out=m_LnRentg mean=m_lnRentg m_rentyield m_hpg_season m_unemp m_incg m_RTI m_Rent2Own ;run;


/*
data test; set modelinp1;where qtr>201500; keep qtr indexcode lnrentg ln_Rentidx Rentidx1; run;
*/
data m_LnRentg; set m_LnRentg;   /* m_rentyield=0; m_lnRentg=0; m_hpg_season=0; m_RTI=0; */  run;

data Ncbsa; set modelinp0b; where qtr=201702 ; keep indexcode  ; proc sort nodup; by indexcode ; run;
data Ncbsa; set Ncbsa;by indexcode ; retain Ncbsa;  Ncbsa+1; keep indexcode  Ncbsa ; run;

proc sql noprint; select max(Ncbsa) into: Ncbsa from Ncbsa;
%put &Ncbsa; %Let Ncbsa=%eval(&Ncbsa);

data modelinp1; merge modelinp1(in=f1) Ncbsa m_LnRentg landelastic (in=f2); by indexcode ; if f1; array Z(*) Z_1-Z_&Ncbsa; 
do i=1 to dim(Z); if i=Ncbsa then Z(i)=1; else Z(i)=0; end; 
length elasType $25.;
if landelastic=. then elasType='Missing Elas';
else if landelastic<-0.4649909 then elasType='Very Elas';
else if landelastic>0.6500187 then  elasType='Very Inelas';
else elasType='Normal Elas';
PITI_nolog=exp(PITI);
run;

proc means data=modelinp1 noprint; weight housing; var  incg LnRentg hpg_season rentyield_l1 rentyield RTI RTI_l1 Rent2Own Rent2Own_l1 Rent2Own_l2 Rent2Own_l3 Rent2Own_l4 Rent2Own_l6 Rent2Own_l8 PITI_nolog
taxrate insurerate hpi_l1_medhp; class qtr;
output out=US_Lnrentg(where=(qtr>0)) mean=US_incg US_Lnrentg US_hpg_season US_rentyield_l1 US_rentyield US_RTI US_RTI_l1
US_Rent2Own US_Rent2Own_l1 US_Rent2Own_l2 US_Rent2Own_l3 US_Rent2Own_l4 US_Rent2Own_l6 US_Rent2Own_l8 US_PITI_nolog US_taxrate US_insurerate US_hpi_l1_medhp;run;
proc means data=US_Lnrentg noprint; var US_incg US_RTI_l1 US_Lnrentg US_hpg_season US_rentyield US_Rent2Own; output out=m_US mean=m_US_incg m_US_RTI m_US_Lnrentg m_US_hpg_season m_US_rentyield m_US_Rent2Own;run;

data _Null_; set m_US; call symput("m_US_RTI",m_US_RTI); call symput("m_US_hpg_season",m_US_hpg_season);  run;
%put &m_US_RTI;

data USrent; set allrentIdx; where indexcode='US'; US_prodLnRentG=lnRentG; keep indexmonth US_prodLnRentG qtr;  run;
data USHPA; set testbed.load_asg_hpi_dt; where indexcode='US'; US_ProdHPI=aggregate; if mod(date,100) in (3,6,9,12); indexmonth=input(put(date*100+1,8.),YYMMDD10.);format indexmonth  MMDDYYD10.; 
keep indexmonth date US_ProdHPI; run; proc sort nodup; by date;

proc x12 data=USHPA date=indexmonth noprint interval=QTR;  var US_ProdHPI;   x11;    output out=USHPI_SA0 d11;    ods select d11; run;

data USHPI_SA; set USHPI_SA0; by indexmonth; US_ProdHPG=log(US_ProdHPI_D11/lag(US_ProdHPI_D11)); qtr=year(indexmonth)*100+month(indexmonth)/3; keep qtr indexmonth us_prodHPG;run;

data USRentHPAAdj; merge USrent USHPI_SA US_Lnrentg; by qtr; qtr=year(indexmonth)*100+month(indexmonth)/3; if indexmonth<today();
US_ProdHPG=US_hpg_season; US_prodLnRentG=US_Lnrentg;if US_ProdHPG ne . and US_prodLnRentG ne .; 

keep indexmonth qtr  US_ProdHPG US_prodLnRentG US_rentyield US_rentyield_l1 US_RTI US_RTI_l1 US_Rent2Own: US_incg; run;

data USRentHPAAdj; set USRentHPAAdj; US_prodLnRentG_l1=lag(US_prodLnRentG); US_prodLnRentG_l2=lag(US_prodLnRentG_l1);
 US_prodLnRentG_l3=lag(US_prodLnRentG_l2); US_prodLnRentG_l4=lag(US_prodLnRentG_l3); run;



/*
proc import datafile="\\tvodevw10.CORP.amherst.com\T$\\Thu Output\HPI\HPI Forecast\sp500.csv" out=tv_sp500      dbms=csv      replace; datarow=2;  getnames=yes;   run;
proc sort nodup; by date ;run;
proc delete data=irs.tv_sp500; run;
data irs.tv_sp500; set tv_sp500; run;
*/

data tv_sp500; set irs.tv_sp500; run;
data sp500;  format month BEST12.; run;
%add_fredRent(inp=sp500,sm_url=http://research.stlouisfed.org/fred2/data/sp500.txt, sm_var=sp500, sm_firstobs=16);
proc sort data =tv_sp500; by date;
proc sort data =sp500; by month; run;
data tv_sp500; merge tv_sp500 sp500(rename=month= date); by date; 
if sp500=. then sp500=close*1; keep date sp500; run;

data tv_sp500; set tv_sp500 (where=(mod(date,100) in (3,6,9,12))); by date; sp500_chg=sp500/lag(sp500)-1; qtr=int(date/100)*100+mod(date,100)/3; sp500_chg_l1=lag(sp500_chg); 
keep sp500_chg qtr sp500_chg_l1 sp500; run;

proc sql; create table modelinp1 as select distinct * from modelinp1 a left join tv_sp500 b on a.qtr=b.qtr
left join cpiqtr_withFC CPI on a.qtr=cpi.qtr
left join USRentHPAAdj us on a.qtr=us.qtr
join m_US(keep=m_US:) on 1=1;run; proc sort nodup; by indexcode qtr; run;
data modelinp1; set modelinp1; by indexcode qtr; hpg_season_l1=lag(hpg_season); 

if indexcode ne lag(indexcode) then hpg_season_l1=.; run;
proc means data=modelinp1 noprint; var LnRentg rentyield hpg_season unemp incg RTI_l1 Rent2Own inflation; by indexcode; output out=m_LnRentg mean=m_lnRentg m_rentyield m_hpg_season m_unemp m_incg m_RTI m_Rent2Own m_inflation;run;
data _null_; set m_lnRentg; call symput("m_inflation",m_inflation);run;
data modelinp1; set modelinp1; RTI_m_l1=RTI_l1-m_RTI;
RTI_US=RTI_l1-US_RTI_l1;
RTI_m_US_m=RTI_l1-m_RTI-(US_RTI_l1-m_US_RTI);
RTI_US_l1=RTI_l1-US_RTI_l1; rentyield_US_l1=rentyield_l1-US_rentyield_l1;
length RTIType $10;
if m_RTI<0.3158344 then RTIType='LowRTI';
else if m_RTI<0.4152359 then RTITYpe='MidRTI';
else RTIType='HighRTI' ;

length YieldType $10;
if m_rentyield<0.0053706 then YieldType='LowYield';
else if m_rentyield<0.0068274 then YieldType='MidYield';
else YieldType='HighYield' ; ;run;
data insample; set modelinp1; *if qtr<=201711; run;


/*
proc means data=insample0 noprint; by indexcode ; var rentGDev; output out=Adj mean=adjrentGDev; run;
*/

%let amherst='12060','13820','15980','16740','19124','19660','20500','23104','23580','24660','26420','27260','28140','29460','31140','32820','33124','33460','34940',
'34980','35840','36420','36740','37340','38940','39460','39580','41700','42680','45300','49180';


proc sort data= inSample nodup; by indexcode; 


data insample_LT; merge insample m_LnRentg(keep=indexcode m_lnRentg m_rentyield m_hpg_season m_unemp m_incg m_RTI); by indexcode; 

lnrentg_US=lnrentg-US_prodLnRentG;
rentg_l1_US=rentg_l1-US_prodLnRentG_l1;
rentg_l2_US=rentg_l2-US_prodLnRentG_l2;
rentg_l3_US=rentg_l3-US_prodLnRentG_l3;
rentg_l4_US=rentg_l4-US_prodLnRentG_l4;

LnRentg_netCPI=LnRentg-log(1+inflation);
LnRentg_netCPI_l1=lag(LnRentg_netCPI);  if indexcode ne lag(indexcode) then LnRentg_netCPI_l1=.;
LnRentg_netCPI_l2=lag(LnRentg_netCPI_l1); if indexcode ne lag(indexcode) then LnRentg_netCPI_l2=.;
LnRentg_netCPI_l3=lag(LnRentg_netCPI_l2); if indexcode ne lag(indexcode) then LnRentg_netCPI_l3=.;
LnRentg_netCPI_l4=lag(LnRentg_netCPI_l3); if indexcode ne lag(indexcode) then LnRentg_netCPI_l4=.;


Lnrentg_m=lnRentg-m_lnRentg;
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

rentyield_l1_m=rentyield_l1-m_rentyield;
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


array Z1_US(*)  rentg1_US_1-rentg1_US_&Ncbsa.;
array Z2_US(*)  rentg2_US_1-rentg2_US_&Ncbsa.;
array Z3_US(*)  rentg3_US_1-rentg3_US_&Ncbsa.;
array Z4_US(*)  rentg4_US_1-rentg4_US_&Ncbsa.;


array ZRTI(*)  RTI_1-RTI_&Ncbsa.;
array ZRTI_m(*)  RTI_m_1-RTI_m_&Ncbsa.;
array ZRTI_US(*)  RTI_US_1-RTI_US_&Ncbsa.;
array ZRTI_m_US_m(*)  RTI_m_US_m_1-RTI_m_US_m_&Ncbsa.;


do i=1 to dim(Z1); 
Z1(i)=Z(i)*rentg_l1;
Z2(i)=Z(i)*rentg_l2;
Z3(i)=Z(i)*rentg_l3;
Z4(i)=Z(i)*rentg_l4;

Z1_m(i)=Z(i)*rentg_l1_m;
Z2_m(i)=Z(i)*rentg_l2_m;
Z3_m(i)=Z(i)*rentg_l3_m;
Z4_m(i)=Z(i)*rentg_l4_m;

Z1_US(i)=Z(i)*rentg_l1_US;
Z2_US(i)=Z(i)*rentg_l2_US;
Z3_US(i)=Z(i)*rentg_l3_US;
Z4_US(i)=Z(i)*rentg_l4_US;

ZRTI(i)=Z(i)*RTI_l1;
ZRTI_m(i)=Z(i)*RTI_m_l1;
ZRTI_US(i)=Z(i)*RTI_US;
ZRTI_m_US_m(i)=Z(i)*RTI_m_US_m;
end;

caprate_yoy=pprcaprate-pprcaprate_l4;
caprate_yoy_l1=lag(caprate_yoy);
sp500_yoy=sp500/lag4(sp500)-1;
unemp_yoy=unemp-lag4(unemp);


RTI_m_US_m_l2=lag(RTI_m_US_m);
RTI_m_US_m_l3=lag2(RTI_m_US_m);
RTI_m_US_m_l4=lag3(RTI_m_US_m);


RTI_m_l2=lag(RTI_m_l1);
RTI_m_l3=lag2(RTI_m_l1);
RTI_m_l4=lag3(RTI_m_l1);


RTI_US_l2=lag(RTI_US);
RTI_US_l3=lag2(RTI_US);
RTI_US_l4=lag3(RTI_US);

if lag3(indexcode) ne indexcode then do;RTI_m_US_m_l4=.;  RTI_US_l4=.; RTI_m_l4=.;end;
if lag2(indexcode) ne indexcode then do;RTI_m_US_m_l3=.;  RTI_US_l3=.;RTI_m_l3=.; end;
if lag(indexcode) ne indexcode then do; RTI_m_US_m_l2=.; RTI_US_l2=.; RTI_m_l2=.;end;
if lag4(indexcode) ne indexcode then do; caprate_yoy_l1=.; sp500_yoy=.; unemp_yoy=.; RTI_m_US_m_l4=.; end;

rent2own_l1_US=rent2own_l1-US_rent2own_l1;
rent2own_l2_US=rent2own_l2-US_rent2own_l2;
rent2own_l3_US=rent2own_l3-US_rent2own_l3;
rent2own_l4_US=rent2own_l4-US_rent2own_l4;
rent2own_l6_US=rent2own_l6-US_rent2own_l6;
rent2own_l8_US=rent2own_l8-US_rent2own_l8;

incg_US=incg-US_incg;
run;
data insample_LT_tp;set insample_LT; by indexcode qtr; inflation_l1=lag(inflation);
US_ProdHPG_l1=lag(US_ProdHPG);
US_rent2own_l2= lag(US_rent2own_l1);
US_incg_l1=lag(US_incg);
if first.indexcode then do;
inflation_l1=.;US_ProdHPG_l1=.;US_rent2own_l2=.;US_incg_l1=.;
end;
run;

LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;


proc delete data=testbed.TV_RENTMODELINP;
data testbed.TV_RENTMODELINP; set modelinp1; keep qtr indexcode rent2own PITI factor refi_Rate medRent hpi_sa_medhp;run;
%macro rentmodelfit();

proc reg data=insample_LT outest=parm adjrsq tableout noprint ; weight housing; 
model US_prodLnRentG=US_prodLnRentG_l1 chgslope0_1 inflation US_ProdHPG US_RTI_l1 US_rent2own_l1 US_incg/selection=stepwise sle=0.01; 

 run; quit; proc print data=parm (where=(_TYPE_ in ('PARMS','T'))); ;run;
 
proc reg data=insample_LT outest=parmCBSA_orig adjrsq tableout noprint;  weight housing; *where derived=0 ;
model LnRentg_US=m_lnRentg rentg_l1_US  RTI_US_l1  rent2own_l1_US incg_US/selection=stepwise sle=0.01 ; 
output out=CBSA_resid r=CBSA_resid; run; proc print data=parmCBSA_orig (where=(_TYPE_ in ('PARMS','T'))); ;run;


data parmCBSA1; merge parmCBSA_orig(rename=(Intercept=int) where=(_TYPE_='PARMS')) ;
array Z(*) Z_1-Z_&Ncbsa; 
do Ncbsa=1 to dim(Z);
	if Z(NCBSA)=. then Intercept=int; else Intercept=int+Z(NCBSA); output;
end;
if rentg_l2_US=. then rentg_l2_US=0;
if rentg_l3_US=. then rentg_l3_US=0;
if rentg_l4_US=. then rentg_l4_US=0;

if m_lnRentg=. then m_lnRentg=0;
if rentyield_US_l1=. then rentyield_US_l1=0;
if RTI_US_l1=. then RTI_US_l1=0;
if rent2own_l1_US=. then rent2own_l1_US=0;
if incg_US=. then incg_US=0;
keep m_lnRentg rentg_l1_US rentg_l2_US rentg_l3_US rentg_l4_US RTI_US_l1 rentyield_US_l1
rent2own_l1_US incg_US Intercept Ncbsa ;
run;
proc sort nodup; by Ncbsa;

data parmCBSA1; merge parmCBSA1(in=f2) Ncbsa(in=f1)  ; by nCBSA ; if f1 and f2; drop Ncbsa; run;

data parmCBSA1; merge parmcbsa1 (in=f1) m_lnRentg(keep=m_lnRentg indexcode rename=m_lnRentg=m_lnRentg0); by  indexcode; 
if m_lnRentg*m_lnRentg0 ne . then Intercept=Intercept+m_lnRentg*m_lnRentg0; drop m_lnRentg0 m_lnRentg;
if rent2own_l1_US=. then rent2own_l1_US=0;
run;

%let parmCBSA=parmCBSA1;
data parm; set parm;
if US_prodLnRentG_l1=. then US_prodLnRentG_l1=0;
if US_prodLnRentG_l2=. then US_prodLnRentG_l2=0;
if US_prodLnRentG_l3=. then US_prodLnRentG_l3=0;
if US_prodLnRentG_l4=. then US_prodLnRentG_l4=0;

data parmSFRent; merge parm(rename=(intercept=int US_prodLnRentG_l1=rentg_l1_0 US_prodLnRentG_l2=rentg_l2_0 US_prodLnRentG_l3=rentg_l3_0 US_prodLnRentG_l4=rentg_l4_0 
 ))  ;  if int=. then int=0; if int2=. then int2=0; incg=US_incg;
if US_ProdHPG=. then US_ProdHPG=0;  if unemp=. then unemp=0;  if inflation=. then inflation=0;
if incg=. then incg=0; if US_rentyield_l1=. then US_rentyield_l1=0;
if US_RTI_l1 =. then US_RTI_l1=0; if US_rent2own_l1 =. then  US_rent2own_l1=0;
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
if Z1(ncbsa)=. then Z1(ncbsa)=0; US_prodLnRentG_l1=rentg_l1_0+Z1(ncbsa);
if Z2(ncbsa)=. then Z2(ncbsa)=0; US_prodLnRentG_l2=rentg_l2_0+Z2(ncbsa);
if Z3(ncbsa)=. then Z3(ncbsa)=0; US_prodLnRentG_l3=rentg_l3_0+Z3(ncbsa);
if Z4(ncbsa)=. then Z4(ncbsa)=0; US_prodLnRentG_l4=rentg_l4_0+Z4(ncbsa);
output;

end; keep Ncbsa Intercept US_prodLnRentG_l1-US_prodLnRentG_l4 US_ProdHPG unemp incg US_rentyield_l1 chgslope0_1 inflation  US_RTI_l1 US_rent2own_l1
;run;

data parmSFRent; merge parmSFRent(in=f2) Ncbsa(in=f1) ; if f1 and f2;
drop Ncbsa; run;


data parmSFRent; merge parmSFRent 
&parmCBSA
(rename=(intercept=psf_intercept_dev rentg_l1_US=psf_rentg_l1_US  rentg_l2_US=psf_rentg_l2_US
rentg_l3_US=psf_rentg_l3_US  rentg_l4_US=psf_rentg_l4_US  RTI_US_l1=psf_RTI_US_l1 rent2own_l1_US=psf_rent2own_l1_US rentyield_US_l1=psf_rentyield_US_l1 incg_US=psf_incg_US
))

; by indexcode;
if psf_rentg_l1_US=. then psf_rentg_l1_US=0;
if psf_rentg_l2_US=. then psf_rentg_l2_US=0;
if psf_rentg_l3_US=. then psf_rentg_l3_US=0;
if psf_rentg_l4_US=. then psf_rentg_l4_US=0;
if psf_intercept_dev=. then psf_intercept_dev=0;
 run;

data parmSF.parmSFRent; set parmSFRent(rename=(Intercept=psf_Intercept 
US_prodLnRentG_l1=psf_US_prodLnRentG_l1  US_prodLnRentG_l2=psf_US_prodLnRentG_l2   US_prodLnRentG_l3=psf_US_prodLnRentG_l3
US_prodLnRentG_l4=psf_US_prodLnRentG_l4 US_ProdHPG=psf_US_ProdHPG
unemp=psf_unemp incg=psf_incg US_rentyield_l1=psf_US_rentyield_l1  chgslope0_1=psf_chgslope0_1 inflation=psf_inflation US_rent2own_l1=psf_US_rent2own_l1
US_RTI_l1=psf_US_RTI_l1  ));
keep indexcode psf_:; run; 
%mend;
*%rentmodelfit;


/*
proc sql; create table allparm as select * from  parmSF.parmSFRent, parmSF.parmC, parmSF.parmV, parmSF.parmsp;run;

proc delete data=irs.rentforecast_parm;  
data irs.rentforecast_parm;  set allparm; run;
*/


data allparm; set  irs.rentforecast_parm;  run;
data allparm;set allparm;  array parm(*) p:;  do i=1 to dim(parm); if parm(i)=. then parm(i)=0; end; drop i;run;

/*
proc delete data=irs.errmat_rentfc; 
data irs.errmat_rentfc(insertbuff=32000); set parm.errmat_rentfc; run;
proc delete data=irs.errmat_sp1; 
data irs.errmat_sp1(insertbuff=32000); set parm.errmat_sp1; run;

data errmat_rentfc; set parm.errmat_rentfc;run;
data errmat_sp1; set parm.errmat_sp1;run;

*/

data errmat_rentfc; set irs.errmat_rentfc;run;
data errmat_sp1; set irs.errmat_sp1;run;
*%genResid;
*%getRandomErrAll;
*%getSimRates;

data ln_seasonality0; set allrentIdx; by indexcode  qtr; seasonality=log(Rentidx/lag(Rentidx))-log(Rentidx_sa/lag(Rentidx_sa));
if first.indexcode  then delete; proc sort; by indexcode  DESCENDING qtr;run;
data ln_seasonality; set ln_seasonality0; if indexcode ne lag4(indexcode) ;  qtridx=qtr-int(qtr/100)*100; 
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
 keep path_num  month refi_rate cmt_2yr cmt_10yr libor_3m rate_timestamp; proc sort nodup; by month; run;

data rate_frm_mo2; set rate_frm_mo;  do path_num=0 to 1000; output; end; run;
data rate_frm2_0; set rate_frm_mo2 rate2(in=f2); if f2 then priority=0; else priority=1; 
if mod(month,100)<=12;
run;
proc sort nodup; by path_num month priority;
data rate_frm2_0; set rate_frm2_0; by path_num month priority; if last.month;run; 

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
proc means data=rate_frm2; var refi_rate; class qtr; where qtr<202800; quit;

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
/*
data irs.sp500forecast_parm; set parmSF.parmSP; run;
*/
data parmSP_Sim; set  irs.sp500forecast_parm; do path_num=0 to 1000; output; end; run; 


data simHPI_US; set irs.simHPIpath;*simHPI.FIXEDSIM0-simHPI.FIXEDSIM1000; 
keep qtr path_num us_unemp; if qtr>0 and us_unemp>0; run; proc sort nodup; by path_num qtr us_unemp;run;
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

proc sql; create table CPI_Sim0 as select distinct * from cpiqtr_withFC join cbsa on 1=1 where qtr>=201600;run; quit;

data CPI_Sim; set  CPI_Sim0; do simid=0 to 1000; output; end; proc sort nodup; by simid indexcode qtr; run; 


data qtr; set cbsa; do simid=0 to 1000; do year=2018 to int(&fcqtrStart/100)+11;
do qidx=1 to 4; if &fcqtrStart-200<=year*100+qidx<=&fcqtrStart+30000 then do; qtr=year*100+qidx; output; end; end; end;
end; keep indexcode  qtr simid; proc sort nodup; by simid indexcode qtr; run;

proc sort data=ipums5; by indexcode qtr; run;
data ipums5_sim0; set ipums5(keep=indexcode qtr hhpers_income); by indexcode qtr; lnIncChg0=log(hhpers_income/lag(hhpers_income));
drop hhpers_income; if first.indexcode then lnIncChg0=.; if lnIncChg0 ne .; run;
data ipums5_sim0; merge ipums5_sim0(in=f1) ; if qtr>=201600;do simid=0 to 1000; output; end;run; proc sort nodup; by simid indexcode qtr; run;

data ipums5_sim0_1; merge ipums5_sim0 qtr(in=f1); by simid indexcode qtr; if f1; retain lnIncChg; if first.indexcode then lnIncChg=.;
if lnIncChg0 ne . then lnIncChg=lnIncChg0;drop lnIncChg0; run;
proc sort nodup; by indexcode simid qtr;
data housing; set irs.housing; indexcode=put(cbsa_code,$5.); keep indexcode housing;run;

data ipums5_sim0_2; merge ipums5_sim0_1 (in=f1) housing NCBSA (in=f2); by indexcode; if f1 and f2;; run;

proc means data=ipums5_sim0_2 noprint; weight housing; class simid qtr; var lnIncChg; output out=US_lnIncChg mean=US_lnIncChg;run;

data US_lnIncChg; set US_lnIncChg; where simid>0 and qtr>0; indexcode='US'; 
proc sort data=ipums5_sim0_2 nodup;  by simid qtr;
proc sort data=US_lnIncChg nodup;  by simid qtr;
data ipums5_sim; merge ipums5_sim0_2 US_lnIncChg(keep=simid qtr US_lnIncChg); by simid qtr; proc sort nodup; by simid indexcode qtr; run;

%let HPAshock=0; %let  infShock=0; 

%let startsim=1; %let endsim=10;

%macro loopSim(startsim=,endsim=, infShock=0, HPAShock=0);
%put &startsim;
data simHistHPA; set HP; if qtr>=201600; do simid=&startsim to &endsim; output; end; 
keep qtr hpg_season indexcode simid; proc sort nodup; by simid indexcode qtr; run;

data simHistHPA; set simHistHPA; ; by simid indexcode qtr; hpg_season_l1=lag(hpg_season);  if  first.indexcode then hpg_season_l1=.;  run;

proc sort data=errMat_RentFC; by  indexcode simid qtridx; run;
data errMat2; set errMat_RentFC(where=(&startsim<=simid<=&endsim));by indexcode simid qtridx; retain qtr; if first.simid then qtr=&fcqtrStart; 
else do; 
if mod(qtr,100)=4 then qtr=qtr+100-3; else qtr+1;
end; drop qtridx; run;


data qtr0; set cbsa; do simid=&startsim to &endsim; do year=2018 to int(&fcqtrStart/100)+10;
do qidx=1 to 4; if &fcqtrStart-300<=year*100+qidx<=&fcqtrStart+30000 then do; qtr=year*100+qidx; output; end; end; end;
end; keep indexcode  qtr simid; run;

proc sort data=qtr0; by indexcode qtr; 

data qtr; merge qtr0  m_LnRentg(keep=indexcode m_LnRentg m_hpg_season m_RTI m_inflation); by indexcode ; m_US_RTI=&m_US_RTI;  m_US_hpg_season=&m_US_hpg_season; run;
proc sort data=qtr nodup; by simid qtr; run;
proc sort data=rate_frm2; by path_num qtr; run;

data qtr; merge qtr(in=f1) rate_frm2(where=(&startsim<=simid<=&endsim) in=f2 rename=path_num=simid keep=path_num qtr
cmt_10yr cmt_10yr_g slope slope_l1 refi_rate cmt_2yr cmt_10yr libor_3m chgslope0_1) fcSP_FINAl; by simid qtr; if f1 and f2; run;

proc sort data=qtr; by simid indexcode qtr ; proc sort data=errMat2; by simid indexcode qtr ; 
data SimHPI; set  irs.simHPIpath(where=(PATH_NUm>=&startsim. and path_num<=&endsim.));
by path_num cbsa_Code qtr;
hpg_season=ln_hpi_season-lag(ln_hpi_season);
if first.cbsa_code  then do; incg=.; hpg_season=.; end;
hpg_season_l1=lag(hpg_season);
if first.cbsa_Code then hpg_season_l1=.; 
indexcode=put(cbsa_Code,$5.); simid=path_num;drop cbsa_Code  path_num; run;
proc sort data=SimHPI; by simid indexcode qtr; run;
proc sort data=qtr; by simid indexcode qtr; run;

data qtr; merge qtr(in=f1) SimHPI(in=f2 keep=simid m_ln_hpi_season hpg_season unemp qtr indexcode unemp SFDHousehold  hpg_season_l1 inc_mean inc_p50
ln_hpi_season rename=(hpg_season=hpg_season_sim hpg_season_l1=hpg_l1_sim)) simHistHPA errMat2; *;
by simid indexcode qtr ; if f1 and f2;
if hpg_season=. then hpg_season=hpg_season_sim; if hpg_season_l1=. then hpg_season_l1=hpg_l1_sim; drop hpg_l1_sim hpg_season_sim; proc sort nodup; by indexcode simid;run;

data qtr1; merge qtr(in=f1) allparm housing; by indexcode; if f1;
proc means data=qtr1 noprint; class qtr simid; weight housing; var hpg_season; output out=us_simHPA  mean=US_ProdHPG;run;
proc sort data=qtr1; by qtr simid; run;
data qtr1; merge qtr1 us_simHPA(keep=qtr simid US_ProdHPG); by qtr simid; where qtr >0 and simid>0;

proc sort nodup; by simid indexcode qtr;run;


data fc2; set fc(keep= US_rent2own rent2own indexcode qtr  LnRentg ln_LTLine rentyield  RTI US_RTI ln_rentidx US_ProdLnRentG US_rentyield RTI_m_US_m taxrate
vacancy pprcaprate capr_ust10y rename=(LNrentg=LNrentg0 vacancy=vacancy0 pprcaprate=pprcaprate0  ln_LTLine=ln_LTLine0  ln_rentidx=ln_rentidx0
rentyield=rentyield0 US_rentyield=US_rentyield0 capr_ust10y=capr_ust10y0 US_ProdLnRentG=US_ProdLnRentG0 RTI=RTI0 US_RTI=US_RTI0 RTI_m_US_m=RTI_m_US_m0
US_rent2own=US_rent2own0 rent2own=rent2own0 taxrate=taxrate0));
if qtr>=&fcqtrStart-300;do simid=&startsim to &endsim; output; end;

proc sort nodup; by simid indexcode qtr;run;

data fc3; retain indexcode simid qtr vacancy pprcaprate  US_ProdLnRentG US_ProdLnRentG_l1 psf_US_ProdLnRentG_l1 US_ProdLnRentG_l2 psf_US_ProdLnRentG_l2 US_ProdLnRentG_l3 
psf_US_ProdLnRentG_l3 US_ProdLnRentG_l4 psf_US_ProdLnRentG_l4
US_ProdHPG psf_US_ProdHPG unemp psf_unemp  psf_incg inflation psf_inflation
US_rentyield_l1 psf_US_rentyield_l1 psf_Intercept resid_USrent psf_chgslope0_1 chgslope0_1
;  merge fc2 qtr1 cpi_sim ipums5_sim; by simid indexcode qtr;where simid>=&startsim. and simid<=&endsim. ;
retain factor_l1 factor
vacancy_l1 vac_xbeta_l1 rentyield_l1 US_rentyield_l1
vacancy vac_xbeta rentyield US_rentyield RTI RTI_l1 RTI_m_l1 US_RTI US_RTI_l1
capr_ust10y_l3 capr_ust10y_l2 capr_ust10y_l1 capr_ust10y
rentg_l1 rentg_l2 rentg_l3 rentg_l4  lnRentg US_ProdLnRentG US_ProdLnRentG_l1 US_ProdLnRentG_l2 US_ProdLnRentG_l3 US_ProdLnRentG_l4 
pprcaprate  capr_ust10y_g_l2 capr_ust10y_g_l1 capr_ust10y_g ln_rentidx_l1 ln_rentidx  pprcaprate_l1 capr_g_l1 capr_g   RTI_m_US_m rent2own_l1 rent2own US_rent2own US_rent2own_l1 taxrate
;

refi_l1=lag(refi_rate);  

if resid_rent=. then resid_rent=0; if resid_vacancy=. then resid_vacancy=0; if resid_caprate=. then resid_caprate=0; if resid_sp=. then resid_sp=0;
inflation=inflation_BE;
hpg_season=hpg_season+&HPAshock;
US_ProdHPG=US_ProdHPG+&HPAshock;
inflation=inflation+&infShock;

if first.indexcode  then do; 

refi_l1=.; vacancy_l1 =.;vac_xbeta_l1=.; rentyield_l1=.; US_rentyield_l1=.;  pprcaprate_l1=.; capr_g_l1 =.;capr_g =.; 
vacancy=.; vac_xbeta=.; rentyield=.; US_rentyield=.; RTI=.;RTI_l1=.; RTI_m_l1=.; US_RTI=.; US_RTI_l1=.;
capr_ust10y_l3=.; capr_ust10y_l2=.; capr_ust10y_l1 =.;capr_ust10y=.; RTI_m_US_m=.;
rentg_l1 =.;rentg_l2=.; rentg_l3=.; rentg_l4=.;  lnRentg=.; taxrate=taxrate0;
US_ProdLnRentG_l1 =.;US_ProdLnRentG_l2=.; US_ProdLnRentG_l3=.; US_ProdLnRentG_l4=.;  US_ProdLnRentG=.;
pprcaprate=.;  capr_ust10y_g_l2=.;capr_ust10y_g_l1=.;capr_ust10y_g=.; ln_rentidx_l1 =.; ln_rentidx=.; factor_l1=.; US_rent2own=.; US_rent2own_l1=.; rent2own=.; rent2own_l1=.;
end;
if taxrate0 ne . then taxrate=taxrate0;
hi_unemp=unemp>0.07;hi_ust=cmt_10yr>5.5;lo_ust=cmt_10yr<2.5;   ln_rentidx_l1=ln_rentidx; 
vacancy_l1=vacancy; vac_xbeta_l1=vac_xbeta;rentyield_l1=rentyield; US_rentyield_l1=US_rentyield;
RTI_l1=RTI; RTI_m_l1=RTI_l1-m_RTI; US_RTI_l1=US_RTI;  factor_l1= factor;
capr_ust10y_l3=capr_ust10y_l2; capr_ust10y_l2=capr_ust10y_l1; capr_ust10y_l1=capr_ust10y;
rentg_l4=rentg_l3; rentg_l3=rentg_l2; rentg_l2=rentg_l1; rentg_l1=lnRentg;
rent2own_l1=rent2own; US_rent2own_l1=US_rent2own; 
US_ProdLnRentG_l4=US_ProdLnRentG_l3; US_ProdLnRentG_l3=US_ProdLnRentG_l2; US_ProdLnRentG_l2=US_ProdLnRentG_l1; US_ProdLnRentG_l1=US_ProdLnRentG;

capr_ust10y_g_l2=capr_ust10y_g_l1; capr_ust10y_g_l1=capr_ust10y_g; 
pprcaprate_l1=pprcaprate; capr_g_l1=capr_g; unemp_g=unemp-lag(unemp);

if qtr<&fcqtrStart  and pprcaprate0>0 then do;
rentyield=rentyield0; US_rentyield=US_rentyield0; RTI=RTI0; US_RTI=US_RTI0;
vacancy=vacancy0;   if vacancy=0 then vac_xbeta=-7;		else if Vacancy=1 then 
vac_xbeta=7;	else vac_xbeta=max(-7,min(7,log(Vacancy/(1-Vacancy))));
pprcaprate=pprcaprate0; capr_ust10y=capr_ust10y0;
capr_ust10y_g=capr_ust10y-capr_ust10y_l1; 
lnRentg=lnRentg0; ln_rentidx=ln_rentidx0;  US_ProdLnRentG=US_ProdLnRentG0;
capr_g=pprcaprate0-pprcaprate_l1; US_rent2own=US_rent2own0; rent2own=rent2own0; 

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
resid_USrent=0;

if pprcaprate<=0.001 then do; pprcaprate=0.001; capr_ust10y=pprcaprate-cmt_10yr/100; 
capr_ust10y_g=capr_ust10y-capr_ust10y_l1;  end;
US_ProdLnRentG=(US_ProdLnRentG_l1)*psf_US_ProdLnRentG_l1+(US_ProdLnRentG_l2)*psf_US_ProdLnRentG_l2+(US_ProdLnRentG_l3)*psf_US_ProdLnRentG_l3+(US_ProdLnRentG_l4)*psf_US_ProdLnRentG_l4
+US_ProdHPG*psf_US_ProdHPG+unemp*psf_unemp+US_lnIncChg*psf_incg+inflation*psf_inflation
+US_rentyield_l1*psf_US_rentyield_l1+psf_Intercept+resid_USrent+psf_chgslope0_1*chgslope0_1+psf_US_RTI_l1*US_RTI_l1+psf_US_rent2own_l1*US_rent2own_l1;

lnRentG=psf_intercept_dev+(rentg_l1-US_ProdLnRentG_l1)*psf_rentg_l1_US+(rentg_l2-US_ProdLnRentG_l2)*psf_rentg_l2_US+(rentg_l3-US_ProdLnRentG_l3)*psf_rentg_l3_US
+(rentg_l4-US_ProdLnRentG_l4)*psf_rentg_l4_US+US_ProdLnRentG+psf_rentyield_US_l1*(rentyield_l1-US_rentyield_l1)+psf_RTI_US_l1*(RTI_l1-US_RTI_l1)
+psf_rent2own_l1_US*(rent2own_l1-US_rent2own_l1)+(lnIncChg-US_lnIncChg)*psf_incg_US;

if lnRentg0 ne . then lnRentg=lnRentg0; if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0;
US_rentyield=US_rentyield_l1*exp(US_ProdLnRentG)/exp(US_ProdHPG);
rentyield=rentyield_l1*exp(lnrentg)/exp(hpg_season); 
RTI=RTI_l1*exp(lnRentG)/exp(lnIncChg);

ln_rentidx=ln_rentidx_l1+lnrentg;
US_RTI=US_RTI_l1*exp(US_ProdLnRentG)/exp(US_lnIncChg);

factor= (refi_Rate/1200+1)**360*(refi_Rate/1200) /((refi_Rate/1200+1)**360 -1)*0.8 +(taxrate+0.5/100)/12;

Rent2Own=Rent2Own*exp(lnRentg)*factor_l1/(factor*exp(hpg_season));
US_Rent2Own=US_Rent2Own*exp(US_ProdLnRentG)*factor_l1/(factor*exp(US_ProdHPG));


if lnRentg0 ne . then lnRentg=lnRentg0; if ln_rentidx0 ne . then ln_rentidx=ln_rentidx0; if US_RTI0 ne . then US_RTI=US_RTI0; if RTI0 ne . then RTI=RTI0;
if US_rent2own0 ne . then US_rent2own= US_rent2own0; if rent2own0 ne . then rent2own=rent2own0;
end;
fundspread=ln_hpi_season-m_ln_hpi_season;
qtridx=mod(qtr,100);
keep qtr qtridx indexcode simid  vacancy pprcaprate lnRentg hpg_season fundspread  unemp inc_p50 inc_mean
SFDHousehold  refi_rate cmt_2yr cmt_10yr libor_3m capr_ust10y_g_l1 capr_ust10y_g capr_ust10y cmt_10yr pprcaprate sp500_chg sp500_idx inflation;
run;

data hpiseasonality; set  irs.ln_seasonality(rename=(qtr=qtridx seasonality=seasonalityHPI));
indexcode=put(cbsa_code,$5.); drop cbsa_code;run;

proc sort data=fc3; by indexcode  qtridx; 
data fc4; merge fc3(in=f1) ln_seasonality hpiseasonality; by indexcode  qtridx; 
rentg=exp(lnrentg+seasonality)-1;  hpg=exp(hpg_season+seasonality)-1; 
rentg_sa = exp(lnRentg)-1; 
if f1; if rentg=. then rentg=lnrentg; if hpg=. then hpg=hpg_season;
if qtr>=&fcqtrstart; if rentg ne . and hpg ne .; run;
/*proc means;run;*/
proc sort nodup; by simid indexcode qtr; run;

data fc5; set fc4; by simid indexcode qtr; 
retain rentidx rentidx_sa HPI cpi;
if first.indexcode then do; rentidx=1; rentidx_sa=1; hpi=1; CPI=1; end;
else do; rentidx=rentidx*(1+rentg); rentidx_sa = rentidx_sa *(1+rentg_sa);
hpi=hpi*(1+hpg);CPI=CPI*(1+inflation); end;
keep qtr  indexcode simid  vacancy pprcaprate rentidx hpi fundspread  unemp SFDHousehold Inc_p50 Inc_mean refi_rate
cmt_2yr cmt_10yr libor_3m  capr_ust10y_g_l1 capr_ust10y_g capr_ust10y cmt_10yr pprcaprate sp500_chg sp500_idx CPI inc_p50 inc_mean
rentg rentg_sa hpg rentidx_sa
;
run;
data allSim; set allSim fc5; if simid ne .; run;
%mend;

data ALlSim;run;
%loopSim(startsim=0, endsim=500, infShock=0, HPAShock=0);
 %loopSim(startsim=501, endsim=1000, infShock=0, HPAShock=0);
LIBNAME irs ODBC DSN='irs' schema=dbo;

proc sort data=allsim; by indexcode simid qtr; run;
data allsim; merge allsim(in=f1) housing ; by indexcode; if f1; run;

proc means DATA =ALLSIM; class qtr; var rentg rentg_sa; weight housing; where qtr<202800 AND INDEXCODE='12060'; run;

proc sql; select count(distinct indexcode) from allsim; quit;

/*
proc sql; create table allsim as select distinct * from allsim b join (select distinct simid,indexcode,qtr from allsimprod) a
on b.simid=a.simid and b.indexcode=a.indexcode and b.qtr=a.qtr;run;
*/


data allrent ;  merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index; 
if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0;
else index=index*(1+rentg); drop rentidx0  ; 
datefmt=input(put(date*100+1,8.),yymmdd10.);format datefmt  date9.;
if month(datefmt) in (3,6,9,12);
qtr = year(datefmt)*100+qtr(datefmt);
if date<=&fcqtrmo.;
keep indexcode qtr index datefmt; run;

proc x12 data=allrent date=datefmt noprint interval=qtr; by indexcode; var index;    x11;    output out=sa d11;    ods select d11; run;

data rent_sa; merge allrent(in=f1) sa(in=f2 rename=(index_d11 = index_sa)); by indexcode datefmt; run;
data rent_sa; set rent_sa; by indexcode datefmt;
rentg= index/lag(index)-1;
rentg_sa = index_sa/lag(index_sa)-1;
if first.indexcode then do; rentg=.; rentg_sa=.; end;
run;

proc sql; create table CBSADerivedState as select distinct  a.state  ,b.datefmt
,sum(a.weight*(rentg))/sum(a.weight) as DerivedRentg
,sum(a.weight*(rentg_sa))/sum(a.weight) as DerivedRentg_SA
from irs.tv_CBSAstate a  
join rent_sa b on a.cbsa_div=indexcode
where b.qtr<=&fcqtrstart. 
and cbsa_div in (select distinct indexcode from allsim(where=(qtr>&fcqtrstart.)) )
group by a.state,b.datefmt;
quit;

data allstate; merge CBSADerivedState(in=f1) rent_sa(in=f2 where=(length(state)=2) rename=(indexcode=state)); by state datefmt; if f1 and f2;
keep state qtr datefmt rentg rentg_sa DerivedRentg DerivedRentg_SA; 
run;

proc sql; create table derivedUS as 
select distinct a.*,b.housing 
from rent_sa a, housing  b
where a.indexcode=b.indexcode 
and a.indexcode in (select distinct indexcode from allsim(where=(qtr>&fcqtrstart.)) )
order by a.indexcode,a.datefmt;
quit;

data derivedUS; set derivedUS; by indexcode datefmt; 
if first.indexcode then delete;  run;
proc means noprint nway; weight housing; var rentg rentg_SA; class datefmt; output out=derivedUSRentg mean=DerivedRentg DerivedRentg_SA; run;

data allus; merge rent_sa(where=(indexcode='US') in=f1) derivedUSRentg(in=f2); by datefmt; if f1 and f2; 
keep indexcode qtr datefmt rentg rentg_sa DerivedRentg DerivedRentg_SA; 
run;


proc sql; create table missingstate as 
select distinct *
from rent_sa a 
join derivedUSRentg b 
on a.datefmt=b.datefmt and indexcode not in (select state from allstate)
and length(a.indexcode)=2
and a.indexcode ne 'US';
;
quit;
proc sort nodup; by indexcode datefmt; run;


data CombineStateUS; set AllState  AllUS(RENAME=(indexcode=STATE)) missingstate(RENAME=(indexcode=STATE)); by state datefmt;
qtr = year(datefmt)*100+qtr(datefmt);
keep state qtr rentg rentg_sa Derivedrentg derivedrentg_SA datefmt; 
run;

proc reg data=CombineStateUS outest=parmDerivedStateUS noprint; where 201001<=qtr<=201912; by state; model rentg=Derivedrentg/selection=stepwise;run;
data  parmDerivedStateUS; set parmDerivedStateUS; keep state intercept Derivedrentg; if Derivedrentg ne . and Derivedrentg>0; run;

proc sql; create table missingParm as 
select distinct a.state, b.intercept, b.derivedRentg
from CombineStateUS a
join parmDerivedStateUS b
on b.state='US'
where a.state not in (select state from parmDerivedStateUS) 
order by state;
quit;

data parmDerivedStateUS; set parmDerivedStateUS missingParm; run;


proc means data=allsim noprint nway; class   simid qtr; weight housing; var vacancy --hpi cmt_10yr sp500_chg sp500_idx CPI; output out=US mean= sumwgt=housing;run;

data CBSAstate; merge myresult.CBSAStateAVM(in=f1) myresult.CBSAStateSFD(in=f2); by state cbsa_div; if f1 and f2;
weight=N*priceAVM; keep state cbsa_div weight; if cbsa_div ne ''; run;
proc sort; by cbsa_div; run;

PROC SQL; create table allsim_state as
select distinct a.*, b.state, b.weight
from allsim a
join irs.tv_CBSAstate b
on a.indexcode=b.cbsa_div 
order by state, indexcode, simid;
quit;
proc means data= allsim_state noprint nway; class   state simid qtr; var vacancy --hpi sp500_chg sp500_idx cmt_10yr; weight weight; output out=state mean=; run;

data fcCbsaState; set state(in=f1) us(in=f2); if f2 then state='US'; 
rename rentg=derivedRentg rentg_sa = derivedRentg_sa;
keep state simid rentg rentg_sa qtr;
run;


proc sql; create table fcCbsaState as 
select distinct  a.state ,qtr, simid,  intercept+a.derivedRentg*b.derivedRentg as rentg, intercept+a.derivedRentg_SA*b.derivedRentg as rentg_sa 
from fcCbsaState a 
join parmDerivedStateUS b 
on a.state=b.state ;
quit;
proc sort; by state simid qtr; run;

data fccbsastate; set fccbsastate; by state simid qtr;
retain rentidx rentidx_sa;
if first.simid then do; rentidx=1; rentidx_sa=1;
end;
else do;
rentidx = rentidx*(1+rentg);
rentidx_sa = rentidx_sa*(1+rentg_sa);
end;
run;

data state1; merge state(in=f1 drop=rentidx rentidx_sa rentg rentg_sa) fccbsastate(in=f2 keep=state qtr simid rentidx rentidx_sa rentg rentg_sa); by state simid qtr;if f1; run;
data us1; merge us(in=f1 drop=rentidx rentidx_sa rentg rentg_sa) fccbsastate(in=f2 keep=state qtr simid rentidx rentidx_sa rentg rentg_sa where=(state='US'));
by  simid qtr;if f1; run;

proc means data=state1 noprint nway; class    state qtr; var vacancy --hpi rentidx rentidx_sa rentg rentg_sa; output out=state_mean mean=; run;
proc means data=US1 noprint nway; class    qtr;  var vacancy --hpi housing sp500_chg sp500_idx  cmt_10yr CPI rentidx rentidx_sa rentg rentg_sa; output out=US_mean mean= ; run;


data US_mean1; set US_mean; if qtr in (202302,202502,202702);rentg=rentidx-1; indexcode='US'; hpa=hpi-1; CPI=CPI-1; keep   indexcode qtr rentg hpa inflation CPI; run;
proc transpose data=US_mean1 out=US_mean1; by   indexcode; id qtr;run; /* proc print data=US_mean1;run; proc print data=Amherst_mean1;run;*/

data allsim_output; set allsim us1(in=f1 rename=(state=indexcode)) state1(rename=(state=indexcode) in=f2);
*if f1 then indexcode='US'; 
if  indexcode ne '' and qtr>0;
drop housing _TYPE_ _FREQ_;
run;

proc sort data=allsim_output; by   indexcode simid descending qtr; run;
data allsim_output_Monthly0; set allsim_output; by   indexcode simid descending qtr; 
date=int(qtr/100)*100+mod(qtr,100)*3;
idx=1;
vacancy_l1=lag(vacancy); 
pprcaprate_l1=lag(pprcaprate);
sp500_idx_l1=lag(sp500_idx); 
unemp_l1=lag(unemp);
inc_p50_l1=lag(inc_p50);
inc_mean_l1=lag(inc_mean);
capr_ust10y_l1=lag(capr_ust10y);
rentidx_l1=lag(rentidx);
rentidx_sa_l1 = lag(rentidx_sa);
HPI_l1=lag(HPI);
fundspread_l1= fundspread;
drop sp500_chg capr_ust10y_g_l1 capr_ust10y_g qtr;
if first.simid then do; vacancy_l1=.;pprcaprate_l1=.; sp500_idx_l1=.; unemp_l1=.; inc_p50_l1=.; inc_mean_l1=.; capr_ust10y_l1=.; rentidx_l1=.; rentidx_sa_l1=.; hpi_l1=.; fundspread_l1=.; end;
run;
data allsim_output_Monthly1; set allsim_output_Monthly0; idx+1; 
if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;
data allsim_output_Monthly2; set allsim_output_Monthly1; idx+1;  
if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;

data allsim_output_Monthly; set allsim_output_Monthly0 allsim_output_Monthly1 allsim_output_Monthly2; 
by    indexcode simid descending date;
if vacancy_l1 ne . then do;
vacancy=(vacancy_l1-vacancy)*((idx-1)/3)+vacancy;  
pprcaprate=(pprcaprate_l1-pprcaprate)*((idx-1)/3)+pprcaprate;  
sp500_idx=(sp500_idx_l1/sp500_idx)**((idx-1)/3)*sp500_idx;  
unemp=(unemp_l1-unemp)*((idx-1)/3)+unemp  ;
inc_p50=(inc_p50_l1/inc_p50)**((idx-1)/3)*inc_p50  ;
inc_mean=(inc_mean_l1/inc_mean)**((idx-1)/3)*inc_mean  ;
capr_ust10y=(capr_ust10y_l1-capr_ust10y)*((idx-1)/3)+capr_ust10y  ;
rentidx=(rentidx_l1/rentidx)**((idx-1)/3)*rentidx  ;
rentidx_sa = (rentidx_sa_l1/rentidx_sa)**((idx-1)/3)*rentidx_sa  ;
HPI=(HPI_l1/HPI)**((idx-1)/3)*HPI  ;;

fundspread= (fundspread_l1-fundspread)*((idx-1)/3)+fundspread  ;
end;
if vacancy_l1 ne . or idx=1;  if sp500_idx ne .;
if date>&histEndMon.;
drop  vacancy_l1 pprcaprate_l1  sp500_idx_l1  unemp_l1  inc_p50_l1  inc_mean_l1  capr_ust10y_l1  rentidx_l1  hpi_l1  fundspread_l1 idx rentidx_sa_l1;
run;
data keepCBSA_agg; set allsim_output_Monthly; where date=202502 and rentidx ne .;keep indexcode; proc sort nodup; by indexcode; run;
proc sort data=keepCBSA_agg; by indexcode ;
proc sort data=allsim_output_Monthly; by indexcode  date;
data allsim_output_Monthly; merge allsim_output_Monthly(in=f1)  keepCBSA_agg(in=f2); by indexcode; if f1 and f2;run;
proc means data=allsim_output_Monthly(where=(simid>0)) noprint; class   indexcode date; output out=meanPath mean=;run;
data MeanPath; set MeanPath; if indexcode ne '' and date>0 ; drop  _TYPE_ _FREQ_ simid path_num capr_ust10y_g_l1 capr_ust10y_g_l1  capr_ust10y; run;

%put &enddate.;
proc delete data=testbed.bak_RentHPIMeanPath_&enddate.; run;
data testbed.bak_RentHPIMeanPath_&enddate.(insertbuff=32000); set irs.RentHPIMeanPath_monthly;*_v2022; *_FIXEDHPA;run;

proc delete data=irs.RentHPIMeanPath_monthly;*_v2022; *_FIXEDHPA;
data irs.RentHPIMeanPath_monthly(insertbuff=30000);*_v2022; set MeanPath ;  if indexcode ne ''  and date>0; 
RateasofDate=20210305; drop simid _TYPE_ _FREQ_ capr_ust10y_g_l1 capr_ust10y_g_l1  capr_ust10y; run;

proc delete data=testbed.bak_SimRentHpi_monthly_&enddate.;
data  testbed.bak_SimRentHpi_monthly_&enddate.(insertbuff=32000); set irs.SimRentHpi_monthly;run;


proc delete data=irs.SimRentHpi_monthly;*_v2022;
data irs.SimRentHpi_monthly(insertbuff=30000); set allsim_output_Monthly; run;

proc delete data=irs.HistMFcaprateVac;
data irs.HistMFcaprateVac(insertbuff=30000); set pprCBSA; run;


data sf_rentIdx_month_dt0 ;  merge rawRentidxOrg(rename=index=rentidx0) rawrentIdx3; by indexcode date; retain index; 
if first.indexcode then index=.;
if rentidx0>0 then index=rentidx0;
else index=index*(1+rentg); drop rentidx0  ; 
if date<=&histendmon.;
datefmt=input(put(date*100+1,8.),yymmdd10.);format datefmt  date9.;
keep indexcode date index datefmt; run;


** SA for historical period only;
proc x12 data=sf_rentIdx_month_dt0 date=datefmt noprint interval=month; by indexcode; var index;    x11;    output out=sa_hist d11;    ods select d11; run;

data sf_rentIdx_month_dt0a; merge sf_rentIdx_month_dt0(in=f1) sa_hist(rename = (index_d11= index_sa)); by indexcode datefmt; if f1; 
rentg = index/lag(index)-1;
rentg_sa = index_sa/lag(index_sa)-1;
if first.indexcode then do; rentg=.; rentg_sa=.; end;
run;


proc sql; create table cbsa2state as 
select distinct case when cbsadiv='' then cbsa else cbsadiv end as indexcode, date, avg(rentg) as rentg_st , avg(rentg_sa) as rentg_sa_st
from thirdp.county_dt c
join meanpath m
on state=m.indexcode 
join (select distinct indexcode from sf_rentIdx_month_dt0a) r
on r.indexcode = coalesce(c.cbsadiv, c.cbsa)
where cbsa ne ''
group by case when cbsadiv='' then cbsa else cbsadiv end,date 
order by case when cbsadiv='' then cbsa else cbsadiv end,date;
run;


proc sql; create table cbsa2US as 
select distinct case when cbsadiv='' then cbsa else cbsadiv end as indexcode, date, avg(rentg) as rentg_us , avg(rentg_sa) as rentg_sa_us
from thirdp.county_dt  c
join meanpath m
on 'US'=indexcode
join (select distinct indexcode from sf_rentIdx_month_dt0a) r
on r.indexcode = coalesce(c.cbsadiv, c.cbsa)
where cbsa ne ''
group by case when cbsadiv='' then cbsa else cbsadiv end ,date 
order by case when cbsadiv='' then cbsa else cbsadiv end ,date;
run;

data meanPath1; set meanPath;
if rentg ne . then rentg=(1+rentg)**(1/3)-1;
if rentg_sa ne . then rentg_sa = (1+rentg_sa)**(1/3)-1;
run;

data sf_rentIdx_month_dt; merge sf_rentIdx_month_dt0a(rename=(index=index0 index_sa = index_sa0))
meanpath1(keep=indexcode date rentg rentg_sa) cbsa2state cbsa2US; by indexcode date;
retain index index_sa;

if rentg=. then rentg=rentg_st; 
if rentg=. then rentg=rentg_us;
if rentg_sa=. then rentg_Sa=rentg_sa_st; 
if rentg_sa=. then rentg_sa=rentg_sa_us;
/*
if rentg ne . then rentg=(1+rentg)**(1/3)-1;
if rentg_sa ne . then rentg_sa = (1+rentg_sa)**(1/3)-1;
*/
if first.indexcode then do; index=index0; index_sa = index_sa0; end;
else do; index = index * (1+rentg); index_sa = index_sa*(1+rentg_sa); end;
/*
if index0 ne . then do; index=index0; index_last=index0; end; else index=index_last*rentidx;
*/
indexmonth=input(put(date*100+1,8.),YYMMDD10.); format indexmonth date9.;

keep index index_sa date indexcode  indexmonth;
run;
proc sort; by indexcode indexmonth; run;

proc sql; create table sf_rentIdx_month_dt
as select distinct *
from sf_rentIdx_month_dt a
where index ne .
group by indexcode
having  min(date)<202102
order by indexcode, date;
quit;
/*
proc x12 data=sf_rentIdx_month_dt date=indexmonth noprint interval=month; by indexcode ; var index;   
x11;    output out=sa_all d11;    ods select d11; run;

data sf_rentidx_month_dt; merge sf_rentIdx_month_dt(in=f1) sa_all(in=f2 rename=(index_d11 = index_sa)); by indexcode indexmonth; run;
*/

proc delete data=devhu.bak_sf_rentIdx_month_dt2;
data devhu.bak_sf_rentIdx_month_dt2&enddate.(insertbuff=30000); set irs.sf_rentIdx_month_dt;run;

proc delete data=testbed.bak_sf_rentIdx_month_dt_&enddate.;
data testbed.bak_sf_rentIdx_month_dt_&enddate.(insertbuff=30000); set irs.sf_rentIdx_month_dt;run;
** upload to load table first;

proc delete data=testbed.load_sf_rentIdx_month_dt;
data  testbed.load_sf_rentIdx_month_dt(insertbuff=30000); set sf_rentIdx_month_dt; cluster='agg'; 
;FORMAT indexmonth date9.; drop date; run;

DATA TP; SET US_MEAN;

data test;set irs.sf_rentidx_month_dt; run;
proc sort; by indexcode indexmonth; run;

proc x12 data=test date=indexmonth noprint interval=month; by indexcode ; var index;   
x11;    output out=sa_all d11;    ods select d11; run;
LIBNAME devhu ODBC DSN='devhu' schema=dbo;

data devhu.tp_sa(insertbuff=32000); set sa_all; run;
proc means data=rate2; where month>202212 and month<202900;; var refi_rate; class month; run;
