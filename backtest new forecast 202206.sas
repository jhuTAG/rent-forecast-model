option compress=yes error=10;

LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;
LIBNAME irs ODBC DSN='irs' schema=dbo;

data histidx; set irs.sf_rentidx_month_dt(where=(indexmonth<='01JUN2022'D)); 
keep indexcode index indexmonth;
run;
proc sort; by indexcode indexmonth; run;

data histidx_qtr; set histidx; 
where month(indexmonth) in (3,6,9,12);
run;
proc sort; by indexcode indexmonth; run;

proc x12 data=histidx date=indexmonth noprint; by indexcode; var index;  x11 ;  output out=sa d11;  ods select d11; run;
proc x12 data=histidx_QTR date=indexmonth interval=QTR  noprint; by indexcode; var index;  x11 ;  output out=sa_qtr d11;  ods select d11; run;

data hist_sa; merge histidx(in=f1) sa(in=f2 rename=(index_d11=index_sa)); by indexcode indexmonth; if f1 and f2;
seasonality=index/lag(index)-index_sa/lag(index_sa);
if indexcode ne lag(indexcode) then seasonality=.;
if first.indexcode then delete;
run;
data histqtr_sa; merge histidx_qtr(in=f1) sa_qtr(in=f2 rename=(index_d11=index_sa)); by indexcode indexmonth; if f1 and f2;
seasonality=index/lag(index)-index_sa/lag(index_sa);
if indexcode ne lag(indexcode) then seasonality=.;
if first.indexcode then delete;
run;

data allfc00; set testbed.tv_rentFC_backtest_v2022(keep=indexcode startqtr qtr lnRentg lnRentg_wEC lnRentg_wEC_ann); 
rentg=exp(lnRentg)-1;
rentg_wEC = exp(lnRentg_wEC)-1;
rentg_wEC_ann = exp(lnRentg_wEC_ann)-1;
month_g =  (1+rentg)**(1/3)-1;
month_g_wEC = (1+rentg_wEC)**(1/3)-1;
month_g_wEC_ann = (1+rentg_wEC_ann)**(1/3)-1;
if startqtr>=201501;
run;
proc sort; by indexcode startqtr qtr; run;

data startqtr; set allfc00; keep indexcode startqtr; proc sort nodup; by indexcode startqtr; run;
data startqtr; set startqtr;
tp = int(startqtr/100)*100+(mod(startqtr,100)-1)*3+1;
startqtr_fmt = input(put(tp*100+1,8.),yymmdd10.);
sa_start = intnx('month',startqtr_fmt,-12);
format startqtr_fmt sa_start date9.;
run;

proc sql; create table seasonality as 
select s.indexcode, s.startqtr, h.indexmonth, month(h.indexmonth) as monthidx, h.seasonality
from startqtr s
left join hist_sa h
on s.indexcode=h.indexcode and h.indexmonth>=s.sa_start and h.indexmonth<s.startqtr_fmt
order by s.indexcode, startqtr, indexmonth;
quit;

proc sql; create table sa_month as 
select indexcode, startqtr, monthidx, seasonality - sum(seasonality)/12 as seasonality
from seasonality
group by indexcode, startqtr
order by indexcode, startqtr, monthidx;quit;

data year; 
do i =2015 to 2023; 
	do m = 1 to 12;
 	tpm = i*10000+m*100+1;
	month = i*100+m;
	output; 
	end;
end; run;

data year; set year;
monthfmt =input(put(month*100+1,8.),yymmdd10.);
format monthfmt date9.;
qtr = year(monthfmt)*100+qtr(monthfmt);
keep qtr monthfmt month;
run;

proc sql; create table allfc01 as
select a.indexcode, a.startqtr, a.qtr, b.month, b.monthfmt, a.lnrentg, a.lnrentg_wEC, a.rentg, a.rentg_wEC, a.rentg_wEC_ann, a.month_g, a.month_g_wEC, a.month_g_wEC_ann
from allfc00 a
left join year b
on a.qtr = b.qtr
order by indexcode, startqtr, qtr, monthfmt; quit;

proc sql; create table allfc02 as
select a.*, s.seasonality, month_g+seasonality as rentg_sa, month_g_wEC + seasonality as rentg_wEC_sa
, month_g_wEC_ann +seasonality as rentg_wEC_ann_sa
from allfc01 a
join sa_month s
on a.startqtr=s.startqtr and a.indexcode = s.indexcode 
and month(a.monthfmt) = s.monthidx
order by indexcode, startqtr, qtr, monthfmt; quit;

data allfc03; set allfc02; by indexcode startqtr qtr monthfmt;
retain rentidx rentidx_wEC rentidx_wEC_ann; 
if first.startqtr then do; rentidx=1; rentidx_wEC=1; rentidx_wEC_ann=1;end;
rentidx = rentidx*(1+rentg_sa);
rentidx_wEC = rentidx_wEC * (1+rentg_wEC_sa);
rentidx_wEC_ann = rentidx_wEC_ann * (1+rentg_wEC_ann_sa);
run;

data baktest; set allfc03; keep rentidx indexcode startqtr qtr monthfmt month; run;

data truefc; set testbed.tv_RentModel_v2022_DiffVersion(where=( model='Production HPI+Breakeven Inflation+NO Rent Error Correction'));
keep indexcode date rentidx;
run;
proc sort; by indexcode date; run;

LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;

proc delete data=testbed.tp_rentfc_backtest_monthly;
data testbed.tp_rentfc_backtest_monthly(insertbuff=32000); set allfc03(keep=indexcode startqtr qtr monthfmt rentidx rentidx_wEC rentidx_wEC_ann); run;


data allfc00_sa; set testbed.tv_rentFC_backtest_v2022(keep=indexcode startqtr qtr lnRentg_withSeason lnRentg_wSeason_wEC lnRentg_wSeason_wEC_ann); 
rentg=exp(lnRentg_withSeason)-1;
rentg_wEC = exp(lnRentg_wSeason_wEC)-1;
rentg_wEC_ann = exp(lnRentg_wSeason_wEC_ann)-1;
month_g =  (1+rentg)**(1/3)-1;
month_g_wEC = (1+rentg_wEC)**(1/3)-1;
month_g_wEC_ann = (1+rentg_wEC_ann)**(1/3)-1;
if startqtr>=201501;
run;

proc sql; create table allfc01_sa as
select a.indexcode, a.startqtr, a.qtr, b.month, b.monthfmt, a.lnRentg_withSeason, a.lnRentg_wSeason_wEC, a.lnRentg_wSeason_wEC_ann, a.rentg, a.rentg_wEC, a.rentg_wEC_ann, a.month_g, a.month_g_wEC, a.month_g_wEC_ann
from allfc00_sa a
left a.qtr = b.qtr
order by indexcode, startqtr, qtr, monthfmt; quit;

data allfc02_sa; set allfc01_sa; by indexcode startqtr qtr;
retain rentidx rentidx_wEC rentidx_wEC_ann; 
if first.startqtr then do; rentidx=1; rentidx_wEC=1; rentidx_wEC_ann=1;end;
rentidx = rentidx*(1+month_g);
rentidx_wEC = rentidx_wEC * (1+month_g_wEC);
rentidx_wEC_ann = rentidx_wEC_ann * (1+month_g_wEC_ann);
tp = int(startqtr/100)*100+(mod(startqtr,100)-1)*3+1;
startqtr_fmt = input(put(tp*100+1,8.),yymmdd10.);
format startqtr_fmt date9.;
run;




data baktest; set allfc02_sa; keep rentidx indexcode startqtr qtr monthfmt month startqtr_fmt; run;

proc sql; create table month as 
select distinct a.indexcode, a.startQtr, a.startQtr_fmt, b.monthfmt
from baktest a
join year b
on b.monthfmt>=a.startQtr_fmt
and b.monthfmt<intnx('month',a.startQtr_fmt,24)
order by indexcode, startqtr, monthfmt;
quit;

data baktest1; merge month(in=f1) baktest(in=f2); by indexcode startqtr monthfmt; if f1;
month = year(monthfmt)*100+month(monthfmt);
run;

data truefc; set testbed.tv_RentModel_v2022_DiffVersion(where=( model='Production HPI+Breakeven Inflation+NO Rent Error Correction'));
keep indexcode date rentidx;
run;
proc sort; by indexcode date; run;

data truefc; set truefc; by indexcode date;
rentg=rentidx/lag(rentidx)-1;
if first.indexcode then rentg=.;
run;

proc sort data=baktest1; by indexcode month startqtr;
data allfc03_sa; merge baktest1(in=f1 rename=(rentidx=rentidx0)) truefc(in=f2 rename=(date=month) drop=rentidx); by indexcode month; if f1; run;
proc sort; by indexcode startqtr month; run;

data allfc03_sa; set allfc03_sa; by indexcode startqtr month;
retain rentidx; 
if rentidx0 ne . then rentidx=rentidx0;
else rentidx = rentidx*(1+rentg);
run;


LIBNAME testbed ODBC DSN='modeltestbed' schema=dbo;

proc delete data=testbed.tp_rentfc_backtest_monthly;
data testbed.tp_rentfc_backtest_monthly(insertbuff=32000); set allfc03_sa(keep=indexcode startqtr  monthfmt rentidx); run;



proc sort; by indexcode startqtr descending qtr; run;


data allfc_monthly0; set allfc01; by indexcode startqtr descending qtr;
date=int(qtr/100)*100+(mod(qtr,100)-1)*3+1;
idx=1;
rentidx_l1=lag(rentidx);
if first.startqtr then do; rentidx_l1=.;  end;
run;
data allfc_monthly1; set allfc_monthly0; idx+1; if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;
data allfc_monthly2; set allfc_monthly1; idx+1;  if mod(date,100)<12 then date=date+1; else date=int(date/100)*100+101;  run;


data allsim_output_Monthly; set allsim_output_Monthly0 allsim_output_Monthly1 allsim_output_Monthly2; by indexcode simid descending date;
if rentidx_l1 ne . then do;
rentidx=(rentidx_l1/rentidx)**((idx-1)/3)*rentidx  ;
end;
if rentidx_l1 ne . or idx=1;  
drop  vacancy_l1 pprcaprate_l1  sp500_idx_l1  unemp_l1  inc_p50_l1  inc_mean_l1  capr_ust10y_l1  rentidx_l1  hpi_l1  fundspread_l1 idx;
run;
