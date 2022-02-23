
%macro add_fredRent(inp,sm_url, /* URL of text data on FRED */ sm_var, /* name of variarent_gble */ 
	sm_firstobs /* line of first data (if you are not sure and don't need the oldest data, ~25 is often safe) */);
filename fredRent url "&sm_url";
data fred_new;  infile fredRent  firstobs=&sm_firstobs;   format date yymmdd10.; input          @1 date yymmdd10.          @13 &sm_var; 
month=year(date)*100+month(date);run; 
proc means data=fred_new noprint; class month;var &sm_var; output out=fred_new mean=;run;
filename fred; /* close file reference */
data fred_new; set fred_new; if month ne .; drop _TYPE_ _FREQ_; run;
data &inp; merge &inp(in=f1) fred_new(in=f2); by month; if f1 or f2; if month ne .; drop Date; run;
%mend;


%Macro LoadIn_Hist_IR_Format();
data cpi; month=.; run;
%add_fredRent(inp=cpi,sm_url=http://research.stlouisfed.org/fred2/data/CPILFESL.txt,sm_var=cpi,sm_firstobs=16);run;
data cpi; set cpi; if cpi ne .;run;

data libor3mo; month=.; run;
%add_fredRent(inp=libor3mo,sm_url=http://research.stlouisfed.org/fred2/data/USD3MTD156N.txt,sm_var=libor3mo,sm_firstobs=16);run;
data libor3mo; set libor3mo; if libor3mo ne .;run;

data libor1mo; month=.; run;
%add_fredRent(inp=libor1mo,sm_url=http://research.stlouisfed.org/fred2/data/USD3MTD156N.txt,sm_var=libor1mo,sm_firstobs=16);run;
data libor1mo; set libor1mo; if libor1mo ne .;run;


proc SQL; connect to odbc(DSN='amhersthpi');
create table PMMs as select * from connection to odbc
( 
    select year(period)*100+month(period) as date, [PMMS30]+pts30/3.5 as pmms_rate 
      ,[PMMS15]+  [Pts15]/3.5 as pmms_15r
    from [Agency].[dbo].[RateHist_dt] a
); disconnect from odbc;
quit;
proc sort nodup; by date;
	
data swap;	set krlive.marketRates_dt; where indexName in ('USSWAP10' 'USSWAP1' 'MTGEFNCL' 'USGG10YR'); 
date=int(indexDate/100);	keep indexName indexValue date;	proc sort ; by date indexname;
proc means noprint;	by date indexName; var indexValue; output out=swaps mean=;	run;

data swaps; set swaps;	by date indexName; 	retain swap10yr swap1yr ust10yr cc_rate; 
if indexName='USSWAP10' then swap10yr=indexValue;	else if indexName='USGG10YR' then ust10yr=indexValue;	
else if indexName='USSWAP1' then swap1yr=indexValue;	else if indexName='MTGEFNCL' then cc_rate=indexValue;
keep date ust10yr cc_rate swap10yr swap1yr ;	if swap10yr^=. and swap1yr^=.;	if last.date; run;

data irHist; merge swaps(in=f1) PMMs(in=f2) cpi(rename=month=date) libor1mo(rename=month=date) libor3mo(rename=month=date); 	by date;	if f1 ; 
ps_spread=pmms_rate-cc_rate; 	 slope=swap10yr-swap1yr;	refi_spread=cc_rate-ust10yr; pmms_rate=lag(pmms_rate);	pmms_15r=lag(pmms_15r);	
if pmms_15y<=0 or pmms_15y>15 then pmms_15y=pmms_rate;	if slope ne . and ust10yr ne .; 
if date-int(date/100)*100<=3 then qtr=int(date/100)*100+1; else if date-int(date/100)*100<=6 then qtr=int(date/100)*100+2;
else if date-int(date/100)*100<=9 then qtr=int(date/100)*100+3; else qtr=int(date/100)*100+4;run;

proc means data=irhist noprint; class qtr;   output out=irhist_qtr mean=;run;
data irhist_qtr;  set irhist_qtr; where qtr ne . and _FREQ_=3; drop _TYPE_ _FREQ_ date qtridx; run;

data irhist_l1; set irhist_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; run; proc sql noprint; select cats(name,'=',name,'_l1') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRHIST_L1' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irhist_l1;  	rename &suffixlist; quit;

data irhist_l2; set irhist_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
proc sql noprint; select cats(name,'=',name,'_l2') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRHIST_L2' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irhist_l2;  	rename &suffixlist; quit;

data irhist_l3; set irhist_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1;  proc sql noprint; select cats(name,'=',name,'_l3') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRHIST_L3' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irhist_l3;  	rename &suffixlist; quit;

data irhist_l4; set irhist_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
proc sql noprint; select cats(name,'=',name,'_l4') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRHIST_L4' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irhist_l4;  	rename &suffixlist; quit;

data irhist_withlags; merge irhist_qtr(in=f1) irhist_l1 irhist_l2 irhist_l3 irhist_l4; by qtr; if f1;
drop cpi: ps_spread: cc_rate: refi_spread:; run;
%MEND;

%macro process_ppr_proj();
data metroonly; set irs.PPRScenario_dt(keep=period metrocode propertytypecode vacancy  slice round pprcaprate  simulation
rename=(metrocode=metro_code   vacancy=vac1 pprcaprate=ppr_cap_rate) where=(slice='All' and simulation='Costar Base Case' and propertytypecode='APT')); 
round1=substr(round,1,4)*100+substr(round,6,1)*3; 
date=year(period)*100+month(period); if date=. then date=substr(period,1,4)*100+substr(period,6,2);
;*substr(period,1,4)*100+substr(period,6,2); drop period; *if date<=round1;
qtr=int(date/100)*100+(date-int(date/100)*100)/3;   if date<=round1;
vacancy=vac1*1.0; if vacancy=1 or vacancy=0 then vacancy=.; 
if occupancy=1 or occupancy=0 then occupancy=.; 
occupancy=(1-vacancy)*1.0;  pprcaprate=ppr_cap_rate*1.0;
length metrocode $20.;metrocode=strip(metro_code); 
if propertytypecode='APT' then asgproptype='MF'; else if propertytypecode='IND' then asgproptype='IN';
else if propertytypecode='OFF' then asgproptype='OF'; else if propertytypecode='RET' then asgproptype='RT';
if asgproptype ne ''; keep metrocode qtr  propertytypecode asgproptype occupancy pprcaprate  vacancy; 
if occupancy ne . ;  proc sort nodup; by metrocode propertytypecode qtr; run;

data ppr0_a; set metroonly  ; 
if metrocode='G32478' then metrocode='ZPPR54'; proc sort nodup; by metrocode asgproptype qtr; run;


data exclude; merge ppr0_a ppr0_a(firstobs=2 keep=asgproptype metrocode qtr vacancy  pprcaprate
rename=(asgproptype=next_type metrocode=next_code qtr=next_qtr vacancy=next_vacancy  pprcaprate=next_pprcaprate));
if not(asgproptype=next_type and metrocode=next_code and int(next_qtr/100)*4+next_qtr-int(next_qtr/100)*100-(int(qtr/100)*4+qtr-int(qtr/100)*100)=1)
then do; next_vacancy=.; next_pprcaprate=.;  end; drop next_type next_code next_qtr; 
vacancy_chg=next_vacancy-vacancy; 
pprcaprate_chg=next_pprcaprate-pprcaprate; 
if (abs(vacancy_chg)>0.2 ) and (length(metrocode)>4 ); 
keep metrocode asgproptype; proc sort nodup; by metrocode asgproptype ; run;
data keep3; set ppr0_a; if qtr=201603; if vacancy ne .  ; keep metrocode asgproptype; run;
proc sort nodup; by metrocode asgproptype; run;
data ppr0; merge ppr0_a(in=f3) exclude(in=f1) keep3(in=f2); by  metrocode asgproptype; if not f1; if f2 and f3;run;


data str; set cre.SmithTravelHistory_dt; if seriescode ne 'USSC'; 
if month(date)<=3 then qtr=year(date)*100+1;  
else if month(date)<=6 then qtr=year(date)*100+2;  
else if month(date)<=9 then qtr=year(date)*100+3;  
else  qtr=year(date)*100+4;  length metrocode $20.; occupancy=occupancy/100;
length metrocode $7.;
metrocode=compress(strip(marketcode)||chainscale);
demand_nsa=roomssold; stock=roomsavailable;askingrent_nsa=avgdailyrate; vacancy_nsa=1-occupancy; 
keep revpar demand_nsa stock askingrent_nsa qtr metrocode asgproptype vacancy_nsa occupancy;  asgproptype='HT'; 
if askingrent_nsa>0 and demand_nsa>0 and vacancy_nsa>0;
proc sort nodup; by metrocode asgproptype qtr; run;
proc means data=str noprint; class metrocode asgproptype qtr; output out=str mean=;run;
data str; set str; if metrocode ne '' and asgproptype ne '' and qtr ne .;  
date=input(put(int(qtr/100)*10000+(qtr-int(qtr/100)*100)*300+1,8.),yymmdd10.); format date monyy.; run;

proc x12 data=str date=date noprint interval=QTR; by metrocode asgproptype; var demand_nsa;    x11;    output out=sa_demand d11;    ods select d11; run;
proc x12 data=str date=date noprint interval=QTR; by metrocode asgproptype; var vacancy_nsa;    x11;    output out=sa_vacancy d11;    ods select d11; run;
proc x12 data=str date=date noprint interval=QTR; by metrocode asgproptype; var askingrent_nsa;    x11;    output out=sa_rent d11;    ods select d11; run;

data str; merge str(in=f1) sa_demand(in=f2 rename=demand_nsa_d11=demand) sa_vacancy(in=f3 rename=vacancy_nsa_d11=vacancy) sa_rent(rename=askingrent_nsa_D11=askingrent); 
by metrocode asgproptype date; if f1 and f2 and f3; occupancy_nsa=1-vacancy_nsa; keep revpar demand_nsa stock askingrent_nsa qtr metrocode asgproptype vacancy_nsa occupancy 
occupancy_nsademand askingrent vacancy;  run;

proc sql noprint; create table maxqtr as select distinct metrocode,max(qtr) as lastqtr from str group by metrocode;run;
data _null_; set maxqtr; call symput(compress("lastqtr"||metrocode),lastqtr);run;

data seasonality; merge str(keep=metrocode asgproptype qtr  askingrent askingrent_nsa vacancy vacancy_nsa);
by metrocode asgproptype qtr;   askingrent_nsa_l1=lag(askingrent_nsa); askingrent_l1=lag(askingrent);
vacancy_nsa_l1=lag(vacancy_nsa); vacancy_l1=lag(vacancy);
if first.asgproptype then do; askingrent_l1=.; vacancy_l1=.; end;
askingrent_season=log(askingrent_nsa/askingrent_nsa_l1)-log(askingrent/askingrent_l1);
vacancy_season=vacancy_nsa-vacancy_nsa_l1-(vacancy-vacancy_l1); 
if qtr>symget(compress("lastqtr"||metrocode))-100 and qtr<=symget(compress("lastqtr"||metrocode)); qtr=qtr-int(qtr/100)*100;
keep metrocode asgproptype qtr  askingrent_season vacancy_season;
if metrocode='000000' then metrocode='US'; run; proc sort nodup; by metrocode asgproptype qtr;  run;

proc means data=seasonality noprint; class metrocode asgproptype; var askingrent_season vacancy_season; output out=m_season mean=/autoname; run;
data seasonality; merge seasonality m_season; by metrocode asgproptype; where metrocode ne '' and asgproptype ne ''; 
askingrent_season=askingrent_season-askingrent_season_mean; vacancy_season =vacancy_season-vacancy_season_mean;
keep metrocode asgproptype qtr  askingrent_season vacancy_season;;run;


data link_str_ppr; merge cre.SmithTravelZipToMarket2010_dt(in=f1 keep=zipcode marketcode) cre.PPRZipToMarketMapping_dt(in=f2 keep=zipcode metrocode ); by zipcode;
if f1 and f2; keep marketcode metrocode; proc sort nodup; by metrocode; run;
data m_caprate; merge ppr0 link_str_ppr ; by metrocode; run;

proc means data=m_caprate noprint; class marketcode  qtr; where pprcaprate>0; var pprcaprate; output out=m_caprate_str mean=;run;
proc means data=ppr0 noprint; class qtr;  where  metrocode = 'ZPPR54'; var pprcaprate; output out=m_caprate_us mean=us_pprcaprate;run;

proc sql; create table str0 as select distinct a.*,b.pprcaprate,c.us_pprcaprate 
from str a left outer join m_caprate_str b on a.metrocode=b.marketcode and a.qtr=b.qtr
left outer join m_caprate_us  c on a.qtr=c.qtr;


data str0; set str0; if pprcaprate=. then pprcaprate=us_pprcaprate; 
length maincode $7.; maincode=metrocode; drop us_pprcaprate; if pprcaprate>0; run;
/*
proc means data=ppr0 noprint; class metrocode qtr; where pprcaprate>0; var pprcaprate; output out=m_caprate_ppr mean=;run;
data str0; merge str(in=f1) m_caprate_ppr(in=f2); by metrocode qtr; where metrocode ne '' and qtr ne .; if f1 and f2; run;
*/
data ppr0_1; set  str0 ppr0(in=f1 where=(asgproptype ne 'HT')); drop demand_nsa askingrent_nsa vacancy_nsa;
if maincode='' then maincode=metrocode; keep metrocode occupancy vacancy askingrent pprcaprate
maincode qtr date stock asgproptype;proc sort nodup; by metrocode asgproptype qtr; run;


proc sql; create table ppr1 as select distinct case when a.pprcaprate=. then b.pprcaprate else 
a.pprcaprate end as pprcaprate,a.* from ppr0_1 a left outer join ppr0_1 b
on a.metrocode ne a.maincode and a.maincode=b.metrocode and a.qtr=b.qtr and a.asgproptype=b.asgproptype
where a.occupancy>0 order by metrocode, asgproptype, qtr; run;

data ppr1; set ppr1; if askingrent<=0 then askingrent=.; if vacancy<=0 then vacancy=.; if revPar<=0 then revPar=.; if stock<=0 then stock=.;
if demand<=0 then demand=.; if not (askingrent=. and vacancy=. and revPar=.);run;

data map_sub_metro; merge cre.PPRZipToMarketMapping_dt cre.PPRSubmarket_dt(in=f2);  by submarketcode; 
if propertytypecode='APT' then asgPropType='MF'; 
else if propertytypecode='IND' then asgPropType='IN'; 
else if propertytypecode='RET' then asgPropType='RT'; 
else if propertytypecode='OFF' then asgPropType='OF';
keep SubmarketCode zipCode asgPropType metrocode spatialoverlappercent;		 proc sort nodup; by zipcode  asgproptype descending  spatialoverlappercent ; run;

data map_sub_metro; set map_sub_metro; by zipcode  asgproptype descending spatialoverlappercent ; if first.asgproptype; drop spatialoverlappercent; run;

data map_hotel; set cre.SmithTravelZipToMarket2010_dt (keep=zipcode marketcode); asgPropType='HT'; metrocode=marketcode;	
drop marketcode; proc sort nodup; by zipcode asgproptype; run;

data mapping0; merge map_hotel map_sub_metro ; by zipcode asgproptype;
proc sort nodup; by metrocode asgproptype; run;

proc sql; create table statemapping0 as select distinct state,c.* 
from mapping0 a, wlres.zipcodesdotcom_dt b, ppr1 c where a.zipcode=b.zipcode and a.metrocode =c.metrocode  and a.asgproptype=c.asgproptype and (length(c.metrocode)=4 or c.asgproptype='HT')
order by state,asgproptype, metrocode,qtr;quit;

data statemapping1; set statemapping0; keep state asgproptype metrocode qtr; if qtr-int(qtr/100)*100=4 then qtr=qtr+100-3; else qtr=qtr+1; run;

data statemapping2; merge statemapping0(in=f2 rename=(pprcaprate=pprcaprate2 vacancy=vacancy2 revPar=revPar2 
askingrent=askingrent2 stock=stock2 demand=demand2)) statemapping1(in=f1); by state asgproptype metrocode qtr; if f1 and f2; run;
data statemapping3; merge statemapping0 statemapping2; by state asgproptype metrocode qtr;run;

proc means data=statemapping3 noprint; where  ; weight stock; class state asgproptype qtr; var pprcaprate vacancy revPar askingrent
pprcaprate2 vacancy2 revPar2 askingrent2; output out=state_ppr1 mean=;run;


proc means data=statemapping3 noprint; where  metrocode=maincode; class state asgproptype qtr; var demand demand2 stock stock2;
output out=state_ppr2 sum= ;run;

data state_ppr3; merge state_ppr1 state_ppr2; where state ne '' and asgproptype ne '' and qtr ne .; 
by state asgproptype qtr; length maincode metrocode $5.; maincode=state; metrocode=state;   drop state ; 
proc sort nodup; by metrocode asgproptype descending qtr; run;

data state_ppr4; set state_ppr3(rename=(pprcaprate=pprcaprate1 vacancy=vacancy1 revPar=revPar1
askingrent=askingrent1 demand=demand1 stock=stock1)); by metrocode asgproptype descending qtr; retain vac rent caprate roomrate dm st;
if first.asgproptype then do; vac=vacancy1-vacancy2; rent=askingrent1/askingrent2; caprate=pprcaprate1-pprcaprate2; 
roomrate=revPar1/revPar2; dm=demand1/demand2; st=stock1/stock2; vacancy=vacancy1; askingrent=askingrent1; pprcaprate=pprcaprate1;
revPar=revPar1; demand=demand1; stock=stock1; if vac=. then vac=0; if rent=. then rent=1; if caprate=. then caprate=0;
if roomrate=. then roomrate=1; if dm=. then dm=1; if st=. then st=1; end; else do;  vacancy=vacancy1+vac; askingrent=askingrent1*rent; pprcaprate=pprcaprate1+caprate;
revPar=revPar1*roomrate; demand=demand1*dm; stock=stock1*st; vac=vac+vacancy1-vacancy2; rent=rent*askingrent1/askingrent2; 
caprate=caprate+pprcaprate1-pprcaprate2; roomrate=roomrate*revPar1/revPar2; dm=dm*demand1/demand2; st=st*stock1/stock2; 
if rent=. then rent=1; if caprate=. then caprate=0; if roomrate=. then roomrate=1; if dm=. then dm=1; if st=. then st=1; end;
drop pprcaprate1 vacancy1 revpar1 askingrent1 demand1 stock1 pprcaprate2 vacancy2 revpar2 askingrent2 demand2 
stock2 vac rent caprate roomrate dm st;proc sort nodup; by metrocode asgproptype qtr; run; 

data ppr2_a; set  ppr1 state_ppr4; drop occupancy _TYPE_ _FREQ_; if metrocode in ('ZPPR54','000000') then do; metrocode='US'; maincode='US';end; run;
proc sort nodup; by metrocode asgproptype qtr; run;
data keep; set cre.PPRZipToMarketMapping_dt; keep metrocode ;   proc sort nodup; by metrocode; run;

data ppr2; merge ppr2_a(in=f1) keep(in=f2); by metrocode ; 
if (f1 and f2 ) or (asgproptype='HT' or metrocode='US');run; 
proc sort nodup; by metrocode asgproptype qtr; run;

data lag1; set ppr2; by metrocode asgproptype qtr; pprcaprate_l1=lag(pprcaprate); vacancy_l1=lag(vacancy); askingrent_l1=lag(askingrent); stock_l1=lag(stock);
if first.asgproptype then do; pprcaprate_l1=.;vacancy_l1=.;askingrent_l1=.; stock_l1=.; end; 
if pprcaprate_l1 ne . then pprcaprate2=pprcaprate;  if vacancy_l1 ne . then vacancy2=vacancy; if stock_l1 ne . then stock2=stock; 
if askingrent_l1 ne . then askingrent2=askingrent; drop pprcaprate_l1 vacancy_l1 askingrent_l1 stock_l1; run;

proc means data=lag1 noprint; where asgproptype not in ('MF' 'HT') ; weight stock; class metrocode qtr; var pprcaprate vacancy askingrent
pprcaprate2 vacancy2 askingrent2; output out=other_ppr1 mean=;run;
proc means data=lag1 noprint; where asgproptype not in ('MF' 'HT') ;  class metrocode qtr; var  stock stock2; output out=other_ppr2 sum= ;run;

data other_ppr1; set other_ppr1; where metrocode ne '' and qtr ne .; proc sort ; by metrocode descending qtr;
data other_ppr2; set other_ppr2; where metrocode ne '' and qtr ne .; proc sort ; by metrocode descending qtr; run;
data other; merge other_ppr1(rename=(pprcaprate=pprcaprate1 vacancy=vacancy1 
askingrent=askingrent1  )) other_ppr2(rename=stock=stock1); by metrocode  descending qtr; retain vac rent caprate   st;
if first.metrocode then do; vac=vacancy1-vacancy2; rent=askingrent1/askingrent2; caprate=pprcaprate1-pprcaprate2; 
st=stock1/stock2; vacancy=vacancy1; askingrent=askingrent1; pprcaprate=pprcaprate1;
stock=stock1; if vac=. then vac=0; if rent=. then rent=1; if caprate=. then caprate=0;
if st=. then st=1; end; else do;  vacancy=vacancy1+vac; askingrent=askingrent1*rent; pprcaprate=pprcaprate1+caprate;
 stock=stock1*st; vac=vac+vacancy1-vacancy2; rent=rent*askingrent1/askingrent2; 
caprate=caprate+pprcaprate1-pprcaprate2; st=st*stock1/stock2; 
if rent=. then rent=1; if caprate=. then caprate=0;  if st=. then st=1; end;
drop pprcaprate1 vacancy1 askingrent1  stock1 pprcaprate2 vacancy2  askingrent2  
stock2 vac rent caprate  st; asgproptype='OT'; proc sort nodup; by metrocode  qtr; run; 

data ppr3; set ppr2 other;  drop maincode;  proc sort nodup; by qtr; run;

data keep; set ppr3; keep metrocode asgproptype; proc sort nodup; by metrocode asgproptype;run;
data mapping; merge mapping0(in=f1) keep(in=f2); by metrocode asgproptype; if f1 and f2;run;
data costar.mapping; set mapping;run;

proc sort data=ppr3; by asgproptype metrocode qtr;


data ppr; merge ppr3 ppr3(firstobs=2 keep=asgproptype metrocode qtr vacancy askingrent pprcaprate
rename=(asgproptype=next_type metrocode=next_code qtr=next_qtr vacancy=next_vacancy askingrent=next_rent pprcaprate=next_pprcaprate));
if not(asgproptype=next_type and metrocode=next_code and int(next_qtr/100)*4+next_qtr-int(next_qtr/100)*100-(int(qtr/100)*4+qtr-int(qtr/100)*100)=1)
then do; next_vacancy=.; next_pprcaprate=.; next_rent=.; end; drop next_type next_code next_qtr; 

data ppr; set ppr; by asgproptype metrocode qtr;
vacancy_l1=lag(vacancy); pprcaprate_l1=lag(pprcaprate); askingrent_l1=lag(askingrent); 
if first.metrocode then do; vacancy_l1=.; pprcaprate_l1=.; askingrent_l1=.; end; drop _TYPE_ _FREQ_; run;

data exclude; set ppr;vacancy_chg=next_vacancy-vacancy; rent_chg=next_rent/askingrent-1;
pprcaprate_chg=next_pprcaprate-pprcaprate; if (abs(vacancy_chg)>0.2 or abs(rent_chg)>0.2) and length(metrocode)>4; 
keep asgproptype metrocode; proc sort nodup; by asgproptype metrocode; run;

data ppr; merge ppr exclude(in=f1); by asgproptype metrocode; if not f1;run;

proc sql; create table peakvac as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionPeakVac, a.vacancy-b.vacancy as Vac_g_peak
from ppr a, ppr b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr  and b.vacancy>0 and a.vacancy>0
group by a.metrocode, a.qtr,a.asgproptype having max(b.vacancy)=b.vacancy order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table troughvac as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionTroughVac,a.vacancy-b.vacancy as Vac_g_trough
from ppr a, ppr b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.vacancy>0 and a.vacancy>0
group by a.metrocode, a.qtr,a.asgproptype having min(b.vacancy)=b.vacancy order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table peakrent as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionPeakRent, a.askingrent/b.askingrent-1 as rent_g_Peak
from ppr a, ppr b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.askingrent>0 and a.askingrent>0
group by a.metrocode, a.qtr,a.asgproptype having max(b.askingrent)=b.askingrent order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table troughrent as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionTroughRent, a.askingrent/b.askingrent-1 as rent_g_Trough
from ppr a, ppr b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.askingrent>0 and a.askingrent>0
group by a.metrocode, a.qtr,a.asgproptype having min(b.askingrent)=b.askingrent order by a.metrocode,a.asgproptype,a.qtr;

data peaktrough; merge peakvac troughvac peakrent troughrent;
by metrocode asgproptype qtr;  proc sort data=peaktrough nodup; by asgproptype qtr; run;


data peaktrough; merge peaktrough(in=f1) peaktrough(where=(metrocodeUS='US') 
rename= (Vac_g_peak=USVac_g_peak Vac_g_trough=USVac_g_trough rent_g_Peak=USrent_g_Peak
rent_g_Trough=USrent_g_Trough tRegionPeakVac=tUSPeakVac tRegionTroughVac=tUSTroughVac 
tRegionPeakRent=tUSPeakRent tRegionTroughRent=tUSTroughRent metrocode=metrocodeUS)
keep= asgproptype qtr Vac_g_peak Vac_g_trough rent_g_Peak rent_g_Trough tRegionPeakVac
tRegionTroughVac tRegionPeakRent tRegionTroughRent metrocode);
by asgproptype qtr; if f1; drop metrocodeUS; proc sort nodup; by metrocode asgproptype qtr; run;

proc sort data=ppr nodup; by metrocode asgproptype qtr; run;
data ppr_1y; set ppr(keep=vacancy askingrent metrocode asgproptype qtr rename=(vacancy=vacancy_1y askingrent=askingrent_1y)); qtr=qtr+100; 
data ppr_2y; set ppr(keep=vacancy askingrent metrocode asgproptype qtr rename=(vacancy=vacancy_2y askingrent=askingrent_2y)); qtr=qtr+200;

data pprhist; merge ppr(in=f1) peaktrough ppr_1y ppr_2y ; by metrocode asgproptype qtr;  if f1;
vacancy_g_1y=vacancy-vacancy_1y; vacancy_g_2y=vacancy-vacancy_2y;
if vacancy_g_1y=. then do; missingvacancy_g_1y=1; vacancy_g_1y=0; end; else missingvacancy_g_1y=0;
if vacancy_g_2y=. then do; missingvacancy_g_2y=1; vacancy_g_2y=0; end; else missingvacancy_g_2y=0;
askingrent_g_1y=askingrent/askingrent_1y-1; askingrent_g_2y=askingrent/askingrent_2y-1;
if askingrent_g_1y=. then do; missingaskingrent_g_1y=1; askingrent_g_1y=0; end; else missingaskingrent_g_1y=0;
if askingrent_g_2y=. then do; missingaskingrent_g_2y=1; askingrent_g_2y=0; end; else missingaskingrent_g_2y=0; run;
proc sort data=pprhist nodup; by qtr; proc sort data=irhist_withlags nodup; by qtr; run;

data costar.pprhist; merge pprhist(in=f1) irhist_withlags; by qtr;  if f1; if asgproptype='OT' and length(metrocode)>4 then delete;
proc sort nodup; by metrocode asgproptype qtr; run;

/*
data cremacr.PPRSubmarketHistory_dt; set cre.PPRSubmarketHistory_dt;run;
data cremacr.PPRSubmarket_dt; set cre.PPRSubmarket_dt;run;
data cremacr.PPRScenario_dt; set cre.PPRScenario_dt;run;
data cremacr.SmithTravelHistory_dt; set cre.SmithTravelHistory_dt;run;
data cremacr.SmithTravelZipToMarket2010_dt; set cre.SmithTravelZipToMarket2010_dt;run;
data cremacr.PPRZipToMarketMapping_dt; set cre.PPRZipToMarketMapping_dt;run;

data cremacr.ppr; set ppr;run;
data cremacr.pprCBSA; set pprCBSA;run;
data cremacr.mapCBSA; set mapCBSA;run;

*/
%mend;

%macro getProjPaths(simid=);
data irproj_qtr;  set macro.cmbs_macrovars_proj(keep=qtr simid ust10yr swap10yr swap1yr where=(simid=&simid)); where qtr ne .;
slope=swap10yr-swap1yr; drop simid; proc sort nodup; by qtr; run;

data irproj_l1; set irproj_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; run; proc sql noprint; select cats(name,'=',name,'_l1') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRPROJ_L1' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irproj_l1;  	rename &suffixlist; quit;

data irproj_l2; set irproj_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
proc sql noprint; select cats(name,'=',name,'_l2') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRPROJ_L2' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irproj_l2;  	rename &suffixlist; quit;

data irproj_l3; set irproj_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1;  proc sql noprint; select cats(name,'=',name,'_l3') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRPROJ_L3' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irproj_l3;  	rename &suffixlist; quit;

data irproj_l4; set irproj_qtr; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; if qtr-int(qtr/100)*100 =4 then qtr=qtr+100-3; else qtr=qtr+1; 
proc sql noprint; select cats(name,'=',name,'_l4') into :suffixlist separated by ' ' from dictionary.columns 
where libname = 'WORK' and memname = 'IRPROJ_L4' and name ne 'qtr'; quit; proc datasets library = work nolist; 	modify irproj_l4;  	rename &suffixlist; quit;

data irproj_withlags; merge irproj_qtr(in=f1) irproj_l1 irproj_l2 irproj_l3 irproj_l4; by qtr; if f1; run;

data keepmetro; set macro.cmbs_macrovars_proj; keep asgproptype metrocode; proc sort nodup; by asgproptype metrocode;run;
proc sort data=costar.pprhist; by asgproptype metrocode;

data pprhist_dups; merge costar.pprhist(keep=asgproptype metrocode vacancy pprcaprate askingrent ust10yr swap10yr swap1yr stock qtr where=(qtr>=201001)) keepmetro(in=f1);
by asgproptype metrocode; if f1;  proc sort nodup; by asgproptype metrocode qtr; run;
 
proc sort data=macro.cmbs_macrovars_proj; by simid asgproptype metrocode; run;

data pprproj1; merge macro.cmbs_macrovars_proj(where=(simid=&simid) keep=simid asgproptype metrocode vacancy pprcaprate askingrent  qtr)  
pprhist_dups(keep= asgproptype metrocode vacancy pprcaprate askingrent  stock qtr rename=stock=stock0);
by  asgproptype metrocode qtr; retain stock; if first.metrocode or stock0 ne . then stock=stock0; drop stock0 simid; 
if askingrent<=0 then askingrent=.; if vacancy<=0 then vacancy=.; if stock<=0 then stock=.; 
if not (askingrent=. and vacancy=. ); if asgproptype='HT' or metrocode='ZPPR54' then maincode=metrocode; else maincode=substr(metrocode,1,4); run;

data map_sub; set cmbs.ppr_ZipToSubmarket_dt;keep SubmarketCode zipCode asgPropType;		 proc sort nodup; by zipcode asgproptype;
data map_metro; set cmbs.ppr_ZipToMetro_dt (keep=zipcode metrocode); asgPropType='IN'; output;	
asgPropType='MF'; output;	asgPropType='OF'; output;	asgPropType='RT'; output; 	 proc sort nodup; by zipcode asgproptype;run; 
data map_hotel; set cmbs.str_ZipToMarket_dt (keep=zipcode marketcode); asgPropType='HT'; metrocode=marketcode;	
drop marketcode; proc sort nodup; by zipcode asgproptype; run;

data mapping0; merge map_hotel map_sub map_metro ; by zipcode asgproptype;
SubmarketCode=substr(SubmarketCode,find(SubmarketCode,"-")+1,length(SubmarketCode)); 
SubmarketCode=compress(tranwrd(SubmarketCode,"-",""));  
proc sort nodup; by metrocode asgproptype; run;

proc sql; create table statemapping0 as select distinct state,c.* 
from mapping0 a, wlres.zipcodesdotcom_dt b, pprproj1 c where a.zipcode=b.zipcode and a.metrocode =c.metrocode  and a.asgproptype=c.asgproptype
order by state,asgproptype, metrocode,qtr;run;

data statemapping1; set statemapping0; keep state asgproptype metrocode qtr; if qtr-int(qtr/100)*100=4 then qtr=qtr+100-3; else qtr=qtr+1; run;

data statemapping2; merge statemapping0(in=f2 rename=(pprcaprate=pprcaprate2 vacancy=vacancy2  
askingrent=askingrent2 stock=stock2 )) statemapping1(in=f1); by state asgproptype metrocode qtr; if f1 and f2; run;
data statemapping3; merge statemapping0 statemapping2; by state asgproptype metrocode qtr;run;

proc means data=statemapping3 noprint; where  ; weight stock; class state asgproptype qtr; var pprcaprate vacancy  askingrent
pprcaprate2 vacancy2  askingrent2; output out=state_ppr1 mean=;run;

proc means data=statemapping3 noprint; where  metrocode=maincode; class state asgproptype qtr; var stock stock2;
output out=state_ppr2 sum= ;run;

data state_ppr1; set state_ppr1; if state='' then state='US'; proc sort nodup; by state asgproptype qtr;
data state_ppr2; set state_ppr2;  if state='' then state='US'; proc sort nodup; by state asgproptype qtr;
data state_ppr3; merge state_ppr1 state_ppr2; where state ne '' and asgproptype ne '' and qtr ne .; 
by state asgproptype qtr; length maincode metrocode $5.; maincode=state; metrocode=state;   drop state ; 
proc sort nodup; by metrocode asgproptype descending qtr; run;

data state_ppr4; set state_ppr3(rename=(pprcaprate=pprcaprate1 vacancy=vacancy1 
askingrent=askingrent1  stock=stock1)); by metrocode asgproptype descending qtr; retain vac rent caprate   st;
if first.asgproptype then do; vac=vacancy1-vacancy2; rent=askingrent1/askingrent2; caprate=pprcaprate1-pprcaprate2; 
 st=stock1/stock2; vacancy=vacancy1; askingrent=askingrent1; pprcaprate=pprcaprate1;
  stock=stock1; if vac=. then vac=0; if rent=. then rent=1; if caprate=. then caprate=0;
if roomrate=. then roomrate=1; if dm=. then dm=1; if st=. then st=1; end; else do;  vacancy=vacancy1+vac; askingrent=askingrent1*rent; pprcaprate=pprcaprate1+caprate;
 stock=stock1*st; vac=vac+vacancy1-vacancy2; rent=rent*askingrent1/askingrent2; 
caprate=caprate+pprcaprate1-pprcaprate2;  st=st*stock1/stock2; 
if rent=. then rent=1; if caprate=. then caprate=0; if roomrate=. then roomrate=1; if dm=. then dm=1; if st=. then st=1; end;
drop pprcaprate1 vacancy1  askingrent1  stock1 pprcaprate2 vacancy2  askingrent2 
stock2 vac rent caprate   st;proc sort nodup; by metrocode asgproptype qtr; run; 

data pprproj2; set  pprproj1 state_ppr4; drop occupancy; drop _TYPE_ _FREQ_; proc sort nodup; by metrocode asgproptype qtr; run;
proc means data=pprproj2 noprint; where asgproptype ne 'HT'; weight stock; class metrocode qtr; var pprcaprate vacancy askingrent; output out=other_pprproj1 mean=;run;
proc means data=pprproj2 noprint; where asgproptype ne 'HT';  class metrocode qtr; var  stock; output out=other_pprproj2 sum= ;run;

data other; merge other_pprproj1 other_pprproj2; by metrocode qtr; where metrocode ne '' and qtr ne .; asgproptype='OT'; run;

data pprproj3; set pprproj2 other;  drop maincode;  proc sort nodup; by qtr; run;

data keep; set pprproj3; keep metrocode asgproptype; proc sort nodup; by metrocode asgproptype;run;
data mappingproj; merge mapping0(in=f1) keep(in=f2); by metrocode asgproptype; if f1 and f2;run;

proc sort data=pprproj3; by asgproptype metrocode qtr;

data pprproj; merge pprproj3 pprproj3(firstobs=2 keep=asgproptype metrocode qtr vacancy askingrent pprcaprate
rename=(asgproptype=next_type metrocode=next_code qtr=next_qtr vacancy=next_vacancy askingrent=next_rent pprcaprate=next_pprcaprate));
if not(asgproptype=next_type and metrocode=next_code and int(next_qtr/100)*4+next_qtr-int(next_qtr/100)*100-(int(qtr/100)*4+qtr-int(qtr/100)*100)=1)
then do; next_vacancy=.; next_pprcaprate=.; next_rent=.; end; drop next_type next_code next_qtr; 
data pprproj; set pprproj; by asgproptype metrocode qtr;
vacancy_l1=lag(vacancy); pprcaprate_l1=lag(pprcaprate); askingrent_l1=lag(askingrent); 
if first.metrocode then do; vacancy_l1=.; pprcaprate_l1=.; askingrent_l1=.; end; drop _TYPE_ _FREQ_; run;

data pprproj_tmp; set pprproj; if next_vacancy >0 and vacancy>0 and next_rent>0 and askingrent>0; proc sort nodup; by qtr; run;

proc sql; create table peakvac as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionPeakVac, a.vacancy-b.vacancy as Vac_g_peak
from pprproj_tmp a, pprproj_tmp b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr  and b.vacancy>0 and a.vacancy>0
group by a.metrocode, a.qtr,a.asgproptype having max(b.vacancy)=b.vacancy order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table troughvac as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionTroughVac,a.vacancy-b.vacancy as Vac_g_trough
from pprproj_tmp a, pprproj_tmp b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.vacancy>0 and a.vacancy>0
group by a.metrocode, a.qtr,a.asgproptype having min(b.vacancy)=b.vacancy order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table peakrent as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionPeakRent, a.askingrent/b.askingrent-1 as rent_g_Peak
from pprproj_tmp a, pprproj_tmp b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.askingrent>0 and a.askingrent>0
group by a.metrocode, a.qtr,a.asgproptype having max(b.askingrent)=b.askingrent order by a.metrocode,a.asgproptype,a.qtr;

proc sql; create table troughrent as select distinct a.qtr, a.metrocode, a.asgproptype, 
int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100 - max(int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100) as tRegionTroughRent, a.askingrent/b.askingrent-1 as rent_g_Trough
from pprproj_tmp a, pprproj_tmp b where a.metrocode=b.metrocode and a.asgproptype=b.asgproptype and a.qtr-1000<=b.qtr<=a.qtr and b.askingrent>0 and a.askingrent>0
group by a.metrocode, a.qtr,a.asgproptype having min(b.askingrent)=b.askingrent order by a.metrocode,a.asgproptype,a.qtr;

data peaktrough; merge peakvac troughvac peakrent troughrent;
by metrocode asgproptype qtr;  proc sort data=peaktrough nodup; by asgproptype qtr; run;


data peaktrough; merge peaktrough(in=f1) peaktrough(where=(metrocodeUS='US') rename= (Vac_g_peak=USVac_g_peak Vac_g_trough=USVac_g_trough rent_g_Peak=USrent_g_Peak
rent_g_Trough=USrent_g_Trough tRegionPeakVac=tUSPeakVac tRegionTroughVac=tUSTroughVac tRegionPeakRent=tUSPeakRent tRegionTroughRent=tUSTroughRent)
keep=metrocodeUS asgproptype qtr Vac_g_peak Vac_g_trough rent_g_Peak rent_g_Trough tRegionPeakVac tRegionTroughVac tRegionPeakRent tRegionTroughRent);
by asgproptype qtr; if f1; drop metrocodeUS; proc sort nodup; by metrocode asgproptype qtr; run;

proc sort data=pprproj_tmp nodup; by metrocode asgproptype qtr; run;
data pprproj_1y; set pprproj_tmp(keep=vacancy askingrent metrocode asgproptype qtr rename=(vacancy=vacancy_1y askingrent=askingrent_1y)); qtr=qtr-100; 
data pprproj_2y; set pprproj_tmp(keep=vacancy askingrent metrocode asgproptype qtr rename=(vacancy=vacancy_2y askingrent=askingrent_2y)); qtr=qtr-200;
data pprprojhist0; merge pprproj_tmp peaktrough pprproj_1y pprproj_2y; by metrocode asgproptype qtr; 
vacancy_g_1y=vacancy-vacancy_1y; vacancy_g_2y=vacancy-vacancy_2y;
if vacancy_g_1y=. then do; missingvacancy_g_1y=1; vacancy_g_1y=0; end; else missingvacancy_g_1y=0;
if vacancy_g_2y=. then do; missingvacancy_g_2y=1; vacancy_g_2y=0; end; else missingvacancy_g_2y=0;
askingrent_g_1y=askingrent/askingrent_1y-1; askingrent_g_2y=askingrent/askingrent_2y-1;
if askingrent_g_1y=. then do; missingaskingrent_g_1y=1; askingrent_g_1y=0; end; else missingaskingrent_g_1y=0;
if askingrent_g_2y=. then do; missingaskingrent_g_2y=1; askingrent_g_2y=0; end; else missingaskingrent_g_2y=0;
drop roomate dm; 
proc sort data=pprprojhist0 nodup; by qtr;
proc sort data=irproj_withlags nodup; by qtr;
data costar.pprprojhist&simid.; merge pprprojhist0(in=f1) irproj_withlags; by qtr;  if f1;
proc sort nodup; by metrocode asgproptype qtr; run;
%mend;

%macro genprop(inp1=,inp2=,qtr=,reportMon=);
/*
data occ_ts; set testbed.CoStar_Historical(rename=(qtr=qtr1 occupiedpct=occ));  qtr=yr*100+qtr1; drop yr qtr1; OccupiedPct=occ/100; 
if 0<=OccupiedPct<=1; drop occ;  reportmon=int(qtr/100)*100+(qtr-int(qtr/100)*100)*3; run;


data lastobs; set testbed.CoStar_Prop(rename=(PropertyTypeName=PropertyType  NumberOfStories=NoOfStory RentableBuildingArea=RentableArea
LandAreaAC=AreaAC StateCode=State TotalAvailableSpaceSF=AvailableSpaceSF
WeightedAverageRent=AverageRent YearBuilt=BuiltYear YearRenovated=RenovateYear ZipCode=ZipCode1)
drop=IndustrialSubmarketName OfficeSubmarketName AlternativeSubmarketDisplayName 
BuildingPark   MonthBuilt MonthRenovated 
stateid MapPage TypicalFloorSize PercentLeased  ParkingRatio
RentLow RentHigh  Latitude Longitude CountyName SubmarketClusterName PrimaryLeasingCompanyName PrimaryAgentName
PrimaryAgentSort PrimaryAgentPhone OwnerName Sewer LoadingDocksSort CeilingHeightSum CeilingHeightSort PropertyTypeDisplay
RailSum ColumnSpacingSum SalePricePerSF CranesSort DriveInsSort ProposedLandUse LandPricePerAC IsOwnerOccupied
PropertySqlDate SalesCompanyName SalesAgentName IsPropertyForSale SalePrice PropertyAdType Zoning
IsTotalSfAvailForSale LeedEnergyStarStatus numunits avgUnitSize totalNumRooms AnchorGLA AnchorTenants
BuildingOperatingExpenses BuildingTaxExpenses ); where rentablearea ne .; ZipCode=ZipCode1*1; drop ZipCode1; drop percLeased occupiedpct;
qtr=201604; reportmon=201612;  proc sort nodup; by propertyid;  run;

proc sort data=occ_ts; by propertyid descending reportmon;
proc sort data=costar.Allprop_timeseries_cj; by propertyid descending reportmon; 
proc sort data=lastobs; by propertyid descending reportmon;run;

data costar.Allprop_timeseries; merge occ_ts(in=f1 rename=OccupiedPct=PercLeased)   costar.Allprop_timeseries_cj(drop=VacantSpace PercLeased totalArea LeasedSqft AreaAC BuildingAddress SaleStatus SubmarketName City 
rename=(AvailableSpaceSF=AvailableSpaceSF0 AverageRent=AverageRent0 BuildingClass=BuildingClass0  BuildingName=BuildingName0 BuildingStatus=BuildingStatus0
BuiltYear=BuiltYear0 NoOfStory=NoOfStory0 PropertyType=PropertyType0 RenovateYear=RenovateYear0 RentableArea=RentableArea0
SecondaryType=SecondaryType0 State=State0 Tenancy=Tenancy0   ZipCode=ZipCode0)) lastobs(drop=VacantSpace AreaAC BuildingAddress  SaleStatus SubmarketName City 
rename=(AvailableSpaceSF=AvailableSpaceSF0 AverageRent=AverageRent0 BuildingClass=BuildingClass0  BuildingName=BuildingName0 BuildingStatus=BuildingStatus0
BuiltYear=BuiltYear0 NoOfStory=NoOfStory0 PropertyType=PropertyType0 RenovateYear=RenovateYear0 RentableArea=RentableArea0
SecondaryType=SecondaryType0 State=State0 Tenancy=Tenancy0   ZipCode=ZipCode0)); by propertyid descending reportmon; if f1;
retain availablespacesf averagerent buildingclass buildingname buildingstatus builtyear noofStory propertytype renovateyear rentablearea secondarytype state tenancy  zipcode;
if rentablearea0 ne . or first.propertyid then do;
AvailableSpaceSF=AvailableSpaceSF0;AverageRent=AverageRent0;BuildingClass=BuildingClass0;;BuildingName=BuildingName0;BuildingStatus=BuildingStatus0;
BuiltYear=BuiltYear0;NoOfStory=NoOfStory0;PropertyType=PropertyType0;RenovateYear=RenovateYear0;RentableArea=RentableArea0;
SecondaryType=SecondaryType0;State=State0;Tenancy=Tenancy0; ZipCode=ZipCode0;
end; leasedsqft=percLeased*rentableArea; vacantspace=rentableArea-leasedsqft;
if (reportMon-int(reportMon/100)*100)<=3 then qtr=int(reportMon/100)*100+1;  else if (reportMon-int(reportMon/100)*100)<=6 then qtr=int(reportMon/100)*100+2;  
else if (reportMon-int(reportMon/100)*100)<=9 then qtr=int(reportMon/100)*100+3; else if (reportMon-int(reportMon/100)*100)<=12 then qtr=int(reportMon/100)*100+4;
 drop AvailableSpaceSF0 AverageRent0 BuildingClass0  BuildingName0 BuildingStatus0 BuiltYear0 NoOfStory0 PropertyType0 RenovateYear0 RentableArea0 SecondaryType0  State0 Tenancy0 ZipCode0; run;
proc sort data=costar.Allprop_timeseries; by propertyid  reportmon;run;

*/
data prop_tmp; set &inp1.(in=f1 where=(reportMon>=&reportMon-300 and reportmon<=&reportMon));  if (reportMon-int(reportMon/100)*100) in (3,6,9,12);
proc sort data=prop_tmp nodup; by PropertyID reportMon; run;
data prop_tmp; set prop_tmp; by PropertyID reportMon; PercLeased_lag1=lag(PercLeased); PercLeased_lag2=lag2(PercLeased); PercLeased_lag3=lag3(PercLeased); PercLeased_lag4=lag4(PercLeased);
PercLeased_lag5=lag5(PercLeased); PercLeased_lag6=lag6(PercLeased); PercLeased_lag7=lag7(PercLeased); PercLeased_lag8=lag8(PercLeased);
if propertyid ne lag(propertyid) then PercLeased_lag1=.; if propertyid ne lag2(propertyid) then PercLeased_lag2=.;
if propertyid ne lag3(propertyid) then PercLeased_lag3=.; if propertyid ne lag4(propertyid) then PercLeased_lag4=.;
if propertyid ne lag5(propertyid) then PercLeased_lag5=.; if propertyid ne lag6(propertyid) then PercLeased_lag6=.;
if propertyid ne lag7(propertyid) then PercLeased_lag7=.; if propertyid ne lag8(propertyid) then PercLeased_lag8=.;
if qtr=&qtr; run;

data prop; merge prop_tmp(in=f1 ) &inp2.(in=f2); by PropertyID; if f1 and f2; 
tInMonths= int(reportMon/100)*12 +reportMon-int(reportMon/100)*100; tp1= int(rYr_0/100)*12 +rYr_0-int(rYr_0/100)*100;
tFromR_1=tInMonths-tp1;	**time to renovate, time from latest Renovate; nRenovate=0; array rYr(*) rYr_0-rYr_&maxren.; 
do i=2 to dim(rYr); if reportMon>rYr(i-1) and rYr(i)>rYr(i-1) then do; tFromR_1=tInMonths-tp1;	nRenovate=nRenovate+1;	
tp1= int(rYr(i)/100)*12 +rYr(i)-int(rYr(i)/100)*100;	end; end;	if reportMon>rYr(&maxren_1.) and rYr(&maxren_1.)>rYr(&maxren.) then 
tFromR_1=tInMonths-tp1;		length asgPropType $5.;		if PropertyType='Multi-Family' then asgPropType='MF';	
else if PropertyType='Office' then asgPropType='OF';	else if PropertyType='Specialty' then asgPropType='SP';		
else if PropertyType='Retail' then asgPropType='RT';	else if PropertyType='Mixed-Use' then asgPropType='MX';	
else if PropertyType='Flex' then asgPropType='FX';		else if PropertyType='Hospitality' then asgPropType='HT';	
else if PropertyType='Land' then asgPropType='LN';		else if PropertyType='Health Care' then asgPropType='MD';	
else if PropertyType='Industrial' then asgPropType='IN'; else asgPropType='UK';	drop i tp1 tInMonths; 	
if (reportMon-int(reportMon/100)*100)<=3 then qtr=int(reportMon/100)*100+1; 
else if (reportMon-int(reportMon/100)*100)<=6 then qtr=int(reportMon/100)*100+2;  
else if (reportMon-int(reportMon/100)*100)<=9 then qtr=int(reportMon/100)*100+3; 
else if (reportMon-int(reportMon/100)*100)<=12 then qtr=int(reportMon/100)*100+4;  substr(reportQrt,6,1)=qtr;	
highRise=(NoOfStory>10);	midRise=(NoOfStory>4);	classB=(BuildingClass='B');	 classA=(BuildingClass='A');
if asgPropType in ('RT' 'IN') then do;	highRise=(NoOfStory>3);	midRise=(NoOfStory>1);	end; 
drop AreaAC BuildingAddress MoodysFIPS SaleStatus SubmarketName City Submarket first_Mon MoodysFIPS NoOfStory;


if AvailableSpaceSF<=0 then AvailableSpaceSF=VacantSpace;	demolish=(BuildingStatus='Demolished');	
if RentableArea>0 and PercLeased>=0;	possible_reBuild=(BuildingStatus not in ('Existing' 'Demolished'));	
if AskingRent<=0 then AskingRent=AverageRent;	hasRenovate=(nRenovate>0);			multiRenovate=(nRenovate>1);
if AverageRent<=0 then AverageRent=AskingRent;	singleTenant=(Tenancy='Single'); 	tFromR_2=min(120,max(tFromR_1,0)); 		
if PercLeased=0 then lease_xbeta=-7;	else if PercLeased=1 then lease_xbeta=7;	
else lease_xbeta=max(-7,min(7,log(PercLeased/(1-PercLeased))));		retain tEmpty; 
full=(PercLeased>0.995); empty=(PercLeased=0); 
if first.propertyid or Empty=0 then tEmpty=0;  if Empty=1 then tEmpty+1; 

proptype_orig=asgproptype; 
if asgproptype='SS' then asgproptype='IN';  if asgproptype='SP' then asgproptype='RT'; 
if asgproptype not in ('MF','IN','OF','RT','HT') then asgproptype='OT'; 
if zipcode ne . and asgproptype ne '' then do;
submetro=symget(compress("submetro"||asgproptype||zipcode));
metrocode=symget(compress("metro"||asgproptype||zipcode));
end;
if state in (&statelist) then statecode=state; else statecode='US';
proc sort nodup; by asgproptype submetro  qtr; run;


proc sort data=pprprojhist; by asgproptype metrocode qtr; run;

data propmetro0 propsub; merge prop(in=f1) pprprojhist(rename=metrocode=submetro); by asgproptype submetro qtr; if f1; 
if vacancy=. then output propmetro0; else output propsub;
proc sort data=propmetro0 nodup; by asgproptype metrocode qtr;  run;

data propstate0 propmetro; merge propmetro0 (in=f1 drop=ust10yr--slope_l4) pprprojhist; by asgproptype metrocode qtr; if f1; 
if vacancy=. then output propstate0; else output propmetro;
proc sort data=propstate0 nodup; by asgproptype statecode qtr; 

data propstate propus0; merge propstate0(in=f1 drop=ust10yr--slope_l4) pprprojhist(rename=metrocode=statecode); by asgproptype statecode qtr; if f1; 
if vacancy ne . then output propstate; else output propus0; run;
proc sort data=propus0; by asgproptype qtr;
data propus; merge propus0(in=f1 drop=ust10yr--slope_l4) pprprojhist(where=(metrocode='US')); by asgproptype qtr;  if f1;run;

data prop2; set propmetro(in=f1) propsub(in=f2) propstate(in=f3) propus(in=f4);
if f1 then metrocode_match=metrocode; else if f2 then metrocode_match=submetro; else if f3 then metrocode_match=statecode; else
if f4 then metrocode_match='US' ; proc sort data=prop2 nodup; by asgproptype propertyid qtr;run;

data costar.ThisPeriodCostar_ppr_merged; merge prop2 prop2(firstobs=2 keep=leasedsqft propertyid qtr rentablearea 
lease_xbeta PercLeased RentableArea rename=(leasedsqft=next_leasedsqft propertyid=next_id qtr=next_qtr
lease_xbeta=next_lease_xbeta PercLeased=next_percLease  rentablearea=next_RentableArea)); 

if next_id ne propertyid or int(qtr/100)*4+qtr-int(qtr/100)*100+1 ne int(next_qtr/100)*4+next_qtr-int(next_qtr/100)*100 
then do; next_leasedsqft=.; next_lease_xbeta=.; next_percLease=.; next_RentableArea=.; end; 
PercLeased_l1=lag(PercLeased); leasedSqft_l1=lag(leasedSqft); 
if propertyid ne lag(propertyid) then do; PercLeased_l1=.; leasedSqft_l1=.; end; 
moveout=leasedSqft-leasedSqft_l1; moveoutpct=percleased-percLeased_l1; drop next_qtr next_id; 

if vacancy ne . then do; if Vacancy=0 then occ_xbeta=-7;		else if Vacancy=1 then occ_xbeta=7;	
else occ_xbeta=max(-7,min(7,log((1-Vacancy)/Vacancy))); vacancy_hi2=(vacancy>0.15);	vacancy_hi=(vacancy>0.08);	
vacancy_hi3=(vacancy>0.3); end;	classU=(BuildingClass in ('F' 'U'));	

vacancy_g=next_Vacancy-Vacancy;		rent_g=next_rent/AskingRent-1; 	rent_g_2=min(0.1,max(rent_g+0.05,0));	
rent_g_1=(rent_g<-0.02);	vacancy_g_1=(vacancy_g<-0.05);	vacancy_g_2=min(0.1,max(vacancy_g+0.1,0));	
if vacancy_g=. then do; vacancy_g_1=.;vacancy_g_2=.;end;

if vacancy ne . and PercLeased ne . then do;
empty_bad1=(PercLeased<0.45)*max(vacancy-0.05,0); 	empty_bad2=(PercLeased<0.45)*max(vacancy-0.12,0); 
empty_bad3=(PercLeased<0.35)*max(vacancy-0.05,0); 	empty_bad4=(PercLeased<0.35)*max(vacancy-0.12,0);
empty_bad5=(PercLeased<0.15)*max(vacancy-0.05,0); 	empty_bad6=(PercLeased<0.15)*max(vacancy-0.12,0);
empty_bad7=(PercLeased<0.05)*max(vacancy-0.05,0); 	empty_bad8=(PercLeased<0.05)*max(vacancy-0.12,0);

empty_ren1=max(0.1-PercLeased,0)*(tFromR_1<12); 	empty_ren2=max(0.1-PercLeased,0)*(tFromR_1<30); 
empty_ren3=max(0.25-PercLeased,0)*(tFromR_1<12);  	empty_ren4=max(0.25-PercLeased,0)*(tFromR_1<30); 
empty_ren5=max(0.45-PercLeased,0)*(tFromR_1<12);  	empty_ren6=max(0.45-PercLeased,0)*(tFromR_1<30); 

full_bad1=(PercLeased>0.98)*max(vacancy-0.05,0);	full_bad2=(PercLeased>0.98)*max(vacancy-0.15,0);	
end;
if PercLeased ne . then do;
demolish_empty=demolish*max(0.5-PercLeased,0);	rebuild_full=possible_reBuild*max(PercLeased-0.8,0); 
PercLeased_lo=(PercLeased<0.05);
end;
if lease_xbeta ne . then do;
lease_lag_0=min(max(-2,lease_xbeta+3),1.1);			lease_lag_1=min(max(-1.1,lease_xbeta+1.9),0.8);	
lease_lag_2=min(max(-0.8,lease_xbeta+1.1),0.7);		lease_lag_3=min(max(-0.7,lease_xbeta+0.4),0.6);	
lease_lag_4=min(max(-0.6,lease_xbeta-0.2),1.0);		lease_lag_5=min(1.0, max(0,lease_xbeta-1.2));	
lease_lag_6=min(1.0, max(0,lease_xbeta-2.2));		lease_lag_7=min(1.8, max(0,lease_xbeta-3.2));	
**-3 vs 4.9%, -1.9 vs 13%, -1.1 vs 25%, -0.4 vs 40%, 0.2 vs 55%, 1.2 vs 77%, 2.2 vs 90%, 3.2 vs 96.1%;
end;
if asgPropType in ('MF' 'IN') then do;	buildSize_2=(RentableArea>80000);	buildSize_4=(RentableArea>4000);
buildSize_1=(RentableArea>150000);	buildSize_3=(RentableArea<10000); 	end; 	else do;
buildSize_4=(RentableArea>2000); 	buildSize_1=(RentableArea>80000);	buildSize_2=(RentableArea>40000);
buildSize_3=(RentableArea<5000); end; 
BuildAge_1=(rYr_0<195000);	BuildAge_2=(rYr_0<199000);	BuildAge_3=(rYr_0>200500);	RenovateAge_4=(tFromR_1>12); 	
RenovateAge_1=(tFromR_1>360);	RenovateAge_2=(tFromR_1>120);	RenovateAge_3=(tFromR_1>36); 

PercLeased_g=PercLeased-lag(PercLeased); if propertyid ne lag(propertyid) then PercLeased_g=.;
PercLeased_g_l1=lag(PercLeased_g);PercLeased_g_l2=lag2(PercLeased_g);PercLeased_g_l3=lag3(PercLeased_g);
PercLeased_g_l4=lag4(PercLeased_g);PercLeased_g_l5=lag5(PercLeased_g);PercLeased_g_l6=lag6(PercLeased_g);
PercLeased_g_l7=lag7(PercLeased_g);PercLeased_g_l8=lag8(PercLeased_g);
if  propertyid ne lag(propertyid) then PercLeased_g_l1=.; if propertyid ne lag2(propertyid) then PercLeased_g_l2=.; 
if propertyid ne lag3(propertyid) then PercLeased_g_l3=.; if propertyid ne lag4(propertyid) then PercLeased_g_l2=.; 
if propertyid ne lag5(propertyid) then PercLeased_g_l5=.; if propertyid ne lag6(propertyid) then PercLeased_g_l6=.; 
if propertyid ne lag7(propertyid) then PercLeased_g_l7=.; if propertyid ne lag8(propertyid) then PercLeased_g_l8=.; 

tEmpty_PercLeased_g_l1=tEmpty*PercLeased_g_l1; tEmpty_sq=tEmpty**2; 
empty_lease_g_l1=empty*PercLeased_g_l1;  empty_lease_g_l2=empty*PercLeased_g_l2;
empty_lease_g_l3=empty*PercLeased_g_l1;  empty_lease_g_l4=empty*PercLeased_g_l2;
empty_lease_g_l5=empty*PercLeased_g_l1;  empty_lease_g_l6=empty*PercLeased_g_l2;
empty_lease_g_l7=empty*PercLeased_g_l1;  empty_lease_g_l8=empty*PercLeased_g_l2;

full_lease_g_l1=full*PercLeased_g_l1;  full_lease_g_l2=full*PercLeased_g_l2;
full_lease_g_l3=full*PercLeased_g_l1;  full_lease_g_l4=full*PercLeased_g_l2;
full_lease_g_l5=full*PercLeased_g_l1;  full_lease_g_l6=full*PercLeased_g_l2;
full_lease_g_l7=full*PercLeased_g_l1;  full_lease_g_l8=full*PercLeased_g_l2;

lowlease_lease_g_l1=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l2=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l3=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l4=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l5=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l6=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l7=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l8=PercLeased_lo*PercLeased_g_l2;

tEmpty_PercLeased=tEmpty*PercLeased; 
empty_lease=empty*lease_xbeta;  full_lease=full*lease_xbeta; lowlease_lease=PercLeased_lo*PercLeased;
drop rYr: nRenovate next_rent next_vacancy;  run; 
%mend;

%macro processprop();
proc sql; select count(distinct propertyid) from costar.allprop_timeseries;run;
data firstobs; set costar.allprop_timeseries; by propertyid reportmon;
if first.propertyid; drop reportmon;  run;

data occ_ts; set testbed.CoStar_Historical(rename=(qtr=qtr1 occupiedpct=occ));  qtr=yr*100+qtr1; drop yr qtr1; OccupiedPct=occ/100; 
if 0<=OccupiedPct<=1; drop occ; proc sort nodup; by propertyid qtr; run;

data delete costar.allprop_timeseries2; merge occ_ts(where=(qtr<=201100) in=f1) firstobs; by propertyid; if f1;
if renovateYear>qtr then RenovateYear=BuiltYear; percLeased=OccupiedPct; leasedsqft=percLeased*rentableArea;
vacantspace=rentableArea-leasedsqft; availableSpaceSF=vacantspace; drop OccupiedPct; reportMon=int(qtr/100)*100+(qtr-int(qtr/100)*100)*3;
averagerent=.;  if BuiltYear>qtr then output delete; else output costar.allprop_timeseries2;
run;

data costar.allprop_timeseries2; set costar.allprop_timeseries2 costar.allprop_timeseries; by propertyid reportmon;

proc sort data=coStar.allProp_Summary2; by propertyid;
proc sort data=coStar.allprop_timeseries; by propertyid reportmon;


data prop1; merge coStar.allprop_timeseries(in=f1) coStar.allProp_Summary2(in=f2); by PropertyID; if f1 and f2;
tInMonths= int(reportMon/100)*12 +reportMon-int(reportMon/100)*100; tp1= int(rYr_0/100)*12 +rYr_0-int(rYr_0/100)*100;
tFromR_1=tInMonths-tp1;	**time to renovate, time from latest Renovate; nRenovate=0; array rYr(5) rYr_0-rYr_4; 
do i=2 to 5; if reportMon>rYr(i-1) and rYr(i)>rYr(i-1) then do; tFromR_1=tInMonths-tp1;	nRenovate=nRenovate+1;	
tp1= int(rYr(i)/100)*12 +rYr(i)-int(rYr(i)/100)*100;	end; end;	if reportMon>rYr(5) and rYr(5)>rYr(4) then 
tFromR_1=tInMonths-tp1;		length asgPropType $5.;		if PropertyType='Multi-Family' then asgPropType='MF';	
else if PropertyType='Office' then asgPropType='OF';	else if PropertyType='Specialty' then asgPropType='SP';		
else if PropertyType='Retail' then asgPropType='RT';	else if PropertyType='Mixed-Use' then asgPropType='MX';	
else if PropertyType='Flex' then asgPropType='FX';		else if PropertyType='Hospitality' then asgPropType='HT';	
else if PropertyType='Land' then asgPropType='LN';		else if PropertyType='Health Care' then asgPropType='MD';
else if PropertyType='Industrial' then asgPropType='IN'; else asgPropType='UK';	drop i tp1 tInMonths; 	
if (reportMon-int(reportMon/100)*100)<=3 then qtr=int(reportMon/100)*100+1; 
else if (reportMon-int(reportMon/100)*100)<=6 then qtr=int(reportMon/100)*100+2;  
else if (reportMon-int(reportMon/100)*100)<=9 then qtr=int(reportMon/100)*100+3; 
else if (reportMon-int(reportMon/100)*100)<=12 then qtr=int(reportMon/100)*100+4;  substr(reportQrt,6,1)=qtr;	
if asgPropType='MF' and qtr>=200701 then delete;
highRise=(NoOfStory>10);	midRise=(NoOfStory>4);	classB=(BuildingClass='B');	 classA=(BuildingClass='A');
if asgPropType in ('RT' 'IN') then do;	highRise=(NoOfStory>3);	midRise=(NoOfStory>1);	end; 
drop AreaAC BuildingAddress MoodysFIPS SaleStatus SubmarketName City Submarket first_Mon MoodysFIPS NoOfStory;

if AvailableSpaceSF<=0 then AvailableSpaceSF=VacantSpace;	demolish=(BuildingStatus='Demolished');	
if RentableArea>0;	possible_reBuild=(BuildingStatus not in ('Existing' 'Demolished'));	
if AskingRent<=0 then AskingRent=AverageRent;	hasRenovate=(nRenovate>0);			multiRenovate=(nRenovate>1);
if AverageRent<=0 then AverageRent=AskingRent;	singleTenant=(Tenancy='Single'); 	tFromR_2=min(120,max(tFromR_1,0)); 		
if PercLeased>1 or PercLeased<0 then PercLeased=.;
if PercLeased ne . then do;
if PercLeased=0 then lease_xbeta=-7;	else if PercLeased=1 then lease_xbeta=7;	
else lease_xbeta=max(-7,min(7,log(PercLeased/(1-PercLeased))));	end;	retain tEmpty; 
full=(PercLeased>0.995); empty=(PercLeased=0); 
if first.propertyid or Empty=0 then tEmpty=0;  if Empty=1 then tEmpty+1; 
 proptype_orig=asgproptype; 
if asgproptype not in ('MF','IN','OF','RT','HT') then asgproptype='OT'; 
if zipcode ne . and asgproptype ne '' then do;
if zipcode>=10000 then do;
submetro=symget(compress("submetro"||asgproptype||zipcode));
metrocode=symget(compress("metro"||asgproptype||zipcode));
end;
else if zipcode>=1000 then do;
submetro=symget(compress("submetro"||asgproptype||"0"||zipcode));
metrocode=symget(compress("metro"||asgproptype||"0"||zipcode));
end; else do;
submetro=symget(compress("submetro"||asgproptype||"00"||zipcode));
metrocode=symget(compress("metro"||asgproptype||"00"||zipcode));
end; end; run;
proc sort data=prop1 nodup; by asgproptype submetro  qtr; run;

proc sort data=costar.pprhist; by asgproptype metrocode qtr; run;

data propmetro0 propsub; merge prop1(in=f1) costar.pprhist(rename=metrocode=submetro); by asgproptype submetro qtr; if f1; 
if vacancy=. then output propmetro0; else output propsub;

data propmetro0; set propmetro0 (drop=pprcaprate--pmms_15y_l4);  run;
proc sort data=propmetro0 nodup; by asgproptype metrocode qtr;  run;

data propstate0 propmetro; merge propmetro0 (in=f1) costar.pprhist; by asgproptype metrocode qtr; if f1; 
if vacancy=. then output propstate0; else output propmetro; 
data propstate0; set propstate0(drop=pprcaprate--pmms_15y_l4) ; proc sort data=propstate0 nodup; by asgproptype state qtr; 

data propstate propus0; merge propstate0(in=f1) costar.pprhist(rename=metrocode=state); by asgproptype state qtr; if f1; 
if vacancy ne . then output propstate; else output propus0; run;
data propus0; set propus0(drop=pprcaprate--pmms_15y_l4) ; proc sort data=propstate0 nodup; by asgproptype state qtr; 
proc sort data=propus0; by asgproptype qtr;
data propus; merge propus0(in=f1) costar.pprhist(where=(metrocode='US')); by asgproptype qtr;  if f1;run;

proc delete data=propmetro0; proc delete data=propstate0; 
proc delete data=propus0;run;
proc sort data=propmetro nodup; by asgproptype propertyid qtr;
proc sort data=propsub nodup; by asgproptype propertyid qtr;
proc sort data=propstate nodup; by asgproptype propertyid qtr;
proc sort data=propus nodup; by asgproptype propertyid qtr; run;

data prop2; set propmetro(in=f1) propsub(in=f2) propstate(in=f3) propus(in=f4);  by asgproptype propertyid qtr;
if f1 then metrocode_match=metrocode; else if f2 then metrocode_match=submetro; else if f3 then metrocode_match=state; else
if f4 then metrocode_match='US' ;

data costar.costar_ppr_merged; merge prop2 prop2(firstobs=2 keep=leasedsqft propertyid qtr rentablearea 
lease_xbeta PercLeased RentableArea rename=(leasedsqft=next_leasedsqft propertyid=next_id qtr=next_qtr
lease_xbeta=next_lease_xbeta PercLeased=next_percLease  rentablearea=next_RentableArea)); 

if next_id ne propertyid or int(qtr/100)*4+qtr-int(qtr/100)*100+1 ne int(next_qtr/100)*4+next_qtr-int(next_qtr/100)*100 
then do; next_leasedsqft=.; next_lease_xbeta=.; next_percLease=.; next_RentableArea=.; end; 
PercLeased_l1=lag(PercLeased); leasedSqft_l1=lag(leasedSqft); 
if propertyid ne lag(propertyid) then do; PercLeased_l1=.; leasedSqft_l1=.; end; 
moveout=leasedSqft-leasedSqft_l1; moveoutpct=percleased-percLeased_l1; drop next_qtr next_id; 

if vacancy ne . then do; if Vacancy=0 then occ_xbeta=-7;		else if Vacancy=1 then occ_xbeta=7;	
else occ_xbeta=max(-7,min(7,log((1-Vacancy)/Vacancy))); vacancy_hi2=(vacancy>0.15);	vacancy_hi=(vacancy>0.08);	
vacancy_hi3=(vacancy>0.3); end;	classU=(BuildingClass in ('F' 'U'));	

vacancy_g=next_Vacancy-Vacancy;		rent_g=next_rent/AskingRent-1; 	rent_g_2=min(0.1,max(rent_g+0.05,0));	
rent_g_1=(rent_g<-0.02);	vacancy_g_1=(vacancy_g<-0.05);	vacancy_g_2=min(0.1,max(vacancy_g+0.1,0));	
if vacancy_g=. then do; vacancy_g_1=.;vacancy_g_2=.;end;

if vacancy ne . and PercLeased ne . then do;
empty_bad1=(PercLeased<0.45)*max(vacancy-0.05,0); 	empty_bad2=(PercLeased<0.45)*max(vacancy-0.12,0); 
empty_bad3=(PercLeased<0.35)*max(vacancy-0.05,0); 	empty_bad4=(PercLeased<0.35)*max(vacancy-0.12,0);
empty_bad5=(PercLeased<0.15)*max(vacancy-0.05,0); 	empty_bad6=(PercLeased<0.15)*max(vacancy-0.12,0);
empty_bad7=(PercLeased<0.05)*max(vacancy-0.05,0); 	empty_bad8=(PercLeased<0.05)*max(vacancy-0.12,0);

empty_ren1=max(0.1-PercLeased,0)*(tFromR_1<12); 	empty_ren2=max(0.1-PercLeased,0)*(tFromR_1<30); 
empty_ren3=max(0.25-PercLeased,0)*(tFromR_1<12);  	empty_ren4=max(0.25-PercLeased,0)*(tFromR_1<30); 
empty_ren5=max(0.45-PercLeased,0)*(tFromR_1<12);  	empty_ren6=max(0.45-PercLeased,0)*(tFromR_1<30); 

full_bad1=(PercLeased>0.98)*max(vacancy-0.05,0);	full_bad2=(PercLeased>0.98)*max(vacancy-0.15,0);	
end;
if PercLeased ne . then do;
demolish_empty=demolish*max(0.5-PercLeased,0);	rebuild_full=possible_reBuild*max(PercLeased-0.8,0); 
PercLeased_lo=(PercLeased<0.05);
end;
if lease_xbeta ne . then do;
lease_lag_0=min(max(-2,lease_xbeta+3),1.1);			lease_lag_1=min(max(-1.1,lease_xbeta+1.9),0.8);	
lease_lag_2=min(max(-0.8,lease_xbeta+1.1),0.7);		lease_lag_3=min(max(-0.7,lease_xbeta+0.4),0.6);	
lease_lag_4=min(max(-0.6,lease_xbeta-0.2),1.0);		lease_lag_5=min(1.0, max(0,lease_xbeta-1.2));	
lease_lag_6=min(1.0, max(0,lease_xbeta-2.2));		lease_lag_7=min(1.8, max(0,lease_xbeta-3.2));	
**-3 vs 4.9%, -1.9 vs 13%, -1.1 vs 25%, -0.4 vs 40%, 0.2 vs 55%, 1.2 vs 77%, 2.2 vs 90%, 3.2 vs 96.1%;
end;
if asgPropType in ('MF' 'IN') then do;	buildSize_2=(RentableArea>80000);	buildSize_4=(RentableArea>4000);
buildSize_1=(RentableArea>150000);	buildSize_3=(RentableArea<10000); 	end; 	else do;
buildSize_4=(RentableArea>2000); 	buildSize_1=(RentableArea>80000);	buildSize_2=(RentableArea>40000);
buildSize_3=(RentableArea<5000); end; 
BuildAge_1=(rYr_0<195000);	BuildAge_2=(rYr_0<199000);	BuildAge_3=(rYr_0>200500);	RenovateAge_4=(tFromR_1>12); 	
RenovateAge_1=(tFromR_1>360);	RenovateAge_2=(tFromR_1>120);	RenovateAge_3=(tFromR_1>36); 

PercLeased_g=PercLeased-lag(PercLeased); if propertyid ne lag(propertyid) then PercLeased_g=.;
PercLeased_g_l1=lag(PercLeased_g);PercLeased_g_l2=lag2(PercLeased_g);PercLeased_g_l3=lag3(PercLeased_g);
PercLeased_g_l4=lag4(PercLeased_g);PercLeased_g_l5=lag5(PercLeased_g);PercLeased_g_l6=lag6(PercLeased_g);
PercLeased_g_l7=lag7(PercLeased_g);PercLeased_g_l8=lag8(PercLeased_g);
if  propertyid ne lag(propertyid) then PercLeased_g_l1=.; if propertyid ne lag2(propertyid) then PercLeased_g_l2=.; 
if propertyid ne lag3(propertyid) then PercLeased_g_l3=.; if propertyid ne lag4(propertyid) then PercLeased_g_l2=.; 
if propertyid ne lag5(propertyid) then PercLeased_g_l5=.; if propertyid ne lag6(propertyid) then PercLeased_g_l6=.; 
if propertyid ne lag7(propertyid) then PercLeased_g_l7=.; if propertyid ne lag8(propertyid) then PercLeased_g_l8=.; 

tEmpty_PercLeased_g_l1=tEmpty*PercLeased_g_l1; tEmpty_sq=tEmpty**2; 
empty_lease_g_l1=empty*PercLeased_g_l1;  empty_lease_g_l2=empty*PercLeased_g_l2;
empty_lease_g_l3=empty*PercLeased_g_l1;  empty_lease_g_l4=empty*PercLeased_g_l2;
empty_lease_g_l5=empty*PercLeased_g_l1;  empty_lease_g_l6=empty*PercLeased_g_l2;
empty_lease_g_l7=empty*PercLeased_g_l1;  empty_lease_g_l8=empty*PercLeased_g_l2;

full_lease_g_l1=full*PercLeased_g_l1;  full_lease_g_l2=full*PercLeased_g_l2;
full_lease_g_l3=full*PercLeased_g_l1;  full_lease_g_l4=full*PercLeased_g_l2;
full_lease_g_l5=full*PercLeased_g_l1;  full_lease_g_l6=full*PercLeased_g_l2;
full_lease_g_l7=full*PercLeased_g_l1;  full_lease_g_l8=full*PercLeased_g_l2;

lowlease_lease_g_l1=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l2=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l3=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l4=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l5=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l6=PercLeased_lo*PercLeased_g_l2;
lowlease_lease_g_l7=PercLeased_lo*PercLeased_g_l1;  lowlease_lease_g_l8=PercLeased_lo*PercLeased_g_l2;

tEmpty_PercLeased=tEmpty*PercLeased; 
empty_lease=empty*lease_xbeta;  full_lease=full*lease_xbeta; lowlease_lease=PercLeased_lo*PercLeased;
drop rYr_0-rYr_4 nRenovate next_rent next_vacancy; run; 

proc means; class asgproptype qtr; var percLeased; run;

proc delete data=propsub; proc delete data=propmetro; proc delete data=propstate;
proc delete data=propus;   proc delete data=prop1; run; proc delete data=prop2;  run;
%mend;

%macro process_spacetrans();

data transact;	set cmbs.CoStar_SpaceTransaction_dt(rename=(TenantCompanyID=TenantCoID TransactionTypeDesc=TransactTypeDesc
OriginalSignDate=OrigSignDate OriginalMoveDate=OrigMoveDate ExpirationDate=ExpireDate TransactionNetSqFt=TransactNetSqFt TenantCompanyName=TenantCoName
transactiondesc=TransactDesc LastExpirationDate=LastExpireDate));if missing(SignDate) then SignDate=SignDateSort;	if missing(MoveDate) then 
MoveDate=MoveDateSort;	move_date=year(datepart(MoveDate))*100 +month(datepart(MoveDate));	if move_date<=199001 then move_date
=year(datepart(MoveDateSort))*100 +month(datepart(MoveDateSort));	Sign_date=year(datepart(SignDate))*100 +month(datepart(SignDate));
if sign_date<=199001 then sign_date=year(datepart(SignDateSort))*100 +month(datepart(SignDateSort));
origSign_date=year(datepart(OrigSignDate))*100 +month(datepart(OrigSignDate)); if sign_date<=199001 then sign_date=origSign_date; 
origMove_date=year(datepart(OrigMoveDate))*100 +month(datepart(OrigMoveDate)); 	if move_date<=199001 then move_date=origMove_date;			
if move_date<=199001 then move_date=sign_date;	last_expire_date=year(datepart(LastExpireDate))*100 +month(datepart(LastExpireDate)); 
Update_date=year(datepart(UpdateDate))*100 +month(datepart(UpdateDate)); expire_date=year(datepart(ExpireDate))*100 +month(datepart(ExpireDate));  
if expire_date<=max(last_expire_date,max(move_date,199001)) then expire_date=last_expire_date;	
move_date=min(expire_date, max(move_date, sign_date));		sign_date=min(move_date, max(sign_date, expire_date));
if TransactTypeDesc not in ('Move In' 'Move Out' 'Renewal') then TransactTypeDesc='.';	if SpaceTypeName not in 
('New' 'Relet' 'Sublet') then SpaceTypeName='.';	update_date=max(update_date, max(move_date, sign_date));	
if expire_date ne . and move_date ne . and expire_date>move_date then 
LeaseTerm=(int(expire_date/100)*12+expire_date-int(expire_date/100)*100)-(int(move_date/100)*12+move_date-int(move_date/100)*100);
if (move_date-int(move_date/100)*100)<=3 then qtr=int(move_date/100)*100+1; 
else if (move_date-int(move_date/100)*100)<=6 then qtr=int(move_date/100)*100+2; 
else if (move_date-int(move_date/100)*100)<=9 then qtr=int(move_date/100)*100+3; 
else if (move_date-int(move_date/100)*100)<=12 then qtr=int(move_date/100)*100+4; 
TenantCoName=UPCASE(TenantCoName);
/*length fmtname $100.; fmtname=compress("Name"||tenantCoID||TenantCoName);
if fmtname='' then fmtname=compress("Name2"||SpaceTransactionID);*/
keep qtr SpaceTransactionID PropertyID TenantCoID TransactTypeDesc SpaceTypeName TransactNetSqFt OccupiedSqFt RentRate 
sign_date move_date MoveDate signdate expire_date TransactDesc update_date CurrentlyOccupied TenantCoName servicetypedesc 
LeaseTerm origMove_date ;  proc sort nodup; by PropertyID  movedate TenantCoName signdate;	run;

proc sql; create table transact0 as select distinct max(TenantCoID) as TenantCoID, *
from transact  group by propertyid, TenantCoName order by propertyid, TenantCoName; run;

data transact0; set transact0; tenantconame=tranwrd(tenantconame, " AND ", " & ");run;
proc sort  nodup; by propertyid TenantCoName;run;

data transact1; set transact0; orig_id=tenantcoid; orig_name=tenantconame; run;

%macro findsimilar0();
%let loop=1;
%do %while (&loop>=1);

proc sql; create table findsimilar as select distinct a.propertyid,a.tenantconame, a.tenantcoid, case when a.tenantcoid>b.tenantcoid 
or b.tenantcoid=. then a.tenantcoid else b.tenantcoid end as newtenantcoid, case when a.tenantcoid>b.tenantcoid or  (b.tenantcoid=. and a.tenantconame>b.tenantconame) 
then a.tenantconame else b.tenantconame end as newtenantconame from transact1 a, transact1 b where soundex(a.tenantconame)=soundex(b.tenantconame)
and (a.tenantcoid ne b.tenantcoid or a.tenantcoid=. or b.tenantcoid=.)
and not (a.tenantcoid=b.tenantcoid and a.tenantconame=b.tenantconame)
and a.tenantconame ne '' and a.propertyid=b.propertyid; proc sort nodup; by propertyid tenantconame; run;

data findsimilar; set findsimilar;  by propertyid tenantconame newtenantcoid newtenantconame; if last.tenantconame; run;

proc sql noprint; select count(1) into: loop from findsimilar;run;
%if (&loop>=1) %then %do;
data transact1; merge transact1 findsimilar; 
by propertyid tenantconame;  if newtenantconame ne '' then tenantconame=newtenantconame;
if newtenantcoid ne '' then tenantcoid=newtenantcoid; else tenantcoid=orig_id; drop newtenantconame newtenantcoid; 
proc sort nodup; by propertyid tenantconame;run;
proc sort nodup; by propertyid tenantconame;run;
%end; %end;
%mend;
%findsimilar0;

%macro findsimilar1();
%let loop=1;
%do %while (&loop>=1);
proc sql; create table findsimilar1 as select distinct a.propertyid,a.tenantconame, a.tenantcoid, case when a.tenantcoid>b.tenantcoid 
or b.tenantcoid=. then a.tenantcoid else b.tenantcoid end as newtenantcoid, case when a.tenantcoid>b.tenantcoid or (b.tenantcoid=. and a.tenantconame>b.tenantconame) 
then a.tenantconame else b.tenantconame end as newtenantconame from transact1 a, transact1 b where (soundex(compress(a.tenantconame))=soundex(compress(b.tenantconame)))
and (a.tenantcoid ne b.tenantcoid or a.tenantcoid=. or b.tenantcoid=.) 
and not (a.tenantcoid=b.tenantcoid and a.tenantconame=b.tenantconame)
and a.tenantconame ne '' and a.propertyid=b.propertyid; proc sort nodup; by propertyid tenantconame newtenantcoid newtenantconame; run;

data findsimilar1; set findsimilar1;   by propertyid tenantconame newtenantcoid newtenantconame; if last.tenantconame; run;

proc sql noprint; select count(1) into: loop from findsimilar1;run;
%if (&loop>=1) %then %do;
data transact1; merge transact1 findsimilar1(drop=tenantcoid); 
by propertyid tenantconame; if newtenantconame ne '' then tenantconame=newtenantconame;
if newtenantcoid ne '' then tenantcoid=newtenantcoid; drop newtenantconame newtenantcoid; run;
proc sort nodup; by propertyid tenantconame;run;
%end; %end;
%mend;
%findsimilar1;
data transact2; set transact1;run;
%macro findsimilarreverse();
%let loop=1;
%do %while (&loop>=1);
proc sql; create table findsimilar2 as select distinct  a.propertyid,a.tenantconame, a.tenantcoid, 
case when a.tenantcoid>b.tenantcoid or b.tenantcoid=. then a.tenantcoid else b.tenantcoid end as newtenantcoid, 
case when a.tenantcoid>b.tenantcoid or (b.tenantcoid=. and a.tenantconame>b.tenantconame)  then a.tenantconame
else b.tenantconame end as newtenantconame
from transact2 a, transact2 b where
 not (a.tenantcoid=b.tenantcoid and a.tenantconame=b.tenantconame) and
 (a.tenantcoid ne b.tenantcoid or a.tenantcoid=. or b.tenantcoid=.) and a.tenantconame ne '' and a.propertyid=b.propertyid
and (compress(a.tenantconame)=compress(b.tenantconame) or
((substr(soundex(reverse(compress(a.tenantconame))),1,10)=substr(soundex(reverse(compress(b.tenantconame))),1,10)
and substr(soundex((compress(a.tenantconame))),1,1)=substr(soundex((compress(b.tenantconame))),1,1))
or (substr(soundex((compress(a.tenantconame))),1,10)=substr(soundex((compress(b.tenantconame))),1,10)
and substr(soundex(reverse(compress(a.tenantconame))),1,1)=substr(soundex(reverse(compress(b.tenantconame))),1,1)))
and (a.move_date=b.move_date or (a.expire_date ne . and (a.expire_date=b.move_date or a.expire_date=b.expire_date))
or (a.transactnetsqft>0 and a.transactnetsqft=b.transactnetsqft)
or (a.occupiedsqft>0 and a.occupiedsqft=b.occupiedsqft)
or (a.occupiedsqft>0 and a.occupiedsqft=b.transactnetsqft)
or (b.occupiedsqft>0 and b.occupiedsqft=a.transactnetsqft))); run;  proc sort nodup; by propertyid tenantconame; run;


data findsimilar2; set findsimilar2;   by propertyid tenantconame newtenantcoid newtenantconame; if last.tenantconame; run;

proc sql noprint; select count(1) into: loop from findsimilar2;run;
%if (&loop>=1) %then %do;
data transact2; merge transact2 findsimilar2(drop=tenantcoid); 
by propertyid tenantconame; if newtenantconame ne '' then tenantconame=newtenantconame;
if newtenantcoid ne '' then tenantcoid=newtenantcoid; drop newtenantconame newtenantcoid; run;
proc sort nodup; by propertyid tenantconame;run;
%end; %end; %mend;
%findsimilarreverse;
/*
data tmp; set transact2; if tenantcoid=. then tenantcoid=0;
proc sql; create table tmp1 as select distinct * from tmp where tenantconame ne '' group by propertyid,tenantconame having count(distinct tenantcoid )>1;run;
data tmp3; set tmp1; keep tenantconame tenantcoid; proc sort nodup; by tenantconame;run;
proc sql; create table tmp2 as select distinct * from tmp where tenantcoid >0 group by propertyid,tenantcoid having count(distinct tenantconame )>1;run;
*/
data transact2; retain propertyid fmtname tenantconame tenantcoid TransactTypeDesc MoveDate expire_date TransactNetSqFt OccupiedSqFt CurrentlyOccupied; 
set transact2; retain id; length fmtname $100.; if tenantCoID ne . and TenantCoName ne '' then fmtname=compress("Name1_"||tenantCoID||"_"||TenantCoName);
else if tenantCoID ne .  then fmtname=compress("Name2_"||TenantCoName);
else if TenantCoName ne ''  then fmtname=compress("Name3_"||TenantCoName);
else fmtname=compress("Name4_"||SpaceTransactionID); id+1;
proc sort nodup; by propertyid fmtname; run;
/*if transacttypedesc="Move In" then transacttype=1; 
else if transacttypedesc="Renewal" then transacttype=2;
else if transacttypedesc="Move Out" then transacttype=3;*/

proc sql; create table findsimilar3a as select distinct a.propertyid,a.fmtname, case when a.fmtname<b.fmtname
then a.tenantcoid else b.tenantcoid end as newtenantcoid, case when a.fmtname<b.fmtname then a.tenantconame else b.tenantconame end as newtenantconame,
case when a.fmtname<b.fmtname then a.fmtname else b.fmtname end as newfmtname , b.fmtname as name2
/*,a.move_date,b.move_date as date2, a.expire_date, b.expire_date as expire2, a.transactnetsqft, b.transactnetsqft as sqft2,
a.occupiedsqft, b.occupiedsqft as occupied2, a.transacttypedesc, b.transacttypedesc as desc2 */
from transact2 a, transact2 b where   a.propertyid=b.propertyid and a.fmtname ne b.fmtname
and ((a.SpaceTransactionID+1=b.SpaceTransactionID and a.transacttypedesc ne 'Move Out' and 
(((a.expire_date ne . and (a.expire_date=b.move_date or a.expire_date=b.expire_date)))
and ((a.transactnetsqft>0 and a.transactnetsqft=b.transactnetsqft) or (a.occupiedsqft>0 and a.occupiedsqft=b.occupiedsqft)
or (a.occupiedsqft>0 and a.occupiedsqft=b.transactnetsqft) or (b.occupiedsqft>0 and b.occupiedsqft=a.transactnetsqft)) )
and a.transacttypedesc ne 'Move Out' and (a.transacttypedesc ne b.transacttypedesc
or a.transacttypedesc='Renewal' or b.transacttypedesc='Renewal')) or (b.SpaceTransactionID+1=a.SpaceTransactionID and a.transacttypedesc ne 'Move Out' and 
(( (b.expire_date ne . and (b.expire_date=a.move_date or b.expire_date=a.expire_date)))
and ((b.transactnetsqft>0 and b.transactnetsqft=a.transactnetsqft) or (b.occupiedsqft>0 and a.occupiedsqft=b.occupiedsqft)
or (b.occupiedsqft>0 and b.occupiedsqft=a.transactnetsqft) or (a.occupiedsqft>0 and a.occupiedsqft=b.transactnetsqft)) )
and b.transacttypedesc ne 'Move Out' and (a.transacttypedesc ne b.transacttypedesc
or a.transacttypedesc='Renewal' or b.transacttypedesc='Renewal'))) 
and ((substr(a.fmtname,1,5) = "Name4" and substr(b.fmtname,1,5) = "Name4")
or (a.tenantconame ne '' and b.tenantconame ne '' and substr(a.tenantconame,1,5)=substr(b.tenantconame,1,5)))
order by a.propertyid, a.fmtname;; run; *and (substr(a.fmtname,1,5)="Name4" or substr(b.fmtname,1,5)="Name4");

proc sql; create table findsimilar3b as select distinct b.propertyid,b.fmtname, case when a.fmtname<b.fmtname
then a.tenantcoid else b.tenantcoid end as newtenantcoid, case when a.fmtname<b.fmtname then a.tenantconame else b.tenantconame end as newtenantconame,
case when a.fmtname<b.fmtname then a.fmtname else b.fmtname end as newfmtname , b.fmtname as name2
from transact2 a, transact2 b where   a.propertyid=b.propertyid and a.fmtname ne b.fmtname
and ((a.SpaceTransactionID+1=b.SpaceTransactionID and a.transacttypedesc ne 'Move Out' and 
(( (a.expire_date ne . and (a.expire_date=b.move_date or a.expire_date=b.expire_date)))
and ((a.transactnetsqft>0 and a.transactnetsqft=b.transactnetsqft) or (a.occupiedsqft>0 and a.occupiedsqft=b.occupiedsqft)
or (a.occupiedsqft>0 and a.occupiedsqft=b.transactnetsqft) or (b.occupiedsqft>0 and b.occupiedsqft=a.transactnetsqft)) )
and a.transacttypedesc ne 'Move Out' and (a.transacttypedesc ne b.transacttypedesc
or a.transacttypedesc='Renewal' or b.transacttypedesc='Renewal')) or (b.SpaceTransactionID+1=a.SpaceTransactionID and a.transacttypedesc ne 'Move Out' and 
(((b.expire_date ne . and (b.expire_date=a.move_date or b.expire_date=a.expire_date)))
and ((b.transactnetsqft>0 and b.transactnetsqft=a.transactnetsqft) or (b.occupiedsqft>0 and a.occupiedsqft=b.occupiedsqft)
or (b.occupiedsqft>0 and b.occupiedsqft=a.transactnetsqft) or (a.occupiedsqft>0 and a.occupiedsqft=b.transactnetsqft)) )
and b.transacttypedesc ne 'Move Out' and (a.transacttypedesc ne b.transacttypedesc
or a.transacttypedesc='Renewal' or b.transacttypedesc='Renewal'))) 
and ((substr(a.fmtname,1,5) = "Name4" and substr(b.fmtname,1,5) = "Name4")
or (a.tenantconame ne '' and b.tenantconame ne '' and substr(a.tenantconame,1,5)=substr(b.tenantconame,1,5)))
order by b.propertyid, b.fmtname;; run; *and (substr(a.fmtname,1,5)="Name4" or substr(b.fmtname,1,5)="Name4");

data findsimilar3c; set findsimilar3a findsimilar3b; proc sort nodup; by propertyid fmtname newfmtname; run;
proc sql; create table findsimilar3c as select distinct min(newfmtname) as newfmtname, * from findsimilar3c group by propertyid,fmtname
order by propertyid,fmtname;run;

proc sql; create table findsimilar3d as select distinct 
case when a.newfmtname<b.newfmtname then a.newfmtname else b.newfmtname end as newfmtname, a.* from findsimilar3c a, findsimilar3c b
where a.fmtname ne b.fmtname and a.fmtname=b.name2 order by propertyid,fmtname;

data findsimilar3e; merge findsimilar3c findsimilar3d(in=f1); by propertyid fmtname; if not f1;run;
data findsimilar3; merge findsimilar3e findsimilar3d; by propertyid fmtname; drop name2; proc sort nodup; by propertyid fmtname; run;

data transact3; retain propertyid fmtname move_date expire_date; merge transact2 findsimilar3; 
by propertyid fmtname; if newtenantconame ne '' then tenantconame=newtenantconame;
if newtenantcoid ne '' then tenantcoid=newtenantcoid; 
if newfmtname ne '' then fmtname=newfmtname; 
drop newtenantconame newtenantcoid newfmtname; 
if (expire_date-int(expire_date/100)*100)<=3 then expireqtr=int(expire_date/100)*100+1; 
else if (expire_date-int(expire_date/100)*100)<=6 then expireqtr=int(expire_date/100)*100+2; 
else if (expire_date-int(expire_date/100)*100)<=9 then expireqtr=int(expire_date/100)*100+3; 
else if (expire_date-int(expire_date/100)*100)<=12 then expireqtr=int(expire_date/100)*100+4;  
if transacttypedesc='Move Out' and expire_date=. and move_date ne . then do;
if (move_date-int(move_date/100)*100)<=3 then expireqtr=int(move_date/100)*100+1; 
else if (move_date-int(move_date/100)*100)<=6 then expireqtr=int(move_date/100)*100+2; 
else if (move_date-int(move_date/100)*100)<=9 then expireqtr=int(move_date/100)*100+3; 
else if (move_date-int(move_date/100)*100)<=12 then expireqtr=int(move_date/100)*100+4;  
end; run; 

proc sort data=transact3; by propertyid fmtname move_date;run;
/*
proc sql; create table transact4 as select a.*,case when b.transactnetsqft<=0 then b.occupiedsqft else b.transactnetsqft end as renewsqft 
from transact3 a left outer join  transact3 b on a.propertyid=b.propertyid and a.expire_date=b.move_date and b.transacttypedesc='Renewal'
order by propertyid,fmtname,move_date,transacttypedesc; run;

data transact5; set transact4; by propertyid fmtname move_date transacttypedesc;
desc_l1=lag(transacttypedesc); if first.fmtname then do; desc_l1=.; end;
if next_renew=1 and transacttypedesc='Move Out' and desc_l1='Move In' 
then delete; drop desc_l1; 
if (expire_date-int(expire_date/100)*100)<=3 then expireqtr=int(expire_date/100)*100+1; 
else if (expire_date-int(expire_date/100)*100)<=6 then expireqtr=int(expire_date/100)*100+2; 
else if (expire_date-int(expire_date/100)*100)<=9 then expireqtr=int(expire_date/100)*100+3; 
else if (expire_date-int(expire_date/100)*100)<=12 then expireqtr=int(expire_date/100)*100+4;  run; 
*/

data costar_ppr_merged; set costar.costar_ppr_merged(keep=propertyid qtr leasedsqft asgproptype
propertytype buildingstatus next_leasedsqft moveout rentablearea); run;

proc sql; create table proptransact as select distinct a.propertyid,a.qtr,a.leasedsqft, a.asgproptype,
a.PropertyType, a.buildingstatus, a.rentablearea, b.fmtname, min(b.qtr) as moveqtr, max(b.expireqtr) as expireqtr,
a.moveout as sqftchg,a.next_leasedsqft-a.leasedsqft as nextchng, min(case when b.origMove_date>0 then b.origmove_date else 999999 end ) as origmove_date, avg(case when rentrate>0 then rentrate else . end)
as rentrate, max(case when b.occupiedsqft>b.transactnetsqft then b.occupiedsqft else b.transactnetsqft end)
as sqft from  costar_ppr_merged a, transact3 b where a.propertyid=b.propertyid and
int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100-1<=int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100<=int(b.expireqtr/100)*4+b.expireqtr-int(b.expireqtr/100)*100+1
group by a.propertyid,b.fmtname,a.qtr;run;

data proptransact1; set proptransact; if int(moveqtr/100)*4+moveqtr-int(moveqtr/100)*100+1=int(qtr/100)*4+qtr-int(qtr/100)*100  and nextchng=sqft then delete; 
if int(moveqtr/100)*4+moveqtr-int(moveqtr/100)*100-1=int(qtr/100)*4+qtr-int(qtr/100)*100 and sqftchg ne sqft then delete; 
if int(expireqtr/100)*4+expireqtr-int(expireqtr/100)*100+1=int(qtr/100)*4+qtr-int(qtr/100)*100 and sqftchg ne sqft then delete; 
if sqft<=0 then delete; if origmove_date ne 999999 and origmove_date ne . then do;
if (origmove_date-int(origmove_date/100)*100)<=3 then origmove_qtr=int(origmove_date/100)*100+1; 
else if (origmove_date-int(origmove_date/100)*100)<=6 then origmove_qtr=int(origmove_date/100)*100+2;  
else if (origmove_date-int(origmove_date/100)*100)<=9 then origmove_qtr=int(origmove_date/100)*100+3; 
else if (origmove_date-int(origmove_date/100)*100)<=12 then origmove_qtr=int(origmove_date/100)*100+4;  
end; drop origmove_date;  if sqft>0; proc sort nodup; by propertyid fmtname qtr; run;

data discontinuity; merge proptransact1; by propertyid fmtname qtr; leasedsqft_l1=lag(leasedsqft);
qtr_l1=lag(qtr); sqft_l1=lag(sqft); if first.fmtname then do; qtr_l1=.;sqft_l1=.;leasedsqft_l1=.;end; diffsqft=leasedsqft-lag(leasedsqft);
if  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100+1 and qtr_l1 ne . and sqft_l1=sqft;
diff=int(qtr/100)*4+qtr-int(qtr/100)*100 -( int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100);
if buildingstatus='Existing'; run;d

data notdiscont1; set discontinuity; if  (diff<=6 and leasedsqft>0) or (origmove_qtr<=qtr_l1 and origmove_qtr ne . and leasedsqft>0); run;

proc sql; create table trulydiscont as select distinct a.propertyid,b.fmtname from costar_ppr_merged a, discontinuity b
where b.qtr_l1<=a.qtr<=b.qtr and a.propertyid=b.propertyid and (a.next_leasedsqft-a.leasedsqft=-b.sqft or a.moveout=-b.sqft); run;

data notdiscont2; merge notdiscont1(in=f2) trulydiscont(in=f1); by propertyid fmtname; if not f1 or (diffsqft=0 and (origmove_qtr<=qtr_l1 and origmove_qtr ne .));
keep propertyid fmtname qtr qtr_l1 sqft leasedsqft leasedsqft_l1
rentablearea; proc sort nodup; by propertyid fmtname; run; 

data notdiscont3; set notdiscont2(rename=qtr=qtr_n1 rename=(leasedsqft=leasedsqft_n1)); 
do i=int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100 to int(qtr_n1/100)*4+qtr_n1-int(qtr_n1/100)*100;
qtr=int((i-1)/4)*100+i-int((i-1)/4)*4;  output; end; run;

proc sql; create table notdiscont4 as select distinct a.*,b.leasedsqft,std(b.leasedsqft) as std from notdiscont3 a , costar.costar_ppr_merged b
where a.propertyid=b.propertyid and a.qtr=b.qtr group by a.propertyid,a.fmtname; run;
/*
proc sql; create table notdiscont5 as select distinct a.*,b.leasedsqft as leasedsqft_m1,
c.leasedsqft as leasedsqft_p1 from notdiscont4 a left outer join prop2 b on a.propertyid=b.propertyid and 
int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100-1=int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100
 left outer join prop2 c on a.propertyid=c.propertyid and 
int(c.qtr/100)*4+c.qtr-int(c.qtr/100)*100+1=int(a.qtr_n1/100)*4+a.qtr_n1-int(a.qtr_n1/100)*100;run;
*/
data notdiscont6; set notdiscont4 (where=(std=0)); run;
data proptransact2; set proptransact1 notdiscont6; keep propertyid fmtname qtr sqft 
leasedsqft propertytype asgproptype rentablearea; proc sort nodup; by propertyid fmtname qtr sqft; run;


proc sql; create table proptransact2 as select distinct * from proptransact2 group by propertyid,fmtname having std(sqft)=0 or std(sqft) =. order by propertyid,fmtname,qtr;run;

data moveinoutdate; set proptransact2(keep=propertyid fmtname qtr); retain origmovedate origorigdate; by propertyid fmtname qtr; 
qtr_l1=lag(qtr); if first.fmtname or  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100+1  then
origmovedate=qtr; if first.fmtname then origorigdate=qtr; drop qtr_l1; proc sort nodup; by propertyid fmtname descending qtr; run;

data moveinoutdate; set moveinoutdate; retain lastmovedate lastlastdate; by propertyid fmtname descending qtr; 
qtr_l1=lag(qtr);  if first.fmtname or  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100-1  then
lastmovedate=qtr; if first.fmtname then lastlastdate=qtr; keep propertyid fmtname origmovedate lastmovedate origorigdate;  
proc sort nodup; by propertyid fmtname; run;
data exclude; set moveinoutdate; if origorigdate ne origmovedate; keep propertyid fmtname; proc sort nodup; by propertyid fmtname;run;

proc sql; create table moveinoutdate2 as select distinct propertyid,fmtname, min(case when qtr>0 then qtr else 99999 end) as origmoveqtr, max(case when expireqtr>qtr then expireqtr else qtr end) 
as lastmoveqtr,min(case when origmove_date>0 then origmove_date else 99999 end) as origmove_date from transact3 group by propertyid, fmtname order by propertyid,fmtname;  run;

data moveinoutdate3; set moveinoutdate2 ; by propertyid fmtname;
if origmove_date ne 999999 and origmove_date ne . then do;
if (origmove_date-int(origmove_date/100)*100)<=3 then origmove_qtr=int(origmove_date/100)*100+1; 
else if (origmove_date-int(origmove_date/100)*100)<=6 then origmove_qtr=int(origmove_date/100)*100+2;  
else if (origmove_date-int(origmove_date/100)*100)<=9 then origmove_qtr=int(origmove_date/100)*100+3; 
else if (origmove_date-int(origmove_date/100)*100)<=12 then origmove_qtr=int(origmove_date/100)*100+4;  
end; drop origmove_date; if origmove_qtr<origmoveqtr then origmoveqtr=origmove_qtr; 
if origmoveqtr=99999 then delete; keep propertyid fmtname origmoveqtr lastmoveqtr; run;

data moveinoutdate4; merge moveinoutdate3 moveinoutdate(in=f1) exclude(in=f2); by propertyid fmtname; if f1 and not f2;
if origmoveqtr=. or origmovedate<origmoveqtr then origmoveqtr=origmovedate;
if lastmoveqtr=. or lastmovedate>lastmoveqtr then lastmoveqtr=lastmovedate;run;

data renewal; set transact3; if transacttypedesc='Renewal' then do; renewdate=move_date; output; renewdate=expire_date; output;  end;
if transacttypedesc='Move Out' then do; if expire_date>move_date then do; renewdate=expire_date; output; end;
else do;renewdate=move_date; output; end; end;
if transacttypedesc='Move In' then do; if expire_date ne . then renewdate=expire_date; output; end;
else do; if move_date ne . then renewdate=move_date; output;end; run;

data renewal1; set renewal(in=f1 keep=propertyid fmtname renewdate) moveinoutdate4(keep=propertyid fmtname lastmoveqtr rename=lastmoveqtr=renewqtr); 
if f1 then do; if renewdate-int(renewdate/100)*100<=3 then renewqtr=int(renewdate/100)*100+1;  
else if renewdate-int(renewdate/100)*100<=6 then renewqtr=int(renewdate/100)*100+2;  
else if renewdate-int(renewdate/100)*100<=9 then renewqtr=int(renewdate/100)*100+3;  
else  renewqtr=int(renewdate/100)*100+4; end;  if renewqtr ne .; 
keep propertyid fmtname renewqtr; proc sort nodup; by propertyid fmtname renewqtr; run; 

data renewal2; set renewal1; by propertyid fmtname renewqtr; date_l1=lag(renewqtr);  if first.fmtname then date_l1=.;
if not last.fmtname and date_l1 ne . and int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) <=2
or int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) >100  then delete; drop date_l1 ;
proc sort nodup; by propertyid fmtname descending renewqtr; run; 

data renewal3; set renewal2; by propertyid fmtname descending renewqtr;  date_l1=lag(renewqtr);  if first.fmtname then date_l1=.;
if not last.fmtname and date_l1 ne . and int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) <=2
or int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) >100  then delete; drop date_l1;
proc sort nodup; by propertyid fmtname renewqtr; run; 

data proptransact3; merge proptransact2 moveinoutdate4(in=f1) ; by propertyid fmtname; if f1; 
proc sort nodup;by propertyid fmtname qtr;run;

data renewal3_qtr2; set renewal3; qtr=renewqtr-1; if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; drop renewqtr;
data renewal3_qtr3; set renewal3_qtr2; qtr=qtr-1; if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; 
data renewal3_qtr4; set renewal3_qtr3; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr5; set renewal3_qtr4; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr6; set renewal3_qtr5; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr7; set renewal3_qtr6; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr8; set renewal3_qtr7; qtr=qtr-1;if qtr/100=int(qtr/100)  then qtr=(int(qtr/100)-1)*100+4;run;
data renewal3_qtr9; set renewal3_qtr8; qtr=qtr-1;if qtr/100=int(qtr/100)  then qtr=(int(qtr/100)-1)*100+4;run;

data proptransact4; merge proptransact3(in=f1) renewal3(rename=renewqtr=qtr in=f2) renewal3_qtr2(in=f3) renewal3_qtr3 (in=f4) renewal3_qtr4(in=f5)
renewal3_qtr5(in=f6) renewal3_qtr6(in=f7) renewal3_qtr7(in=f8) renewal3_qtr8(in=f9) renewal3_qtr9(in=f10); 
by propertyid fmtname qtr; if f1;  if f2 then expire0=1; else expire0=0; if f3 then expire1=1; else expire1=0; 
if f4 then expire2=1; else expire2=0;  if f5 then expire3=1; else expire3=0; if f6 then expire4=1; else expire4=0;
if f7 then expire5=1; else expire5=0; if f8 then expire6=1; else expire6=0; if f9 then expire7=1; else expire7=0;
if f10 then expire8=1; else expire8=0; tSinceOrig=int(qtr/100)*4+qtr-int(qtr/100)*100 -(int(origmoveqtr/100)*4+origmoveqtr-int(origmoveqtr/100)*100); 
tTillExpire= int(lastmoveqtr/100)*4+lastmoveqtr-int(lastmoveqtr/100)*100-(int(qtr/100)*4+qtr-int(qtr/100)*100);  
proc sort nodup; by propertyid qtr fmtname; run;

*4475737;
proc sql; create table knownsqft as select distinct propertyid,qtr,leasedsqft,propertytype,asgproptype, sum(sqft) as knownsqft
from proptransact4 group by propertyid,qtr;run;

data exclude3; set knownsqft(where=(not(knownsqft<=leasedsqft or abs(knownsqft/leasedsqft-1)<0.01 or leasedsqft=0))); keep propertyid qtr;
proc sort nodup; by propertyid qtr; run; 

data costar.Processed_SpaceTransactions; merge proptransact4 exclude3(in=f1); by propertyid qtr; if not f1;
proc sort nodup; by asgproptype propertyid qtr; run;
%mend;

%macro processTransactRecent;
proc sql; create table proptransact as select distinct a.propertyid,a.qtr,a.leasedsqft, a.asgproptype,
a.PropertyType, a.buildingstatus, b.fmtname, min(b.qtr) as moveqtr, max(b.expireqtr) as expireqtr,
a.moveout as sqftchg,a.next_leasedsqft-a.leasedsqft as nextchng, min(case when b.origMove_date>0 then b.origmove_date else 999999 end ) as origmove_date, avg(case when rentrate>0 then rentrate else . end)
as rentrate, max(case when b.occupiedsqft>b.transactnetsqft then b.occupiedsqft else b.transactnetsqft end)
as sqft from costar_ppr_merged a, transact3 b where a.propertyid=b.propertyid and
int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100-1<=int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100<=int(b.expireqtr/100)*4+b.expireqtr-int(b.expireqtr/100)*100+1
group by a.propertyid,b.fmtname,a.qtr;run;

data proptransact1; set proptransact; if int(moveqtr/100)*4+moveqtr-int(moveqtr/100)*100+1=int(qtr/100)*4+qtr-int(qtr/100)*100  and nextchng=sqft then delete; 
if int(moveqtr/100)*4+moveqtr-int(moveqtr/100)*100-1=int(qtr/100)*4+qtr-int(qtr/100)*100 and sqftchg ne sqft then delete; 
if int(expireqtr/100)*4+expireqtr-int(expireqtr/100)*100+1=int(qtr/100)*4+qtr-int(qtr/100)*100 and sqftchg ne sqft then delete; 
if sqft<=0 then delete; if origmove_date ne 999999 and origmove_date ne . then do;
if (origmove_date-int(origmove_date/100)*100)<=3 then origmove_qtr=int(origmove_date/100)*100+1; 
else if (origmove_date-int(origmove_date/100)*100)<=6 then origmove_qtr=int(origmove_date/100)*100+2;  
else if (origmove_date-int(origmove_date/100)*100)<=9 then origmove_qtr=int(origmove_date/100)*100+3; 
else if (origmove_date-int(origmove_date/100)*100)<=12 then origmove_qtr=int(origmove_date/100)*100+4;  
end; drop origmove_date;  if sqft>0; proc sort nodup; by propertyid fmtname qtr; run;

data discontinuity; merge proptransact1; by propertyid fmtname qtr; leasedsqft_l1=lag(leasedsqft);
qtr_l1=lag(qtr); sqft_l1=lag(sqft); if first.fmtname then do; qtr_l1=.;sqft_l1=.;leasedsqft_l1=.;end; diffsqft=leasedsqft-lag(leasedsqft);
if  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100+1 and qtr_l1 ne . and sqft_l1=sqft;
diff=int(qtr/100)*4+qtr-int(qtr/100)*100 -( int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100);
if buildingstatus='Existing'; run;

data notdiscont1; set discontinuity; if  (diff<=6 and leasedsqft>0) or (origmove_qtr<=qtr_l1 and origmove_qtr ne . and leasedsqft>0); run;

proc sql; create table trulydiscont as select distinct a.propertyid,b.fmtname from costar_ppr_merged a, discontinuity b
where b.qtr_l1<=a.qtr<=b.qtr and a.propertyid=b.propertyid and (a.next_leasedsqft-a.leasedsqft=-b.sqft or a.moveout=-b.sqft); run;

data notdiscont2; merge notdiscont1(in=f2) trulydiscont(in=f1); by propertyid fmtname; if not f1 or (diffsqft=0 and (origmove_qtr<=qtr_l1 and origmove_qtr ne .));
keep propertyid fmtname qtr qtr_l1 sqft leasedsqft leasedsqft_l1; proc sort nodup; by propertyid fmtname; run; 

data notdiscont3; set notdiscont2(rename=qtr=qtr_n1 rename=(leasedsqft=leasedsqft_n1)); 
do i=int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100 to int(qtr_n1/100)*4+qtr_n1-int(qtr_n1/100)*100;
qtr=int((i-1)/4)*100+i-int((i-1)/4)*4;  output; end; run;

proc sql; create table notdiscont4 as select distinct a.*,b.leasedsqft,std(b.leasedsqft) as std from notdiscont3 a , costar_ppr_merged b
where a.propertyid=b.propertyid and a.qtr=b.qtr group by a.propertyid,a.fmtname; run;
/*
proc sql; create table notdiscont5 as select distinct a.*,b.leasedsqft as leasedsqft_m1,
c.leasedsqft as leasedsqft_p1 from notdiscont4 a left outer join prop2 b on a.propertyid=b.propertyid and 
int(b.qtr/100)*4+b.qtr-int(b.qtr/100)*100-1=int(a.qtr/100)*4+a.qtr-int(a.qtr/100)*100
 left outer join prop2 c on a.propertyid=c.propertyid and 
int(c.qtr/100)*4+c.qtr-int(c.qtr/100)*100+1=int(a.qtr_n1/100)*4+a.qtr_n1-int(a.qtr_n1/100)*100;run;
*/
data notdiscont6; set notdiscont4 (where=(std=0)); run;
data proptransact2; set proptransact1 notdiscont6; keep propertyid fmtname qtr sqft leasedsqft propertytype asgproptype; proc sort nodup; by propertyid fmtname qtr sqft; run;


proc sql; create table proptransact2 as select distinct * from proptransact2 group by propertyid,fmtname having std(sqft)=0 or std(sqft) =. order by propertyid,fmtname,qtr;run;

data moveinoutdate; set proptransact2(keep=propertyid fmtname qtr); retain origmovedate origorigdate; by propertyid fmtname qtr; 
qtr_l1=lag(qtr); if first.fmtname or  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100+1  then
origmovedate=qtr; if first.fmtname then origorigdate=qtr; drop qtr_l1; proc sort nodup; by propertyid fmtname descending qtr; run;

data moveinoutdate; set moveinoutdate; retain lastmovedate lastlastdate; by propertyid fmtname descending qtr; 
qtr_l1=lag(qtr);  if first.fmtname or  int(qtr/100)*4+qtr-int(qtr/100)*100 ne int(qtr_l1/100)*4+qtr_l1-int(qtr_l1/100)*100-1  then
lastmovedate=qtr; if first.fmtname then lastlastdate=qtr; keep propertyid fmtname origmovedate lastmovedate origorigdate;  
proc sort nodup; by propertyid fmtname; run;
data exclude; set moveinoutdate; if origorigdate ne origmovedate; keep propertyid fmtname; proc sort nodup; by propertyid fmtname;run;

proc sql; create table moveinoutdate2 as select distinct propertyid,fmtname, min(case when qtr>0 then qtr else 99999 end) as origmoveqtr, max(case when expireqtr>qtr then expireqtr else qtr end) 
as lastmoveqtr,min(case when origmove_date>0 then origmove_date else 99999 end) as origmove_date from transact3 group by propertyid, fmtname order by propertyid,fmtname;  run;

data moveinoutdate3; set moveinoutdate2 ; by propertyid fmtname;
if origmove_date ne 999999 and origmove_date ne . then do;
if (origmove_date-int(origmove_date/100)*100)<=3 then origmove_qtr=int(origmove_date/100)*100+1; 
else if (origmove_date-int(origmove_date/100)*100)<=6 then origmove_qtr=int(origmove_date/100)*100+2;  
else if (origmove_date-int(origmove_date/100)*100)<=9 then origmove_qtr=int(origmove_date/100)*100+3; 
else if (origmove_date-int(origmove_date/100)*100)<=12 then origmove_qtr=int(origmove_date/100)*100+4;  
end; drop origmove_date; if origmove_qtr<origmoveqtr then origmoveqtr=origmove_qtr; 
if origmoveqtr=99999 then delete; keep propertyid fmtname origmoveqtr lastmoveqtr; run;

data moveinoutdate4; merge moveinoutdate3 moveinoutdate(in=f1) exclude(in=f2); by propertyid fmtname; if f1 and not f2;
if origmoveqtr=. or origmovedate<origmoveqtr then origmoveqtr=origmovedate;
if lastmoveqtr=. or lastmovedate>lastmoveqtr then lastmoveqtr=lastmovedate;run;

data renewal; set transact3; if transacttypedesc='Renewal' then do; renewdate=move_date; output; renewdate=expire_date; output;  end;
if transacttypedesc='Move Out' then do; if expire_date>move_date then do; renewdate=expire_date; output; end;
else do;renewdate=move_date; output; end; end;
if transacttypedesc='Move In' then do; if expire_date ne . then renewdate=expire_date; output; end;
else do; if move_date ne . then renewdate=move_date; output;end; run;

data renewal1; set renewal(in=f1 keep=propertyid fmtname renewdate) moveinoutdate4(keep=propertyid fmtname lastmoveqtr rename=lastmoveqtr=renewqtr); 
if f1 then do; if renewdate-int(renewdate/100)*100<=3 then renewqtr=int(renewdate/100)*100+1;  
else if renewdate-int(renewdate/100)*100<=6 then renewqtr=int(renewdate/100)*100+2;  
else if renewdate-int(renewdate/100)*100<=9 then renewqtr=int(renewdate/100)*100+3;  
else  renewqtr=int(renewdate/100)*100+4; end;  if renewqtr ne .; 
keep propertyid fmtname renewqtr; proc sort nodup; by propertyid fmtname renewqtr; run; 

data renewal2; set renewal1; by propertyid fmtname renewqtr; date_l1=lag(renewqtr);  if first.fmtname then date_l1=.;
if not last.fmtname and date_l1 ne . and int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) <=2
or int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) >100  then delete; drop date_l1 ;
proc sort nodup; by propertyid fmtname descending renewqtr; run; 

data renewal3; set renewal2; by propertyid fmtname descending renewqtr;  date_l1=lag(renewqtr);  if first.fmtname then date_l1=.;
if not last.fmtname and date_l1 ne . and int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) <=2
or int(renewqtr/100)*4+renewqtr-int(renewqtr/100)*100 -(int(date_l1/100)*4+date_l1-int(date_l1 /100)*100) >100  then delete; drop date_l1;
proc sort nodup; by propertyid fmtname renewqtr; run; 

data proptransact3; merge proptransact2 moveinoutdate4(in=f1) ; by propertyid fmtname; if f1; 
proc sort nodup;by propertyid fmtname qtr;run;

data renewal3_qtr2; set renewal3; qtr=renewqtr-1; if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; drop renewqtr;
data renewal3_qtr3; set renewal3_qtr2; qtr=qtr-1; if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; 
data renewal3_qtr4; set renewal3_qtr3; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr5; set renewal3_qtr4; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr6; set renewal3_qtr5; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr7; set renewal3_qtr6; qtr=qtr-1;if qtr/100=int(qtr/100) then qtr=(int(qtr/100)-1)*100+4; run;
data renewal3_qtr8; set renewal3_qtr7; qtr=qtr-1;if qtr/100=int(qtr/100)  then qtr=(int(qtr/100)-1)*100+4;run;
data renewal3_qtr9; set renewal3_qtr8; qtr=qtr-1;if qtr/100=int(qtr/100)  then qtr=(int(qtr/100)-1)*100+4;run;

data proptransact4; merge proptransact3(in=f1) renewal3(rename=renewqtr=qtr in=f2) renewal3_qtr2(in=f3) renewal3_qtr3 (in=f4) renewal3_qtr4(in=f5)
renewal3_qtr5(in=f6) renewal3_qtr6(in=f7) renewal3_qtr7(in=f8) renewal3_qtr8(in=f9) renewal3_qtr9(in=f10); 
by propertyid fmtname qtr; if f1;  if f2 then expire0=1; else expire0=0; if f3 then expire1=1; else expire1=0; 
if f4 then expire2=1; else expire2=0;  if f5 then expire3=1; else expire3=0; if f6 then expire4=1; else expire4=0;
if f7 then expire5=1; else expire5=0; if f8 then expire6=1; else expire6=0; if f9 then expire7=1; else expire7=0;
if f10 then expire8=1; else expire8=0; tSinceOrig=int(qtr/100)*4+qtr-int(qtr/100)*100 -(int(origmoveqtr/100)*4+origmoveqtr-int(origmoveqtr/100)*100); 
tTillExpire= int(lastmoveqtr/100)*4+lastmoveqtr-int(lastmoveqtr/100)*100-(int(qtr/100)*4+qtr-int(qtr/100)*100);  
proc sort nodup; by propertyid qtr fmtname; run;

*4475737;
proc sql; create table knownsqft as select distinct propertyid,qtr,leasedsqft,propertytype,asgproptype, sum(sqft) as knownsqft
from proptransact4 group by propertyid,qtr;run;

data exclude3; set knownsqft(where=(not(knownsqft<=leasedsqft or abs(knownsqft/leasedsqft-1)<0.01 or leasedsqft=0))); keep propertyid qtr;
proc sort nodup; by propertyid qtr; run; 

data costar.ThisPeriodProcessed_spacetransactions; merge proptransact4 exclude3(in=f1); by propertyid qtr; if not f1;
proc sort nodup; by asgproptype propertyid qtr; run;
%mend;
%let inp=process0;

%macro Make_Model_Variables(inp=,outp=,maxqtr=,cutoff=,simid=);
proc sql; select count(1) into: N from &inp;

data random; seed=12345; do i=1 to &N;	call ranuni(seed, ran_num);	output;	 end;	run;
data &outp; merge &inp random(keep=ran_num); if ran_num<&cutoff;run;

data &outp; merge &outp(in=f2) parmlease(in=f1); by asgproptype;  if f1 and f2;
array unk_leased_arr(*) unk_leased_arr_0-unk_leased_arr_&maxqtr.;
array unk_rentablearea_arr(*) unk_rentablearea_arr_0-unk_rentablearea_arr_&maxqtr.;
array leasedsqft_arr(*) leasedsqft_arr_0-leasedsqft_arr_&maxqtr.; 
array atrisksqft_arr(*) atrisksqft_0-atrisksqft_&maxqtr.; array qtr_arr(*) qtr_0-qtr_&maxqtr.;
array PercLeased_arr(*) PercLeased_arr_0-PercLeased_arr_&maxqtr.;
Intercept=1; if knownsqft=. then knownsqft=0; 
do i=1 to dim(atrisksqft_arr); if atrisksqft_arr[i]<=0 then atrisksqft_arr[i]=0;
if i=1 then   qtr_arr[i]=qtr ;
else do; if qtr_arr[i-1]-int(qtr_arr[i-1]/100)*100=4 then qtr_arr[i]=(int(qtr_arr[i-1]/100)+1)*100+1; else qtr_arr[i]=qtr_arr[i-1]+1;
tFromR_1=tFromR_1+3; end; end;

do i=1 to &maxqtr; call symput ("i",i); 
if i=1 then do; unk_leased_arr[i]=leasedsqft-knownsqft+atrisksqft_arr[i]; unk_rentablearea_arr[i]=rentablearea-knownsqft+atrisksqft_arr[i]; 
leasedsqft_arr[i]=leasedsqft; PercLeased_arr[i]=leasedsqft_arr[i]/rentablearea;  end;

if unk_leased_arr[i] ne . then do;
unk_leasedpct=unk_leased_arr[i]/unk_rentablearea_arr[i]; PercLeased=PercLeased_arr[i];
full=(PercLeased>0.995); empty=(PercLeased=0);  if i>1 and empty=1 then tEmpty=tEmpty+3; else tEmpty=0;

if unk_leasedpct<=0 then unk_xbeta=-7;	else if unk_leasedpct>=1 then unk_xbeta=7;	
else unk_xbeta=max(-7,min(7,log(unk_leasedpct/(1-unk_leasedpct))));		
leaseXunk=lease_xbeta*unk_xbeta;
unklease_lag_0=min(max(-2,unk_xbeta+3),1.1);		unklease_lag_1=min(max(-1.1,unk_xbeta+1.9),0.8);	
unklease_lag_2=min(max(-0.8,unk_xbeta+1.1),0.7);	unklease_lag_3=min(max(-0.7,unk_xbeta+0.4),0.6);	
unklease_lag_4=min(max(-0.6,unk_xbeta-0.2),1.0);	unklease_lag_5=min(1.0, max(0,unk_xbeta-1.2));	
unklease_lag_6=min(1.0, max(0,unk_xbeta-2.2));		unklease_lag_7=min(1.8, max(0,unk_xbeta-3.2));

tFromR_2=min(120,max(tFromR_1,0)); 		
if PercLeased=0 then lease_xbeta=-7;	else if PercLeased=1 then lease_xbeta=7;	
else lease_xbeta=max(-7,min(7,log(PercLeased/(1-PercLeased))));		


vacancy=symget(compress("vac"||simid||metrocode||asgproptype||qtr_arr[i]))*1; vacancy_g=symget(compress("vac_g"||simid||metrocode||asgproptype||qtr_arr[i+1]))*1;
askingrent=symget(compress("rent"||simid||metrocode||asgproptype||qtr_arr[i]))*1; rent_g=symget(compress("rent_g"||simid||metrocode||asgproptype||qtr_arr[i+1]))*1;
rent_g_1=(rent_g<-0.02);	vacancy_g_1=(vacancy_g<-0.05);	vacancy_g_2=min(0.1,max(vacancy_g+0.1,0));	
if vacancy_g=. then do; vacancy_g_1=.;vacancy_g_2=.;end;

empty_bad1=(PercLeased<0.45)*max(vacancy-0.05,0); 	empty_bad2=(PercLeased<0.45)*max(vacancy-0.12,0); 
empty_bad3=(PercLeased<0.35)*max(vacancy-0.05,0); 	empty_bad4=(PercLeased<0.35)*max(vacancy-0.12,0);
empty_bad5=(PercLeased<0.15)*max(vacancy-0.05,0); 	empty_bad6=(PercLeased<0.15)*max(vacancy-0.12,0);
empty_bad7=(PercLeased<0.05)*max(vacancy-0.05,0); 	empty_bad8=(PercLeased<0.05)*max(vacancy-0.12,0);

empty_ren1=max(0.1-PercLeased,0)*(tFromR_1<12); 	empty_ren2=max(0.1-PercLeased,0)*(tFromR_1<30); 
empty_ren3=max(0.25-PercLeased,0)*(tFromR_1<12);  	empty_ren4=max(0.25-PercLeased,0)*(tFromR_1<30); 
empty_ren5=max(0.45-PercLeased,0)*(tFromR_1<12);  	empty_ren6=max(0.45-PercLeased,0)*(tFromR_1<30); 

full_bad1=(PercLeased>0.98)*max(vacancy-0.05,0);	full_bad2=(PercLeased>0.98)*max(vacancy-0.15,0);	
demolish_empty=demolish*max(0.5-PercLeased,0);	rebuild_full=possible_reBuild*max(PercLeased-0.8,0); 

lease_lag_0=min(max(-2,lease_xbeta+3),1.1);			lease_lag_1=min(max(-1.1,lease_xbeta+1.9),0.8);	
lease_lag_2=min(max(-0.8,lease_xbeta+1.1),0.7);		lease_lag_3=min(max(-0.7,lease_xbeta+0.4),0.6);	
lease_lag_4=min(max(-0.6,lease_xbeta-0.2),1.0);		lease_lag_5=min(1.0, max(0,lease_xbeta-1.2));	
lease_lag_6=min(1.0, max(0,lease_xbeta-2.2));		lease_lag_7=min(1.8, max(0,lease_xbeta-3.2));	
 
vacancy_hi2=(vacancy>0.15);	vacancy_hi=(vacancy>0.08);	vacancy_hi3=(vacancy>0.3); PercLeased_lo=(PercLeased<0.05);
RenovateAge_1=(tFromR_1>360);	RenovateAge_2=(tFromR_1>120);	RenovateAge_3=(tFromR_1>36); 

%let name1=unk_xbeta; 
RenovateAge_1&name1.=RenovateAge_1*&name1.; RenovateAge_2&name1.=RenovateAge_2*&name1.; RenovateAge_3&name1.=RenovateAge_3*&name1.;
midRise&name1.=midRise*&name1.; highRise&name1.=highRise*&name1.; classA&name1.=classA*&name1.; classB&name1.=classB*&name1.; classU&name1.=classU*&name1.;
demolish&name1.=demolish*&name1.; possible_reBuild&name1.=possible_reBuild*&name1.; BuildAge_1&name1.=BuildAge_1*&name1.; BuildAge_2&name1.=BuildAge_2*&name1.; BuildAge_3&name1.=BuildAge_3*&name1.;
singleTenant&name1.=singleTenant*&name1.; empty&name1.=empty*&name1.; full&name1.=full*&name1.; full_bad1&name1.=full_bad1*&name1.; full_bad2&name1.=full_bad2*&name1.; empty_bad1&name1.=empty_bad1*&name1.;
empty_bad2&name1.=empty_bad2*&name1.; empty_bad3&name1.=empty_bad3*&name1.; empty_bad4&name1.=empty_bad4*&name1.; empty_bad5&name1.=empty_bad5*&name1.; empty_bad6&name1.=empty_bad6*&name1.; 
empty_bad7&name1.=empty_bad7*&name1.; empty_bad8&name1.=empty_bad8*&name1.; empty_ren1&name1.=empty_ren1*&name1.; empty_ren2&name1.=empty_ren2*&name1.; empty_ren3&name1.=empty_ren3*&name1.;
empty_ren4&name1.=empty_ren4*&name1.; empty_ren5&name1.=empty_ren5*&name1.; empty_ren6&name1.=empty_ren6*&name1.; buildSize_1&name1.=buildSize_1*&name1.; buildSize_2&name1.=buildSize_2*&name1.;
buildSize_3&name1.=buildSize_3*&name1.; buildSize_4&name1.=buildSize_4*&name1.; hasRenovate&name1.=hasRenovate*&name1.;
multiRenovate&name1.=multiRenovate*&name1.; rebuild_full&name1.=rebuild_full*&name1.; demolish_empty&name1.=demolish_empty*&name1.; vacancy_g_1&name1.=vacancy_g_1*&name1.;
vacancy_g_2&name1.=vacancy_g_2*&name1.; rent_g_1&name1.=rent_g_1*&name1.; rent_g_2&name1.=rent_g_2*&name1.; vacancy&name1.=vacancy*&name1.;
vacancy_hi&name1.=vacancy_hi*&name1.; vacancy_hi2&name1.=vacancy_hi2*&name1.; tEmpty&name1.=tEmpty*&name1.; tEmpty_sq&name1.=tEmpty_sq*&name1.;

%let name1=lease_xbeta;
RenovateAge_1&name1.=RenovateAge_1*&name1.; RenovateAge_2&name1.=RenovateAge_2*&name1.; RenovateAge_3&name1.=RenovateAge_3*&name1.;
midRise&name1.=midRise*&name1.; highRise&name1.=highRise*&name1.; classA&name1.=classA*&name1.; classB&name1.=classB*&name1.; classU&name1.=classU*&name1.;
demolish&name1.=demolish*&name1.; possible_reBuild&name1.=possible_reBuild*&name1.; BuildAge_1&name1.=BuildAge_1*&name1.; BuildAge_2&name1.=BuildAge_2*&name1.; BuildAge_3&name1.=BuildAge_3*&name1.;
singleTenant&name1.=singleTenant*&name1.; empty&name1.=empty*&name1.; full&name1.=full*&name1.; full_bad1&name1.=full_bad1*&name1.; full_bad2&name1.=full_bad2*&name1.; empty_bad1&name1.=empty_bad1*&name1.;
empty_bad2&name1.=empty_bad2*&name1.; empty_bad3&name1.=empty_bad3*&name1.; empty_bad4&name1.=empty_bad4*&name1.; empty_bad5&name1.=empty_bad5*&name1.; empty_bad6&name1.=empty_bad6*&name1.; 
empty_bad7&name1.=empty_bad7*&name1.; empty_bad8&name1.=empty_bad8*&name1.; empty_ren1&name1.=empty_ren1*&name1.; empty_ren2&name1.=empty_ren2*&name1.; empty_ren3&name1.=empty_ren3*&name1.;
empty_ren4&name1.=empty_ren4*&name1.; empty_ren5&name1.=empty_ren5*&name1.; empty_ren6&name1.=empty_ren6*&name1.; buildSize_1&name1.=buildSize_1*&name1.; buildSize_2&name1.=buildSize_2*&name1.;
buildSize_3&name1.=buildSize_3*&name1.; buildSize_4&name1.=buildSize_4*&name1.; hasRenovate&name1.=hasRenovate*&name1.;
multiRenovate&name1.=multiRenovate*&name1.; rebuild_full&name1.=rebuild_full*&name1.; demolish_empty&name1.=demolish_empty*&name1.; vacancy_g_1&name1.=vacancy_g_1*&name1.;
vacancy_g_2&name1.=vacancy_g_2*&name1.; rent_g_1&name1.=rent_g_1*&name1.; rent_g_2&name1.=rent_g_2*&name1.; vacancy&name1.=vacancy*&name1.;
vacancy_hi&name1.=vacancy_hi*&name1.; vacancy_hi2&name1.=vacancy_hi2*&name1.; tEmpty&name1.=tEmpty*&name1.; tEmpty_sq&name1.=tEmpty_sq*&name1.;

%let name1=leaseXunk;
RenovateAge_1&name1.=RenovateAge_1*&name1.; RenovateAge_2&name1.=RenovateAge_2*&name1.; RenovateAge_3&name1.=RenovateAge_3*&name1.;
midRise&name1.=midRise*&name1.; highRise&name1.=highRise*&name1.; classA&name1.=classA*&name1.; classB&name1.=classB*&name1.; classU&name1.=classU*&name1.;
demolish&name1.=demolish*&name1.; possible_reBuild&name1.=possible_reBuild*&name1.; BuildAge_1&name1.=BuildAge_1*&name1.; BuildAge_2&name1.=BuildAge_2*&name1.; BuildAge_3&name1.=BuildAge_3*&name1.;
singleTenant&name1.=singleTenant*&name1.; empty&name1.=empty*&name1.; full&name1.=full*&name1.; full_bad1&name1.=full_bad1*&name1.; full_bad2&name1.=full_bad2*&name1.; empty_bad1&name1.=empty_bad1*&name1.;
empty_bad2&name1.=empty_bad2*&name1.; empty_bad3&name1.=empty_bad3*&name1.; empty_bad4&name1.=empty_bad4*&name1.; empty_bad5&name1.=empty_bad5*&name1.; empty_bad6&name1.=empty_bad6*&name1.; 
empty_bad7&name1.=empty_bad7*&name1.; empty_bad8&name1.=empty_bad8*&name1.; empty_ren1&name1.=empty_ren1*&name1.; empty_ren2&name1.=empty_ren2*&name1.; empty_ren3&name1.=empty_ren3*&name1.;
empty_ren4&name1.=empty_ren4*&name1.; empty_ren5&name1.=empty_ren5*&name1.; empty_ren6&name1.=empty_ren6*&name1.; buildSize_1&name1.=buildSize_1*&name1.; buildSize_2&name1.=buildSize_2*&name1.;
buildSize_3&name1.=buildSize_3*&name1.; buildSize_4&name1.=buildSize_4*&name1.; hasRenovate&name1.=hasRenovate*&name1.;
multiRenovate&name1.=multiRenovate*&name1.; rebuild_full&name1.=rebuild_full*&name1.; demolish_empty&name1.=demolish_empty*&name1.; vacancy_g_1&name1.=vacancy_g_1*&name1.;
vacancy_g_2&name1.=vacancy_g_2*&name1.; rent_g_1&name1.=rent_g_1*&name1.; rent_g_2&name1.=rent_g_2*&name1.; vacancy&name1.=vacancy*&name1.;
vacancy_hi&name1.=vacancy_hi*&name1.; vacancy_hi2&name1.=vacancy_hi2*&name1.; tEmpty&name1.=tEmpty*&name1.; tEmpty_sq&name1.=tEmpty_sq*&name1.;

next_unk_leased_xbeta=&leaseeq+0;
if next_unk_leased_xbeta ne .  then do;
if next_unk_leased_xbeta<=-7 then next_unk_leased=0; else if next_unk_leased_xbeta>=7 then next_unk_leased=1;
else next_unk_leased=exp(next_unk_leased_xbeta)/(1+exp(next_unk_leased_xbeta));

unk_leased_arr[i+1]=next_unk_leased*unk_rentablearea_arr[i]+atrisksqft_arr[i+1];
leasedsqft_arr[i+1]=leasedsqft_arr[i]-unk_leased_arr[i]+next_unk_leased*unk_rentablearea_arr[i];
unk_rentablearea_arr[i+1]=rentablearea-(leasedsqft_arr[i+1]-unk_leased_arr[i+1]);
PercLeased_arr[i+1]=leasedsqft_arr[i+1]/rentablearea; 
end; end; else i=&maxqtr;end; 
drop parm: RenovateAge_1unk_xbeta--tEmpty_sqleaseXunk; run;

data &outp.2; set &outp.(keep=qtr_0-qtr_&maxqtr. PercLeased_arr_0-PercLeased_arr_&maxqtr. propertyid);
array PercLeased_arr(*) PercLeased_arr_0-PercLeased_arr_&maxqtr.;
array qtr_arr(*) qtr_0-qtr_&maxqtr. ; 
do i=2 to dim(qtr_arr); if PercLeased_arr[i] ne . then do;
PercLeased=PercLeased_arr[i]; qtr=qtr_arr[i]; qtrfc=i-1; output; end; end; keep propertyid PercLeased qtr qtrfc; run;
proc sort nodup; by propertyid qtr;run;

data compare; merge costar.costar_ppr_merged(in=f2 keep=propertyid PercLeased qtr asgproptype rentablearea) &outp.2(in=f1 rename=(PercLeased=PercLeased_fc)); 
by propertyid qtr; if f1 and f2;
diff=PercLeased-PercLeased_fc; proc means; class asgproptype qtrfc; weight rentablearea; var diff PercLeased PercLeased_fc; run;
proc means; class asgproptype qtrfc; where diff ne .; var diff PercLeased PercLeased_fc; run;
%mend;




%macro process_rentrate();
data transact;	set cmbs.CoStar_SpaceTransaction_dt(rename=(TenantCompanyID=TenantCoID TransactionTypeDesc=TransactTypeDesc
OriginalSignDate=OrigSignDate OriginalMoveDate=OrigMoveDate ExpirationDate=ExpireDate TransactionNetSqFt=TransactNetSqFt TenantCompanyName=TenantCoName
transactiondesc=TransactDesc LastExpirationDate=LastExpireDate));if missing(SignDate) then SignDate=SignDateSort;	if missing(MoveDate) then 
MoveDate=MoveDateSort;	move_date=year(datepart(MoveDate))*100 +month(datepart(MoveDate));	if move_date<=199001 then move_date
=year(datepart(MoveDateSort))*100 +month(datepart(MoveDateSort));	Sign_date=year(datepart(SignDate))*100 +month(datepart(SignDate));
if sign_date<=199001 then sign_date=year(datepart(SignDateSort))*100 +month(datepart(SignDateSort));
origSign_date=year(datepart(OrigSignDate))*100 +month(datepart(OrigSignDate)); if sign_date<=199001 then sign_date=origSign_date; 
origMove_date=year(datepart(OrigMoveDate))*100 +month(datepart(OrigMoveDate)); 	if move_date<=199001 then move_date=origMove_date;			
if move_date<=199001 then move_date=sign_date;	last_expire_date=year(datepart(LastExpireDate))*100 +month(datepart(LastExpireDate)); 
Update_date=year(datepart(UpdateDate))*100 +month(datepart(UpdateDate)); expire_date=year(datepart(ExpireDate))*100 +month(datepart(ExpireDate));  
if expire_date<=max(last_expire_date,max(move_date,199001)) then expire_date=last_expire_date;	
move_date=min(expire_date, max(move_date, sign_date));		sign_date=min(move_date, max(sign_date, expire_date));
if TransactTypeDesc not in ('Move In' 'Move Out' 'Renewal') then TransactTypeDesc='.';	if SpaceTypeName not in 
('New' 'Relet' 'Sublet') then SpaceTypeName='.';	update_date=max(update_date, max(move_date, sign_date));	
if expire_date ne . and move_date ne . and expire_date>move_date then 
LeaseTerm=(int(expire_date/100)*12+expire_date-int(expire_date/100)*100)-(int(move_date/100)*12+move_date-int(move_date/100)*100);
if (move_date-int(move_date/100)*100)<=3 then qtr=int(move_date/100)*100+1; 
else if (move_date-int(move_date/100)*100)<=6 then qtr=int(move_date/100)*100+2; 
else if (move_date-int(move_date/100)*100)<=9 then qtr=int(move_date/100)*100+3; 
else if (move_date-int(move_date/100)*100)<=12 then qtr=int(move_date/100)*100+4; 
TenantCoName=UPCASE(TenantCoName);
/*length fmtname $100.; fmtname=compress("Name"||tenantCoID||TenantCoName);
if fmtname='' then fmtname=compress("Name2"||SpaceTransactionID);*/
keep qtr SpaceTransactionID PropertyID TenantCoID TransactTypeDesc SpaceTypeName TransactNetSqFt OccupiedSqFt RentRate 
sign_date move_date MoveDate signdate expire_date TransactDesc update_date CurrentlyOccupied TenantCoName servicetypedesc 
LeaseTerm origMove_date ;  proc sort nodup; by PropertyID  movedate TenantCoName signdate;	run;

proc means data=transact noprint; where TransactTypeDesc in ('Move In' 'Renewal') and rentrate>0;
class propertyid qtr; var rentrate leaseterm transactnetsqft;  output out=rentrate0 mean=; run;


data proptype; set costar.costar_ppr_merged(keep=propertyid asgproptype propertytype
submetro metrocode statecode propertyid proptype_orig); 
proc sort nodup; by propertyid asgproptype propertytype;


data rentrate1; merge rentrate0(in=f1 where=(propertyid ne . and qtr ne .) ) 
proptype; by propertyid; if f1; drop _TYPE_ _FREQ_;
prevtrans_rentrate=lag(rentrate); prevtrans_leaseterm=lag(leaseterm); prevtrans_TransactNetSqFt=lag(transactNetSqft);
prevtrans_qtr=lag(qtr); if qtr-int(qtr/100)*100=1 then qtr_l1=(int(qtr/100)-1)*100+4; 
else qtr_l1=qtr-1; if propertyid ne lag(propertyid) then do; prevtrans_rentrate=.; prevtrans_leaseterm=.; prevtrans_TransactNetSqFt=.;
prevtrans_qtr=.; end; proc sort nodup; by asgproptype propertyid qtr; run;



proc sql noprint; select cats(name,'=prevtrans_',name) into :suffixlist_prevtrans separated by ' ' from dictionary.columns where libname = 'COSTAR' 
and memname = 'COSTAR_PPR_MERGED' and upper(name) not in ( "BUILDINGNAME","TENANCY", "SECONDARYTYPE","STATE","ZIPCODE","_TYPE_",
"_FREQ_" ,"PROPERTYTYPE", "BUILDINGCLASS","BUILDINGSTATUS","REPORTQRT","METROCODE","MAINCODE","ASGPROPTYPE","PROPERTYID","AVERAGERENT",
"SUBMETRO","STATECODE","METROCODE","PROPTYPE_ORIG","METROCODE_MATCH") and upper(name) not like 'NEXT%'; ; quit;

proc sql noprint; select cats(name) into :keep_l1 separated by ' ' from dictionary.columns where libname = 'COSTAR' 
and memname = 'COSTAR_PPR_MERGED' and upper(name) not in ( "BUILDINGNAME","TENANCY", "SECONDARYTYPE","STATE","ZIPCODE","_TYPE_",
"_FREQ_" ,"PROPERTYTYPE", "BUILDINGCLASS","BUILDINGSTATUS","REPORTQRT","METROCODE","MAINCODE","ASGPROPTYPE",'AVERAGERENT'
"SUBMETRO","STATECODE","METROCODE","PROPTYPE_ORIG","METROCODE_MATCH")  and upper(name) not like 'NEXT%'; ; quit;

%put &suffixlist_l1; 
data rentrate2; merge rentrate1(in=f1) costar.costar_ppr_merged(drop=AverageRent next:); by asgproptype propertyid qtr; if f1; 
data rentrate3; merge rentrate2(in=f1) costar.costar_ppr_merged( keep=asgproptype &keep_l1. 
rename=(&suffixlist_prevtrans.)); by asgproptype propertyid prevtrans_qtr; if f1; run;
proc sort data=rentrate3 nodup;by asgproptype submetro qtr; run;

data propmetro0 propsub; merge rentrate3(in=f1
drop=askingrent: tregion: tus: slope: ust:) costar.pprhist(rename=metrocode=submetro); by asgproptype submetro qtr; if f1; 
if vacancy=. then output propmetro0; else output propsub;
proc sort data=propmetro0 nodup; by asgproptype metrocode qtr;  run;

data propstate0 propmetro; merge propmetro0 (in=f1 drop=pprcaprate--pmms_15y_l4) costar.pprhist; by asgproptype metrocode qtr; if f1; 
if vacancy=. then output propstate0; else output propmetro;
proc sort data=propstate0 nodup; by asgproptype statecode qtr; 

data propstate propus0; merge propstate0(in=f1 drop=pprcaprate--pmms_15y_l4) costar.pprhist(rename=metrocode=statecode); by asgproptype statecode qtr; if f1; 
if vacancy ne . then output propstate; else output propus0; run;
proc sort data=propus0; by asgproptype qtr;
data propus; merge propus0(in=f1 drop=pprcaprate--pmms_15y_l4) costar.pprhist(where=(metrocode='US')); by asgproptype qtr;  if f1;run;

data rentrate5; set propmetro propsub propstate propus; proc sort data=rentrate5 nodup; by asgproptype propertyid qtr;run;

/*
proc sort data=pprhist; by asgproptype metrocode qtr; run;

data propmetro propsub; merge prop1(in=f1) pprhist(rename=metrocode=submetro); by asgproptype submetro qtr; if f1; 
if vacancy=. then output propmetro; else output propsub;
proc sort data=propmetro nodup; by asgproptype metrocode qtr;  run;

data propstate propmetro; merge propmetro (in=f1) pprhist; by asgproptype metrocode qtr; if f1; 
if vacancy=. then output propstate; else output propmetro;
proc sort data=propstate nodup; by asgproptype statecode qtr; 

data propstate propus; merge propstate(in=f1) pprhist(rename=metrocode=statecode); by asgproptype statecode qtr; if f1; 
if vacancy ne . then output propstate; else output propus; run;
proc sort data=propus; by asgproptype qtr;
data propus; merge propus(in=f1) pprhist(where=(metrocode='US')); by asgproptype qtr;  if f1;run;

data prop2; set propmetro propsub propstate propus; proc sort nodup; by propertyid qtr;run;

*/
%put &suffixlist_prevtrans;
data rentrate6; retain asgproptype propertyid qtr; set rentrate5(drop=reportQrt);  by asgproptype PropertyID qtr;

if slope ne . then do; hi_slope=min(1,max(0,slope-2.2));	lo_slope=min(0.2,max(0.1-slope,0));  end; 
hi_ust10yr=min(1.5,max(0,ust10yr-5.5));	lo_ust10yr=min(1.5,max(2.5-ust10yr,0));

slope_g_l1=slope-slope_l1; slope_g_l2=slope-slope_l2; slope_g_l3=slope-slope_l3; slope_g_l4=slope-slope_l4;
ust10yr_g_l1=ust10yr-ust10yr_l1; ust10yr_g_l2=ust10yr-ust10yr_l2; ust10yr_g_l3=ust10yr-ust10yr_l3; ust10yr_g_l4=ust10yr-ust10yr_l4;

percLease_g=PercLeased-prevtrans_PercLeased;

time_pass=log((int(qtr/100)*4+qtr-int(qtr/100)*100)-(int(prevtrans_qtr/100)*4+prevtrans_qtr-int(prevtrans_qtr/100)*100)) ;
if (int(qtr/100)*4+qtr-int(qtr/100)*100)-(int(prevtrans_qtr/100)*4+prevtrans_qtr-int(prevtrans_qtr/100)*100)>8 then do;
array arr0a(*) prevtrans_:; do i=1 to dim(arr0a); if arr0a[i]=. then arr0a[i]=0; end;
time_pass=.; end;
ln_rentrate=log(rentrate); prevtrans_ln_rentrate=log(prevtrans_rentrate);
ln_askingrent=log(askingrent); prevtrans_ln_askingrent=log(prevtrans_askingrent);

if empty=. then empty=0; if full=. then full=0; if tEmpty=. then tEmpty=0;
diffleasetermpct=leaseterm/prevtrans_leaseterm-1; diffnetsqftpct=transactnetsqft/prevtrans_transactnetsqft-1;
diffrentablearea=log(RentableArea/prevtrans_RentableArea); 
diffvacancy=vacancy-prevtrans_vacancy; diffaskingrent=log(askingrent/prevtrans_askingrent);
diffvacancy_mkt=prevtrans_PercLeased-prevtrans_vacancy; 
ln_askingrentadj=log(askingrent*prevtrans_rentrate/prevtrans_askingrent);
if ln_askingrentadj=. then ln_askingrentadj=0;
if vacancy=. then missingpprdata=1; else missingpprdata=0;
if diffvacancy=. then missingprevpprdata=1; else missingprevpprdata=0;

array arr0(*) diff:; do i=1 to dim(arr0); if arr0[i]=. then arr0[i]=0; end;

ln_sqft_l1=log(prevtrans_transactnetsqft); ln_term_l1=log(prevtrans_leaseterm); ln_sqft=log(TransactNetSqFt); ln_leaseterm=log(leaseterm);
if ln_sqft_l1=. then do; ln_sqft_l1=0; missingsqft_prev=1;  end; else missingsqft_prev=0;
if ln_term_l1=. then do; ln_term_l1=0; missingterm_prev=1; diffleastermpct=1; end; else do; missingterm_prev=0;diffleastermpct=0;end;
if prevtrans_rentrate=. then do; prevtrans_rentrate=0; missingrent=1; time_pass=0; end; else missingrent=0;

ln_tFromR_2=log(tFromR_2);  ln_tFromR_1=log(tFromR_1);
if ln_tFromR_2=. then ln_tFromR_2=0; if ln_tFromR_1=. then ln_tFromR_1=0;


%let name1=_rent; %let name1b=prevtrans_ln_rentrate;
slope&name1.=slope*&name1b.; hi_slope&name1.=hi_slope*&name1b.; lo_slope&name1.=lo_slope*&name1b.;
slope_g_l1&name1.=slope_g_l1*&name1b.; slope_g_l2&name1.=slope_g_l2*&name1b.; 
slope_g_l3&name1.=slope_g_l3*&name1b.; slope_g_l4&name1.=slope_g_l4*&name1b.;

ust10yr&name1.=ust10yr*&name1b.; hi_ust10yr&name1.=hi_ust10yr*&name1b.; lo_ust10yr&name1.=lo_ust10yr*&name1b.;
ust10yr_g_l1&name1.=ust10yr_g_l1*&name1b.; ust10yr_g_l2&name1.=ust10yr_g_l2*&name1b.; 
ust10yr_g_l3&name1.=ust10yr_g_l3*&name1b.; ust10yr_g_l4&name1.=ust10yr_g_l4*&name1b.;

vacancy_g_1y&name1.=vacancy_g_1y*&name1b.; vacancy_g_2y&name1.=vacancy_g_2y*&name1b.;
tRegionPeakVac&name1.=tRegionPeakVac*&name1b.; tRegionTroughVac&name1.=tRegionTroughVac*&name1b.;
tUSPeakVac&name1.=tUSPeakVac*&name1b.; tUSTroughVac&name1.=tUSTroughVac*&name1b.;
Vac_g_peak&name1.=Vac_g_peak*&name1b.; Vac_g_trough&name1.=Vac_g_trough*&name1b.; USVac_g_peak&name1.=USVac_g_peak*&name1b.; USVac_g_trough&name1.=USVac_g_trough*&name1b.;
tRegionPeakRent&name1.=tRegionPeakRent*&name1b.; tRegionTroughRent&name1.=tRegionTroughRent*&name1b.; tUSPeakRent&name1.=tUSPeakRent*&name1b.; tUSTroughRent&name1.=tUSTroughRent*&name1b.;


RenovateAge_1&name1.=RenovateAge_1*&name1b.; RenovateAge_2&name1.=RenovateAge_2*&name1b.; RenovateAge_3&name1.=RenovateAge_3*&name1b.;
midRise&name1.=midRise*&name1b.; highRise&name1.=highRise*&name1b.; classA&name1.=classA*&name1b.; classB&name1.=classB*&name1b.; classU&name1.=classU*&name1b.;
demolish&name1.=demolish*&name1b.; possible_reBuild&name1.=possible_reBuild*&name1b.; BuildAge_1&name1.=BuildAge_1*&name1b.; BuildAge_2&name1.=BuildAge_2*&name1b.; BuildAge_3&name1.=BuildAge_3*&name1b.;
singleTenant&name1.=singleTenant*&name1b.; empty&name1.=empty*&name1b.; full&name1.=full*&name1b.; full_bad1&name1.=full_bad1*&name1b.; full_bad2&name1.=full_bad2*&name1b.; empty_bad1&name1.=empty_bad1*&name1b.;
empty_bad2&name1.=empty_bad2*&name1b.; empty_bad3&name1.=empty_bad3*&name1b.; empty_bad4&name1.=empty_bad4*&name1b.; empty_bad5&name1.=empty_bad5*&name1b.; empty_bad6&name1.=empty_bad6*&name1b.; 
empty_bad7&name1.=empty_bad7*&name1b.; empty_bad8&name1.=empty_bad8*&name1b.; empty_ren1&name1.=empty_ren1*&name1b.; empty_ren2&name1.=empty_ren2*&name1b.; empty_ren3&name1.=empty_ren3*&name1b.;
empty_ren4&name1.=empty_ren4*&name1b.; empty_ren5&name1.=empty_ren5*&name1b.; empty_ren6&name1.=empty_ren6*&name1b.; buildSize_1&name1.=buildSize_1*&name1b.; buildSize_2&name1.=buildSize_2*&name1b.;
buildSize_3&name1.=buildSize_3*&name1b.; buildSize_4&name1.=buildSize_4*&name1b.; hasRenovate&name1.=hasRenovate*&name1b.;
multiRenovate&name1.=multiRenovate*&name1b.; rebuild_full&name1.=rebuild_full*&name1b.; demolish_empty&name1.=demolish_empty*&name1b.; vacancy_g_1&name1.=vacancy_g_1*&name1b.;
vacancy_g_2&name1.=vacancy_g_2*&name1b.; rent_g_1&name1.=rent_g_1*&name1b.; rent_g_2&name1.=rent_g_2*&name1b.; vacancy&name1.=vacancy*&name1b.;
vacancy_hi&name1.=vacancy_hi*&name1b.; vacancy_hi2&name1.=vacancy_hi2*&name1b.; tEmpty&name1.=tEmpty*&name1b.; tEmpty_sq&name1.=tEmpty_sq*&name1b.;
PercLeased_lo&name1.=PercLeased_lo*&name1b.; timepass&name1.=time_pass*&name1b.; percLease_g&name1.=percLease_g*&name1b.;

%let name1=_askingrent; %let name1b=ln_askingrent;
slope&name1.=slope*&name1b.; hi_slope&name1.=hi_slope*&name1b.; lo_slope&name1.=lo_slope*&name1b.;
slope_g_l1&name1.=slope_g_l1*&name1b.; slope_g_l2&name1.=slope_g_l2*&name1b.; 
slope_g_l3&name1.=slope_g_l3*&name1b.; slope_g_l4&name1.=slope_g_l4*&name1b.;

ust10yr&name1.=ust10yr*&name1b.; hi_ust10yr&name1.=hi_ust10yr*&name1b.; lo_ust10yr&name1.=lo_ust10yr*&name1b.;
ust10yr_g_l1&name1.=ust10yr_g_l1*&name1b.; ust10yr_g_l2&name1.=ust10yr_g_l2*&name1b.; 
ust10yr_g_l3&name1.=ust10yr_g_l3*&name1b.; ust10yr_g_l4&name1.=ust10yr_g_l4*&name1b.;

vacancy_g_1y&name1.=vacancy_g_1y*&name1b.; vacancy_g_2y&name1.=vacancy_g_2y*&name1b.;
tRegionPeakVac&name1.=tRegionPeakVac*&name1b.; tRegionTroughVac&name1.=tRegionTroughVac*&name1b.;
tUSPeakVac&name1.=tUSPeakVac*&name1b.; tUSTroughVac&name1.=tUSTroughVac*&name1b.;
Vac_g_peak&name1.=Vac_g_peak*&name1b.; Vac_g_trough&name1.=Vac_g_trough*&name1b.; USVac_g_peak&name1.=USVac_g_peak*&name1b.; USVac_g_trough&name1.=USVac_g_trough*&name1b.;
tRegionPeakRent&name1.=tRegionPeakRent*&name1b.; tRegionTroughRent&name1.=tRegionTroughRent*&name1b.; tUSPeakRent&name1.=tUSPeakRent*&name1b.; tUSTroughRent&name1.=tUSTroughRent*&name1b.;

RenovateAge_1&name1.=RenovateAge_1*&name1b.; RenovateAge_2&name1.=RenovateAge_2*&name1b.; RenovateAge_3&name1.=RenovateAge_3*&name1b.;
midRise&name1.=midRise*&name1b.; highRise&name1.=highRise*&name1b.; classA&name1.=classA*&name1b.; classB&name1.=classB*&name1b.; classU&name1.=classU*&name1b.;
demolish&name1.=demolish*&name1b.; possible_reBuild&name1.=possible_reBuild*&name1b.; BuildAge_1&name1.=BuildAge_1*&name1b.; BuildAge_2&name1.=BuildAge_2*&name1b.; BuildAge_3&name1.=BuildAge_3*&name1b.;
singleTenant&name1.=singleTenant*&name1b.; empty&name1.=empty*&name1b.; full&name1.=full*&name1b.; full_bad1&name1.=full_bad1*&name1b.; full_bad2&name1.=full_bad2*&name1b.; empty_bad1&name1.=empty_bad1*&name1b.;
empty_bad2&name1.=empty_bad2*&name1b.; empty_bad3&name1.=empty_bad3*&name1b.; empty_bad4&name1.=empty_bad4*&name1b.; empty_bad5&name1.=empty_bad5*&name1b.; empty_bad6&name1.=empty_bad6*&name1b.; 
empty_bad7&name1.=empty_bad7*&name1b.; empty_bad8&name1.=empty_bad8*&name1b.; empty_ren1&name1.=empty_ren1*&name1b.; empty_ren2&name1.=empty_ren2*&name1b.; empty_ren3&name1.=empty_ren3*&name1b.;
empty_ren4&name1.=empty_ren4*&name1b.; empty_ren5&name1.=empty_ren5*&name1b.; empty_ren6&name1.=empty_ren6*&name1b.; buildSize_1&name1.=buildSize_1*&name1b.; buildSize_2&name1.=buildSize_2*&name1b.;
buildSize_3&name1.=buildSize_3*&name1b.; buildSize_4&name1.=buildSize_4*&name1b.; hasRenovate&name1.=hasRenovate*&name1b.;
multiRenovate&name1.=multiRenovate*&name1b.; rebuild_full&name1.=rebuild_full*&name1b.; demolish_empty&name1.=demolish_empty*&name1b.; vacancy_g_1&name1.=vacancy_g_1*&name1b.;
vacancy_g_2&name1.=vacancy_g_2*&name1b.; rent_g_1&name1.=rent_g_1*&name1b.; rent_g_2&name1.=rent_g_2*&name1b.; vacancy&name1.=vacancy*&name1b.;
vacancy_hi&name1.=vacancy_hi*&name1b.; vacancy_hi2&name1.=vacancy_hi2*&name1b.; tEmpty&name1.=tEmpty*&name1b.; tEmpty_sq&name1.=tEmpty_sq*&name1b.;
PercLeased_lo&name1.=PercLeased_lo*&name1b.; timepass&name1.=time_pass*&name1b.; percLease_g&name1.=percLease_g*&name1b.;

if lease_xbeta=. then  missingpropdata=1; else missingpropdata=0;
if prevtrans_lease_xbeta=. then  missingprevpropdata=1; else missingprevpropdata=0;


if missingpropdata=1  then do; array arr1(*) RenovateAge_1_rent--percLease_g_rent RenovateAge_1_askingrent--percLease_g_askingrent
RenovateAge_1-RenovateAge_3 midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq empty_lease full_lease lowlease_lease PercLeased_lo percLease_g;   
do i=1 to dim(arr1); arr1[i]=0; end; end;

if missingprevpropdata=1 then do; array arr2(*) &keep_l1 ; 
do i=1 to dim(arr2); arr2[i]=0; end; end;

if missingpprdata=1 then do; array arr3(*) vacancy: askingrent:  RenovateAge_1_askingrent--percLease_g_askingrent ; 
do i=1 to dim(arr3); arr3[i]=0; end; end;
if prevtrans_lease_xbeta_askingrent=. then prevtrans_lease_xbeta_askingrent=0;
drop i;  proc sort nodup; by asgproptype propertyid qtr; run; 

proc means; var prevtrans_ln_rentrate ln_askingrent ln_askingrentadj ln_tFromR_1 ln_tFromR_2  time_pass lease_xbeta tFromR_2 RenovateAge_1-RenovateAge_3
midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq diff:  slope_askingrent--percLease_g_askingrent slope_rent--percLease_g_rent
missingpropdata missingpprdata missingprevpropdata missingprevpprdata;run;

data tmp; set costar.costar_ppr_merged(where=(propertyid=283519) firstobs=1 obs=20);run;



%let rentvarlist=prevtrans_ln_rentrate ln_askingrent ln_askingrentadj ln_tFromR_1 ln_tFromR_2  time_pass lease_xbeta tFromR_2 RenovateAge_1-RenovateAge_3
midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq diff:  slope_askingrent--percLease_g_askingrent slope_rent--percLease_g_rent
missingpropdata missingpprdata missingprevpropdata missingprevpprdata;

proc reg data=rentrate6 outest=coStar.Rent_byProp_Parm
EDF TABLEOUT ADJRSQ noprint;  by asgproptype;
model ln_rentrate=prevtrans_ln_rentrate ln_askingrent ln_askingrentadj ln_tFromR_1 ln_tFromR_2  time_pass lease_xbeta tFromR_2 RenovateAge_1-RenovateAge_3
midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq diff:  slope_askingrent--percLease_g_askingrent slope_rent--percLease_g_rent
missingpropdata missingpprdata missingprevpropdata missingprevpprdata/selection=stepwise;	**weight RentableArea; output out=out_prop r=r_rentrate p=p_rentrate;	run;
 
proc sort data=rentrate6; by asgproptype;

proc reg data=rentrate6 outest=coStar.Rent_byProp_Parm_withprev
EDF TABLEOUT ADJRSQ noprint;  by asgproptype; where missingrent=0;
model ln_rentrate=prevtrans_ln_rentrate ln_askingrent ln_askingrentadj ln_tFromR_1 ln_tFromR_2  time_pass lease_xbeta tFromR_2 RenovateAge_1-RenovateAge_3
midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq diff:  slope_askingrent--percLease_g_askingrent slope_rent--percLease_g_rent
missingpropdata missingpprdata missingprevpropdata missingprevpprdata/selection=stepwise;	**weight RentableArea; output out=out_prop r=r_rentrate p=p_rentrate;	run;
 

proc reg data=rentrate6 outest=coStar.Rent_byProp_Parm_missingprev
EDF TABLEOUT ADJRSQ noprint;  by asgproptype; where missingrent=1;
model ln_rentrate=ln_askingrent ln_askingrentadj ln_tFromR_1 ln_tFromR_2  time_pass lease_xbeta tFromR_2 RenovateAge_1-RenovateAge_3
midRise highRise classA classB classU demolish possible_reBuild BuildAge_1-BuildAge_3 singleTenant empty 
full full_bad1 full_bad2 empty_bad1-empty_bad8 empty_ren1-empty_ren6 buildSize_1-buildSize_4 hasRenovate 
multiRenovate rebuild_full demolish_empty vacancy_g_1-vacancy_g_2 rent_g_1-rent_g_2 vacancy vacancy_hi
vacancy_hi2 tEmpty tEmpty_sq diff:  slope_askingrent--percLease_g_askingrent slope_rent--percLease_g_rent
missingpropdata missingpprdata missingprevpropdata missingprevpprdata/selection=stepwise;	**weight RentableArea; output out=out_prop r=r_rentrate p=p_rentrate;	run;
 

%mend;
