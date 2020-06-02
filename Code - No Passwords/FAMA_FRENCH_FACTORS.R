###############################################################################################################
### REPLICATE FAMA-FRENCH-CARHART 4-FACTOR MODEL ####
# Code by Wayne Chang (https://sites.google.com/site/waynelinchang/home)
# Latest update assistance from Yifan Liu 
# Last Update: June 22, 2018
# Based on methodology described on Ken French website and relevant Fama-French papers

###############################################################################################################
### SETUP ###

library(tidyverse); library(zoo)
require(data.table); library(lubridate)
setwd("")

### WRDS CONNECTION ###

library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  user='',
                  password='',
                  dbname='wrds',
                  sslmode='require')

###############################################################################################################
### LOAD COMPUSTAT FROM WRDS ###
  # Downloads Compustat and Compustat/Crsp merging link from WRDS
  # adds the linked permno to the compustat dataset
  # no filtering except must have PERMNO link

# retrieve Compustat annual data (takes 10mins each below)
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEAR, SICH, NAICSH,
                            AT, LT, SEQ, CEQ, PSTKL, PSTKRV, PSTK, TXDITC, TXDB, ITCB,
                            REVT, COGS, XINT, XSGA, IB, TXDI, DVC, ACT, CHE, LCT,
                            DLC, TXP, DP, PPEGT, INVT 
                    from COMP.FUNDA
                    where INDFMT='INDL' and DATAFMT='STD' and CONSOL='C' and POPSRC='D'") # STD is unrestatd data
data.comp.funda <- dbFetch(res, n = -1) # n=-1 denotes no max but retrieve all record
save(data.comp.funda, file = "180619 data.comp.funda.RData")
                       
# retrieve Compustat quarterly data
res <- dbSendQuery(wrds,"select GVKEY, CUSIP, DATADATE, FYR, FYEARQ,
                            ATQ, LTQ, SEQQ, CEQQ, PSTKRQ, PSTKQ, TXDITCQ, TXDBQ,
                            REVTQ, COGSQ, XINTQ, XSGAQ, IBQ, TXDIQ, ACTQ, CHEQ, LCTQ,
                            DLCQ, TXPQ, DPQ, PPEGTQ, INVTQ, EPSPXQ, RDQ
                           from COMPM.FUNDQ
                           where INDFMT='INDL' and DATAFMT='STD' and CONSOL='C' and POPSRC='D'") # STD is unrestatd data
data.comp.fundq <- dbFetch(res, n = -1) # n=-1 denotes no max but retrieve all record
save(data.comp.fundq, file = "180619 data.comp.fundq.RData")

# retrieve Merged Compustat/CRSP link table
res <- dbSendQuery(wrds,"select GVKEY, LPERMNO, LINKDT, LINKENDDT, LINKTYPE, LINKPRIM
                    from crsp.ccmxpf_lnkhist")
data.ccmlink <- dbFetch(res, n = -1) 
save(data.ccmlink, file = "180619 data.ccmlink.RData")

# Merge the linked Permno onto Compustat dataset
# compared to SAS code based on WRDS FF Research macro, I don't include all Linktypes but add J Linkprim
  # including J linkprim is key bc/ allows me to get the post-2010 Berkshire history
  # omitting non-primary linktypes led to 1% fewer obs (2,000) but cleaner data (datadate<="2013-12-31" for comparability)
data.ccm <-  data.ccmlink %>%
  # use only primary links (from WRDS Merged Compustat/CRSP examples)
  filter(linktype %in% c("LU", "LC", "LS")) %>%
  filter(linkprim %in% c("P", "C", "J")) %>%
  merge(data.comp.funda, by="gvkey") %>% # inner join, keep only if permno exists
  mutate(datadate = as.Date(datadate), 
         permno = as.factor(lpermno),
         linkdt = as.Date(linkdt),
         linkenddt = as.Date(linkenddt),
         linktype = factor(linktype, levels=c("LC", "LU", "LS")),
         linkprim = factor(linkprim, levels=c("P", "C", "J"))) %>%
  # remove compustat fiscal ends that do not fall within linked period; linkenddt=NA (from .E) means ongoing  
  filter(datadate >= linkdt & (datadate <= linkenddt | is.na(linkenddt))) %>%
  # prioritize linktype, linkprim based on order of preference/primary if duplicate
  arrange(datadate, permno, linktype, linkprim) %>%
  distinct(datadate, permno, .keep_all = TRUE)
save(data.ccm, file = "180619 data.ccm.RData")
rm(data.comp.funda, data.ccmlink)

###############################################################################################################
### COMPUSTAT CLEANING AND VAR CALC ###

# load("180619 data.ccm.RData")
data.comp <- data.ccm %>%
  rename(PERMNO=permno) %>% data.table %>% # ensure col names match crsp's
  group_by(PERMNO) %>% 
  mutate(datadate = as.yearmon(datadate),
         comp.count = n()) %>% # number of years in data; future option to cut first year data; works but leads to warnings
    # tests based on BE spread show FF no longer impose this condition (even though mentioned in FF'93)
  ungroup %>% arrange(datadate, PERMNO) %>% data.frame %>%
  distinct(datadate, PERMNO, .keep_all = TRUE) # hasn't been issue but just in case

data.comp.a <- data.comp %>%
  group_by(PERMNO) %>%
  mutate(BE = coalesce(seq, ceq + pstk, at - lt) + coalesce(txditc, txdb + itcb, 0) - 
              coalesce(pstkrv, pstkl, pstk, 0), # consistent w/ French website variable definitions
         OpProf = (revt - coalesce(cogs, 0) - coalesce(xint, 0) - coalesce(xsga,0)),
         OpProf = as.numeric(ifelse(is.na(cogs) & is.na(xint) & is.na(xsga), NA, OpProf)), # FF condition
         GrProf = (revt - cogs),
         Cflow = ib + coalesce(txdi, 0) + dp,  # operating; consistent w/ French website variable definitions
         Inv = (coalesce(ppegt - lag(ppegt), 0) + coalesce(invt - lag(invt), 0)) / lag(at),
         AstChg = (at - lag(at)) / lag(at) # note that lags use previously available (may be different from 1 yr)
         ) %>% ungroup %>%
  arrange(datadate, PERMNO) %>%
  select(datadate, PERMNO, comp.count, at, revt, ib, dvc, BE:AstChg) %>%
  mutate_if(is.numeric, funs(ifelse(is.infinite(.), NA, .))) %>% # replace Inf w/ NA's
  mutate_if(is.numeric, funs(round(., 5))) # round to 5 decimal places (for some reason, 0's not properly coded in some instances)
save(data.comp.a, file="180619 data.comp.a.RData")
rm(data.ccm, data.comp)

###############################################################################################################
### LOAD CRSP FROM WRDS ###
  # Downloads CRSP MSE, MSF, and MSEDELIST tables from WRDS
  # merges, cleans, and for market cap calc, combines permco's with multiple permnos (eg berkshire)
  # no filtering 

# SLOW CODE (30 mins)
res <- dbSendQuery(wrds, "select DATE, PERMNO, PERMCO, CFACPR, CFACSHR, SHROUT, PRC, RET, RETX, VOL
                   from CRSP.MSF")
#                   where PRC is not null")
crsp.msf <- dbFetch(res, n = -1) 
save(crsp.msf, file = "180619 crsp.msf.RData")

res <- dbSendQuery(wrds, "select DATE, PERMNO, SHRCD, EXCHCD
                   from CRSP.MSE")
#                   where SHRCD is not null")
crsp.mse <- dbFetch(res, n = -1)
save(crsp.mse, file = "180619 crsp.mse.RData")

res <- dbSendQuery(wrds, "select DLSTDT, PERMNO, dlret
                   from crspq.msedelist")
#                   where dlret is not null")
crsp.msedelist <- dbFetch(res, n = -1)
save(crsp.msedelist, file = "180619 crsp.msedelist.RData")

# clean and marge data
crsp.msf <- crsp.msf %>%
  filter(!is.na(prc)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.mse <- crsp.mse %>%
  filter(!is.na(shrcd)) %>%
  mutate(Date = as.yearmon(as.Date(date))) %>%
  select(-date)
crsp.msedelist <- crsp.msedelist %>%
  filter(!is.na(dlret)) %>%
  mutate(Date = as.yearmon(as.Date(dlstdt))) %>%
  select(-dlstdt)
data.crsp.m <- crsp.msf %>%
  merge(crsp.mse, by=c("Date", "permno"), all=TRUE, allow.cartesian=TRUE) %>%
  merge(crsp.msedelist, by=c("Date", "permno"), all=TRUE, allow.cartesian=TRUE) %>%
  rename(PERMNO=permno) %>% 
  mutate_at(vars(PERMNO, permco, shrcd, exchcd), funs(as.factor)) %>%
  mutate(retadj=ifelse(!is.na(ret), ret, ifelse(!is.na(dlret), dlret, NA))) %>% # create retadj by merging ret and dlret
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%    
  mutate_at(vars(shrcd, exchcd), funs(na.locf(., na.rm=FALSE)))  # fill in NA's with latest available (must sort by Date and group by PERMNO)
  
data.crsp.m <- data.crsp.m %>%
  mutate(meq = shrout * abs(prc)) %>% # me for each permno
  group_by(Date, permco) %>%
  mutate(ME = sum(meq)) %>% # to calc market cap, merge permnos with same permnco
  arrange(Date, permco, desc(meq)) %>%
  group_by(Date, permco) %>%
  slice(1) %>% # keep only permno with largest meq
  ungroup

save(data.crsp.m, file = "180619 data.crsp.m.RData")
rm(crsp.mse, crsp.msf, crsp.msedelist)

###############################################################################################################
### CRSP CLEANING ###
  # filters EXCHCD (NYSE, NASDAQ, AMEX) and SHRCD (10,11)

Fill_TS_NAs <- function(main) {
  # takes datat frame with Date and PERMNO as columns and fills in NAs where there are gaps
  
  core <- select(main, Date, PERMNO)
  # find first and last dates of each PERMNO
  date.bookends <- core %>%
    group_by(PERMNO) %>%
    summarize(first = first(Date), last = last(Date))
  
  # generate all dates for all PERMNOs then trim those outside of each PERMNO's first and last dates
  output <- core %>%
    mutate(temp = 1) %>% # create 3rd column so spread can be applied
    spread(., PERMNO, temp) %>%
    gather(., PERMNO, temp, -Date) %>%
    merge(date.bookends, by="PERMNO", all.x=TRUE) %>%
    group_by(PERMNO) %>%
    filter(Date>=first & Date<=last) %>%
    select(Date, PERMNO) %>%
    merge(., main, by=c("Date", "PERMNO"), all.x=TRUE)
  
  return(output)
}

# SLOW CODE (5 mins)
load("180619 data.crsp.m.RData")
data.crsp.cln <- data.crsp.m %>%
  select(Date, PERMNO, shrcd, exchcd, cfacpr, cfacshr, shrout, prc, vol, retx, retadj, ME) %>%
  mutate(ME = ME/1000) %>%  # convert from thousands to millions (consistent with compustat values)
  filter((shrcd == 10 | shrcd == 11) & (exchcd == 1 | exchcd == 2 | exchcd == 3)) %>%
  Fill_TS_NAs %>% # fill in gap dates within each PERMNO with NAs to uses lead/lag (lead to NAs for SHRCD and EXCHCD); fn in AnoDecomp_Support
  mutate(PERMNO = as.factor(PERMNO)) %>%
  group_by(PERMNO) %>%
  mutate(port.weight = as.numeric(ifelse(!is.na(lag(ME)), lag(ME), ME/(1+retx))), # calc portweight as ME at beginning of period
         port.weight = ifelse(is.na(retadj) & is.na(prc), NA, port.weight)) %>% # remove portweights calc for date gaps
  ungroup %>% 
  rename(retadj.1mn = retadj) %>%
  arrange(Date, PERMNO) %>%
  distinct(Date, PERMNO, .keep_all = TRUE) # hasn't been issue but just in case

save(data.crsp.cln, file = "180619 data.crsp.cln.RData")
rm(data.crsp.m)

###############################################################################################################
### MERGE COMPUSTAT AND CRSP ###
  # Merges CRSP and Compustat data fundamentals by PERMNO and DATE (annual-June-end portfolio formation)
  # Also get Davis book equity data (Compustat match begins 1951 but Davis book data available starting 20s)
  # Keep all CRSP info (drop Compustat if can't find CRSP)
  # Match Compustat and Davis data based on FF methodology (to following year June when data is first known at month end)

load("180619 data.crsp.cln.RData")
load("180619 data.comp.a.RData")

data.Davis.bkeq <- read.csv("~/OneDrive/Research/Data/French/Davis Book Equity.csv")
data.Davis.bkeq[data.Davis.bkeq == -999 | data.Davis.bkeq == -99.99] <- NA
data.Davis.bkeq <- data.Davis.bkeq %>%
  mutate(PERMNO = factor(PERMNO)) %>%
  data.table %>%
  select(-FirstYr, -LastYr) %>%
  gather(Date, Davis.bkeq, -PERMNO, na.rm=TRUE) %>%
  mutate(Date = as.yearmon(ymd(paste0(substr(Date, 2, 5),"-6-01"))))

na_locf_until = function(x, n) {
  # in time series data, fill in na's untill indicated n
  l <- cumsum(! is.na(x))
  c(NA, x[! is.na(x)])[replace(l, ave(l, l, FUN=seq_along) > (n+1), 0) + 1]
}

# SLOW CODE (5 mins)
data.both.m <- data.comp.a %>%  
  mutate(Date = datadate + (18-month(datadate))/12) %>% # map to next year June period when data is known (must occur in previous year)
  merge(data.crsp.cln, ., by=c("Date", "PERMNO"), all.x=TRUE, allow.cartesian=TRUE) %>%  # keep all CRSP records (Compustat only goes back to 1950)
  merge(data.Davis.bkeq, by=c("Date", "PERMNO"), all.x=TRUE, allow.cartesian=TRUE) %>%
  arrange(PERMNO, Date, desc(datadate)) %>%
  distinct(PERMNO, Date, .keep_all = TRUE) %>% # drop older datadates (must sort by desc(datadate))
  group_by(PERMNO) %>%
  # fill in Compustat and Davis data NA's with latest available for subsequent year (must sort by Date and group by PERMNO)
  # filling max of 11 previous months means gaps may appear when fiscal year end changes (very strict)
  mutate_at(vars(datadate:Davis.bkeq), funs(na_locf_until(., 11))) %>%
  ungroup %>% 
  mutate(datadate = yearmon(datadate)) %>%
  arrange(Date, PERMNO)

save(data.both.m, file = "180619 data.both.m.RData") 
  # company info has no Date gaps (filled with NA's)
  # all data publicly available by end of Date period (Compustat first data is June-1950 matched to CRSP Jun-51))
  # includes all CRSP (but only Compustat/Davis data that matches CRSP)
    # CRSP first month price data Dec-25, return data Jan-26
    # CRSP last month data Dec-17 (Compustat 2017 data available but discarded bc/ must be mapped to CRSP 2018 data)
  # 180619 3.507 MM obs (Old: 170226 3.463 MM obs)
rm(data.comp.a, data.crsp.cln, data.Davis.bkeq)

###############################################################################################################
### Add FF Variables ###

# SLOW CODE (10 mins)
load("180619 data.both.m.RData") 
data.both.FF.m <- data.both.m %>%
  group_by(PERMNO) %>%
  mutate(d.shares = (shrout*cfacshr)/(lag(shrout)*lag(cfacshr))-1, # change in monthly share count (adjusted for splits)
         ret.12t2 = (lag(retadj.1mn,1)+1)*(lag(retadj.1mn,2)+1)*(lag(retadj.1mn,3)+1)*(lag(retadj.1mn,4)+1)*
                    (lag(retadj.1mn,5)+1)*(lag(retadj.1mn,6)+1)*(lag(retadj.1mn,7)+1)*(lag(retadj.1mn,8)+1)*
                    (lag(retadj.1mn,9)+1)*(lag(retadj.1mn,10)+1)*(lag(retadj.1mn,11)+1)-1, # to calc momentum spread
         BE = coalesce(BE, Davis.bkeq), # data available by end-of-Jun based on Compustat Date mapping 
         ME.Dec = as.numeric(ifelse(month(Date)==6 & lag(ME,6)>0, lag(ME,6), NA)), # previous Dec ME 
         ME.Jun = as.numeric(ifelse(month(Date)==6, ME, NA)), 
         BM.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, BE/ME.Dec, NA)), 
         OpIB = as.numeric(ifelse(month(Date)==6 & BE>0, OpProf/BE, NA)), 
         GrIA = as.numeric(ifelse(month(Date)==6 & at>0, GrProf/at, NA)),
         CFP.FF = as.numeric(ifelse(month(Date)==6 & ME.Dec>0, Cflow/ME.Dec, NA)),
         BM.m = BE/ME, # monthly updated version for spread calc
         CFP.m = Cflow/ME, # monthly updated version for spread calc
         lag.ME.Jun = lag(ME.Jun), # monthly data so only lag by 1 mn
         lag.BM.FF = lag(BM.FF),
         lag.ret.12t2 = lag(ret.12t2),
         lag.OpIB = lag(OpIB),
         lag.AstChg = lag(AstChg))

data.both.FF.m %<>%
  mutate_at(vars(d.shares:lag.AstChg), funs(ifelse(!is.infinite(.), ., NA))) %>% # code Inf values as NAs
  select(Date, datadate, PERMNO, exchcd, comp.count, prc, vol, retadj.1mn, d.shares, ME, port.weight, 
         ret.12t2, at:AstChg, ME.Jun:lag.AstChg) %>%
  arrange(Date, PERMNO) %>%
  group_by(PERMNO) %>%
  mutate_at(vars(ME.Jun:CFP.FF, lag.ME.Jun:lag.AstChg), funs(na_locf_until(., 11))) %>%
  ungroup %>%
  mutate(port.weight = ifelse(is.na(port.weight), 0, port.weight)) # necessary to avoid NAs for weighted ret calc
save(data.both.FF.m, file = "180619 data.both.FF.m.RData")
rm(data.both.m)

###############################################################################################################
### Form FF Factors ###

Form_CharSizePorts2 <- function(main, size, var, wght, ret) { # streamlined version
  # forms 2x3 (size x specificed-characteristc) and forms the 6 portfolios
  # variable broken by 30-70 percentiles, size broken up at 50 percentile (breakpoints uses NYSE data only)
  # requires Date and exchcd
  # outputs portfolio returns for each period,
  
  main.cln <- main %>%
    select(Date, PERMNO, exchcd, !!size, !!var, !!wght, !!ret)
  
  Bkpts.NYSE <- main.cln %>% # create size and var breakpoints based on NYSE stocks only
    filter(exchcd == 1) %>% # NYSE exchange
    group_by(Date) %>%
    summarize(var.P70 = quantile(!!var, probs=.7, na.rm=TRUE),
              var.P30 = quantile(!!var, probs=.3, na.rm=TRUE),
              size.Med = quantile(!!size, probs=.5, na.rm=TRUE))
  
  # calculate size and var portfolio returns
  main.rank <- main.cln %>%
    merge(Bkpts.NYSE, by="Date", all.x=TRUE) %>%
    mutate(Size = ifelse(!!size<size.Med, "Small", "Big"),
           Var = ifelse(!!var<var.P30, "Low", ifelse(!!var>var.P70, "High", "Neutral")),
           Port = paste(Size, Var, sep="."))
  
  Ret <- main.rank %>% # name 2 x 3 size-var portfolios
    group_by(Date, Port) %>%
    summarize(ret.port = weighted.mean(!!ret, !!wght, na.rm=TRUE)) %>% # calc value-weighted returns
    spread(Port, ret.port) %>% # transpose portfolios expressed as rows into seperate columns
    mutate(Small = (Small.High + Small.Neutral + Small.Low)/3,
           Big = (Big.High + Big.Neutral + Big.Low)/3,
           SMB = Small - Big,
           High = (Small.High + Big.High)/2,
           Low = (Small.Low + Big.Low)/2,
           HML = High - Low)
  
  return(Ret)
}

Form_FF6Ports <- function(df) {
  # form FF6 factors from data (SMB, HML, RMW, CMA, UMD)
  output <- df %>%
    group_by(Date) %>%
    summarize(MyMkt = weighted.mean(retadj.1mn, w=port.weight, na.rm=TRUE)) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.BM.FF), quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # SMB, HML
    select(Date:MyMkt, MySMB=SMB, MySMBS=Small, MySMBB=Big, MyHML=HML, MyHMLH=High, MyHMLL=Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.OpIB), quo(port.weight), quo(retadj.1mn)),
          by="Date", all.x=TRUE) %>% # RMW
    select(Date:MyHMLL, MyRMW=HML, MyRMWR=High, MyRMWW=Low) %>%
    merge(Form_CharSizePorts2(df, quo(lag.ME.Jun), quo(lag.AstChg), quo(port.weight), quo(retadj.1mn)),
        by="Date", all.x=TRUE) %>% # CMA
    select(Date:MyRMWW, MyCMA=HML, MyCMAC=Low, MyCMAA=High) %>%
    mutate(MyCMA=-MyCMA) %>%
    merge(Form_CharSizePorts2(df, quo(port.weight), quo(lag.ret.12t2), quo(port.weight), quo(retadj.1mn)), 
          by="Date", all.x=TRUE) %>% # UMD
    select(Date:MyCMAA, MyUMD=HML, MyUMDU=High, MyUMDD=Low)
  return(output)
}

load("180619 data.both.FF.m.RData")
dt.myFF6.m <- Form_FF6Ports(data.both.FF.m) %>%
  filter(year(Date) > 1925 & year(Date) < 2018)
save(dt.myFF6.m, file = "180619 dt.myFF6.m.RData")

###############################################################################################################
### TEST FOR CONSISTENCY WITH POSTED FACTORS ###

Compare_Two_Vectors2 <- function(v1, v2, sqnc=1) { # omits DATE option
  # takes two dataframes where each has a Date and another var column
  # compares the two var columns for similarity
  # v1.date, v2.date denotes col name of date which must be yearmon
  # sqnc affects the plot, e.g. if =3 then plot every third point (so not too crowded)
  # will not work if missing observations in between for one dataframe since cor needs exact same length
  
  # obtain common time segment among two vectors
  lo.date <- max(v1[["Date"]][min(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][min(which(!is.na(v2[[colnames(v2)[2]]])))])
  hi.date <- min(v1[["Date"]][max(which(!is.na(v1[[colnames(v1)[2]]])))],
                 v2[["Date"]][max(which(!is.na(v2[[colnames(v2)[2]]])))])
  v1.trim <- subset(v1, lo.date <= Date & Date <= hi.date)
  v2.trim <- subset(v2, lo.date <= Date & Date <= hi.date)
  
  print("correlation")
  print(cor(v1.trim[[colnames(v1.trim)[2]]], v2.trim[[colnames(v2.trim)[2]]], use="complete.obs"))
  print("v1.trim mean")
  print(mean(v1.trim[[colnames(v1.trim)[2]]], na.rm=TRUE))
  print("v2.trim mean")
  print(mean(v2.trim[[colnames(v2.trim)[2]]], na.rm=TRUE))
  print("means: v1.trim-v2.trim")
  print(mean(v1.trim[[colnames(v1.trim)[2]]], na.rm=TRUE)-mean(v2.trim[[colnames(v2.trim)[2]]], na.rm=TRUE))
  plot(v1.trim[["Date"]][seq(1,nrow(v1.trim),sqnc)],
       v1.trim[[colnames(v1)[2]]][seq(1,nrow(v1.trim),sqnc)], type="l", xlab="Date", ylab="Returns (%)")
  lines(v2.trim[["Date"]][seq(1,nrow(v2.trim),sqnc)],
        v2.trim[[colnames(v2)[2]]][seq(1,nrow(v2.trim),sqnc)], type="l", col="blue")
  legend("topright", c("1", "2"), lty=1, col=c("black","blue"))
}

# import FF6 and Mkt to check data
library(XLConnect)
wb <- loadWorkbook("~/OneDrive/Research/Data/French/180622 French_FF6.xlsx")
dt.FF6.m <- wb %>%
  readWorksheet(sheet = 1) %>%
  mutate(Date = as.yearmon(ymd(paste0(Date,28))),
         Mkt = MktRF + RF) %>%
  mutate_at(vars(-Date), funs(./100)) %>%
  arrange(Date)

# check FF6 factor returns (Jul '26 through Dec '17)
load("180619 dt.myFF6.m.RData")
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MyMkt), select(dt.FF6.m, Date, Mkt), sqnc=12)
# Mkt: cor 99.999%; both means 93.87 bps
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MySMB), select(dt.FF6.m, Date, SMB), sqnc=12)
# SMB: cor 99.6%; means 19.2 bps vs 20.1 bps
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MyHML), select(dt.FF6.m, Date, HML), sqnc=12)
# HML: cor 98.9%; means 37.2 bps vs 38.2 bps 
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MyRMW), select(dt.FF6.m, Date, RMW), sqnc=12)
# RMW: cor 98.3%; means 26.2 bps vs 24.8 bps
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MyCMA), select(dt.FF6.m, Date, CMA), sqnc=12)
# CMA: cor 97.9%; means 23.2 bps vs 28.7 bps (SIZEABLE GAP)
# tested using lag(ME) for port weights instead (avoid using RETX) but doesn't make a diff
Compare_Two_Vectors2(select(dt.myFF6.m, Date, MyUMD), select(dt.FF6.m, Date, UMD), sqnc=12)
# UMD: cor 99.8%; means 65.3 bps vs 65.8 bps

