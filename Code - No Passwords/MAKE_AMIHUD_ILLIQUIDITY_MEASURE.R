library(tidyverse)
library(lubridate)
library(RPostgres)
library(data.table)

# Connect to WRDS Server --------------------------------------------------
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = '',
                  password = '',
                  dbname = 'wrds',
                  sslmode = 'require')

# pull in the crsp variables we need for data after 1985
crsp <- tbl(wrds, sql("SELECT date, cusip, permno, permco, issuno, hsiccd, prc, vol, ret, shrout \
                  FROM crsp.dsf WHERE date >= '1985-01-01'")) %>% 
  collect()

# make amihud liquidity scores
crsp_amihud <- setDT(crsp)
crsp_amihud <- crsp_amihud %>% 
  # drop if volume is zero bc the score breaks down
  .[vol != 0] %>% 
  # make a year variable
  .[, year := year(date)]

# set keys
setkey(crsp_amihud, permno, year)

# calculate by permno year
amihud <- crsp_amihud %>% 
  .[, amihud := 1000 * sqrt(abs(ret)/(abs(prc) * vol))] %>% 
  .[, .(amihud = mean(amihud, na.rm = TRUE)), keyby = .(permno, year)]

# save 
saveRDS(amihud, here::here("Cleaned_Data", "amihud.rds"))