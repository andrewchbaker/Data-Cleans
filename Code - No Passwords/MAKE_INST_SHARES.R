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

# download institutional holdings 
institutional_data <- tbl(wrds, sql("SELECT rdate, fdate, mgrno, shares, cusip, shrout2 \ 
                                    FROM tfn.s34")) %>% 
  # keep just the first report date per mgr/cusip/reporting date
  group_by(rdate, mgrno, cusip) %>% 
  filter(fdate == min(fdate, na.rm = TRUE)) %>% 
  ungroup() %>% 
  # sum shares over managers by reporting date, cusip
  group_by(rdate, cusip) %>% 
  summarize(inst_shares = sum(shares, na.rm = TRUE)) %>% 
  ungroup() %>% 
  collect() %>% 
  drop_na()

# save 
saveRDS(institutional_data, here::here("Cleaned_Data", "institutional_shares.rds"))