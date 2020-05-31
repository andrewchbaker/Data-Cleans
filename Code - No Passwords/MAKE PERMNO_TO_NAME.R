library(tidyverse)
library(lubridate)
library(RPostgres)
library(data.table)

# NOTE - THIS FILE WILL GIVE YOU A DATASET THAT GOES FROM PERMNO/DATE -> NAME AND TICKER INFO.
# BECAUSE A COMPANY MIGHT HAVE MULTIPLE PERMNOS THIS WILL NOT NECESSARILY EQUAL 
# GVKEY/DATE -> PERMNO, AND BECAUSE A PERMNO MAY HAVE DATA IN CRSP EVEN THOUGH IT IS NO LONGER 
# ASSOCIATED WITH A GVKEY,  IT IS LONGER THAN PERMNO/DATE -> GVKEY

# Connect to WRDS Server --------------------------------------------------
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = '',
                  password = '',
                  dbname = 'wrds',
                  sslmode = 'require')

# bring in names and ticker
names <- tbl(wrds, sql("SELECT * FROM crsp.dsenames")) %>% 
  collect()

# expand dates from start to end date in linking file
names <- names %>% 
  # expand date by row
  rowwise() %>% 
  do(tibble(permno = .$permno, cusip = .$ncusip, 
            ticker = .$ticker, comnam = .$comnam,
            datadate = seq.Date(.$namedt, .$nameendt, by = "day"))) %>% 
  ungroup() %>% 
  setDT()

# save the dataset
saveRDS(names, here::here("Cleaned_Data", "permno_day_to_name.rds"))

