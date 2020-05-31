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

# download compustat data
comp <- tbl(wrds, sql("SELECT * FROM comp.funda")) %>%
  # filter the bullshit as per usual
  filter(indfmt == 'INDL' & datafmt == 'STD' & popsrc == 'D' & consol == 'C') %>% 
  collect()

# download the link file from wrds
# get the CRSP - Compustat link file
link <- tbl(wrds, sql("SELECT * FROM crsp.ccmxpf_lnkhist")) %>%
  filter(linktype %in% c("LC", "LU", "LS")) %>% 
  collect()

# expand dates from start to end date in linking file
link <- link %>% 
  # if linkeendt is missing set to today
  mutate(linkenddt = if_else(is.na(linkenddt), 
                             lubridate::today(), linkenddt)) %>% 
  # expand date by row
  rowwise() %>% 
  do(tibble(gvkey = .$gvkey, permno = .$lpermno, permco = .$lpermco,
            linkprim = .$linkprim, liid = .$liid,
            datadate = seq.Date(.$linkdt, .$linkenddt, by = "day"))) %>% 
  ungroup()

# set key in data table to make this go faster
link <- setDT(link)
setkey(link, gvkey, datadate)

link <- link %>% 
  .[, count := .N, by = list(gvkey, datadate)] %>% 
  .[count == 1 | linkprim == "P"] %>% 
  .[, count := NULL] %>% 
  as_tibble()

# bring in link variable to compustat
comp <- left_join(comp, link, by = c("gvkey", "datadate")) %>% 
  as_tibble()

# save the two datasets - one a day by gvkey/permno combo file for any stock based method
# and the other the full compustat universe with permno matched.
saveRDS(link, here::here("Cleaned_Data", "gvkey_permno_day.rds"))
saveRDS(comp, here::here("Cleaned_Data", "crsp_compustat_merged.rds"))

