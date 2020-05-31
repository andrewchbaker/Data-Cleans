library(tidyverse)
library(lubridate)
library(RPostgres)
library(data.table)

# NOTE - THIS FILE WILL GIVE YOU A DATASET THAT GOES FROM PERMNO/DATE -> GVKEY.
# BECAUSE A COMPANY MIGHT HAVE MULTIPLE PERMNOS THIS WILL NOT NECESSARILY EQUAL 
# GVKEY/DATE -> PERMNO

# Connect to WRDS Server --------------------------------------------------
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = '',
                  password = '',
                  dbname = 'wrds',
                  sslmode = 'require')

# download the link file from wrds
# get the CRSP - Compustat link file
link <- tbl(wrds, sql("SELECT * FROM crsp.ccmxpf_lnkhist")) %>%
  filter(linktype %in% c("LC", "LU", "LS")) %>% 
  collect()

# fix two date errors which generate a very small number of duplicates at the 
# permno-date level
link$linkenddt[which(link$gvkey == "177446" & link$lpermno == 86812)] <- '2019-07-28'
link$linkenddt[which(link$gvkey == "021998" & link$lpermno == 15075)] <- '2018-01-17'

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
  ungroup() %>% 
  setDT()

# set key in data table to make this go faster
setkey(link, permno, datadate)

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

setkey(names, permno, datadate)

# merge in info to link file
link <- merge(link, names, by = c("permno", "datadate"), all.x = TRUE) %>% 
  # save it as a tibble
  as_tibble()

# save the dataset
saveRDS(link, here::here("Cleaned_Data", "permno_day_to_gvkey.rds"))

