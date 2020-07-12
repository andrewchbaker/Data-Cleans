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
  collect() %>%
  mutate(merge_date = datadate) %>% 
  setDT()

# download the link file from wrds
# get the CRSP - Compustat link file
link <- tbl(wrds, sql("SELECT * FROM crsp.ccmxpf_lnkhist")) %>%
  filter(linktype %in% c("LC", "LU", "LS")) %>% 
  collect() %>% 
  # if linkeendt is missing set to today
  mutate(linkenddt = if_else(is.na(linkenddt), 
                             lubridate::today(), linkenddt)) %>% 
  setDT()

# fix two date errors which generate a very small number of duplicates at the 
# permno-date level
link$linkenddt[which(link$gvkey == "177446" & link$lpermno == 86812)] <- ymd(20190728)
link$linkenddt[which(link$gvkey == "021998" & link$lpermno == 15075)] <- ymd(20180117)
# drop one extra duplicate
link <- link[!(gvkey == "002759" & linkprim == "J")]

# set key in data table to make this go faster
setkey(link, gvkey)

# bring in link variable to compustat and keep just the right hit
comp <- link[comp, 
             on = .(gvkey, linkdt <= merge_date, linkenddt >= merge_date), 
             nomatch = NA] %>% 
  .[, count := .N, by = list(gvkey, datadate)] %>% 
  .[count == 1 | linkprim == "P" | linkprim == "C"] %>% 
  .[, count := NULL] %>% 
  as_tibble()

# save the compustat crsp merged dataset
saveRDS(comp, here::here("Cleaned_Data", "crsp_compustat_merged.rds"))