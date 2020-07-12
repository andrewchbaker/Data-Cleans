library(tidyverse)
library(lubridate)
library(RPostgres)
library(data.table)

# NOTE - THIS FILE WILL THE LINKING FILE BETWEEN CRSP AND COMPUSTAT
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
  collect() %>% 
  # if linkeendt is missing set to today
  mutate(linkenddt = if_else(is.na(linkenddt), 
                             lubridate::today(), linkenddt)) 

# fix two date errors which generate a very small number of duplicates at the 
# permno-date level
link$linkenddt[which(link$gvkey == "177446" & link$lpermno == 86812)] <- ymd(20190728)
link$linkenddt[which(link$gvkey == "021998" & link$lpermno == 15075)] <- ymd(20180117)
# drop one extra duplicate
link <- link %>% filter(!(gvkey == "002759" & linkprim == "J"))

# save the linked dataset
saveRDS(link, here::here("Cleaned_Data", "link.rds"))
