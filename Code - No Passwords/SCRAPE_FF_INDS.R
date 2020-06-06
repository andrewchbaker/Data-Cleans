# This code will scrape Ken French's industry definition file and make a data frame 
# with the industry groupings and SIC code range. This uses Siccodes12 but you can use 
# the other industry definitions he has up there just swap out in the url link.

library(tidyverse)

# url for the industry definitions
url <- "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes12.zip"

# Download the data and unzip it
f <- tempfile() 
download.file(url, f) 
inds <- read.delim(unz(f, "Siccodes12.txt"), header = FALSE)

# make table
inds <- inds %>% 
  # rename the data column
  rename(name = V1) %>% 
  # detect if it is an ID column
  rowwise() %>% 
  # remove the leading space
  mutate(name = str_trim(name, side = "left")) %>% 
  # detect if there are any letters in the name
  mutate(name_id = ifelse(sum(str_detect(name, letters)) >= 1, 1, 0)) %>% 
  # get the id numbers
  mutate(ind_num = ifelse(name_id == 1, as.numeric(str_sub(name, 1, str_locate(name, " ")[1] - 1)), as.numeric(NA))) %>% 
  # get the short name
  mutate(ind_abbrev = ifelse(name_id == 1,
                             str_sub(name, str_locate_all(name, " ")[[1]][1] + 1, 
                                     str_locate_all(name, " ")[[1]][2] - 1), as.character(NA))) %>% 
  # get the full name
  mutate(ind_name = ifelse(name_id == 1,
                           str_sub(name, str_locate_all(name, " ")[[1]][2] + 1,
                                   nchar(name)), as.character(NA))) %>% 
  # fill all the values down
  ungroup() %>%
  fill(ind_num) %>% fill(ind_abbrev) %>% fill(ind_name) %>%
  # drop name id columns
  group_by(ind_num) %>% 
  add_tally %>%
  filter(n == 1 | name_id == 0) %>%
  # get low and high sic by row
  rowwise() %>% 
  mutate(sic_low = ifelse(name_id == 1, as.numeric(NA), as.numeric(str_sub(name, 1, str_locate(name, "-")[1] - 1))),
         sic_high = ifelse(name_id == 1, as.numeric(NA), as.numeric(str_sub(name, str_locate(name, "-")[1] + 1), nchar(name)))) %>%
  ungroup() %>%
  select(ind_num, ind_abbrev, ind_name, sic_low, sic_high)
