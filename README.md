# Data-Cleans
This repo has code to do primary data cleaning for Compustat / Crsp from WRDS.
The included codes do the following data cleaning steps:

1. MAKE CRSP_COMP_MERGED

This code takes the universe of annual compustat financial information and matches to CRSP with the CRSP-specific unique security identifier (permno). In addition, it creates a file that has the associated primary permno for each gvkey/date combination. 

2. MAKE PERMNO_TO_GVKEY

This creates a file that has the associated compustat identifier (gvkey) for each permno/date combination where there is an identified gvkey. Because one firm might have multiple issued securities, this file should be longer than the unique gvkey/date combination file from CRSP_COMP_MERGED.

3. MAKE PERMNO_TO_NAME

This creates a file that has identifying information (firm name, ticker) for each available permno/date combination. This may be longer than the file from PERMNO_TO_GVKEY because a permno may have trading dates where there is no associated GVKEY. 


