# Data-Cleans
This repo has code to do primary data cleaning for Compustat / Crsp from WRDS.
The included codes do the following data cleaning steps:

1. MAKE CRSP_COMP_MERGED

This code takes the universe of annual compustat financial information and matches to CRSP with the CRSP-specific unique security identifier (permno). In addition, it creates a file that has the associated primary permno for each gvkey/date combination. 

2. MAKE PERMNO_TO_GVKEY

This creates a file that has the associated compustat identifier (gvkey) for each permno/date combination. Because one firm might have multiple issued securities, this file should be longer than the unique gvkey/date combination file from CRSP_COMP_MERGED. In addition, there may be observations where there is a name and ticker associated with a permno, but where there is no compustat identifier (gvkey). These observations are retained, with gvkey being blank. 


3. FAMA_FRENCH_FACTORS

This code makes the portfolio based Fama French Factors and was contributed by Gertjan Verdickt.

4. SCRAPE_FF_INDS

This code scrapes the Fama-French Industry definitions from Ken French's website. It uses the 12-industry FF definition, but you can swap out for any of the other industry classifications (e.g. 17, 48) in the url line.

5. MAKE_AMIHUD_ILLIQIDUITY MEASURE

This code will make the Amihud (2002) Illiquidity measure by permno/calendar year (you can update it for different date ranges pretty easily if you so desire). This is measured as:

<img src="https://render.githubusercontent.com/render/math?math=AMIHUD_{iy} = \frac{\sum_{i = 1}^N 1000 \cdot \sqrt{\frac{\left|ret_it\right|}{\left| prc \right| \cdot vol}}}{N}">

6. MAKE_INST_SHARES 

This code will calculate the number of shares held by institutional investors at the permno/quarter level using Thomson Reuters 13F holdings data. 
