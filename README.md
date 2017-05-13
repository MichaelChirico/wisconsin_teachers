This repository provides reproduction code for the paper Teacher Turnover in Wisconsin, which can be downloaded [here](https://github.com/MichaelChirico/wisconsin_teachers/blob/master/turnover_paper.pdf). The paper was compiled with [`rmarkdown`](http://rmarkdown.rstudio.com/http://rmarkdown.rstudio.com/) through [`knitr`](https://yihui.name/knitr/); the `.Rmd` document for the paper is [this one](https://raw.githubusercontent.com/MichaelChirico/wisconsin_teachers/master/turnover_paper.Rmd). 

# Raw Data

There are three public sources of data for this paper:

 1. Wisconsin Department of Public Instruction (DPI) collects and releases annual WISEstaff PI-1202 reports which give teacher- (more specificaly, assignment-) level snapshots of the full panoply of school employees in the state. These are available [here](http://dpi.wi.gov/cst/data-collections/staff/published-data). The R script [`raw_data_cleaner.R`](https://raw.githubusercontent.com/MichaelChirico/wisconsin_teachers/master/raw_data_cleaner.R) will download these files and do some baseline touchup to the raw files (which are mostly in fixed-width format) before producing easy-to-use `.csv` versions of the raw data. The script can be run at the command line with `Rscript raw_data_cleaner.R`; be sure to customize the `wds` variable to your local paths (`wds['data']` is where the data will be downloaded and the `.csv`s saved; `wds['key']` contains the fixed-width file reading dictionaries created by myself).
 
  2. Wisconsin's WKCE test score data is also released by DPI at the district and school level. As per [here](https://dpi.wi.gov/wisedash/about-data/assessment), the WKCE is part of the WSAS battery of tests; the full set of these results can be downloaded through the WINSS historical data file repository [here](https://dpi.wi.gov/wisedash/download-files/type?field_wisedash_upload_type_value=WINSS+Historical+Data+Files&field_wisedash_data_view_value=All&=Apply).
  
  3. The NCES Common Core of Data [District-](https://nces.ed.gov/ccd/pubagency.asp) and [School-](https://nces.ed.gov/ccd/pubschuniv.asp)level data files.

# Pre-Paper Data Cleaning

`turnover_paper.Rmd` runs the `turnover_data_cleaner.R` script internally (which does some final data wrangling and runs the [COBS](https://cran.r-project.org/web/packages/cobs/index.html) routine, but relies on two scripts to be run beforehand:

 1. [`background_data_cleaner.R`](https://raw.githubusercontent.com/MichaelChirico/wisconsin_teachers/master/background_data_cleaner.R) assembles school- and district-level files from the DPI and NCES; be sure to customize `wds` here as well, which tell the script where to find these raw data files and where to write the output.
 
 2. [`teacher_match_and_clean.R`](https://raw.githubusercontent.com/MichaelChirico/wisconsin_teachers/master/teacher_match_and_clean.R) runs the teacher matching algorithm described in the paper's Appendix in order to create a panel of data from the DPI's cross-sections.
 
 Feel free to [file an issue](https://github.com/MichaelChirico/wisconsin_teachers/issues) or e-mail me for any further clarification/concerns.
