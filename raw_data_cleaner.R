#Wisconsin Teacher Project
#Raw Teacher Data Check
#Michael Chirico
#August 23, 2015

rm(list = ls(all = TRUE))
gc()
raw_data_f = "/media/data_drive/wisconsin/teacher_raw_data/"
wds = c(data = paste0(raw_data_f, 'data_files'),
        key = paste0(raw_data_f, "fwf_keys/"))

#access at github.com/MichaelChirico/funchir
library(funchir)
#for scraping URLs
library(rvest)
#for fast FWF input
library(iotools)
#for Excel conversion
library(readxl)
#for everything else
library(data.table)

# Identify all data file URLs
URL = 'https://dpi.wi.gov/cst/data-collections/staff/published-data'
## links identified by being .zip (since 2012-13)
##   or .exe (all earlier years)
URL_xp = '//a[contains(@href, ".zip") or contains(@href, ".exe")]'
data_urls = read_html(URL) %>% 
  html_nodes(xpath = URL_xp) %>% html_attr('href') %>%
  paste0('https://dpi.wi.gov', .)

#Download and extract raw files to data folder
#  Raw files from here: http://lbstat.dpi.wi.gov/lbstat_newasr
#  **TO DO: MAKE SURE NEW URL IS WORKING AS EXPECTED            **
#  **http://dpi.wi.gov/cst/data-collections/staff/published-data**
sapply(data_urls, 
       function(uu){
         tmp <- tempfile()
         download.file(uu, tmp)
         unzip(tmp, exdir = wds["data"])
         unlink(tmp)})

#Remove pesky NUL characters
#  (having checked manually that,
#   as of Dec. 10, 2015,
#   each _pair_ of NUL characters
#   probably is supposed to
#   correspond to a _single_ space)
txt_fls = 
  list.files(wds['data'], full.names = TRUE,
             # (?i) -- case-insensitive
             pattern = "(?i)(txt|dat)")
for (fl in txt_fls){
  r = readBin(fl, raw(), file.info(fl)$size)
  #Remove BOM if it's there:
  if (r[1L] == as.raw(0xef)){
    r = r[-(1L:3L)]
    #don't waste time overwriting file
    #  if we don't find any BOM or
    #  NUL characters
    needToWrite = TRUE
  } else needToWrite = FALSE
  #NUL corresponds to as.raw(0)
  if (any(is.NUL <- r == as.raw(0))){
    #force into matrix with each column
    #  representing a pair
    idx = matrix(which(is.NUL), nrow = 2L)
    if (any(apply(idx, 2L, diff) != 1L))
      stop('unpaired NUL detected!')
    #only overwrite half with a space (as.raw(0x20))
    r[idx[1L, ]] = as.raw(0x20)
    #exclude all remaining NUL and overwrite original file
    writeBin(r[-idx[2L, ]], fl)
    rm(r)
    next
  }
  if (needToWrite) writeBin(r, fl)
  rm(r)
}
rm(fl)

#Now, fix all irregular files -- for the most part,
#  there are certain lines which appear to have been
#  stripped of trailing white space, so simply
#  augment these with enough blanks to make each file
#  flush on its edges.
keys = fread(paste0(raw_data_f, 'fwf_keys.csv'),
             colClasses = c('character', 'character', 'integer', 'character'))
for (fl in txt_fls){
  fl_yr = gsub('[^0-9]', '', fl)
  header_yr = grepl("14|13", fl)
  #for exploring the files, use a variation on the following:
  #   fread(fl, header = FALSE, sep = "^", strip.white = FALSE
  #         )[,{wd<-nchar(V1); idx<-wd!=wdth
  #         table(wd)
  #         .(`Line #`=.I[idx],`Width`=wd[idx],
  #           `ID`=substr(V1[idx],1,9))}]
  #sep = "\n" reads each line as a whole into one variable
  DT = fread(fl, header = FALSE, sep = "\n", strip.white = FALSE,
             #these two years have a header (+ to force integer)
             skip = +header_yr, col.names = 'line')
  
  #keys file constructed by hand from the
  #  Documentation files on DPI website
  width = keys[year == fl_yr, sum(width)]
  
  if (length(idx <- DT[nchar(line) < width, which = TRUE])) {
    #simply append white space to ragged lines, see
    # http://stackoverflow.com/questions/9261961/
    DT[idx, line := sprintf(paste0('%-', width, 's'), line)]
    #overwrite file, mimicking original format as closely as possible
    fwrite(DT, fl, quote = FALSE, row.names = FALSE, col.names = FALSE)
  } else if (header_yr) {
    #these files need to have their header removed
    fwrite(DT, fl, quote = FALSE, row.names = FALSE, col.names = FALSE)
  }
}

#Now, convert all fixed width files to .csv
for (fl in txt_fls){
  fl_yr = gsub('[^0-9]', '', fl)
  with(keys[year == fl_yr], {
    DT = setDT(input.file(fl, formatter = dstrfw,
                          col_types = setNames(type, vname),
                          widths = width))
  })
  #use tab -- embedded , is common
  fwrite(DT, file = gsub("[.].*", ".csv", fl), sep = "\t")
}

#Lastly, convert all excel files to .csv
for (fl in list.files(wds['data'], pattern = '\\.xls', full.names = TRUE)) {
  #which sheet has the data?
  sheet = grep('Staff', excel_sheets(fl), fixed = TRUE, value = TRUE)
  fl_yr = gsub('.*AllStaff[0-9]{2}([0-9]{2}).*', '\\1', fl)
  vname = keys[year == fl_yr, vname]
  vtype = c('skip', rep('text', length(vname) - 1L))
  DT = setDT(read_excel(fl, sheet = sheet, skip = 4L,
                        col_names = vname, col_types = vtype))
  fwrite(DT, file = paste0(wds['data'], '/', fl_yr, 'staff.csv'),
         sep = '\t')
}
