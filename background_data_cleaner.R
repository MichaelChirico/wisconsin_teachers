#Wisconsin Teachers Project
#Background Data Clean-up, organization
#Michael Chirico
#August 25, 2015

###############################################################################
#                             Package Setup                                   #
###############################################################################

rm(list = ls(all = TRUE))
gc()
wds = c(cc.s = "/media/data_drive/common_core/school",
        cc.d = "/media/data_drive/common_core/district",
        data = '/media/data_drive/wisconsin/')
library(funchir)
library(data.table)

###############################################################################
#                           Demographic Data                                  #
###############################################################################

# Urbanicity (school-level, via CCD)
## Focus on included years: 1999-2000 AY through 2007-2008 AY
sf = grep('.*-0[0-8].*W', 
          list.files(wds['cc.s'], full.names = TRUE), value = TRUE)
names(sf) = paste0('20', gsub('.*[0-9]{4}-([0-9]{2})_.*', '\\1', sf))
schools = rbindlist(lapply(sf, function(ff) {
  DT = fread(ff)
  incl_cols = grep('STID|SEASCH|LOCALE|ULOCAL', names(DT))
  DT = DT[FIPST == '55', incl_cols, with = FALSE]
  setnames(DT, c('district', 'school', 'urbanicity'))
}), idcol = 'year')

urban_map = data.table(
  urbanicity = paste0(c(1:8, 11:13, 21:23, 31:33, 41:43)),
  size = c('Large Urban', 'Small Urban', rep('Suburban', 3L),
           rep('Rural', 3L), 'Large Urban', rep('Small Urban', 2L),
           rep('Suburban', 3L), rep('Rural', 6L))
)

schools[urban_map, urbanicity := i.size, on = 'urbanicity']
schools[urbanicity %in% c('M', 'N'), urbanicity := NA]

fwrite(schools, wds['data'] %+% 'school_demographics.csv')

# Ethnicity, poverty (district-level, via CCD)
districts = rbindlist(lapply(sf, function(ff) {
  DT = fread(ff)
  incl_cols = grep('STID|SEASCH|MEMBER|^HISP|^BLACK|LCH', names(DT))
  DT = DT[FIPST == '55', incl_cols, with = FALSE]
  DT[ , (names(DT)) := lapply(.SD, function(x) {x[x < 0] = NA; x})]
  setnames(DT, c('district', 'school', 'free', 
                 'reduced', 'member', 'hisp', 'black'))
  DT[ , lapply(.SD, sum, na.rm = TRUE), by = district,
      .SDcols = !"school"]
}), idcol = 'year')

districts[ , frl := free + reduced]
districts[ , c('free', 'reduced') := NULL]
districts[member == 0, member := NA]

pct_col = c('hisp', 'black', 'frl')
districts[ , (pct_col) := lapply(.SD, `/`, member), .SDcols = pct_col]
setnames(districts, pct_col, paste0('pct_', pct_col))

# Test scores (district-level, via CCD)

