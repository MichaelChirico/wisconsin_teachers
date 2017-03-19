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
        wsas = '/media/data_drive/wisconsin/wsas',
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

## Available metrics
##  - WRCT (Wisconsin Reading Comprehension Test)
##     * "An Assessment of Primary-Level Reading at Grade Three"
##     * 93/94 - 04/05
##     * https://dpi.wi.gov/assessment/historical/wisconsin-reading-comprehension-test
##     * succeeded by WKCE
##  - WSAS (Wisconsin Student Assessment System)
##     * https://dpi.wi.gov/wisedash/about-data/assessment
##     * "The WSAS consists of both the Wisconsin Knowledge and Concepts
##        Examination (WKCE) and the Wisconsin Alternate Assessment for 
##        Students with Disabilities (WAA-SwD)."
##     * "WKCE performance level data for 2001-02 and earlier are NOT 
##        comparable to performance level data for 2002-03 and later 
##        years primarily due to changes in performance standards 
##        (cut scores) associated with each performance level. Score ranges 
##        associated with each WKCE performance level were reset due to 
##        changes in the WKCE and the change in the testing window. 
##        See 2002-03 data changes under Data Changes Over Time below."
##     * Available through WINSS Historical Files through 04/05,
##       then through WISEdash thereafter
##     * Only available at proficiency levels pre-06 
#        (then average scale score available thereafter)

wsas1 = list.files(wds['wsas'], pattern = 'all_students.*-0[0-9]', 
                   full.names = TRUE)
names(wsas1) = paste0('20', gsub('.*-(.*)\\.csv', '\\1', wsas1))

grades1 = rbindlist(lapply(wsas1, function(ff) {
  num = c('number_included_in_percents', 
          'percent_proficient_wkce', 
          'percent_advanced_wkce')
  DT = fread(ff, na.strings = c('N/A', '*', '--'),
             select = c('school_type', 'subject', 'district_number',
                        'school_number', 'tested_grades', num))
  DT = DT[school_type != 'Summary' & 
            subject %in% c('Mathematics', 'Reading')]
  DT[ , (num) := lapply(.SD, as.numeric), .SDcols = num]
  setnames(DT, num, c('n', 'pct_prof', 'pct_advn'))
  DT[ , n_prof := round(n * pct_prof/100)]
  DT[ , n_advn := round(n * pct_advn/100)]
  DT = DT[ , lapply(.SD, sum, na.rm = TRUE), 
           by = .(district = district_number),
           .SDcols = c('n', 'n_prof', 'n_advn')]
  DT
}), idcol = 'year')

districts[grades1, c('n_tested', 'n_prof', 'n_advn') :=
            .(i.n, i.n_prof, i.n_advn), on = c('year', 'district')]

wsas2 =  list.files(wds['wsas'], pattern = 'certified.*-0[0-8]', 
                    full.names = TRUE)
names(wsas2) = paste0('20', gsub('.*-(.*)\\.csv', '\\1', wsas2))

grades2 = rbindlist(lapply(wsas2, function(ff) {
  DT = fread(ff, select = c('GROUP_BY', 'DISTRICT_CODE', 'SCHOOL_NAME',
                            'TEST_SUBJECT', 'TEST_GROUP', 'GROUP_BY_VALUE',
                            'TEST_RESULT', 'STUDENT_COUNT'))
  DT = DT[GROUP_BY == 'All Students' & DISTRICT_CODE != '0000' & 
            SCHOOL_NAME == '[Districtwide]' & TEST_GROUP == 'WKCE' & 
            GROUP_BY_VALUE != '[Data Suppressed]' & TEST_RESULT != 'No WSAS']
  DT[ , STUDENT_COUNT := as.numeric(STUDENT_COUNT)]
  DT = dcast(DT, DISTRICT_CODE ~ TEST_RESULT, 
             value.var = 'STUDENT_COUNT', fun.aggregate = sum)
  DT[ , n := rowSums(.SD), .SDcols = grep('[a-z]', names(DT))]
  setnames(DT, c('DISTRICT_CODE', 'Proficient', 'Advanced'),
           c('district', 'n_prof', 'n_advn'))
  DT[ , c('Minimal Performance', 'Basic') := NULL][]
}), idcol = 'year')

districts[grades2, c('n_tested', 'n_prof', 'n_advn') :=
            .(i.n, i.n_prof, i.n_advn), on = c('year', 'district')]

fwrite(districts, wds['data'] %+% 'district_demographics.csv')
