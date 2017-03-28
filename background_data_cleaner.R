#Wisconsin Teachers Project
#Background Data Clean-up, organization
#Michael Chirico
#August 25, 2015

###############################################################################
#                             Package Setup                                   #
###############################################################################
wds = c(cc.s = "/media/data_drive/common_core/school",
        cc.d = "/media/data_drive/common_core/district",
        wsas = '/media/data_drive/wisconsin/wsas',
        data = '/media/data_drive/wisconsin/')
library(funchir)
library(data.table)

###############################################################################
#                   School-level Urbanicity, Ethnography                      #
###############################################################################
## Focus on included years: 1999-2000 AY through 2007-2008 AY
sf = grep('.*-0[0-8].*W', 
          list.files(wds['cc.s'], full.names = TRUE), value = TRUE)
names(sf) = paste0('20', gsub('.*[0-9]{4}-([0-9]{2})_.*', '\\1', sf))
schools = rbindlist(lapply(sf, function(ff) {
  DT = fread(ff)
  #LOCALE through '05-'06, ULOCAL thereafter
  #  (differing approaches to calculating urbanicity; attempting here
  #   to map between those, but it's not perfect)
  incl_patt = 'STID|SEASCH|LOCALE|ULOCAL|MEMBER|^HISP|^BLACK|TOTFRL'
  incl_cols = grep(incl_patt, names(DT))
  DT = DT[FIPST == '55', ..incl_cols]
  new_names = gsub('(.*)[0-9]{2}$', '\\1', names(DT))
  new_names = gsub('LOCALE|ULOCAL', 'urbanicity', new_names)
  setnames(DT, new_names)
}), idcol = 'year')

setnames(schools, c('STID', 'SEASCH', 'TOTFRL',
                    'MEMBER', 'HISP', 'BLACK'),
         c('district', 'school', 'n_frl',
           'n_students', 'n_hisp', 'n_black'))

#2007-08 begins tab-separated files, so read separately
ff = list.files(wds['cc.s'], pattern = '2007-08', full.names = TRUE)
incl_cols = c('FIPST', c('STID', 'SEASCH', 'ULOCAL',
                         'MEMBER', 'HISP', 'BLACK', 'TOTFRL') %+% '07')
schools08 = fread(ff, select = incl_cols)[FIPST == '55']
schools08[ , c('FIPST', 'year') := .(NULL, '2008')]
setnames(schools08, 
         c('STID', 'SEASCH', 'TOTFRL',
           'MEMBER', 'HISP', 'BLACK', 'ULOCAL') %+% '07',
         c('district', 'school', 'n_frl',
           'n_students', 'n_hisp', 'n_black', 'urbanicity'))
schools = rbind(schools, schools08, 
                #This Downtown Montessori school has employees
                #  recorded from 2000 but shows up in CCD from 2001
                data.table(year = '2000', district = '8101',
                           school = '1056', urbanicity = 1),
                fill = TRUE)

schools[n_students == 0, n_students := NA]

urban_map = data.table(
  urbanicity = paste0(c(1:8, 11:13, 21:23, 31:33, 41:43)),
  size = c('Large Urban', 'Small Urban', rep('Suburban', 3L),
           rep('Rural', 3L), 'Large Urban', rep('Small Urban', 2L),
           rep('Suburban', 3L), rep('Rural', 6L))
)

schools[urban_map, urbanicity := i.size, on = 'urbanicity']
schools[urbanicity %in% c('M', 'N'), urbanicity := NA]

#Aggregate school-level ethnography to district level
districts = 
  schools[ , lapply(.SD, sum, na.rm = TRUE), 
           by = .(district, year),  .SDcols = !c('school', 'urbanicity')]

#Convert to % in both
pct_col = c('n_hisp', 'n_black', 'n_frl')
districts[ , (pct_col) := lapply(.SD, `/`, n_students), .SDcols = pct_col]
schools[ , (pct_col) := lapply(.SD, `/`, n_students), .SDcols = pct_col]
setnames(districts, pct_col, gsub('^n', 'pct', pct_col))
setnames(schools, pct_col, gsub('^n', 'pct', pct_col))

#no longer need in school-level data
schools[ , n_students := NULL]

###############################################################################
#                               WKCE Performance                              #
###############################################################################

## Note on available metrics
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
##       (then average scale score available thereafter)

#Break in historical records means stored in two types of file
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
  DT
}), idcol = 'year')

schools[grades1, c('n_tested', 'n_prof', 'n_advn') :=
          .(i.n, i.n_prof, i.n_advn),
        on = c('year', district = 'district_number', school = 'school_number')]

#on-the-fly school-level aggregation, then join
districts[grades1[ , lapply(.SD, sum, na.rm = TRUE), 
                   by = .(year, district = district_number),
                   .SDcols = c('n', 'n_prof', 'n_advn')], 
          c('n_tested', 'n_prof', 'n_advn') :=
            .(i.n, i.n_prof, i.n_advn), on = c('year', 'district')]

#now for the second type of file
wsas2 =  list.files(wds['wsas'], pattern = 'certified.*-0[0-8]', 
                    full.names = TRUE)
names(wsas2) = paste0('20', gsub('.*-(.*)\\.csv', '\\1', wsas2))

grades2 = rbindlist(lapply(wsas2, function(ff) {
  DT = fread(ff, select = c('GROUP_BY', 'DISTRICT_CODE', 'SCHOOL_CODE',
                            'TEST_SUBJECT', 'TEST_GROUP', 'GROUP_BY_VALUE',
                            'TEST_RESULT', 'STUDENT_COUNT'))
  DT = DT[GROUP_BY == 'All Students' & DISTRICT_CODE != '0000' & 
            nzchar(SCHOOL_CODE) & TEST_GROUP == 'WKCE' & 
            GROUP_BY_VALUE != '[Data Suppressed]' & TEST_RESULT != 'No WSAS' & 
            TEST_SUBJECT %in% c('Mathematics', 'Reading')]
  DT[ , STUDENT_COUNT := as.numeric(STUDENT_COUNT)]
  DT = dcast(DT, DISTRICT_CODE + SCHOOL_CODE ~ TEST_RESULT, 
             value.var = 'STUDENT_COUNT', fun.aggregate = sum)
  DT[ , n := rowSums(.SD), .SDcols = grep('[a-z]', names(DT))]
  setnames(DT, c('DISTRICT_CODE', 'SCHOOL_CODE', 'Proficient', 'Advanced'),
           c('district', 'school', 'n_prof', 'n_advn'))[]
}), idcol = 'year')

grades2[ , c('Minimal Performance', 'Basic') := NULL]

schools[grades2, c('n_tested', 'n_prof', 'n_advn') :=
            .(i.n, i.n_prof, i.n_advn), 
        on = c('year', 'district', 'school')]

schools[ , pct_prof := (n_prof + n_advn)/n_tested]
schools[ , c('n_tested', 'n_prof', 'n_advn') := NULL]

#again, aggregate & join
districts[grades2[ , lapply(.SD, sum, na.rm = TRUE), 
                   by = .(year, district), .SDcols = !"school"],
          c('n_tested', 'n_prof', 'n_advn') :=
            .(i.n, i.n_prof, i.n_advn), on = c('year', 'district')]

districts[ , pct_prof := (n_prof + n_advn)/n_tested]
districts[ , c('n_tested', 'n_prof', 'n_advn') := NULL]

###############################################################################
#             District-Level Urbanicity, Student-Teacher Ratio                #
###############################################################################
df = grep('.*_0[1-8]_', 
          list.files(wds['cc.d'], full.names = TRUE), value = TRUE)
names(df) = paste0('20', gsub('.*[0-9]{4}_([0-9]{2})_.*', '\\1', df))

dist_urb = rbindlist(lapply(df, function(ff) {
  #fread failed to guess column class and errored
  DT = fread(ff, colClasses = 'character')
  incl_cols = grep('LEAID|STID|LOCALE|ULOCAL|MEMBER|TOTTCH', names(DT))
  DT[FIPST == '55', ..incl_cols]
}), idcol = 'year')

setnames(dist_urb, 2L:6L,
         c('leaid', 'district', 'urbanicity', 'n_students', 'n_teachers'))

#yes, dist_urb00 does not include urbanicity. shh...
dist_urb00 = 
  fread(wds['cc.d'] %+% '/universe_1999_00_1b.csv',
        select = c('FIPST', paste0(c('STID', 'MEMBER', 'TOTTCH'), '99')),
        col.names = c('fipst', 'district', 'n_students', 'n_teachers'))
dist_urb00[ , year := '2000']

#Per Mark Glander at Common Core of Data, in 2005-06, Wisconsin
#  is missing all locale codes because Wisconsin had already started
#  using the urban-centric locale codes (which are used in the wider
#  Common Core LEA data starting from 2006-07); he pointed me to this
#  page, which contains data files for WI in '05-06 & for mapping the codes
#  https://nces.ed.gov/ccd/CCDLocaleCodeDistrict.asp
ccd_map = fread(wds['cc.d'] %+% '/al051a.csv',
                select = c('LEAID', 'LSTATE05', 'ulocale'),
                col.names = c('leaid', 'state', 'ulocale'),
                colClasses = 'character')
ccd_map[ , year := '2006']

dist_urb[ccd_map, urbanicity := i.ulocale, on = c('year', 'leaid')]
dist_urb[ , leaid := NULL]

dist_urb = rbind(dist_urb, dist_urb00[fipst == '55', !"fipst"], fill = TRUE)

dist_urb[ , class_size := as.numeric(n_students)/as.numeric(n_teachers)]
dist_urb[ , c('n_teachers', 'n_students') := NULL]

dist_urb[urban_map, urbanicity := i.size, on = 'urbanicity']
dist_urb[urbanicity %in% c('M', 'N'), urbanicity := NA]

###############################################################################
#         'Manual' Calculation of Urbanicity in Certain Cases                 #
###############################################################################
# No urbanicity code at district level in 1999-2000, so use
#   the "maximum" urbanicity at the school level. 59
#   districts have >1 urbanicity at the school level.
lvord = c('Large Urban', 'Small Urban', 'Suburban', 'Rural')
dist_urb[schools[year == 2000][order(factor(urbanicity, levels = lvord)), 
                               .SD[1L], by = district, 
                               .SDcols = c('year', 'district', 'urbanicity')],
         urbanicity := i.urbanicity, on = c('district', 'year')]

districts[dist_urb, `:=`(urbanicity = i.urbanicity, 
                         class_size = i.class_size),
          on = c('district', 'year')]

#For districts that never saw a different urbanicity code,
#  assign the unique code.
districts[districts[ , if(any(is.na(urbanicity)) & 
                          uniqueN(urbanicity, na.rm = TRUE) == 1L)
  .(urbanicity = unique(na.omit(urbanicity))), by = district],
  urbanicity := i.urbanicity, on = 'district']

#For schools that are missing urbanicity at least once
#  but only ever observe one actual urbanicity value, assign this
schools[schools[ , if(any(is.na(urbanicity)) & 
                          uniqueN(urbanicity, na.rm = TRUE) == 1L)
  .(urbanicity = unique(na.omit(urbanicity))), by = .(district, school)],
  urbanicity := i.urbanicity, on = 'district']
#For schools missing urbanicity that are in districts
#  that have only one type of observed urbanicity, assign that
schools[schools[ , if (any(is.na(urbanicity)) & 
                       uniqueN(urbanicity, na.rm = TRUE) == 1L) 
  .(urbanicity = unique(na.omit(urbanicity))), by = district],
  urbanicity := i.urbanicity, on = 'district']
#Lastly, assign district urbanicity to unmatched schools
#  (matches exactly what I would have assigned by inspection)
schools[districts[schools[is.na(urbanicity), .(year, school, district)], 
                  on = c('year', 'district')],
        urbanicity := i.urbanicity, on = c('year', 'district', 'school')]

###############################################################################
#                                Write Output                                 #
###############################################################################
fwrite(schools, wds['data'] %+% 'school_demographics.csv')
fwrite(districts, wds['data'] %+% 'district_demographics.csv')
