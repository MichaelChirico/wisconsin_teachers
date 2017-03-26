## @knitr start_read
#Wisconsin Teacher Project
#Turnover Paper Data Cleaning
#Michael Chirico
#March 24, 2017

###############################################################################
#                   Package Setup & Convenient Functions                      #
###############################################################################
library(data.table)
library(Hmisc) # for weighted quantiles
library(funchir)
library(texreg)

wds = c(data = '/media/data_drive/wisconsin/')

#import main teacher file
incl_cols = c('year', 'cesa', 'district_fill', 'school_fill', 
              'teacher_id', 'highest_degree', 'total_exp_floor',
              'district_next_main', 'school_next_main',
              'full_time_equiv', 'gender', 'ethnicity_main',
              'move_school_next', 'move_district_next', 'quit_next')
colClasses = with(
  fread(wds['data'] %+% 
          'wisconsin_teacher_data_matched_colClass.csv', header = FALSE),
  setNames(V2[V1 %in% incl_cols], V1[V1 %in% incl_cols]))

teachers = fread(wds['data'] %+% 'wisconsin_teacher_data_matched.csv',
                 select = incl_cols, colClasses = colClasses)

#subset to focus timeframe, eliminate teachers outside 
incl_yrs = 2000:2008
incl_rng = range(incl_yrs)
teachers = teachers[year %between% incl_rng & highest_degree %in% 4:5 & 
                      #should exclude this earlier in the pipeline
                      !grepl('^7', district_fill)]

teachers = 
  unique(teachers[order(-full_time_equiv)], by = c('teacher_id', 'year'))

#move_school/district_next if and only if quit_next
teachers[is.na(move_school_next), move_school_next := FALSE]
teachers[is.na(move_district_next), move_district_next := FALSE]

teachers[ , gender := factor(gender, levels = c('M', 'F'))]
teachers[ , move_within_next := move_school_next & !move_district_next]
teachers[ , stay_next := !move_district_next & !quit_next]

exp_lab = c('1-3 years', '4-6 years', '7-11 years', '12-30 years')
teachers[ , exp_split := factor(total_exp_floor)]
levels(teachers$exp_split) = setNames(list(1:3, 4:6, 7:11, 12:30), exp_lab)

districts = fread(wds['data'] %+% 'district_demographics.csv',
                  colClasses = list(character = 'district'), na.strings = '')

#In 2006 all urbanicity is missing. For districts that never
#  saw a different urbanicity code, assign the unique code.
districts[districts[ , if(any(is.na(urbanicity)) & 
                          uniqueN(urbanicity, na.rm = TRUE) == 1L)
  .(urbanicity = unique(na.omit(urbanicity))), by = district],
  urbanicity := i.urbanicity, on = 'district']
#If in 2005 & 2007, there was only one value of urbanicity
#  in a district missing 2006, assign the observed constant value to 2006
districts[districts[year %in% 2005:2007, 
                    if(any(is.na(urbanicity)) & 
                       uniqueN(urbanicity, na.rm = TRUE) == 1L)
                      .(year = 2006L, 
                        urbanicity = unique(na.omit(urbanicity))), 
                    by = district],
  urbanicity := i.urbanicity, on = c('district', 'year')]
#If more than 5 other years featured a single urbanicity,
#  assign that to 2006
districts[districts[year != 2006 & district %in% 
                      districts[is.na(urbanicity), district], .N, 
                    by = .(district, urbanicity)
                    ][N>5, .(year = 2006L, district, urbanicity)],
          urbanicity := i.urbanicity, on = c('district', 'year')]
#Just assign 2005's value for the rest
districts[districts[year == 2005 & district %in% 
                      districts[is.na(urbanicity), district],
                    .(year = 2006L, district, urbanicity)],
          urbanicity := i.urbanicity, on = c('district', 'year')]

schools = fread(wds['data'] %+% 'school_demographics.csv',
                colClasses = list(character = c('district', 'school')), 
                na.strings = '')

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

schools[ , urbanicity := 
           factor(urbanicity, levels = 
                    c('Large Urban', 'Small Urban', 'Suburban', 'Rural'))]
teachers[schools, urbanicity_s := i.urbanicity, 
         on = c(district_fill = 'district', school_fill = 'school', 'year')]

payscales = 
  fread(wds['data'] %+% 'wisconsin_salary_scales_imputed.csv',
        colClasses = list(character = 'district_fill'),
        select = c('year', 'district_fill', 'tenure', 'wage_ba', 'wage_ma'))
payscales = payscales[year %between% incl_rng]

payscales[districts, `:=`(n_students = i.n_students, pct_hisp = i.pct_hisp,
                          pct_black = i.pct_black, pct_frl = i.pct_frl,
                          pct_prof = i.pct_prof,
                          urbanicity = i.urbanicity),
          on = c(district_fill = 'district', 'year')]
#confirmed: 1-1 mapping b/w district & cesa (even across years)
cesa_map = unique(teachers[ , .(district_fill, cesa)])
payscales[cesa_map, cesa := cesa, on = 'district_fill']
payscales[ , lwage_ba_resid := 
             resid(lm(log(wage_ba) ~ cesa + urbanicity + scale(pct_prof) + 
                        pct_black + pct_hisp + pct_frl, 
                      na.action = na.exclude)),
           by = tenure]
payscales[ , lwage_ma_resid := 
             resid(lm(log(wage_ma) ~ cesa + urbanicity + scale(pct_prof) + 
                        pct_black + pct_hisp + pct_frl, 
                      na.action = na.exclude)),
           by = tenure]

teachers[payscales[ , log(wage_ba[1L]), by = .(district_fill, year)], 
         schedule_lbase_ba_salary := i.V1, on = c('year', 'district_fill')]

teachers[melt(payscales, id.vars = c('year', 'district_fill', 'tenure'),
              measure.vars = c('wage_ba', 'wage_ma')
              )[ , highest_degree := 4L + (variable == 'wage_ma')], 
         schedule_lsalary := log(i.value),
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]
teachers[order(year), schedule_lsalary_next := 
           shift(schedule_lsalary, n = 1, type = 'lead'), by = teacher_id]

teachers[melt(payscales, id.vars = c('year', 'district_fill', 'tenure'),
              measure.vars = c('lwage_ba_resid', 'lwage_ma_resid')
              )[ , highest_degree := 4L + (variable == 'lwage_ma_resid')], 
         schedule_lsalary_resid := i.value,
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]
teachers[order(year), schedule_lsalary_resid_next := 
           shift(schedule_lsalary_resid, n = 1, type = 'lead'), 
         by = teacher_id]

teachers[melt(payscales, id.vars = c('year', 'district_fill', 'tenure'),
              measure.vars = c('wage_ba', 'wage_ma')
              )[ , c('highest_degree', 'tenure') := 
                   #increment tenure to get counterfactual subsequent wage
                   #  by matching, e.g, experience 4 to 
                   #  experience 5 in the table
                   .(4L + (variable == 'wage_ma'), tenure + 1L)], 
         schedule_lsalary_next_cf := log(i.value),
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]

teachers[melt(payscales, id.vars = c('year', 'district_fill', 'tenure'),
              measure.vars = c('lwage_ba_resid', 'lwage_ma_resid')
              )[ , c('highest_degree', 'tenure') := 
                   .(4L + (variable == 'lwage_ma_resid'), tenure + 1L)], 
         schedule_lsalary_resid_next_cf := i.value,
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]

districts[payscales[ , mean(lwage_ba_resid + lwage_ma_resid, na.rm = TRUE), 
                     by = .(district_fill, year)], 
          lwage_resid_avg := i.V1, on = c(district = 'district_fill', 'year')]
districts[teachers[ , .N, by = .(district_fill, year)],
          n_teachers := i.N, on = c(district = 'district_fill', 'year')]

q_var = c('lwage_resid_avg', 'pct_prof', 'pct_frl', 'pct_black', 'pct_hisp')
districts[ , paste0(q_var, '_q') := lapply(.SD, function(x) {
  brk = wtd.quantile(x, 0:4/4, weights = n_teachers, 
                     normwt = TRUE, na.rm = TRUE)
  f = cut(x, breaks = brk, labels = 1:4, right = FALSE, include.lowest = TRUE)
  factor(f, levels = 4:1, labels = c('Highest', '3rd', '2nd', 'Lowest'))
}), .SDcols = q_var]

from_dist = c('pct_prof', 'pct_hisp', 'pct_black', 'pct_frl',
              'urbanicity', 'n_students', 'class_size',
              paste0(q_var, '_q'))
teachers[ , (from_dist) := 
            districts[.SD, ..from_dist,
                      on = c('year', district = 'district_fill')]]
setnames(teachers, 'urbanicity', 'urbanicity_d')

teachers[districts, 
         `:=`(pct_prof_next = i.pct_prof, pct_hisp_next = i.pct_hisp, 
              pct_black_next = i.pct_black, pct_frl_next = i.pct_frl, 
              urbanicity_d_next = urbanicity), 
         on = c('year', district_next_main = 'district')]

teachers[schools, 
         `:=`(pct_prof_s = i.pct_prof, pct_hisp_s = i.pct_hisp, 
              pct_black_s = i.pct_black, pct_frl_s = i.pct_frl,
              urbanicity_s = i.urbanicity), 
         on = c('year', district_fill = 'district', school_fill = 'school')]

teachers[schools, 
         `:=`(pct_prof_next_s = i.pct_prof, pct_hisp_next_s = i.pct_hisp, 
              pct_black_next_s = i.pct_black, pct_frl_next_s = i.pct_frl,
              urbanicity_s_next = i.urbanicity), 
         on = c('year', district_next_main = 'district',
                school_next_main = 'school')]

## @knitr stop_read

fwrite(teachers, wds['data'] %+% 'teacher_turnover_data.csv')
