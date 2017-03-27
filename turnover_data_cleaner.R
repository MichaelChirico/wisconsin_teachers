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

###############################################################################
#                              Main Teacher File                              #
###############################################################################
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
                      #should exclude these earlier in the pipeline
                      !grepl('^[79]', district_fill) & nzchar(school_fill) & 
                      (quit_next | (nzchar(district_next_main) & 
                                      nzchar(school_next_main))) & 
                      !grepl('^09', school_fill) & ethnicity_main != '']

teachers = 
  unique(teachers[order(-full_time_equiv)], by = c('teacher_id', 'year'))

teachers[ , ethnicity_main :=
            factor(ethnicity_main, levels = c('White', 'Black', 'Hispanic'))]

#move_school/district_next if and only if quit_next
teachers[is.na(move_school_next), move_school_next := FALSE]
teachers[is.na(move_district_next), move_district_next := FALSE]

teachers[ , gender := factor(gender, levels = c('M', 'F'))]
teachers[ , move_within_next := move_school_next & !move_district_next]
teachers[ , stay_next := !move_district_next & !quit_next]

exp_lab = c('1-3 years', '4-6 years', '7-11 years', '12-30 years')
teachers[ , exp_split := factor(total_exp_floor)]
levels(teachers$exp_split) = setNames(list(1:3, 4:6, 7:11, 12:30), exp_lab)

###############################################################################
#                               Salary Covariates                             #
###############################################################################
payscales = 
  fread(wds['data'] %+% 'wisconsin_salary_scales_imputed.csv',
        colClasses = list(character = 'district_fill'),
        select = c('year', 'district_fill', 'tenure', 'wage_ba', 'wage_ma'))
payscales = melt(payscales[year %between% incl_rng], 
                 id.vars = c('year', 'district_fill', 'tenure'),
                 measure.vars = patterns('^wage_'),
                 variable.name = 'highest_degree', value.name = 'wage')
payscales[ , highest_degree := 4L + (highest_degree == 'wage_ma')]
payscales[ , lwage := log(wage)]

#confirmed: 1-1 mapping b/w district & cesa (even across years)
payscales[unique(teachers[ , .(district_fill, cesa)]), 
          cesa := i.cesa, on = 'district_fill']

#now add controls to generate residual/unexplained wages
districts = fread(wds['data'] %+% 'district_demographics.csv',
                  colClasses = list(character = 'district'), na.strings = '')
dist_cols = setdiff(names(districts), c('district', 'year'))

payscales[ , (dist_cols) :=
             districts[.SD, ..dist_cols, 
                       on = c('year', district = 'district_fill')]]

payscales[ , lwage_resid := 
             resid(lm(log(wage) ~ cesa + urbanicity + scale(pct_prof) + 
                        scale(pct_black) + scale(pct_hisp) + scale(pct_frl), 
                      na.action = na.exclude)),
           by = .(highest_degree, tenure)]

teachers[payscales[, log(wage[highest_degree == 4L & tenure == 1L]),
                   by = .(district_fill, year)], 
         schedule_lbase_ba_salary := i.V1, on = c('year', 'district_fill')]

#what was the scheduled wage for this teacher, 
#  as determined by the year, their district, seniority & certification?
teachers[payscales, schedule_lsalary := i.lwage,
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]
#what is the scheduled wage in the subsequent year
teachers[order(year), schedule_lsalary_next := 
           shift(schedule_lsalary, n = 1L, type = 'lead'), by = teacher_id]

#what is the unexplained part of their scheduled wage?
teachers[payscales, schedule_lsalary_resid := i.lwage_resid,
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]
teachers[order(year), schedule_lsalary_resid_next := 
           shift(schedule_lsalary_resid, n = 1L, type = 'lead'), 
         by = teacher_id]

#if the teacher moved, what would their wage have been had they stayed?
teachers[payscales[ , .(year = year - 1L, district_fill, 
                        tenure = tenure - 1L, highest_degree, 
                        lwage, lwage_resid)], 
         c('schedule_lsalary_next_cf', 'schedule_lsalary_resid_next_cf') := 
           .(i.lwage, i.lwage_resid),
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]

###############################################################################
#                           District-Level Covariates                         #
###############################################################################
teachers[ , paste0(dist_cols, '_next_d') :=
            districts[.SD, ..dist_cols, 
                      on = c('year', district = 'district_next_main')]]
districts[payscales[ , mean(lwage_resid, na.rm = TRUE), 
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

teachers[ , paste0(dist_cols, '_d') :=
            districts[.SD, ..dist_cols,
                      on = c('year', district = 'district_fill')]]
teachers[ , paste0(q_var, '_q') := 
            districts[.SD, paste0(q_var, '_q'), with = FALSE,
                      on = c('year', district = 'district_fill')]]

###############################################################################
#                            School-Level Covariates                          #
###############################################################################
schools = fread(wds['data'] %+% 'school_demographics.csv',
                colClasses = list(character = c('district', 'school')), 
                na.strings = '')

schools[ , urbanicity := 
           factor(urbanicity, levels = 
                    c('Large Urban', 'Small Urban', 'Suburban', 'Rural'))]

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
