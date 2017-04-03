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
library(sp)
library(rgeos)

wds = c(data = '/media/data_drive/wisconsin/')

###############################################################################
#                              Main Teacher File                              #
###############################################################################
incl_cols = c('year', 'cesa', 'district_fill', 'school_fill', 
              'teacher_id', 'highest_degree', 'total_exp_floor',
              'full_time_equiv', 'gender', 'ethnicity_main',
              'move_district_next', 'move_school_next', 'quit_next',
              #only needed for data cleaning
              'position_code', 'area', 'district_work_type', 
              'months_employed', 'days_of_contract', 'category')
colClasses = with(
  fread(wds['data'] %+% 
          'wisconsin_teacher_data_full_colClass.csv', header = FALSE),
  setNames(V2[V1 %in% incl_cols], V1[V1 %in% incl_cols]))

teachers = 
  fread(wds['data'] %+% "wisconsin_teacher_data_full.csv",
        select = incl_cols, colClasses = colClasses,
        key = 'teacher_id,year,district_fill,school_fill')

#subset to focus timeframe, eliminate teachers outside 
incl_yrs = 2000:2010
incl_rng = range(incl_yrs)
#pad right end by 1 since defining subsequent district
#  within this file (o/w force all final year obs. to quit)
teachers = teachers[year %between% (incl_rng + c(0L, 1L))]

N_full = uniqueN(teachers$teacher_id)

#position_code: 53 = full-time teacher
#  mostly eliminating support staff and substitutes;
#  take care to eliminate any teacher who was not a
#  full time teacher in every year they appear -- 
#  definitions were getting hazy when we allowed
#  for teachers to transition in and out of
#  "soft retirement" by going into substitute teaching.
#  Typically, when teachers do so, they switch to being
#  hired by the district instead of a specific school.
#  Whether this is considered "changing schools" is
#  open to interpretation, so we just eliminate any such
#  teachers. We also lose teachers who start their
#  career as substitutes by doing so. Search for
#  commits around this for some exploration of some
#  alternative definitions that were unsatisfactory:
#  13d76a1fdae028df18300f09d8b85ff5799dea1b
teachers = 
  teachers[teachers[ , sum(position_code == '53') >= 1L,
                     by = .(teacher_id, year)
                     ][ , all(V1), by = teacher_id][(V1)],
           on = 'teacher_id']
  
#area: 0050 (all-purpose elementary teachers)
#      0300 (English, typically middle/high school)
#      0400 (Mathematics, typically mid/high school)
#  potential to include for robustness (desc. order of frequency):
#    0550(Art)/0701(Social Studies)/0620(General Science)/
#    0725(History)/0316(Reading)/0605(Biology)/
#    0910(Health)/0610(Chemistry)
teachers = teachers[area %in% c('0050', '0300', '0400')]

#highest_degree: 4 = BA, 5 = MA
teachers = teachers[highest_degree %in% 4L:5L]

#district_fill: 7xxx and 9xxx are positions at a CESA
#  or at an otherwise exceptional school (mental institution, etc.)
teachers = teachers[!grepl('^[79]', district_fill)]

#school_fill: should be assigned to an actual school
#  09xx are district-wide/multiple-school appointments
teachers = teachers[nzchar(school_fill) & !grepl('^09', school_fill)]

#50 years seems a reasonable enough cap
teachers = teachers[total_exp_floor <= 50L & total_exp_floor > 0]
             
#district_work_type: 04 are regular public schools
teachers = teachers[district_work_type %in% c('04', '49')]

#months_employed / days_of_contract
#  Through 2003-04, months used, days thereafter
#    *Eliminate those who never worked >=8.75 months
#    *Eliminate those who never worked >=175 days
teachers = teachers[(months_employed %between% c(875, 1050) | year > 2003) &
                      (days_of_contract %between% c(175, 195) | year <= 2003)]

#category: 1 are professional, regular education teachers
teachers = teachers[category == "1"]

# #eliminate teachers with FTE <= 80
# teachers = teachers[full_time_equiv >= 80]

N_subset_I = uniqueN(teachers$teacher_id)

#ethnicity_main / gender: eliminate teachers for whom this is unstable,
#  and eliminate the small number of non-white/hispanic/black teachers
pct_nonwhite = teachers[ , round(100*mean(ethnicity_main == ''))]
teachers = teachers[ , if (uniqueN(ethnicity_main) == 1L &&
                           !any(ethnicity_main == '') &&
                           uniqueN(gender) == 1L) .SD, by = teacher_id]

N_subset_II = uniqueN(teachers$teacher_id)

#eliminate teachers for whom it's complicated to
#  determine whether they actually switched districts
#  because they're hired at more than one school or
#  district in a given year (only district binds)
teachers = teachers[ , if (uniqueN(district_fill) == 1L) .SD, 
                     by = .(teacher_id, year)]

#eliminate multiple positions for a teacher by choosing the
#  one with the highest intensity (highest FTE)
teachers = 
  unique(teachers[order(-full_time_equiv)], by = c('teacher_id', 'year'))
setkey(teachers, teacher_id, year)

pct_white = teachers[ , round(100*mean(ethnicity_main == 'White'))]

teachers[ , ethnicity_main :=
            factor(ethnicity_main, levels = c('White', 'Black', 'Hispanic'))]
teachers[ , nonwhite := ethnicity_main != 'White']

#define movement indicators
teachers[ , school_next :=
            shift(school_fill, 1L, type = 'lead'), by = teacher_id]
teachers[ , district_next :=
            shift(district_fill, 1L, type = 'lead'), by = teacher_id]

#move_school/district_next missing if and only if quit_next
teachers[is.na(move_school_next), move_school_next := FALSE]
teachers[is.na(move_district_next), move_district_next := FALSE]

#now eliminate final-year teachers
teachers = teachers[year <= incl_rng[2L]]

teachers[ , gender := factor(gender, levels = c('M', 'F'))]
teachers[ , move_within_next := move_school_next & !move_district_next]
teachers[ , stay_next := !move_district_next & !quit_next]
teachers[ , leave_next := !stay_next]

exp_lab = c('1-3 years', '4-6 years', '7-11 years', '12-30 years', '>30 years')
teachers[ , exp_split := factor(total_exp_floor)]
levels(teachers$exp_split) = 
  setNames(list(1:3, 4:6, 7:11, 12:30, 31:50), exp_lab)

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
#don't allow urbanicity to vary by year
districts[order(year), urbanicity := urbanicity[1L], by = district]
dist_cols = setdiff(names(districts), c('district', 'year'))

payscales[ , (dist_cols) :=
             districts[.SD, ..dist_cols, 
                       on = c('year', district = 'district_fill')]]

pr = c('_pred', '_resid')
payscales[ , paste0('lwage', pr) := {
  reg = lm(log(wage) ~ cesa + urbanicity + pct_prof + 
             pct_black + pct_hisp + pct_frl) 
  .(predict(reg, .SD), resid(reg))
}, by = .(highest_degree, tenure)]

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
teachers[payscales, paste0('schedule_lsalary', pr) := 
           .(i.lwage_pred, i.lwage_resid),
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]
teachers[order(year), paste0('schedule_lsalary', pr, '_next') := 
           .(shift(schedule_lsalary_resid, n = 1L, type = 'lead'),
             shift(schedule_lsalary_resid, n = 1L, type = 'lead')), 
         by = teacher_id]

#if the teacher moved, what would their wage have been had they stayed?
teachers[payscales[ , .(year = year - 1L, district_fill, 
                        tenure = tenure - 1L, highest_degree, 
                        lwage, lwage_pred, lwage_resid)], 
         paste0('schedule_lsalary', c('', pr), '_next_cf') := 
           .(i.lwage, i.lwage_pred, i.lwage_resid),
         on = c('year', 'district_fill', 
                total_exp_floor = 'tenure', 'highest_degree')]

###############################################################################
#                           District-Level Covariates                         #
###############################################################################
teachers[ , paste0(dist_cols, '_next_d') :=
            districts[.SD, ..dist_cols, 
                      on = c('year', district = 'district_next')]]
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
#don't allow urbanicity to vary by year
schools[order(year), urbanicity := urbanicity[1L], by = .(district, school)]

urb_lev = c('Large Urban', 'Small Urban', 'Suburban', 'Rural')
schools[ , urbanicity := 
           factor(urbanicity, levels = urb_lev)]

teachers[schools, 
         `:=`(pct_prof_s = i.pct_prof, pct_hisp_s = i.pct_hisp, 
              pct_black_s = i.pct_black, pct_frl_s = i.pct_frl,
              urbanicity_s = i.urbanicity), 
         on = c('year', district_fill = 'district', school_fill = 'school')]

teachers[schools, 
         `:=`(pct_prof_next_s = i.pct_prof, pct_hisp_next_s = i.pct_hisp, 
              pct_black_next_s = i.pct_black, pct_frl_next_s = i.pct_frl,
              urbanicity_s_next = i.urbanicity), 
         on = c('year', district_next = 'district', school_next = 'school')]

###############################################################################
#                             Assorted Quantities                             #
###############################################################################

# Urbanicity distribution of Texas vs. Wisconsin, 2010
wi_ti = fread('/media/data_drive/common_core/district/universe_2009_10_2a.txt',
              select = c('FIPST', 'ULOCAL09'), col.name = c('fipst', 'urb'))
wi_ti = wi_ti[fipst %in% c('48', '55')]
urban_map = data.table(
  urb = paste0(c(11:13, 21:23, 31:33, 41:43)),
  size = factor(rep(urb_lev, c(1L, 2L, 3L, 6L)), levels = urb_lev)
)
wi_ti[urban_map, 'Community Type' := i.size, on = 'urb']
wi_ti[fipst == '48', State := 'Texas']
wi_ti[fipst == '55', State := 'Wisconsin']

#inter-school distance matrix
school_xy = fread(paste0('/media/data_drive/common_core/school/',
                         'CCD_universe_school-level_2007-08.txt'),
                  select = c('FIPST', 'STID07', 'SEASCH07', 
                             'LATCOD07', 'LONCOD07'),
                  col.names = c('fipst', 'district', 'school',
                                'latitude', 'longitude'))
school_xy = school_xy[fipst == '55']

school_sp = SpatialPoints(school_xy[ , cbind(longitude, latitude)],
                          proj4string = CRS('+init=epsg:4326'))
school_sp = spTransform(school_sp, CRS('+init=esri:102753'))

dist_DT = melt(as.data.table(gDistance(school_sp, byid = TRUE),
                             keep.rownames = 'from'),
               id.vars = 'from', value.name = 'distance', 
               variable.name = 'to')
school_xy[ , ID := paste0(.I)]
dist_DT[school_xy, c('district_from', 'school_from') :=
          .(i.district, i.school), on = c(from = 'ID')]
dist_DT[school_xy, c('district_to', 'school_to') :=
          .(i.district, i.school), on = c(to = 'ID')]
#convert to miles
dist_DT[ , distance := distance/5280]

#join to teacher data
teachers[dist_DT, distance_moved := i.distance,
         on = c(district_fill = 'district_from',
                school_fill = 'school_from',
                district_next = 'district_to',
                school_next = 'school_to')]

## @knitr stop_read

fwrite(teachers, wds['data'] %+% 'teacher_turnover_data.csv')
