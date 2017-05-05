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
library(cobs)
library(parallel)
library(quantmod)
`%+%` = funchir::`%+%` #stupid ggplot2

wds = c(data = '/media/data_drive/wisconsin/')

###############################################################################
#                              Main Teacher File                              #
###############################################################################
incl_cols = c('year', 'cesa', 'district_fill', 'school_fill', 'salary',
              'teacher_id', 'highest_degree', 'total_exp_floor', 'fringe',
              'full_time_equiv', 'gender', 'ethnicity_main', 'quit_next',
              #only needed for data cleaning
              'position_code', 'area', 'district_work_type', 
              'months_employed', 'days_of_contract', 'category')
colClasses = with(
  fread(wds['data'] %+% 
          'wisconsin_teacher_data_full_colClass.csv', header = FALSE),
  setNames(V2[V1 %in% incl_cols], V1[V1 %in% incl_cols]))

data_path = wds['data'] %+% "wisconsin_teacher_data_full.csv"
teachers = 
  fread(data_path, select = incl_cols, colClasses = colClasses,
        key = 'teacher_id,year,district_fill,school_fill')

incl_yrs = setNames(nm = 2000:2010)
incl_rng = range(incl_yrs)
N_full = teachers[year %between% incl_rng, uniqueN(teacher_id)]

#eliminate multiple positions for a teacher by choosing the
#  one with the highest intensity (highest FTE)
teachers = 
  unique(teachers[order(-full_time_equiv)], by = c('teacher_id', 'year'))
setkey(teachers, teacher_id, year)

#copy of the data to be used to create payscales; key differences:
#  - less restrictive on position_code: include teachers
#    who sometimes hold positions besides teacher
#  - less restrictive on area: include most regular subject areas
#  - less restrictive on ethnicity/gender stability
#  - more restrictive on observed pay -- require salary + fringe > 10k
#  - more restrictive on small districts: see flags below - 
#    degree, node, teach, sal, and frn count flags
teachers_ps = copy(teachers)

#position_code: 53 = full-time teacher
#Only want full-time teachers
#  (eliminate 1,924,483 = 46%)
#   *Mostly categorized as:
#   "Other Support Staff" [98], "Program Aide" [97],
#   "Short-term Substitute Teacher" [43], or in major
#   administrative/staff positions, e.g.
#   Athletic Coach [77]/Program Coordinator [64]/
#   Principal [51]/Speech-Language Pathologist [84]/
#   Plant Maintenance Personnel [72]/Guidance Counselor [54]/
#   Cafeteria Worker [73]/Clerical Support Staff [68])
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
  teachers[.(teachers[ , all(position_code == '53'),
                       by = teacher_id]$teacher_id)]
teachers_ps = teachers_ps[position_code == '53']
  
#define movement indicators in this file. do 
#  districts first to cover the case when
#  a teacher moves to a new school with the same id #
#  Two older approaches that were unsatisfactory:
#  1) Using district_next_main and move_district_next as
#     defined in teacher_match_and_clean --
#     was causing issues with late-career
#     teachers who switch frequently among
#     positions. Here, we first eliminate
#     any teachers who showed up as substitutes
#     at some point, which seems to solve that problem
#  2) Same as here, but define these indicators later in
#     the file -- this was leading to some teachers
#     who took a break from full-time teaching but
#     ultimately coming back full-time at the same
#     district appear to "switch" to the same district,
teachers[ , district_next :=
            shift(district_fill, 1L, type = 'lead'), by = teacher_id]
teachers[ , move_district_next := 
            #if school_next is missing, it's the last observation
            #  (i.e., quit_next is TRUE)
            !(district_fill == district_next | is.na(district_next))]
teachers[ , school_next :=
            shift(school_fill, 1L, type = 'lead'), by = teacher_id]
teachers[ , move_school_next := 
            #move district implies move school
            move_district_next |
            !(school_fill == school_next | is.na(school_next))]

#reset district switch indicator for exogenous switchers:
#  see dpi.wi.gov/sites/default/files/imce/sms/doc/rg_sdnamechanges.doc
#  1) Glidden School District (2205) and Park Falls School District (4242)
#     merge from 2009-10 to form Chequamegon School District (1071)
#  2) Weyerhaeuser School District (6410) and 
#     Chetek School District (1078) merge from 2010-11 to form 
#     Chetek-Weyerhaueser Area School District (1080)
#  3) Trevor Grade School District (5061) and
#     Wilmot Grade School District (5075)  merge from 2006-07 to form
#     Trevor-Wilmot Consolidated School District (5780)
teachers[year == 2009 & district_fill %in% c('2205', '4242') & 
           district_next == '1071', 
         #can do better than automatically assuming teachers don't
         #  move schools (some do, it seems), but there's no clear
         #  mapping -- some junior highs became middle schools,
         #  etc., so still a lot of exogenous school switching
         paste0('move_', c('school', 'district'), '_next') := FALSE]

teachers[year == 2010 & district_fill %in% c('6410', '1078') & 
           district_next == '1080',
         paste0('move_', c('school', 'district'), '_next') := FALSE]

teachers[year == 2006 & district_fill %in% c('5061', '5075') & 
           district_next == '5780',
         paste0('move_', c('school', 'district'), '_next') := FALSE]

#subset to focus timeframe, eliminate teachers outside 
#  do this after defining switching to allow for any sort of
#  gap year situation
teachers = teachers[year %between% incl_rng]
teachers_ps = teachers_ps[year %between% incl_rng]

#area: 0050 (all-purpose elementary teachers)
#      0300 (English, typically middle/high school)
#      0400 (Mathematics, typically mid/high school)
#  potential to include for robustness (desc. order of frequency):
#    0550(Art)/0701(Social Studies)/0620(General Science)/
#    0725(History)/0316(Reading)/0605(Biology)/
#    0910(Health)/0610(Chemistry)
#  Most common excluded subject areas from payscales data area:
#  [0910] - Health
#  [0002] - Academic Support - Teachers
#  [0940] - Academic Support - Non-Special Education Students
#  [0014] - Gifted & Talented
#  [0935] - At-Risk Tutor
#  [0001] - Non-teaching Time
#  [0952] - Alternative Education
teachers = teachers[area %in% c('0050', '0300', '0400')]
teachers_ps = teachers_ps[substring(area, 2L, 2L) %in% 2:7 | area == '0050']

#highest_degree: 4 = BA, 5 = MA
teachers = teachers[highest_degree %in% 4L:5L]
teachers_ps = teachers_ps[highest_degree %in% 4L:5L]

#district_fill: 7xxx and 9xxx are positions at a CESA
#  or at an otherwise exceptional school (mental institution, etc.)
teachers = teachers[!grepl('^[79]', district_fill)]
teachers_ps = teachers_ps[!grepl('^[79]', district_fill)]

#school_fill: should be assigned to an actual school
#  09xx are district-wide/multiple-school appointments
#  ** only occurred through 2003-04 **
teachers = teachers[nzchar(school_fill) & !grepl('^09', school_fill)]
teachers_ps = teachers_ps[nzchar(school_fill) & !grepl('^09', school_fill)]

#35 years seems a reasonable enough cap (need to be >30 to match HKR)
#Restrict focus to total experience (rounded down) between 1 & 35;
#  Total exp = 0 appears to be some sort of error? Very rare anyway.
#  There are nontrivial #s of teachers beyond 35 years, but 35 seems
#  as good a place as any (see robustness checks). box-plotting
#  pay vs. total experience, we see the series start to change around
#  25 (narrowing of IQR), suggesting some selection effects starting
#  to become important.
# ** surplus of teachers with experience < 1 in 2003-04 **
max_exp = 35L
teachers = teachers[total_exp_floor > 0 & total_exp_floor <= max_exp]
teachers_ps = teachers_ps[total_exp_floor > 0 & total_exp_floor <= max_exp]
             
#district_work_type: 04 are regular public schools
# ** may not actually eliminate any teachers **
teachers = teachers[district_work_type %in% c('04', '49')]
teachers_ps = teachers_ps[district_work_type %in% c('04', '49')]

#months_employed / days_of_contract
#Despite efforts to focus on full-time teachers, some,
#  as per the months_employed construct (used pre-2003)
#  or the days_of_contract construct (used since), still enter
#  with what must be less than full-time contracts;
#  examining, e.g., boxplots of salary against this variable
#  suggest there is still a systemic deficiency in pay for
#  those working less than the "full year"; this increasing
#  relationship appears to stabilize between 8.75 and 10.5 months 
#  (or 175-195 days contracted), so eliminate those outside these bounds
#  (eliminate 14,284 = 1.4%)
#    *Eliminate those who never worked >=8.75 months
#    *Eliminate those who never worked >=175 days
teachers = teachers[(months_employed >= 875 | year > 2003) &
                      (days_of_contract >= 175 | year <= 2003)]
teachers_ps = teachers_ps[(months_employed >= 875 | year > 2003) &
                            (days_of_contract >= 175 | year <= 2003)]

#category: 1 are professional, regular education teachers
#  vast majority dropped are 0: professional, special education
teachers = teachers[category == "1"]
teachers_ps = teachers_ps[category == "1"]

#eliminate teachers with FTE <= 80
teachers = teachers[full_time_equiv >= 80]
teachers_ps = teachers_ps[full_time_equiv >= 80]

N_subset_I = uniqueN(teachers$teacher_id)

#ethnicity_main / gender: eliminate teachers for whom this is unstable,
#  and eliminate the small number of non-white/hispanic/black teachers
pct_nonwhite = teachers[ , round(100*mean(ethnicity_main == ''))]
teachers = teachers[ , if (uniqueN(ethnicity_main) == 1L &&
                           !any(ethnicity_main == '') &&
                           uniqueN(gender) == 1L) .SD, by = teacher_id]

N_subset_II = uniqueN(teachers$teacher_id)

pct_white = teachers[ , round(100*mean(ethnicity_main == 'White'))]

#further restrictions for the payscales data
#Seems unlikely a full-time teacher should be
#  making <10k in a year, so this should be eliminated as erroneous
teachers_ps = teachers_ps[salary + fringe >= 10000]

#Now, eliminate schools with insufficient coverage
yrdsdg = c("year", "district_fill", "highest_degree")
yrds = c('year', 'district_fill')
setkeyv(teachers_ps, yrdsdg)
setindexv(teachers_ps, yrds)
          
#In order to use the paired approach to fitting, need
#  both degree scales represented
teachers_ps[ , degree_count_flag := uniqueN(highest_degree) != 2L, by = yrds]

#Can't interpolate if there are only 2 or 3
#  unique experience cells represented
teachers_ps[ , node_count_flag := uniqueN(total_exp_floor) < 7L, by = yrdsdg]

#Nor if there are too few teachers
teachers_ps[ , teach_count_flag := .N < 20L, by = yrdsdg]

#Also troublesome when there is little variation in salaries like so:
teachers_ps[ , sal_count_flag := uniqueN(salary) < 5L, by = yrdsdg]
teachers_ps[ , frn_count_flag := uniqueN(fringe) < 5L, by = yrdsdg]

# Impose the flag on both certification tracks whenever partially violated
flg = c('node_count', 'teach_count', 'sal_count', 'frn_count') %+% '_flag'
teachers_ps[ , (flg) := lapply(.SD, any), by = yrds, .SDcols = flg]

#Discard any variables hit by a flag
teachers_ps = 
  teachers_ps[!(degree_count_flag | node_count_flag | teach_count_flag |
                sal_count_flag | frn_count_flag)]

#Discard variables not necessary for interpolation
n_teachers = teachers_ps[ , uniqueN(teacher_id)]
teachers_ps = 
  teachers_ps[, .(year, district_fill, highest_degree, 
                total_exp_floor, salary, fringe)]

#some additional derived variables we'll use for the main data
teachers[ , ethnicity_main :=
            factor(ethnicity_main, levels = c('White', 'Black', 'Hispanic'))]
teachers[ , nonwhite := ethnicity_main != 'White']
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
## @knitr interpolator
setindex(teachers_ps, year, district_fill)

#fast prediction - to cut out predict.cobs overhead
fpr = function(cb) with(cb, cobs:::.splValue(2, knots, coef, zs))

#when predicting beyond the range of data, cobs no longer enforces
#  the monotonicity constraint; so extend linearly for all later points
## _could also do this to cover very low predicted wages at very low
##  levels of tenure for sparsely-populated districts, but the
##  implications of this for a model are not as dire_
linear_extend = function(x, idx) {
  x[idx] = x[idx[1L] - 1L] + diff(x[idx[1L] - 2L:1L]) * seq_len(length(idx))
  x
}

#constants
zs = seq_len(max_exp)
## ** ? somehow having a very high top-end constraint breaks cobs
# #salary @ 0 >= 0; salary @ 30 at most sal_max
# end_cons = rbind(c(1, 0, 0),
#                  c(-1, 30, sal_max))
end_cons = cbind(1, 0, 0)

cl <- makeCluster(detectCores())
clusterExport(cl, c('teachers_ps', 'end_cons', 'fpr', 'zs', 'incl_yrs'),
             envir = environment())
clusterEvalQ(cl, {library(data.table); library(cobs)})
payscales = rbindlist(mclapply(incl_yrs, function(yr) {
  teachers_ps[.(yr), {
    ba = highest_degree == 4L
    wage_ba = fpr(cobs(
      total_exp_floor[ba], salary[ba], print.warn = FALSE,
      maxiter = 5000L, print.mesg = FALSE,
      keep.data = FALSE, keep.x.ps = FALSE,
      #lambda selection led to strange fits and caused errors,
      #  but should in principle be specifying lambda = -1
      constraint = c('increase', 'concave'),
      knots.add = TRUE, repeat.delete.add = TRUE,
      pointwise = end_cons
    ))
    #using .01 -- numerical issues cause
    #  linear_extend logic to fail otherwise
    if (length(idx <- which(diff(wage_ba) < -1e-2) + 1L)) {
      wage_ba = linear_extend(wage_ba, idx)
    }
    #some regressions support the hypothesis
    #  that fringe benefits are monotonically
    #  increasing with tenure, even though
    #  there's not strong theoretical support for this
    fringe_ba = fpr(cobs(
      total_exp_floor[ba], fringe[ba], print.warn = FALSE,
      maxiter = 5000L, print.mesg = FALSE,
      keep.data = FALSE, keep.x.ps = FALSE,
      #despite concavity imposition, which is
      #  evidently valid in most cases, some schedules are
      #  returned linear, and this appears to be valid as well --
      #  see for example of seemingly linear schedules:
      #  District: 2625, Years: 2010-2013
      #  District: 2541, Years: 2014
      #  District: 4557, Years: 2012-2013
      constraint = c('increase', 'concave'),
      knots.add = TRUE, repeat.delete.add = TRUE,
      pointwise = end_cons
    ))
    if (length(idx <- which(diff(fringe_ba) < -1e-2) + 1L)) {
      fringe_ba = linear_extend(fringe_ba, idx)
    }
    #some regressions support the hypothesis
    #  that the MA vs. BA premium (difference, not ratio)
    #  is increasing with tenure; this is in line
    #  with the finding that many MA lanes are simply
    #  fixed percentage premiums over the BA lane,
    #  so that the raw difference will increase as BA does
    premium_ma = 
      fpr(cobs(
        total_exp_floor[!ba], 
        #wage premium is salary (for MA holders) minus
        #  wage_ba corresponding to their experience level
        #  (don't need to adjust indexing since, e.g.,
        #   total_exp_floor == 2 means we need wage_ba[2])
        salary[!ba] - wage_ba[total_exp_floor[!ba]], 
        print.warn = FALSE,  maxiter = 5000,
        keep.data = FALSE, keep.x.ps = FALSE,
        print.mesg = FALSE, constraint = 'increase',
        knots.add = TRUE, repeat.delete.add = TRUE, 
        pointwise = end_cons
      ))
    wage_ma = wage_ba + premium_ma
    fringe_ma = fpr(cobs(
      total_exp_floor[!ba], fringe[!ba], print.warn = FALSE,
      maxiter = 5000, print.mesg = FALSE,
      keep.data = FALSE, keep.x.ps = FALSE,
      constraint = c('increase', 'concave'),
      knots.add = TRUE, repeat.delete.add = TRUE,
      pointwise = end_cons
    ))
    if (length(idx <- which(diff(fringe_ma) < -1e-2) + 1L)) {
      fringe_ma = linear_extend(fringe_ma, idx)
    }
    .(tenure = zs, wage_ba = wage_ba, fringe_ba = fringe_ba, 
      wage_ma = wage_ma, fringe_ma = fringe_ma)}, 
    #note: this approach leads year to be assigned as a character
    by = district_fill]}), idcol = 'year')
stopCluster(cl)

payscales[ , year := as.integer(year)]

#post-fit clean-up: real-dollar wages
#provide deflated wage data
#note that data values are recorded at the end
#  of September in each academic year, so,
#  since I index AY by spring year, we use the
#  'prior' year's CPI in October as the base
#Also perpetual reminder: YYstaff.txt is the data for
#  the (YY-1)-YY academic year
oct1s = as.Date(paste0(incl_yrs - 1L, '-10-01'))
oct_cpi = suppressWarnings(
  getSymbols("CPIAUCSL", src = "FRED", auto.assign = FALSE)[oct1s]
)
inflation_index = 
  data.table(year = incl_yrs,
             #coredata returns the "column" as a matrix, so drop it
             index = drop(coredata(oct_cpi))/oct_cpi[[length(oct_cpi)]])
payscales[inflation_index,
          `:=`(wage_ba_real = wage_ba/i.index,
               wage_ma_real = wage_ma/i.index,
               fringe_ba_real = fringe_ba/i.index,
               fringe_ma_real = fringe_ma/i.index),
          on = "year"]

#intermediate output to facilitate debugging
fwrite(payscales, wds['data'] %+% "wisconsin_salary_scales_imputed.csv")

payscales = melt(payscales, id.vars = c('year', 'district_fill', 'tenure'),
                 measure.vars = patterns('^wage_[bm]a$'),
                 variable.name = 'highest_degree', value.name = 'wage')
payscales[ , highest_degree := 4L + (highest_degree == 'wage_ma')]
payscales[ , lwage := log(wage)]

#confirmed: 1-1 mapping b/w district & cesa (even across years)
payscales[unique(teachers[ , .(district_fill, cesa)]), 
          cesa := i.cesa, on = 'district_fill']

#now add controls to generate residual/unexplained wages
districts = fread(wds['data'] %+% 'district_demographics.csv',
                  colClasses = list(character = 'district'), na.strings = '')
#robustness check: force stability of urbanicity definition over time
#districts[order(year), urbanicity := urbanicity[.N], by = district]
dist_cols = setdiff(names(districts), c('district', 'year'))

payscales[ , (dist_cols) :=
             districts[.SD, ..dist_cols, 
                       on = c('year', district = 'district_fill')]]

pr = c('_pred', '_resid')
payscales[ , paste0('lwage', pr) := {
  reg = lm(log(wage) ~ cesa + urbanicity + pct_prof + 
             pct_black + pct_hisp + pct_frl) 
  .(predict(reg, .SD), resid(reg))
}, by = .(year, highest_degree, tenure)]

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
           .(shift(schedule_lsalary_pred, n = 1L, type = 'lead'),
             shift(schedule_lsalary_resid, n = 1L, type = 'lead')), 
         by = teacher_id]

#if the teacher moved, what would their wage have been had they stayed?
teachers[ , year_next := shift(year, type = 'lead'), by = teacher_id]
teachers[ , total_exp_floor_next := 
            shift(total_exp_floor, type = 'lead'), by = teacher_id]
teachers[payscales, 
         paste0('schedule_lsalary', c('', pr), '_next_cf') := 
           .(i.lwage, i.lwage_pred, i.lwage_resid),
         on = c(year_next = 'year', 'district_fill', 
                total_exp_floor_next = 'tenure', 'highest_degree')]

###############################################################################
#                           District-Level Covariates                         #
###############################################################################
## @knitr complete_read
teachers[ , paste0(dist_cols, '_next_d') :=
            districts[.SD, ..dist_cols, 
                      on = c('year', district = 'district_next')]]
districts[payscales[ , mean(lwage_resid, na.rm = TRUE), 
                     by = .(district_fill, year)], 
          lwage_resid_avg := i.V1, on = c(district = 'district_fill', 'year')]
districts[teachers[ , .N, by = .(district_fill, year)],
          n_teachers := i.N, on = c(district = 'district_fill', 'year')]

q_var = c('lwage_resid_avg', 'pct_prof', 'pct_frl', 
          'pct_black', 'pct_hisp')
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

urb_lev = c('Large Urban', 'Small Urban', 'Suburban', 'Rural')
schools[ , urbanicity := 
           factor(urbanicity, levels = urb_lev)]
#robustness check: force stability of urbanicity definition over time
# schools[order(year), urbanicity := urbanicity[.N], by = .(district, school)]

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
