#!/usr/bin/env Rscript
#Wisconsin Teacher Project
#District-Level Pay Scales Fitting
#Michael Chirico
#August 23, 2015

###############################################################################
#                   Package Setup & Convenient Functions                      #
###############################################################################
rm(list = ls(all = TRUE))
gc()
data.path = "/media/data_drive/wisconsin/"
library(funchir)
library(data.table)
library(cobs)
library(parallel)
library(quantmod)
library(RPushbullet)

#Given a stream of income in periods 1,...,T
#  return the discounted sum of future income
#  from t to T on for each period t
discounted_earnings = function(x, r = .05){
  TT = length(x)
  (upper.tri(matrix(1L, TT, TT), diag = TRUE) *
    (1/(1+r)) ^ matrix(rep(0L:TT, TT),
                       nrow = TT+1L, byrow = TRUE)[-(TT+1L), ]) %*% x
}

###############################################################################
#                        Data Import & Subset                                 #
###############################################################################

#Import full matched data created in wisconsin_teacher_data_cleaner.R;
#  impose sample restrictions to get rid of teachers bound to
#  cause noise in the contract fitting procedure, as well as teachers
#  with multiple positions by eliminating all but their highest-FTE positions
full_data = 
  fread(data.path %+% "wisconsin_teacher_data_full.csv",
        select = c("year", "highest_degree", "months_employed", "salary",
                   "fringe", "category", "position_code", "area",
                   "full_time_equiv", "days_of_contract", "total_exp_floor",
                   "teacher_id", "school_fill", "district_fill",
                   "district_work_type"),
        #otherwise assumes integer and truncates
        colClasses = list(character = 'area'))
full_data = 
  full_data[ , if (sum(full_time_equiv, na.rm = TRUE) == 100) .SD,
             by = .(teacher_id, year)
             ][highest_degree %in% 4L:5L & category == 1L &
                 position_code == 53L &
                 #encoding of total activity changed from 2005
                 (months_employed >= 875 | year > 2004) &
                 (days_of_contract >= 175 | year <= 2004) &
                 (substring(area, 2L, 2L) %in% 2:7 |
                    area %in% c("0050", "0910")) &
                 !is.na(school_fill) & !grepl('^9', district_fill) &
                 district_work_type %in% c("04", "49") &
                 !school_fill %in% c("0000", "0999") &
                 total_exp_floor %in% 1L:30L & 
                 #seems like a reasonable floor below which
                 #  no teacher should have been scheduled
                 #  to have received (accounting for
                 #  potential data errors)
                 salary + fringe >= 10000
               ][order(full_time_equiv), .SD[.N],
                 by = .(teacher_id, year, full_time_equiv)
                 ][ , if(uniqueN(highest_degree) == 2L) .SD,
                    by = .(district_fill, year)]

#Now, eliminate schools with insufficient coverage
yrdsdg = c("year", "district_fill", "highest_degree")
yrds = c('year', 'district_fill')
setkeyv(full_data, yrdsdg)
setindexv(full_data, yrds)
          
#Can't interpolate if there are only 2 or 3
#  unique experience cells represented
full_data[ , node_count_flag := 
             uniqueN(total_exp_floor) < 7L, by = yrdsdg]

#Nor if there are too few teachers
full_data[ , teach_count_flag := .N < 20L, by = yrdsdg]

#Also troublesome when there is little variation in salaries like so:
full_data[ , sal_scale_flag :=
             mean(abs(salary - mean(salary))) < 50, by = yrdsdg]
full_data[ , sal_count_flag := uniqueN(salary) < 5L, by = yrdsdg]
full_data[ , frn_scale_flag := 
             mean(abs(salary - mean(fringe))) < 50, by = yrdsdg]
full_data[ , frn_count_flag := uniqueN(fringe) < 5L, by = yrdsdg]

# Impose the flag on both certification tracks
#   whenever partially violated
flgs = paste0(c('node_count', 'teach_count', 'sal_scale',
                'sal_count', 'frn_scale', 'frn_count'), '_flag')
full_data[ , (flgs) := lapply(.SD, any), by = yrds, .SDcols = flgs]

#Discard variables not necessary for interpolation
full_data = 
  full_data[!(node_count_flag | teach_count_flag |
                sal_scale_flag | sal_count_flag |
                frn_scale_flag | frn_count_flag),
            .(year, district_fill, highest_degree,
              total_exp_floor, salary, fringe)]

###############################################################################
#                             Interpolation                                   #
###############################################################################
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
sal_max = full_data[ , max(salary)]
zs = seq_len(30L)
## ** ? somehow having a very high top-end constraint breaks cobs
# #salary @ 0 >= 0; salary @ 30 at most sal_max
# end_cons = rbind(c(1, 0, 0),
#                  c(-1, 30, sal_max))
end_cons = cbind(1, 0, 0)

yrs = setNames(nm = full_data[ , unique(year)])

t0 = proc.time()["elapsed"]
cl <- makeCluster(detectCores())
clusterExport(cl, c('full_data', 'end_cons', 'fpr', 'zs', 'yrs'),
             envir = environment())
clusterEvalQ(cl, {library("data.table"); library("cobs")})
imputed_scales = rbindlist(mclapply(yrs, function(yr) {
  full_data[.(yr), {
    ba = highest_degree == 4
    wage_ba = fpr(cobs(
      total_exp_floor[ba], salary[ba], print.warn = FALSE,
      maxiter = 5000, print.mesg = FALSE,
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
      maxiter = 5000, print.mesg = FALSE,
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

imputed_scales[ , year := as.integer(year)]

pbPost('note', paste('Imputation Done;',
                     proc.time()["elapsed"] - t0,
                     'elapsed.'))

# Post-Fit Clean-up: Real Dollars & Future Values ####
##Provide deflated wage data
#Note that data values are recorded at the end
#  of September in each academic year, so,
#  since I index AY by spring year, we use the
#  'prior' year's CPI in October as the base
#Also perpetual reminder: YYstaff.txt is the data for
#  the (YY-1)-YY academic year
oct1s = as.Date(paste0(yrs - 1L, '-10-01'))
oct_cpi = suppressWarnings(
  getSymbols("CPIAUCSL", src = "FRED", auto.assign = FALSE)[oct1s]
)
inflation_index = 
  data.table(year = yrs,
             #coredata returns the "column" as a matrix, so drop it
             index = drop(coredata(oct_cpi))/oct_cpi[[length(oct_cpi)]])
imputed_scales[inflation_index,
               `:=`(wage_ba_real = wage_ba/i.index,
                    wage_ma_real = wage_ma/i.index,
                    fringe_ba_real = fringe_ba/i.index,
                    fringe_ma_real = fringe_ma/i.index),
               on = "year"]

fwrite(imputed_scales, data.path %+% "wisconsin_salary_scales_imputed.csv")
