#!/usr/bin/env Rscript
#Wisconsin Teacher Project
#District-Level Pay Scales Fitting (IGNORE CERTIFICATION)
#Michael Chirico
#August 23, 2015

###############################################################################
#                               Package Setup                                 #
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

#data created in pay_scales_data_cleaner.R
full_data = fread(data.path %+%
                    'wisconsin_teacher_data_for_payscales_no_cert.csv',
                  colClasses =  list(character = 'district_fill'),
                  key = 'year,district_fill')
setindex(full_data, year, district_fill)

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
    wage = fpr(cobs(
      total_exp_floor, salary, print.warn = FALSE,
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
    if (length(idx <- which(diff(wage) < -1e-2) + 1L)) {
      wage = linear_extend(wage, idx)
    }
    #some regressions support the hypothesis
    #  that fringe benefits are monotonically
    #  increasing with tenure, even though
    #  there's not strong theoretical support for this
    fringe = fpr(cobs(
      total_exp_floor, fringe, print.warn = FALSE,
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
    if (length(idx <- which(diff(fringe) < -1e-2) + 1L)) {
      fringe = linear_extend(fringe, idx)
    }
    .(tenure = zs, wage = wage, fringe = fringe)}, 
    #note: this approach leads year to be assigned as a character
    by = district_fill]}), idcol = 'year')
stopCluster(cl)

imputed_scales[ , year := as.integer(year)]

pbPost('note', paste('Imputation Done;',
                     proc.time()["elapsed"] - t0,
                     'elapsed.'))

###############################################################################
#                     Post-Fit Clean-up: Real-dollar Wages                    #
###############################################################################

#Provide deflated wage data
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
               `:=`(wage_real = wage/i.index,
                    fringe_real = fringe/i.index),
               on = "year"]

fwrite(imputed_scales, data.path %+% "wisconsin_salary_scales_imputed_no_cert.csv")
