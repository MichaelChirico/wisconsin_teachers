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
library(doParallel)
library(quantmod)

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
                   "district_work_type"))
full_data = 
  full_data[highest_degree %in% 4L:5L & category == "1" &
              position_code == 53L &
              (months_employed >= 875 | year > 2004) &
              (days_of_contract >= 175 | year <= 2004) &
              (substring(area, 2L, 2L) %in% 2:7 |
                 area %in% c("0050", "0910")) &
              !is.na(school_fill) & !grepl('^9', district_fill) &
              district_work_type %in% c("04", "49") &
              !school_fill %in% c("0000", "0999") &
              total_exp_floor %in% 1L:30L,
            if (sum(full_time_equiv) == 100L) .SD,
            by = .(teacher_id, year)
            ][order(full_time_equiv), .SD[.N],
              by = .(teacher_id, year, full_time_equiv)]

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
full_data[ , teach_count_flag := .N < 10L, by = yrdsdg]

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

full_data[ , min_exp := min(total_exp_floor), by = yrds]
full_data[ , max_exp := max(total_exp_floor), by = yrds]

###############################################################################
#                             Interpolation                                   #
###############################################################################
#to cut out predict.cobs overhead
fpr = function(cb) with(cb, cobs:::.splValue(2, knots, coef, zs))

#constants
sal_max = full_data[ , max(salary)]
zs = seq_len(30L)
#salary @ 0 >= 0; salary @ 30 at most sal_max
end_cons = rbind(c(1, 0, 0),
                 c(-1, 30, sal_max))
fit = mlw[highest_degree == 4,
  .(zs, fpr(cobs(total_exp_floor, salary,
       constraint = 'increase', lambda = -1,
       knots.add = TRUE, repeat.delete.add = TRUE,
       pointwise = end_cons, lambda.length = 50))),
  by = .(year, highest_degree)]

fit = mlw.09.4[, cobs(total_exp_floor, salary,
                      constraint = 'increase', lambda = -1,
                      knots.add = TRUE, repeat.delete.add = TRUE,
                      pointwise = end_cons, lambda.length = 50)]

#Only send to the cores the necessary data--lots of copying
system.time({
  cl <- makeCluster(detectCores())
  registerDoParallel(cl)
  clusterExport(cl, "full_data", envir = environment())
  clusterEvalQ(cl, {library("data.table"); library("cobs")})
  imputed_scales = foreach(i = 1996L:2015L) %dopar% {
    print(i)
    full_data[.(i)][ , {mnx = min_exp[1L]; mxx=max_exp[1L]
      s = cobs_extrap(total_exp_floor, salary, mnx, mxx)
      f = cobs_extrap(total_exp_floor, fringe, mnx, mxx)
      list(salary = s, fringe = f, total_pay = s+f)},
      by = yrdsdg]
  }
  salary_scales =
    do.call("rbind", imputed_scales)[ , experience := rep(1L:30L, .N/30L)]
  setkeyv(salary_scales, c(yrdsdg, "experience"))
  stopCluster(cl)
})

# Post-Fit Clean-up: Real Dollars & Future Values ####
##Provide deflated wage data
oct_cpi = suppressWarnings(getSymbols(
  "CPIAUCSL", src = "FRED", auto.assign = FALSE
  #Note that data values are recorded at the end
  #  of September in each academic year, so,
  #  since I index AY by spring year, we use the
  #  'prior' year's CPI in October as the base
  )[seq(from = as.Date("1995-10-01"),
      to = as.Date("2014-10-01"), by = "year")])
salary_scales[data.table(year = 1996L:2015L,
                         index = c(coredata(oct_cpi)/
                                   oct_cpi["2014-10-01"][[1L]])),
              `:=`(salary_real = salary/i.index,
                   fringe_real = fringe/i.index,
                   total_pay_real = total_pay/i.index),
              on = "year"]

##Nominal future earnings at every point in the career
salary_scales[ , total_pay_future :=
                discounted_earnings(total_pay), by = yrdsdg]
salary_scales[ , total_pay_future_real :=
                discounted_earnings(total_pay_real), by = yrdsdg]

write.csv(salary_scales,
          data.path %+% "wisconsin_salary_scales_imputed.csv",
          row.names = FALSE)