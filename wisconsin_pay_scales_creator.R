#Wisconsin Teacher Project
#District-Level Pay Scales Fitting
#Michael Chirico
#August 23, 2015

# Package Setup, Convenient Functions ####
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

#Data Import ####

##Import full matched data created
##  in wisconsin_teacher_data_cleaner.R;
##  impose sample restrictions to get rid of
##  teachers bound to cause noise in the
##  contract fitting procedure, as well
##  as teachers with multiple positions
##  by eliminating all but highest-FTE positions
full_data = 
  fread(data.path %+% "wisconsin_teacher_data_full.csv",
        select = c("year", "highest_degree", "months_employed", "salary",
                   "fringe", "category", "position_code", "area",
                   "full_time_equiv", "days_of_contract", "total_exp_floor",
                   "teacher_id", "school_fill", "district_fill",
                   "district_work_type"))
full_data = 
  full_data[highest_degree %in% 4L:5L & category == "1"&
              position_code == 53L &
              (months_employed >= 875 | year > 2004) &
              (days_of_contract >= 175 | year <= 2004) &
              (substring(area, 2L, 2L) %in% 2:7 |
                 area %in% c("0050", "0910")) &
              !is.na(school_fill) &
              substring(district_fill, 1L, 1L) != "9" &
              district_work_type %in% c("04", "49") &
              !school_fill %in% c("0000", "0999") &
              total_exp_floor %in% 1L:30L,
            if (sum(full_time_equiv) == 100L) .SD,
            by = .(teacher_id, year)
            ][order(full_time_equiv), .SD[.N],
              by = .(teacher_id, year, full_time_equiv)]

##Now, eliminate schools with insufficient coverage
yrdsdg = c("year", "district_fill", "highest_degree")
###Can't interpolate if there are only 2 or 3 unique experience cells represented
full_data[ , node_count_flag := 
             uniqueN(total_exp_floor) < 7L, keyby = yrdsdg]
###Nor if there are too few teachers
full_data[ , teach_count_flag := .N < 10L, by = yrdsdg]
###Also troublesome when there is little variation in salaries like so:
full_data[ , sal_scale_flag :=
             mean(abs(salary - mean(salary))) < 50, by = yrdsdg]
full_data[ , sal_count_flag := uniqueN(salary) < 5L, by = yrdsdg]
full_data[ , fri_scale_flag := 
             mean(abs(salary - mean(fringe))) < 50, by = yrdsdg]
full_data[ , fri_count_flag := uniqueN(fringe) < 5L, by = yrdsdg]

###Discard variables not necessary for interpolation
full_data = 
  full_data[!(node_count_flag | teach_count_flag |
                sal_scale_flag | sal_count_flag|
                fri_scale_flag | fri_count_flag),
            .(year, district_fill, highest_degree,
              total_exp_floor, salary, fringe)]

full_data[ , min_exp := min(total_exp_floor), by = yrdsdg]
full_data[ , max_exp := max(total_exp_floor), by = yrdsdg]

# Pay Scale Interpolation ####
cobs_extrap = function(total_exp_floor, outcome, min_exp, max_exp,
                      constraint = "increase", print.mesg = FALSE, nknots = 8,
                      keep.data = FALSE, maxiter = 150L){
  #get in-sample fit
  in_sample = predict(cobs(x = total_exp_floor, y = outcome,
                           constraint = constraint,
                           print.mesg = print.mesg, nknots = nknots,
                           keep.data = keep.data, maxiter = maxiter),
                      z = min_exp:max_exp)[ , "fit"]
  nin = length(in_sample)
  if (sum(abs(in_sample)) < 50){
    in_sample = rep(0, nin)
  }
  #append by linear extension below min_exp
  fit = c(if (min_exp==1L) NULL else in_sample[1L] -
            ((min_exp - 1L):1L) * (in_sample[2L] - in_sample[1L]),
          in_sample,
          #append by linear extension above max_exp
          if (max_exp == 30L) NULL else in_sample[nin] +
            (1L:(30L - max_exp)) * (in_sample[nin] - in_sample[nin-1L]))
  #Just force to 0 anything that wants to go negative
  fit[fit < 0] = 0
  #Also put a ceiling on how high extrapolation can climb--
  #From original fitted data, more than 99% of year-40 to year-20 ratios are below 45%
  mm = 1.45*max(in_sample)
  fit[fit>mm] = mm
  fit
}

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