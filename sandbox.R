#sandboxing with wisconsin data

##exploring pay as a function of age distribution
### exclude tiny districts (median district size is ~50)
teacher_data <- 
  teacher_data[district %in% 
                 teacher_data[ , .N, .(district, year)
                               ][ , if(all(N > 50)) TRUE, 
                                  by = district]$district&
                 fringe>0 & salary>0]

milw <- teacher_data[district == 3619]

milw[ , plot(NULL, xlim = range(year), ylim = range(age),
             xlab = "year", ylab = "age", las = 1,
             main = "Evolution of Age Distribution in Milwaukee")]

milw[order(year), as.list(quantile(age)), by = year
     ][ , matplot(year, .SD[ , !"year", with = FALSE],
                  type = "l", lty = c(3:1, 2:3), lwd = 3,
                  col = "blue", xlab = "year", ylab = "age",
                  main = "Evolution of Age Distribution in Milwaukee")]

###define young districts
cutoff <- 
  teacher_data[ , median(age), .(district, year)
                ][, floor(quantile(V1, .25))]

teacher_data[ , young_dist := median(age) < cutoff, .(district, year)]

teacher_data[age < 30, mean(total_pay, na.rm = TRUE), by = young_dist]
teacher_data[age < 30 & total_pay > 0, 
             summary(lm(log(total_pay) ~ young_dist))]

## average (log) wage of young teachers vs. % young teachers

teacher_data[ , .(youth = mean(idx <- age <= 30),
                  wage = mean(log(total_pay[idx])),.N), 
              by = .(district, year)
              ][ , symbols(youth, wage, circles = sqrt(N))]

teacher_data[ , .(youth = mean(idx <- age <= 30),
                  wage = mean(log(total_pay[idx])),.N), 
              by = .(district, year)
              ][N>150, lm(wage ~ youth + factor(district) + 
                            factor(year))]

