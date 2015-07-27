library(cobs)
library(doParallel)
library(maptools)
library(spdep)
library(quantmod)
library(xtable)
library(xlsx)
library(nnet)
library(texreg)

# Pay Scale Interpolation ####
cobs_extrap<-function(total_exp_floor,outcome,min_exp,max_exp,
                      constraint="increase",print.mesg=F,nknots=8,
                      keep.data=F,maxiter=150){
  #these are passed as vectors
  min_exp<-min_exp[1]
  max_exp<-max_exp[1]
  #get in-sample fit
  in_sample<-predict(cobs(x=total_exp_floor,y=outcome,
                          constraint=constraint,
                          print.mesg=print.mesg,nknots=nknots,
                          keep.data=keep.data,maxiter=maxiter),
                     z=min_exp:max_exp)[,"fit"]
  if (sum(abs(in_sample))<50){
    in_sample<-rep(0,length(in_sample))
  }
  #append by linear extension below min_exp
  fit<-c(if (min_exp==1) NULL else in_sample[1]-
           ((min_exp-1):1)*(in_sample[2]-in_sample[1]),
         in_sample,
         #append by linear extension above max_exp
         if (max_exp==30) NULL else in_sample[length(in_sample)]+
           (1:(30-max_exp))*(in_sample[length(in_sample)]-
                               in_sample[length(in_sample)-1]))
  #Just force to 0 anything that wants to go negative
  fit[fit<0]<-0
  #Also put a ceiling on how high extrapolation can climb--
  #From original fitted data, more than 99% of year-40 to year-20 ratios are below 45%
  fit[fit>1.45*max(in_sample)]<-1.45*max(in_sample)
  fit
}

setkey(teacher_data,year,agency,highest_degree)
teacher_data_sub<-teacher_data[,.(year,agency,highest_degree,total_exp_floor,salary,fringe)]
#Can't interpolate if there are only 2 or 3 unique experience cells represented
teacher_data_sub[,node_count:=length(unique(total_exp_floor)),by=.(year,agency,highest_degree)]
#Nor if there are too few teachers
teacher_data_sub[,teach_count:=.N,by=.(year,agency,highest_degree)]
#Also troublesome when there is little variation in salaries like so:
teacher_data_sub[,sal_scale_flag:=mean(abs(salary-mean(salary)))<50,by=.(year,agency,highest_degree)]
teacher_data_sub[,sal_count_flag:=length(unique(salary))<5,by=.(year,agency,highest_degree)]
teacher_data_sub[,fri_scale_flag:=mean(abs(salary-mean(fringe)))<50,by=.(year,agency,highest_degree)]
teacher_data_sub[,fri_count_flag:=length(unique(fringe))<5,by=.(year,agency,highest_degree)]

teacher_data_sub<-
  teacher_data_sub[node_count>=7&teach_count>=10
                   &sal_scale_flag==0&sal_count_flag==0
                   &fri_scale_flag==0&fri_count_flag==0,]
teacher_data_sub<-teacher_data_sub[,.(year,agency,highest_degree,total_exp_floor,salary,fringe)]
teacher_data_sub[,min_exp:=min(total_exp_floor),by=.(year,agency,highest_degree)]
teacher_data_sub[,max_exp:=max(total_exp_floor),by=.(year,agency,highest_degree)]
#Only send to the cores the necessary data--lots of copying
cl <- makeCluster(detectCores()); 
registerDoParallel(cl); 
clusterExport(cl,c("teacher_data_sub"),envir=environment());
clusterEvalQ(cl,library("data.table"));
clusterEvalQ(cl,library("cobs"));
salary_imp <- foreach(i = 1996:2015) %dopar% {
  teacher_data_sub[.(i)][,.(total_exp_floor=1:30,
                            salary=cobs_extrap(total_exp_floor,salary,min_exp,max_exp)),
                         by=.(year,agency,highest_degree)]
}
salary_imp<-setkey(do.call("rbind",salary_imp),year,agency,highest_degree,total_exp_floor)

fringe_imp <- foreach(i = 1996:2015) %dopar% {
  teacher_data_sub[.(i)][,.(total_exp_floor=1:30,
                            fringe=cobs_extrap(total_exp_floor,fringe,min_exp,max_exp)),
                         by=.(year,agency,highest_degree)]
}
fringe_imp<-setkey(do.call("rbind",fringe_imp),year,agency,highest_degree,total_exp_floor)
rm(teacher_data_sub)
stopCluster(cl)

salary_scales<-fringe_imp[salary_imp][,total_pay:=fringe+salary]
rm(list=ls(pattern="imp"))

#***SOME DISTRICTS STRANGELY HAVE TOO MANY INTERP VALUES--INVESTIGATE***
salary_scales<-salary_scales[!salary_scales[,.N,by=.(year,agency,highest_degree)][N>30,!"N",with=F],]

#nominal future earnings at every point in the career
discounted_earnings<-function(x,r=.05){
  nm1<-length(x)-1
  (sum(x/(1+r)^(0:nm1))-
    c(0,cumsum(x/(1+r)^(0:nm1))[1:nm1]))*(1+r)^(0:nm1)
}
salary_scales[,total_pay_future:=discounted_earnings(total_pay),
              by=.(year,agency,highest_degree)]
##add to teacher data
setkeyv(teacher_data,key(salary_scales))[salary_scales,total_pay_future:=total_pay_future]

## add forward-looking indicators
frwd_vars<-c(paste0("move",c("","_school","_district")),"married","quit",
             "salary","fringe","total_pay","total_pay_future","certified","agency","move_type")
teacher_data[,paste0(frwd_vars,"_next"):=
               shift(.SD,type="lead"),by=teacher_id,
             .SDcols=frwd_vars]
teacher_data[,paste0(frwd_vars,"_pl5"):=
               shift(.SD,n=5L,type="lead"),by=teacher_id,
             .SDcols=frwd_vars]; rm(frwd_vars)

#base wage measures in current district
teacher_data[setkey(salary_scales[total_exp_floor==1,],year,agency,highest_degree),
             `:=`(agency_base_salary=i.salary,
                  agency_base_fringe=i.fringe,
                  agency_base_total_pay=i.total_pay,
                  agency_base_total_pay_future=i.total_pay_future)]

#base wage measures at next chosen district
setkey(teacher_data,year,agency_next,highest_degree
)[setkey(salary_scales[total_exp_floor==1,],year,agency,highest_degree),
  `:=`(agency_next_base_salary=i.salary,
       agency_next_base_fringe=i.fringe,
       agency_next_base_total_pay=i.total_pay,
       agency_next_base_total_pay_future=i.total_pay_future)]

#subsequent wage measures at subsequent district
setkey(teacher_data,year,agency,highest_degree,total_exp_floor
)[setkey(salary_scales[,.(year,agency,highest_degree,exp=total_exp_floor-1,
                          salary,fringe,total_pay,total_pay_future)],
         year,agency,highest_degree,exp),
  `:=`(agency_salary_next=i.salary,
       agency_fringe_next=i.fringe,
       agency_total_pay_next=i.total_pay,
       agency_total_pay_future_next=i.total_pay_future)]

#provide deflated wage data
dollar_cols<-c("salary","fringe","total_pay")
getSymbols("CPIAUCSL",src='FRED')
infl<-data.table(year=1996:2015,
                 index=CPIAUCSL[seq(from=as.Date('1995-10-01'),
                                    by='years',length.out=21)]/
                   as.numeric(CPIAUCSL['1995-10-01']),key="year")
setkey(teacher_data,year,agency,highest_degree,total_exp_floor)[infl,index:=index.CPIAUCSL]
teacher_data[,paste0(dollar_cols,"_real"):=
               lapply(.SD,function(x){x/teacher_data$index}),
             .SDcols=dollar_cols][,index:=NULL]
salary_scales[infl,index:=index.CPIAUCSL]
salary_scales[,paste0(dollar_cols,"_real"):=
                lapply(.SD,function(x){x/salary_scales$index}),
              .SDcols=dollar_cols][,index:=NULL]
rm(infl,dollar_cols)

write.csv(salary_scales,"wisconsin_salary_scales_imputed.csv",row.names=F)
salary_scales<-setkey(fread("wisconsin_salary_scales_imputed.csv"),
                      year,agency,highest_degree,total_exp_floor)