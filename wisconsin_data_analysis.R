#Wisconsin Teachers Project
#Analysis File
#Michael Chirico
#August 27, 2015

# Package Setup, Convenient Functions, Import ####
rm(list=ls(all=T))
gc()
setwd("/home/michael/Desktop/research/Wisconsin Bargaining")
data.wd<-"/media/data_drive/wisconsin/"
library(funchir)
library(data.table)
library(xtable)

fl<-"wisconsin_teacher_data_matched"
teacher_data<-
  fread(data.wd%+%fl%+%".csv",key="year,teacher_id",
        colClasses=fread(data.wd%+%fl%+%
                           "_colClass.csv",header=F)$V2); rm(fl)

## add forward-looking indicators
frwd_vars<-c(paste0("move_",c("school","district")),
             "married","quit_next","salary","fringe",
             "total_pay","certified","district")
teacher_data[,paste0(frwd_vars,"_next"):=
               shift(.SD,type="lead"),by=teacher_id,
             .SDcols=frwd_vars]
teacher_data[,paste0(frwd_vars,"_pl5"):=
               shift(.SD,n=5L,type="lead"),by=teacher_id,
             .SDcols=frwd_vars]; rm(frwd_vars)

#base wage measures in current district
teacher_data[setkey(salary_scales[total_exp_floor==1,],year,district,highest_degree),
             `:=`(district_base_salary=i.salary,
                  district_base_fringe=i.fringe,
                  district_base_total_pay=i.total_pay)]

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

# Reduced Form Analysis: Tables ####

#Plain one-way tables of # Career moves
tbl<-unique(teacher_data)[,.(mvs=sum(move_school&!move_district),
                             mvd=sum(move_district)),by=teacher_id
                          ][,{x<-lapply(list(mvs,mvd),function(x)
                            as.list(table2(x,pct=T,prop=T)))
                          rbindlist(x,fill=T
                                    )[,rn:="Move "%+%
                                        c("School","District")]}]
setcolorder(tbl,c("rn",names(tbl)%\%"rn"))
print.xtable2(xtable(tbl,digits=1))

#Compare my turnover data to results in Texas
#  from Hanushek, Kain, O'Brien and Rivkin (NBER '05, Table A1)
hkor_tbl<-read.table("./literature/hkor_05_table_a1.txt",
                     sep="\t",header=T)
teacher_data[,if(.N==1).SD,by=.(teacher_[district=="3619"&year>1996,
                     .(teacher_id,move_school,move_district,quit_next,
                       exp_cut=cut(total_exp_floor,
                                   breaks=c(1,2,4,7,12,22,31),
                                   include.lowest=T,
                                   labels=c("1","2-3","4-6","7-11",
                                            "12-21",">21"),right=F))
                     ][order(exp_cut),
                       .(to.pct(mean(!quit_next&!move_school,na.rm=T),dig=1),
                         to.pct(mean(move_school&!move_district,na.rm=T),dig=1),
                         to.pct(mean(move_district,na.rm=T),dig=1),
                         to.pct(mean(quit_next,na.rm=T),dig=1)),by=exp_cut
                       ][,!"exp_cut",with=F],
        as.data.table(rbind(c(70.4,11.5,4.0,14.0),
                            c(70.8,11.2,5.0,13.0),
                            c(77.0,10.4,5.4,7.2),
                            c(79.7,10.6,4.3,5.4),
                            c(86.2,8.3,2.0,3.5),
                            c(86.5,5.7,0.7,7.2)))))),ncol=5,
  dimnames=list(rep("",14),
                c("Teacher Experience","No Move",
                  "Change Campus","Change District",
                  "Exit Public Schools"))),
  caption="Teacher Transitions by Teacher Experience, WI and TX",
  label="table:exp_comp_wi_tx",
  align="lp{1.8cm}p{1.5cm}p{1.5cm}p{1.5cm}p{1.8cm}"),
  include.rownames=F,hline.after=c(-1,0,7,14),scalebox=.8)),sep="\n\n")

#Comparing Turnover matrix by urbanicity to that in HKR '04 (JHR) Table 2
teacher_data[move_district_next==1,table2(urbanicity,urbanicity_next,margin=1,pct=T,prop=T,dig=1)]

#Comparing Turnover matrix by received vs. would-be salary & total pay
salary_comparison<-
  teacher_data[move_district_next==1,
               table2(create_quantiles(agency_salary_next,5),
                      create_quantiles(salary_next,5),margin=1,pct=T,dig=1,prop=T)]
rownames(salary_comparison)<-paste0("Q",1:5)
colnames(salary_comparison)<-paste0("Q",1:5)
cat(capture.output(xtable(salary_comparison)),sep="\n\n")

total_pay_comparison<-
  teacher_data[move_district_next==1,
               table2(create_quantiles(agency_total_pay_next,5),
                      create_quantiles(total_pay_next,5),margin=1,pct=T,dig=1,prop=T)]
rownames(total_pay_comparison)<-paste0("Q",1:5)
colnames(total_pay_comparison)<-paste0("Q",1:5)
cat(capture.output(xtable(total_pay_comparison)),sep="\n\n")

total_pay_future_comparison<-
  teacher_data[move_district_next==1,
               table2(create_quantiles(agency_total_pay_future_next,5),
                      create_quantiles(total_pay_future_next,5),margin=1,pct=T,dig=1,prop=T)]
rownames(total_pay_future_comparison)<-paste0("Q",1:5)
colnames(total_pay_future_comparison)<-paste0("Q",1:5)
cat(capture.output(xtable(total_pay_future_comparison)),sep="\n\n")

#Descriptive Statistics on Teachers
t<-unlist(teacher_data[,.(round(100*mean(gender=="F"),1),
                          round(100*mean(black),1),
                          round(100*mean(white),1),
                          round(100*mean(hispanic),1),
                          round(100*mean(highest_degree=="5"),1),
                          round(mean(local_exp),1),
                          round(mean(total_exp),1),
                          round(mean(age),1),round(mean(salary)),
                          round(mean(total_pay)),round(mean(years_tracked),1))])
cat(capture.output(print(xtable(cbind(
  c("% Female","% Black","% White","% Hispanic","% w/ Master's",NA),c(t[1:5],NA),
  c("Avg Local Experience","Avg Total Experience","Avg Age",
    "Avg Salary","Avg Total Pay","Avg Years Tracked"),c(t[6:11]))),
  include.rownames=F,include.colnames=F)),sep="\n\n")

#Simple LPM of moving district
reg_dist_wo_fut<-
  lm(move_district_next~I((salary_next-agency_salary_next)/1e5)+
       I((total_pay_next-agency_total_pay_next)/1e5)+
       ethnicity_main+I(pct_black_pctl_next-pct_black_pctl)+
       I(pct_hispanic_pctl_next-pct_hispanic_pctl)+
       highest_degree+total_exp_floor+urbanicity+as.factor(year),
     data=teacher_data)
reg_dist_w_fut<-
  lm(move_district_next~I((salary_next-agency_salary_next)/1e5)+
       I((total_pay_next-agency_total_pay_next)/1e5)+
       I((total_pay_future_next-agency_total_pay_future_next)/1e6)+
       ethnicity_main+I(pct_black_pctl_next-pct_black_pctl)+
       I(pct_hispanic_pctl_next-pct_hispanic_pctl)+
       highest_degree+total_exp_floor+urbanicity+as.factor(year),
     data=teacher_data)
reg_sch<-
  lm(I(move_school&!move_district_next)~
       ethnicity_main+I(pct_black_pctl_next-pct_black_pctl)+
       I(pct_hispanic_pctl_next-pct_hispanic_pctl)+
       highest_degree+total_exp_floor+urbanicity+as.factor(year),
     data=teacher_data)

cat(capture.output(
  texreg(list(reg_dist_wo_fut,reg_dist_w_fut,reg_sch),
         custom.model.names=c("District","District w/ FP","School"),
         custom.coef.names=c("xxxIntercept","$\\Delta$ Salary","$\\Delta$ Total Pay",
                             "Hispanic","White","$\\Delta$ \\% Black",
                             "$\\Delta$ \\% Hispanic","Master's","Experience",
                             "Rural","Suburb","Town","xxx2012","xxx2013",
                             "$\\Delta$ Future Pay"),
         omit.coef="xxx",scalebox=.55,digits=3,include.rmse=F,
         caption=NULL,label=NULL)),sep="\n\n")


# Reduced Form Analysis: Plots ####

#Evolution of Turnover by Experience Across Time
pdf2("wisconsin_turnover_stats_by_5ennial.pdf")
##By District
layout(mat=matrix(c(1,3,5,2,4,5),nrow=3),heights=c(.4,.4,.2))
par(oma=c(0,0,3,0))
par(mar=c(0,4.1,4.1,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[,.(total_exp_floor,move_district,
                  cut(year,breaks=c(1995,2000,2005,2010,2015),
                      include.lowest=T,
                      labels=c("95-99","00-04","05-09","10-14"),right=F))
               ][,mean(move_district,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  ylab="% Changing Districts",xaxt="n",main="Change Districts",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##By School
par(mar=c(0,4.1,4.1,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[,.(total_exp_floor,move_school&!move_district,
                  cut(year,breaks=c(1995,2000,2005,2010,2015),
                      include.lowest=T,
                      labels=c("95-99","00-04","05-09","10-14"),right=F))
               ][,mean(V2,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  ylab="% Changing Schools",xaxt="n",main="Change Schools",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##Quit rates
par(mar=c(5.1,4.1,0,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[,.(total_exp_floor,quit_next,
                  cut(year,breaks=c(1995,2000,2005,2010,2015),
                      include.lowest=T,
                      labels=c("95-99","00-04","05-09","10-14"),right=F))
               ][,mean(quit_next,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  xlab="Experience",ylab="% Leaving WI",
  main="\n Leaving WI Public Schools",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##Degree Holdings
par(mar=c(5.1,4.1,0,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[year!=1996,.(total_exp_floor,highest_degree,
                            cut(year,breaks=c(1995,2000,2005,2010,2015),
                                include.lowest=T,
                                labels=c("95-99","00-04","05-09","10-14"),right=F))
               ][,.(mean(highest_degree=="4",na.rm=T),
                    mean(highest_degree=="5",na.rm=T)),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var=c("V1","V2"))[,!"total_exp_floor",with=F],
  xlab="Experience",ylab="% Holding",
  main="\n Degree Holdings",
  type="l",lty=c(rep(1,4),rep(2,4)),lwd=3,col=rep(c("black","blue","red","green"),2))
legend("left",legend=c("MA","BA"),lty=c(2,1),lwd=3,bty="n",cex=.5)

par(mar=c(0,0,0,0))
plot(1,type="n",axes=F,xlab="",ylab="")
legend("top",legend=c("95-99","00-04","05-09","10-14"),lty=1,lwd=2,
       col=c("black","blue","red","green"),horiz=T,inset=0)
mtext("WI Turnover Moments",side=3,line=-1.5,outer=T)
dev.off2()

# Total turnover by year
pdf2("wisconsin_turnover_by_year.pdf")
teacher_data[year>1995,
             .(sum(move_school,na.rm=T),
               sum(move_school&!move_district,na.rm=T),
               sum(move_district,na.rm=T)),by=year
             ][,matplot(year,.SD[,V1:V3,with=F],
                        type="l",lty=1,lwd=3,xlab="Year",ylab="# Moves",
                        main="Number of Moves\nBy Year and Type")]
legend("topleft",legend=c("Total","School","District"),
       lwd=3,lty=1,col=c("black","red","green"),cex=.7)
dev.off2()

#plotting the salary schedules for the 5 most populous agencies over the first 30 years
biggest_5<-sort(teacher_data[.(2015,unique(agency)),.N,by=agency][order(N)][(.N-4):.N,agency])
biggest_5_names<-c("Green Bay","Kenosha","Madison","Milwaukee","Racine")
rng<-salary_scales[.(2015,rep(biggest_5,2),rep(c("4","5"),5)),range(range(total_pay),range(salary))]
pdf2("wisconsin_salary_tables_imputed_15_big5_ba.pdf")
matplot(1:30,dcast.data.table(
  salary_scales[.(2015,biggest_5,"4"),
                c("agency","total_exp_floor","salary","total_pay"),
                with=F],total_exp_floor~agency,
  value.var=c("salary","total_pay")
)[,!"total_exp_floor",with=F],
xlab="Experience",ylab="$",main="BA",ylim=rng,
lty=c(rep(1,5),rep(2,5)),type="l",col=rep(c("black","red","blue","green","purple"),2),lwd=3)
legend("topleft",legend=biggest_5_names,bty="n",cex=.6,
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)
legend("bottomright",legend=c("Total Pay","Base Pay"),cex=.6,
       lty=c(1,2),lwd=3,col="black",bty="n")
dev.off2()

pdf2("wisconsin_salary_tables_imputed_15_big5_ma.pdf")
matplot(1:30,dcast.data.table(
  salary_scales[.(2015,biggest_5,"5"),
                c("agency","total_exp_floor","salary","total_pay"),with=F],
  total_exp_floor~agency,value.var=c("salary","total_pay")
)[,!"total_exp_floor",with=F],
xlab="Experience",ylab="$",main="MA",ylim=rng,
lty=c(rep(1,5),rep(2,5)),type="l",col=rep(c("black","red","blue","green","purple"),2),lwd=3)
legend("topleft",legend=biggest_5_names,bty="n",cex=.6,
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)
legend("bottomright",legend=c("Total Pay","Base Pay"),cex=.6,
       lty=c(1,2),lwd=3,col="black",bty="n")
dev.off2()

# Contrast with a sparsely-populated district (say 20 teachers)
# code to find one: teacher_data[max_exp<30&min_exp>1,.N,by=.(year,agency,highest_degree)][N==20,]
# Current example: (2015,"5960","4")
lims<-range(range(teacher_data[.(2015,"5960","4"),.(salary,total_pay)]),
            range(salary_scales[.(2015,"5960","4"),.(salary,total_pay)]))
matplot(1:30,salary_scales[.(2015,"5960","4"),.(salary,total_pay)],
        xlab="Experience",ylab="$",type="l",lty=1,lwd=3,ylim=lims,
        main="Salary and Total Pay \n Kickapoo, WI, 2015, BA")
teacher_data[.(2015,"5960","4"),points(total_exp_floor,salary,col="black")]
teacher_data[.(2015,"5960","4"),points(total_exp_floor,total_pay,col="red")]

#How do wages change surrounding a move?
# (use only one-time switchers for now--SHOULD CHANGE THIS)
one_time_movers<-
  teacher_data[count_moves==1,
               ][,move_year_no:=which(move==1),
                 by=teacher_id][move_year_no>=9&move_year_no<=(years_tracked-8),
                                ][,year_rel:=year-year[move_year_no],by=teacher_id
                                  ][year_rel %in% -8:8,]

one_time_movers[,mean(total_pay),by=year_rel][order(year_rel)][,plot(year_rel,V1)]

#now make some summary plots
##teacher wages, teacher experience, and proportion of 1st-year teachers
pdf('wage_exp_series_avg_1996-2015.pdf')
par(mfrow=c(2,2))
teacher_data[,.(mean(salary),mean(salary_real)),by=year
             ][,matplot(year,cbind(V1,V2),type="l",xlab="Year",ylab="Nominal Wage",
                        main="Average Nominal Wage Among WI Teachers",lwd=3,lty=1,
                        col=c("gray","black"),xaxt="n")]
abline(v=2012-79/365,col=2,lty=2) #wages measured 3rd Friday of September (the 16th in 2011),
#which came 79 days after the enactment of Act 10 in Wisconsin.
legend("topleft",c("Nominal Wage","Real Wage"),col=c("gray","black"),lty=1,lwd=3,bty="n")
axis(1,at=seq(1996,2015,by=2))

teacher_data[,.(mean(total_exp),median(total_exp)),by=year
             ][,matplot(year,cbind(V1,V2),lwd=3,type="l",xlab="Year",
                        ylab="Experience",xaxt="n",lty=1,col=c("black","green"),
                        main="Total Experience Among WI Teachers")]
legend("topright",c("Average","Median"),col=c("black","green"),lty=1,lwd=3,bty="n")
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))

teacher_data[,.(100*mean(total_exp<=1),100*mean(total_exp<=5)),by=year
             ][,matplot(year,cbind(V1,V2),type="l",lwd=3,xlab="Year",
                        col=c("black","green"),ylab="Percentage",lty=1,
                        xaxt="n",main="Prevalence of New Teachers")]
legend("left",c("<=1 Year","<=5 Years"),col=c("black","green"),lty=1,lwd=3,bty="n")
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))

teacher_data[,.(100*mean(highest_degree==4),100*mean(highest_degree==5)),by=year
             ][,matplot(year,cbind(V1,V2),type="l",lwd=3,xlab="Year",col=c("black","green"),
                        lty=1,ylab="Percentage",xaxt="n",main="Degree Holdings of Teachers")]
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))
legend("left",c("BA","MA"),col=c("black","green"),lty=1,lwd=3,bty="n")
dev.off()