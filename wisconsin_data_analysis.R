##Wisconsin Data Exploration

##############################################################################################3
#Using the district-level average wage files for the teachers
setwd("/home/michael/Desktop/research/Wisconsin Bargaining")
library(data.table)
library(quantmod)
library(cobs)

rm(list = ls(all = TRUE))

years <- 1998:2012
##Data is from fall, but I prefer to think of an academic year as its spring year, so add one

for (tt in years){
  data<-setnames(
    setnames(setkey(
      fread(paste("salaries_by_district_wi_",
                  ifelse((tt%%100)<10,paste0("0",tt%%100),
                         tt%%100),".csv",sep=""))[,Year:=NULL],
      dist_no),paste(c("district","dist_no","city_code","cesa",
                       "pos_code","position","low_salary","high_salary","avg_salary",
                       "avg_fringe","avg_exp_local","avg_exp_total"),tt+1,sep="_")),
    paste0("dist_no_",tt+1),"dist_no")
  print(data[is.na(as.numeric(get(paste0("avg_fringe_",tt+1)))),])
  to_numeric<-paste(c("low_salary","high_salary","avg_salary","avg_fringe"),tt+1,sep="_")
  data[,(to_numeric):=lapply(.SD,as.numeric),.SDcols=to_numeric]
  assign(paste("data",tt+1,sep="_"),data)
}
rm(data)

data_2014<-setnames(
  setnames(setkey(
    fread("salaries_by_district_wi_13.csv")[,YEAR:=NULL],dist_no),
    paste(c("district","dist_no","city_code","cesa",
            "pos_code","position","low_salary","high_salary","avg_salary",
            "avg_fringe","avg_exp_local","avg_exp_total"),2014,sep="_")),
  "dist_no_2014","dist_no")

#merge data by district number
##**Need to be more careful about which districts are dropped**
##  (set of districts not stable across time)
data<-Reduce(merge,list(data_1999,data_2000,data_2001,data_2002,
                        data_2003,data_2004,data_2005,data_2006,
                        data_2007,data_2008,data_2009,data_2010,
                        data_2011,data_2012,data_2013,data_2014))
setcolorder(data,sort(names(data)))

write.csv(data,"salaries_by_district_wi.csv",row.names=F,quote=F)

#plot the three series, including a line demarcating the implementation of Act 10
graphics.off()
dev.new()
pdf("wage_series_unweighted_all_dist.pdf")
dev.set(which=dev.list()["RStudioGD"])
plot(cbind(1998,2013),range(data[,colMeans(as.matrix(.SD),na.rm=T),
                                 .SDcols=c(paste0("avg_salary_",1999:2014),
                                           paste0("high_salary_",1999:2014),
                                           paste0("low_salary_",1999:2014))]),
     type='n',xlab="Year",ylab="Nominal Wage",xaxt="n",
     main="Low, Average, and High Salaries \n In Wisconsin Across Time")
axis(1,at=seq(1998,2014,by=2))
lines(1998:2013,data[,colMeans(as.matrix(.SD),na.rm=T),
                     .SDcols=paste0("low_salary_",1999:2014)],
      col="black",lty=2,lwd=3)
lines(1998:2013,data[,colMeans(as.matrix(.SD),na.rm=T),
                     .SDcols=paste0("avg_salary_",1999:2014)],
      col="blue",lty=1,lwd=4)
lines(1998:2013,data[,colMeans(as.matrix(.SD),na.rm=T),
                     .SDcols=paste0("high_salary_",1999:2014)],
      col="black",lty=2,lwd=3)
#wages measured 3rd Friday of September (the 16th in 2011), which came 79 days after the enactment of Act 10 in Wisconsin.
abline(v=2011-79/365,col="red",lty=2,lwd=2)
dev.copy(which=dev.list()["pdf"])
dev.off(which=dev.list()["pdf"])

#########################################################################
#Now, using the teacher-level data files from here:
#http://lbstat.dpi.wi.gov/lbstat_newasr
rm(list = ls(all = TRUE))

#years are '95 to '13
#read and do some data cleaning/uniforming
for (tt in 1995:2014){
  dummy<-
    setkey(fread(paste0(substr(tt,3,4),"staff.csv"),drop="filler",
                 colClasses=fread(paste0(substr(tt,3,4),"dict.csv"))$V3
                 )[,filler:=NULL][,filler:=NULL][,filler:=NULL][,filler:=NULL
                   ][position_code==53&(agency_type %in% c("01","03","04","1","3","4")),
                     ],id)

  #eliminate duplicate observations of teachers who have multiple assignments--relevant variables (for now) are constant across observations
  #**Not constant within teacher rows: SHOULD CHECK**
  dummy<-setkey(dummy[!duplicated(id),][,count:=.N,by=.(first_name,last_name,birth_year)
                                        ][count==1,],first_name,last_name,birth_year)
  
  if (!("nee" %in% names(dummy))){
    dummy[,nee:=""]
    #For through 1999-2000, maiden names were stored in parentheses;
    #  some spillover happened thereafter (through roughly 2003-04)
    dummy[grepl("\\(",last_name),
          nee:=regmatches(last_name,regexpr("(?<=\\().*?(?=\\)|$)",
                                            last_name,perl=T))]
    dummy[grepl("-",last_name),
          nee:=regmatches(last_name,regexpr(".*(?=-)",
                                            last_name,perl=T))]
  }
  strings<-c("first_name","last_name","nee")
  dummy[,(strings):=lapply(.SD,function(x){gsub(paste0("\\s+$|^\\s+|\\s+[a-z]\\.?\\s+|",
                                                       "^[a-z]\\.?\\s+|\\s+[a-z]\\.?$|"),
                                                "",tolower(gsub("[\\.,']"," ",x)))}),
        .SDcols=strings]
  
  #whether middle names are included is sometimes random; use as backup
  dummy[,first_name2:=first_name]
  dummy[grepl("\\s",first_name),
        first_name2:=regmatches(first_name,
                                regexpr(".*(?=\\s)",
                                        first_name,perl=T))]
  
  dummy[,local_exp:=local_exp/10]
  dummy[,total_exp:=total_exp/10]
  
  #create year dummy for appending data next
  dummy[,year:=tt+1]
  
  assign(paste("data",tt+1,sep="_"),dummy)
}
rm(tt,dummy,strings)

############################################################
# MATCHING PROCESS: METICULOUSLY MATCH CONSECUTIVE YEARS-- #
#    HAMMER OUT MATCHES AS MUCH ASS POSSIBLE               #
#    BUILDING UP THE ID SYSTEM IN THE PROCESS              #
############################################################

#Count all teachers in year 1
data_1996[,teacher_id:=.I]

match_on_names<-function(from,to){
  #First match anyone who stayed in the same school
  setkey(from,first_name,last_name,birth_year,agency,school)
  setkey(to,first_name,last_name,birth_year,agency,school
         )[from,teacher_id:=teacher_id]
  #Loosen criteria--find within-district switchers
  setkey(to,first_name,last_name,birth_year,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  #Loosen criteria--find district switchers
  setkey(to,first_name,last_name,birth_year
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  #Find anyone who appears to have gotten married
  setkey(to,first_name,nee,birth_year
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  #some people appear to be missing birth year only
  setkey(from,first_name,last_name,agency)
  setkey(to,first_name,last_name,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  setkey(from,first_name,nee,agency)
  setkey(to,first_name,last_name,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  ##now match some stragglers with missing/included middle names & repeat above
  setkey(from,first_name2,last_name,birth_year,agency)
  setkey(to,first_name2,last_name,birth_year,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  setkey(to,first_name2,last_name,birth_year
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  setkey(to,first_name2,nee,birth_year
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  setkey(from,first_name2,last_name,agency)
  setkey(to,first_name2,last_name,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  setkey(from,first_name2,last_name,agency)
  setkey(to,first_name2,nee,agency
         )[from[!(teacher_id %in% to$teacher_id),],teacher_id:=i.teacher_id]
  #finally, give up and assign new ids to unmatched teachers
  to[is.na(teacher_id),teacher_id:=.I+max(from$teacher_id)]
  to
}

data_1997<-match_on_names(from=data_1996,to=data_1997)
data_1998<-match_on_names(from=data_1997,to=data_1998)
data_1999<-match_on_names(from=data_1998,to=data_1999)
data_2000<-match_on_names(from=data_1999,to=data_2000)
data_2001<-match_on_names(from=data_2000,to=data_2001)
data_2002<-match_on_names(from=data_2001,to=data_2002)
data_2003<-match_on_names(from=data_2002,to=data_2003)
data_2004<-match_on_names(from=data_2003,to=data_2004)
data_2005<-match_on_names(from=data_2004,to=data_2005)
data_2006<-match_on_names(from=data_2005,to=data_2006)
data_2007<-match_on_names(from=data_2006,to=data_2007)
data_2008<-match_on_names(from=data_2007,to=data_2008)
data_2009<-match_on_names(from=data_2008,to=data_2009)
data_2010<-match_on_names(from=data_2009,to=data_2010)
data_2011<-match_on_names(from=data_2010,to=data_2011)
data_2012<-match_on_names(from=data_2011,to=data_2012)
data_2013<-match_on_names(from=data_2012,to=data_2013)
data_2014<-match_on_names(from=data_2013,to=data_2014)
data_2015<-match_on_names(from=data_2014,to=data_2015)

teacher_data<-
  rbindlist(list(data_1996,data_1997,data_1998,data_1999,data_2000,
                 data_2001,data_2002,data_2003,data_2004,data_2005,
                 data_2006,data_2007,data_2008,data_2009,data_2010,
                 data_2011,data_2012,data_2013,data_2014,data_2015),
            fill=T)

teacher_data[,multiples:=.N,by=.(teacher_id,birth_year)]

rm(data_1996,data_1997,data_1998,data_1999,data_2000,
   data_2001,data_2002,data_2003,data_2004,data_2005,
   data_2006,data_2007,data_2008,data_2009,data_2010,
   data_2011,data_2012,data_2013,data_2014,data_2015)

teacher_data[,birth_year:=as.integer(birth_year)]
teacher_data[,total_exp_floor:=floor(total_exp)]
teacher_data[,age:=year-birth_year]
teacher_data[,total_pay:=salary+fringe]

#SHOULD TRY AND DELVE INTO THIS MORE LATER, but for now--delete any record of teachers working since before age 17
teacher_data<-teacher_data[age-total_exp>17,]

#What are the empirical salary scales?
# Empirically, these change year-to-year,
#  so find the average pay within each
#  year x experience x degree cell at each district
salary_scales<-teacher_data[highest_degree %in% c(4,5)&total_exp_floor<=40,
                            .(salary=mean(salary,na.rm=T),
                              fringe=mean(fringe,na.rm=T),
                              total_pay=mean(total_pay)),
                            by=.(year,agency,total_exp_floor,highest_degree)]

#Now, impute the intermediate values for all districts with missing cells
salary_scales_imputed<-
  setkey(salary_scales,year,agency,highest_degree,total_exp_floor)[
    data.table(year=rep(1996:2015,each=length(unique(salary_scales[,agency]))*2*41),
               agency=rep(rep(unique(salary_scales$agency),each=2*41),times=20),
               highest_degree=rep(rep(c("4","5"),each=41),
                                  times=20*length(unique(salary_scales[,agency]))),
               total_exp_floor=rep(0:40,times=20*length(unique(salary_scales[,agency]))*2),
               key=c("year","agency","highest_degree","total_exp_floor"))
    ][!is.na(salary),N:=.N,by=.(year,agency,highest_degree)
      ][,N:=as.integer(max(0,max(N,na.rm=T))),by=.(year,agency,highest_degree)][N>=7,][,N:=NULL]

salary_scales_imputed[!is.na(salary),min_exp:=min(total_exp_floor,na.rm=T),by=.(year,agency)]
salary_scales_imputed[!is.na(salary),max_exp:=max(total_exp_floor,na.rm=T),by=.(year,agency)]

###****THIS PROCESS IS VERY TIME-CONSUMING--RUNNING ABOUT 45000 LOCAL LINEAR REGRESSIONS****
capture.output(salary_scales_imputed[,salary:=
                                       predict(cobs(x=total_exp_floor,y=salary,
                                                    constraint="increase"),z=0:40)[,"fit"],
                                     by=.(year,agency,highest_degree)],file="/dev/null")
capture.output(salary_scales_imputed[,fringe:=
                                       predict(cobs(x=total_exp_floor,y=fringe,
                                                    constraint="increase"),z=0:40)[,"fit"],
                                     by=.(year,agency,highest_degree)],file="/dev/null")
capture.output(salary_scales_imputed[,total_pay:=
                                       predict(cobs(x=total_exp_floor,y=total_pay,
                                                    constraint="increase"),z=0:40)[,"fit"],
                                     by=.(year,agency,highest_degree)],file="/dev/null")

setkey(teacher_data,year,agency,total_exp_floor,highest_degree)

#YSED : Year, School, Experience, Degree

teacher_data[,n_ysed:=.N,by=list(year,agency,total_exp_floor,highest_degree)]
teacher_data[,avg_pay_ysed:=mean(salary,na.rm=T),by=list(year,agency,total_exp_floor,highest_degree)]
teacher_data[,agency_name:=tolower(gsub("^\\s+|\\s+$", "", agency_name))]

salary_tables<-teacher_data[,mean(salary,na.rm=T),by=list(year,agency,total_exp_floor,highest_degree)]
salary_tables<-teacher_data[,mean(fringe,na.rm=T),by=list(year,agency,total_exp_floor,highest_degree)][salary_tables]
salary_tables<-teacher_data[,mean(total_pay,na.rm=T),by=list(year,agency,total_exp_floor,highest_degree)][salary_tables]

setnames(salary_tables,c("year","agency","total_exp_floor","highest_degree","total_pay","fringe","salary"))

#plotting the salary schedules for the 5 most populous agencies over the first 30 years
postscript('wisconsin_salary_tables_imputed_05_big5_base.ps')
par(mfrow=c(1,2))
plot(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==4,]$salary[1:25],ylim=c(25000,70000),yaxt="n",
     main="Imputed Salary Schedules (BA)\n5 Biggest WI Districts, 2004-05",xlab="Total Experience",ylab="$")
lines(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==4,]$salary[1:25],type="p",col="red")
lines(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==4,]$salary[1:25],type="p",col="blue")
lines(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==4,]$salary[1:25],type="p",col="green")
lines(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==4,]$salary[1:25],type="p",col="purple")

lines(lowess(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==4,]$salary[1:25]),type="l",col="black")
lines(lowess(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==4,]$salary[1:25]),type="l",col="red")
lines(lowess(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==4,]$salary[1:25]),type="l",col="blue")
lines(lowess(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==4,]$salary[1:25]),type="l",col="green")
lines(lowess(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==4,]$salary[1:25]),type="l",col="purple")
axis(2,at=seq(30000,70000,by=10000))
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),pch="o")

plot(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==5,]$salary[1:25],ylim=c(25000,70000),yaxt="n",
     main="Imputed Salary Schedules (MA)\n5 Biggest WI Districts, 2004-05",xlab="Total Experience",ylab="$")
lines(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==5,]$salary[1:25],type="p",col="red")
lines(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==5,]$salary[1:25],type="p",col="blue")
lines(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==5,]$salary[1:25],type="p",col="green")
lines(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==5,]$salary[1:25],type="p",col="purple")

lines(lowess(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==5,]$salary[1:25]),type="l",col="black")
lines(lowess(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==5,]$salary[1:25]),type="l",col="red")
lines(lowess(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==5,]$salary[1:25]),type="l",col="blue")
lines(lowess(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==5,]$salary[1:25]),type="l",col="green")
lines(lowess(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==5,]$salary[1:25]),type="l",col="purple")
axis(2,at=seq(30000,70000,by=10000))
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),pch="o")
dev.off()

postscript('wisconsin_salary_tables_imputed_05_big5_total.ps')
par(mfrow=c(1,2))
plot(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==4,]$total_pay[1:25],ylim=c(25000,100000),yaxt="n",
     main="Imputed Total Pay Schedules (BA)\n5 Biggest WI Districts, 2004-05",xlab="Total Experience",ylab="$")
lines(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==4,]$total_pay[1:25],type="p",col="red")
lines(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==4,]$total_pay[1:25],type="p",col="blue")
lines(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==4,]$total_pay[1:25],type="p",col="green")
lines(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==4,]$total_pay[1:25],type="p",col="purple")

lines(lowess(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==4,]$total_pay[1:25]),type="l",col="black")
lines(lowess(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==4,]$total_pay[1:25]),type="l",col="red")
lines(lowess(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==4,]$total_pay[1:25]),type="l",col="blue")
lines(lowess(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==4,]$total_pay[1:25]),type="l",col="green")
lines(lowess(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==4,]$total_pay[1:25]),type="l",col="purple")
axis(2,at=seq(30000,100000,by=10000),labels=seq(30000,100000,by=10000),las=2)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),pch="o")

plot(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==5,]$total_pay[1:25],ylim=c(25000,100000),yaxt="n",
     main="Imputed Salary Schedules (MA)\n5 Biggest WI Districts, 2004-05",xlab="Total Experience",ylab="$")
lines(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==5,]$total_pay[1:25],type="p",col="red")
lines(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==5,]$total_pay[1:25],type="p",col="blue")
lines(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==5,]$total_pay[1:25],type="p",col="green")
lines(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==5,]$total_pay[1:25],type="p",col="purple")

lines(lowess(1:25,salary_tables[agency=="3619"&year==2005&highest_degree==5,]$total_pay[1:25]),type="l",col="black")
lines(lowess(1:25,salary_tables[agency=="3269"&year==2005&highest_degree==5,]$total_pay[1:25]),type="l",col="red")
lines(lowess(1:25,salary_tables[agency=="4620"&year==2005&highest_degree==5,]$total_pay[1:25]),type="l",col="blue")
lines(lowess(1:25,salary_tables[agency=="2793"&year==2005&highest_degree==5,]$total_pay[1:25]),type="l",col="green")
lines(lowess(1:25,salary_tables[agency=="2289"&year==2005&highest_degree==5,]$total_pay[1:25]),type="l",col="purple")
axis(2,at=seq(30000,100000,by=10000),labels=seq(30000,100000,by=10000),las=2)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),pch="o")
dev.off()

#now make some summary plots
##teacher wages, teacher experience, and proportion of 1st-year teachers
postscript('wage_exp_series_avg_1996-2015.ps')
par(mfrow=c(2,2))
getSymbols("CPIAUCSL",src='FRED')
infl_oct1<-CPIAUCSL[c(seq(from=as.Date('1995-10-01'),by='years',length.out=21),as.Date('2014-08-01'))]/as.numeric(CPIAUCSL['1995-10-01'])
avg_wage<-teacher_data[,mean(salary,na.rm=T),by=year]$V1
plot(1996:2015,avg_wage,ylim=c(min(avg_wage),max(max(avg_wage),max(avg_wage[1]*infl_oct1))),
     type="l",xlab="Year",ylab="Nominal Wage",xaxt="n",main="Average Nominal Wage Among WI Teachers",lwd=3)
lines(1996:2015,avg_wage[1]*infl_oct1,type="l",lwd=1,col="gray")
abline(v=2012-79/365,col=2,lty=2) #wages measured 3rd Friday of September (the 16th in 2011),
                                  #which came 79 days after the enactment of Act 10 in Wisconsin.
legend("topleft",c("Nominal Wage","PV of 1995-6 Wage"),col=c("black","gray"),lty=1,lwd=c(3,1))
axis(1,at=seq(1996,2015,by=2))
avg_exp<-teacher_data[,mean(total_exp,na.rm=T),by=year]$V1
plot(1996:2015,avg_exp,ylim=range(avg_exp),lwd=3,
     type='l',xlab="Year",ylab="Experience",xaxt="n",main="Average Total Experience Among WI Teachers")
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))
pct_new_teach<-teacher_data[,mean(total_exp<=1,na.rm=T),by=year]$V1
plot(1996:2015,pct_new_teach,ylim=range(pct_new_teach),type='l',lwd=3,
     xlab="Year",ylab="Percentage",xaxt="n",main="Prevalence of New Teachers")
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))
plot(1996:2015,teacher_data[,mean(highest_degree==4,na.rm=T),by=year]$V1,lwd=3,
     ylim=c(0.3,0.7),type='l',xlab="Year",ylab="Percentage",xaxt="n",main="Degree Holdings of Teachers")
lines(1996:2015,teacher_data[,mean(highest_degree==5,na.rm=T),by=year]$V1,col="green",lwd=3)
abline(v=2012-79/365,col=2,lty=2)
axis(1,at=seq(1996,2015,by=2))
legend(2007.5,.7,c("BA","MA"),col=c('black','green'),lty=1,lwd=3,bty="n")
dev.off()

##student data: ACT scores, AP scores, HS completion, and WSAS
act<-fread("act_data_summary.csv")[2,seq(4,14,by=2),with=F]
ap_n<-fread("ap_data_summary.csv")[38,1:7,with=F]
ap<-fread("ap_data_summary.csv")[38,8:14,with=F]/ap_n # % students with 3 or above on all AP exams
ap_calc_ab_n<-fread("ap_data_summary.csv")[3,1:7,with=F]
ap_calc_ab<-fread("ap_data_summary.csv")[3,8:14,with=F]/ap_n # % students with 3 or above on AP CALC AB
hs<-fread("hs_completion_data_summary.csv")
hs_4yr<-colSums(hs[c(1,4,7),seq(4,10,by=2),with=F])/hs[1,seq(3,9,by=2),with=F] #4-year graduation rate
hs_5yr<-colSums(hs[c(2,5,8),seq(4,10,by=2),with=F])/hs[2,seq(3,9,by=2),with=F] #5-year graduation rate
wsas_math_adv_prof<-colSums(fread("wsas_data_summary.csv")[c(5,8),seq(4,20,by=2),with=F]) #Advanced or Proficient rate on state exam
wsas_math_adv<-fread("wsas_data_summary.csv")[5,seq(4,20,by=2),with=F] #Advanced rate on state exam

postscript('student_outcomes_state_level.ps')
par(mfrow=c(2,2))
plot(cbind(2006,2014),c(0,100),type='n',xlab="Year",ylab="% Students",xaxt="n",main="WSAS (State Exam) Score Performance")
axis(1,at=seq(1996,2014,by=2))
lines(cbind(2006:2014,wsas_math_adv_prof),col=1)
lines(cbind(2006:2014,wsas_math_adv),col=3)
legend(2006,100,c("Advanced or Proficient","Advanced"),col=c('black','green'),lty=1,bty="n")
abline(v=2012-79/365,col=2,lty=2)
plot(cbind(2006,2014),c(0.8,1),type='n',xlab="Year",ylab="% Students",xaxt="n",main="Percent of Students Graduating")
axis(1,at=seq(1996,2014,by=2))
lines(cbind(2010:2013,t(hs_4yr)),col=1)
lines(cbind(2010:2013,t(hs_5yr)),col=3)
legend(2006,1,c("4-year rate","5-year rate"),col=c('black','green'),lty=1,bty="n")
abline(v=2012-79/365,col=2,lty=2)
plot(cbind(2006,2014),range(act),type='n',xlab="Year",ylab="ACT Score",xaxt="n",main="Average ACT Score, WI")
axis(1,at=seq(1996,2014,by=2))
lines(cbind(2008:2013,t(act)))
abline(v=2012-79/365,col=2,lty=2)
plot(cbind(2006,2014),c(0,1),type='n',xlab="Year",ylab="% Students",xaxt="n",main="Percent of Students Achieving 3 \nor Above on AP Exams")
axis(1,at=seq(1996,2014,by=2))
lines(cbind(2007:2013,t(ap)),col=1)
lines(cbind(2007:2013,t(ap_calc_ab)),col=3)
legend(2008,.6,c("All exams","Calculus AB"),col=c('black','green'),lty=1,bty="n")
abline(v=2012-79/365,col=2,lty=2)
dev.off()
##########################################################################
#Convert huge WSAS results files to digestible summary statistics by year
#Also ACT score, AP scores, and HS completion data
setwd("/home/michael/Dropbox/Third year/Merlo/")
library(data.table)

rm(list = ls(all = TRUE))
##WSAS first
years<-paste(substr(as.character(105:113),2,3),substr(as.character(106:114),2,3),sep="-")

for (tt in 1:length(years)){
  data<-fread(paste("wsas_current_20",years[tt],".csv",sep=""))
  data<-data[data$GRADE_GROUP=="[All]"&data$DISTRICT_NAME=="[Statewide]"&data$TEST_RESULT!="No WSAS"&data$TEST_GROUP=="WKCE"&data$GROUP_BY=="All Students",]
  data<-data[,c("TEST_SUBJECT","TEST_RESULT","STUDENT_COUNT","PERCENT_OF_GROUP"),with=F]
  setkey(data,TEST_SUBJECT,TEST_RESULT)
  setnames(data,c("STUDENT_COUNT","PERCENT_OF_GROUP"),c(paste(c("student_count","percent_of_group"),substr(years[tt],4,5),sep="_")))
  assign(paste("data_",substr(years[tt],4,5),sep=""),data)
}
rm(data)

wsas_data_summary<-data_06[data_07][data_08][data_09][data_10][data_11][data_12][data_13][data_14]
setnames(wsas_data_summary,key(wsas_data_summary),tolower(key(wsas_data_summary)))
write.csv(wsas_data_summary,file="wsas_data_summary.csv",row.names=F,quote=F)

rm(list = ls(all = TRUE))
##Now ACT scores
years<-paste(substr(as.character(107:112),2,3),substr(as.character(108:113),2,3),sep="-")

for (tt in 1:length(years)){
  data<-fread(paste("act_certified_20",years[tt],".csv",sep=""))
  data<-data[data$GRADE_GROUP=="[All]"&data$DISTRICT_NAME=="[Statewide]"&data$TEST_RESULT!="*"&data$GROUP_BY=="All Students",]
  data<-data[,c("TEST_SUBJECT","TEST_RESULT","STUDENT_COUNT","AVERAGE_SCORE"),with=F]
  setkey(data,TEST_SUBJECT,TEST_RESULT)
  setnames(data,c("STUDENT_COUNT","AVERAGE_SCORE"),c(paste(c("student_count","average_score"),substr(years[tt],4,5),sep="_")))
  assign(paste("data_",substr(years[tt],4,5),sep=""),data)
}
rm(data)

act_data_summary<-data_08[data_09][data_10][data_11][data_12][data_13]
setnames(act_data_summary,key(act_data_summary),tolower(key(act_data_summary)))
write.csv(act_data_summary,file="act_data_summary.csv",row.names=F,quote=F)

rm(list = ls(all = TRUE))
##Now AP scores
years<-paste(substr(as.character(106:112),2,3),substr(as.character(107:113),2,3),sep="-")

for (tt in 1:length(years)){
  data<-fread(paste("ap_certified_20",years[tt],".csv",sep=""))
  data<-data[data$GRADE_GROUP=="[All]"&data$DISTRICT_NAME=="[Statewide]"&data$GROUP_BY=="All Students",]
  data<-data[,c("TEST_SUBJECT","STUDENTS_TESTED","EXAM_COUNT","EXAMS_3_OR_ABOVE"),with=F]
  data$EXAMS_3_OR_ABOVE<-as.numeric(data$EXAMS_3_OR_ABOVE) #privacy laws prevent disclosure when few enough students sit; eliminate asterisks
  setkey(data,TEST_SUBJECT)
  setnames(data,c("STUDENTS_TESTED","EXAM_COUNT","EXAMS_3_OR_ABOVE"),
                c(paste(tolower(c("STUDENTS_TESTED","EXAM_COUNT","EXAMS_3_OR_ABOVE")),substr(years[tt],4,5),sep="_")))
  assign(paste("data_",substr(years[tt],4,5),sep=""),data)
}
rm(data)

ap_data_summary<-data_07[data_08][data_09][data_10][data_11][data_12][data_13]
ap_data_summary<-data_07[data_13][data_09][data_10][data_11][data_12][data_08] #'08 data encompasses the most exams
setnames(ap_data_summary,key(ap_data_summary),tolower(key(ap_data_summary)))
setcolorder(ap_data_summary,sort(names(ap_data_summary)))
write.csv(ap_data_summary,file="ap_data_summary.csv",row.names=F,quote=F)

rm(list = ls(all = TRUE))
##Finally HS completion
years<-paste(substr(as.character(109:112),2,3),substr(as.character(110:113),2,3),sep="-")

for (tt in 1:length(years)){
  data<-fread(paste("hs_completion_certified_20",years[tt],".csv",sep=""))
  data<-data[data$GRADE_GROUP=="[All]"&data$DISTRICT_NAME=="[Statewide]"&data$GROUP_BY=="All Students",]
  data<-data[,c("COMPLETION_STATUS","COHORT_COUNT","TIMEFRAME","STUDENT_COUNT"),with=F]
  setkey(data,COMPLETION_STATUS,TIMEFRAME)
  setnames(data,c("COHORT_COUNT","STUDENT_COUNT"),c(paste(c("cohort_count","student_count"),substr(years[tt],4,5),sep="_")))
  assign(paste("data_",substr(years[tt],4,5),sep=""),data)
}
rm(data)

hs_completion_data_summary<-data_10[data_11][data_12][data_13] #'08 data encompasses the most exams
setnames(hs_completion_data_summary,key(hs_completion_data_summary),tolower(key(hs_completion_data_summary)))
write.csv(hs_completion_data_summary,file="hs_completion_data_summary.csv",row.names=F,quote=F)