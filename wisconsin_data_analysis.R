##Wisconsin Data Exploration

# Package setup & Convenient Functions ####
setwd("/home/michael/Desktop/research/Wisconsin Bargaining")
library(data.table)
library(cobs)
library(doParallel)
library(maptools)
library(spdep)
library(quantmod)

# Convenient functions
create_quantiles<-function(x,num,right=F,include.lowest=T,na.rm=T){
  cut(x,breaks=quantile(x,probs=seq(0,1,by=1/num),na.rm=T),labels=1:num,right=right,include.lowest=include.lowest)
}

#Using the district-level average wage files for the teachers ####
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

#Analysis of Teacher-Level File ####
#Teacher-level data found here:
#http://lbstat.dpi.wi.gov/lbstat_newasr
rm(list = ls(all = TRUE))

## Data import ####

#years are '95 to '13
#read and do some data cleaning/uniforming
for (tt in 1995:2014){
  dummy<-
    fread(paste0(substr(tt,3,4),"staff.csv"),
          drop=which(scan(file=paste0(substr(tt,3,4),"staff.csv"),
                          what="",sep=",",nlines=1,quiet=T)=="filler"),
                 colClasses=fread(paste0(substr(tt,3,4),"dict.csv"))[,V3]
                 )[position_code==53&(agency_type %in% c("01","03","04","1","3","4","49"))
                     &!is.na(salary),][is.na(fringe),fringe:=0]
  
  if (!("nee" %in% names(dummy))){
    #For through 1999-2000, maiden names were stored in parentheses;
    #  some spillover happened thereafter (through roughly 2003-04)                                  
    dummy[,nee:=ifelse(grepl("\\(",last_name),
                       regmatches(last_name,regexpr("(?<=\\().*?(?=\\)|$)",last_name,perl=T)),
                       ifelse(grepl("-",last_name),
                              regmatches(last_name,regexpr(".*(?=-)",last_name,perl=T)),""))]
  }
  strings<-c("first_name","last_name","nee")
  dummy[,paste0(strings,"_clean"):=
          lapply(.SD,function(x){gsub(paste0("\\s+$|^\\s+|\\s+[a-z]\\.?\\s+|",
                                             "^[a-z]\\.?\\s+|\\s+[a-z]\\.?$|"),
                                      "",tolower(gsub("[\\.,']"," ",x)))}),
        .SDcols=strings]
  #If last name deleted hereby, can confuse 
  # matching algorithm to match empty maiden name with ""
  # So, use the placeholder "_" to denote deleted last name
  dummy[last_name_clean=="",last_name_clean:="_"]
  
  #eliminate duplicate observations of teachers who have multiple assignments--
  #  relevant variables (for now) are constant across observations
  #**Not constant within teacher rows: SHOULD CHECK**
  # Also, sidestep the issue of identifying which teacher is which when there
  # are more than one teachers with the same first,last name and birth year
  dummy<-setkey(dummy[!duplicated(id),
                      ][,count:=.N,by=.(first_name_clean,last_name_clean,birth_year)
                        ][count==1,],first_name_clean,last_name_clean,birth_year)
  
  #whether middle names are included is sometimes random; use as backup
  dummy[,first_name2:=ifelse(grepl("\\s",first_name_clean),
                             regmatches(first_name_clean,
                                        regexpr(".*(?=\\s)",first_name_clean,perl=T)),
                             first_name_clean)]
  
  #Be careful--2012 data stores experience differently
  dummy[,local_exp:=if (tt==2011) local_exp else local_exp/10]
  dummy[,total_exp:=if (tt==2011) total_exp else total_exp/10]
  
  #2012 also stores agency & school differently
  dummy[,agency:=if (tt==2011) substr(paste0("000",agency),nchar(agency),nchar(agency)+3) else agency]
  dummy[,school:=if (tt==2011) substr(paste0("000",school),nchar(school),nchar(school)+3) else school]
  
  #some later useful objects
  dummy[,birth_year:=as.integer(birth_year)]
  dummy[,total_exp_floor:=floor(total_exp)]
  dummy[,age:=tt+1-birth_year]
  dummy[,total_pay:=salary+fringe]
  
  #SHOULD TRY AND DELVE INTO THIS MORE LATER, but for now--delete any record of teachers working since before age 17
  # Also delete teachers earning less than $15,000 and older than 40
  # Also delete teachers with degrees besides bachelors and masters
  # Also delete teachers with outside (0,30] years experience
  # Also delete all CESAs
  # Also delete any teacher with fringe pay > salary (probably typo--0.5% of teachers)
  # Also delete one teacher w fringe < 0
  dummy<-dummy[age-total_exp>17&salary>=15000&highest_degree %in% c("4","5")
               &substr(agency,1,2)!="99"&salary>fringe&fringe>=0&total_exp_floor<=30
               &total_exp_floor>0,]
  
  #create year dummy for appending data next
  dummy[,year:=tt+1]
  
  assign(paste("data",tt+1,sep="_"),dummy)
}; rm(tt,dummy,strings)

## Dynamic teacher matching & Master data compilation ####

#***********************************************************
# MATCHING PROCESS: METICULOUSLY MATCH CONSECUTIVE YEARS-- *
#    HAMMER OUT MATCHES AS MUCH ASS POSSIBLE               *
#    BUILDING UP THE ID SYSTEM IN THE PROCESS              *
#***********************************************************

#Count all teachers in year 1
data_1996[,teacher_id:=.I][,c("move_school","move_district","move",
                              "mismatch_yob","mismatch_inits","mismatch_exp"):=NA
                           ][,new_teacher:=as.integer(total_exp<2)][,married:=as.integer(nee_clean!="")]

#More avenues for exploration:
# * Mis-spelling/typos: e.g. ARYLS OELKE <-> ARLYS OELKE
# * Switching maiden/married names: e.g. DONNA M BACKUS <-> DONNA M WAGNER BACKUS
# * Maternity leaves (??)
# * Indicator for certification (deg=4, t-1 -> deg=5, t)
#Problems remaining:
# * find "married" with common maiden name: e.g., JENNIFER DAVIS -> JENNIFER CASHIN DAVIS, JENNIFER SCHMUHL DAVIS
match_on_names<-function(from,to){
  #1) First match anyone who stayed in the same school
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  setkey(from,first_name_clean,last_name_clean,birth_year,agency,school)
  setkey(to,first_name_clean,last_name_clean,birth_year,agency,school
         )[from,
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=0,mismatch_yob=0,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #2) Loosen criteria--find within-district switchers
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY
  setkey(to,first_name_clean,last_name_clean,birth_year,agency
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
                move=1,married=0,mismatch_yob=0,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #3) Loosen criteria--find district switchers
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR
  setkey(to,first_name_clean,last_name_clean,birth_year
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
                move=1,married=0,mismatch_yob=0,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #4) Find anyone who appears to have gotten married
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  setkey(to,first_name_clean,nee_clean,birth_year,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=1,mismatch_yob=0,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  
  #5) married and changed schools
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY
  setkey(to,first_name_clean,nee_clean,birth_year,agency
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
         move=1,married=1,mismatch_yob=0,mismatch_inits=0,
         mismatch_exp=0,new_teacher=0)]
  
  #6) married and changed districts
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR
  setkey(to,first_name_clean,nee_clean,birth_year
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
         move=1,married=1,mismatch_yob=0,mismatch_inits=0,
         mismatch_exp=0,new_teacher=0)]
  #7) now match some stragglers with missing/included middle names & repeat above
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  setkey(from,first_name2,last_name_clean,birth_year,agency,school)
  setkey(to,first_name2,last_name_clean,birth_year,agency,school
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
         move=0,married=0,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #8) stripped first name + school switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY
  setkey(to,first_name2,last_name_clean,birth_year,agency
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
         move=1,married=0,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #9) stripped first name + district switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR
  setkey(to,first_name2,last_name_clean,birth_year
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
         move=1,married=0,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #10) stripped first name + married
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  setkey(to,first_name2,nee_clean,birth_year,agency,school
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
         move=0,married=1,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #11) stripped first name, married, school switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
  setkey(to,first_name2,nee_clean,birth_year,agency,school
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
         move=1,married=1,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #12) stripped first name, married, district switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
  setkey(to,first_name2,nee_clean,birth_year,agency,school
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
         move=1,married=1,mismatch_yob=0,mismatch_inits=1,
         mismatch_exp=0,new_teacher=0)]
  #13) some people appear to be missing birth year only
  #MATCH ON: FIRST NAME | LAST NAME | AGENCY | SCHOOL ID
  setkey(from,first_name_clean,last_name_clean,agency,school)
  setkey(to,first_name_clean,last_name_clean,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=0,mismatch_yob=1,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #14) among missing YOB-only folks, check school switching
  #MATCH ON: FIRST NAME | LAST NAME | AGENCY
  setkey(from,first_name_clean,last_name_clean,agency)
  setkey(to,first_name_clean,last_name_clean,agency
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
                move=1,married=0,mismatch_yob=1,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #15) among missing YOB-only folks, check marriage
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | AGENCY | SCHOOL ID
  setkey(from,first_name_clean,nee_clean,agency,school)
  setkey(to,first_name_clean,last_name_clean,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=1,mismatch_yob=1,mismatch_inits=0,
                mismatch_exp=0,new_teacher=0)]
  #16) among missing YOB-only folks, check marriage + school switch
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | AGENCY
  setkey(from,first_name_clean,nee_clean,agency)
  setkey(to,first_name_clean,last_name_clean,agency
  )[from[!(teacher_id %in% to$teacher_id),],
    `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
         move=1,married=1,mismatch_yob=1,mismatch_inits=0,
         mismatch_exp=0,new_teacher=0)]
  #17) among missing YOB-only folks, check stripped name
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | AGENCY | SCHOOL ID
  setkey(from,first_name2,last_name_clean,agency,school)
  setkey(to,first_name2,last_name_clean,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=0,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=0,new_teacher=0)]
  #18) among missing YOB-only folks, check stripped name + switch school
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | AGENCY
  setkey(from,first_name2,last_name_clean,agency)
  setkey(to,first_name2,last_name_clean,agency
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
                move=1,married=0,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=0,new_teacher=0)]
  #19) among missing YOB-only folks, check stripped name + married
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME->MAIDEN NAME | AGENCY | SCHOOL ID
  setkey(from,first_name2,last_name_clean,agency,school)
  setkey(to,first_name2,nee_clean,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=0,move_district=0,
                move=0,married=1,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=0,new_teacher=0)]
  #20) among missing YOB-only folks, check stripped name + married + switch school
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME->MAIDEN NAME | AGENCY
  setkey(from,first_name2,last_name_clean,agency,school)
  setkey(to,first_name2,nee_clean,agency,school
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=0,
                move=1,married=1,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=0,new_teacher=0)]
  #21) among missing YOB-only folks, check if experience is within a year
  #MATCH ON: FIRST NAME | LAST NAME | EXPERIENCE
  setkey(from,first_name_clean,last_name_clean,total_exp)
  setkey(to,first_name_clean,last_name_clean,total_exp
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
                move=1,married=0,mismatch_yob=1,mismatch_inits=0,
                mismatch_exp=1,new_teacher=0),roll=-1L]
  #22) among missing YOB-only folks, check experience + marriage
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | EXPERIENCE
  setkey(from,first_name_clean,last_name_clean,total_exp)
  setkey(to,first_name_clean,nee_clean,total_exp
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
                move=1,married=1,mismatch_yob=1,mismatch_inits=0,
                mismatch_exp=1,new_teacher=0),roll=-1L]
  #23) among missing YOB-only folks, check experience + first name mismatch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | EXPERIENCE
  setkey(from,first_name2,last_name_clean,total_exp)
  setkey(to,first_name2,last_name_clean,total_exp
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
                move=1,married=0,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=1,new_teacher=0),roll=-1L]
  #24) among missing YOB-only folks, check experience + first name mismatch + married
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME->MAIDEN NAME | EXPERIENCE
  setkey(from,first_name2,last_name_clean,total_exp)
  setkey(to,first_name2,nee_clean,total_exp
         )[from[!(teacher_id %in% to$teacher_id),],
           `:=`(teacher_id=i.teacher_id,move_school=1,move_district=1,
                move=1,married=1,mismatch_yob=1,mismatch_inits=1,
                mismatch_exp=1,new_teacher=0),roll=-1L]
                                  
  #25) finally, give up and assign new ids to new (read: unmatched) teachers
  to[is.na(teacher_id),`:=`(teacher_id=.I+max(from$teacher_id),move_school=0,move_district=0,
                            move=0,married=0,mismatch_yob=0,mismatch_inits=0,
                            mismatch_exp=0,new_teacher=1)]
  #Now look forward from `from`--if we matched them, they didn't quit/retire
  from[teacher_id %in% to$teacher_id   ,quit:=0]
  from[!(teacher_id %in% to$teacher_id),quit:=1]
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
            fill=T); rm(list=ls(pattern="data_"))

teacher_data[,multiples:=.N,by=.(teacher_id,year)]
#Delete history for any multi-matched teachers (down to only a few at this point)
teacher_data<-teacher_data[!(teacher_id %in% teacher_data[multiples!=1,teacher_id]),]
setkey(teacher_data,teacher_id,year)

#some full-sample variables:
## did the teacher certify?
teacher_data[,certified:=shift(highest_degree)[[1]]!=highest_degree,by=teacher_id]
## how many years of data do we have for each teacher?
teacher_data[,years_tracked:=year[.N]-year[1]+1,by=teacher_id]
## useful to track first and last recorded year for each teacher
teacher_data[,c("first_year","last_year"):=.(year[1],year[.N]),by=teacher_id]
## is my trajectory for this teacher censored by the data's beginning or end?
teacher_data[,paste0(c("left_","right_"),"censored"):=.(first_year==1996,last_year==2015),by=teacher_id]
## add forward-looking indicators
frwd_vars<-c(paste0("move",c("","_school","_district")),"married","quit","salary","fringe","total_pay","certified")
teacher_data[,paste0(frwd_vars,"_next"):=shift(.SD,type="lead"),by=teacher_id,.SDcols=frwd_vars]
teacher_data[,paste0(frwd_vars,"_pl5"):=shift(.SD,n=5L,type="lead"),by=teacher_id,.SDcols=frwd_vars]; rm(frwd_vars)

## Pay Scale Interpolation ####
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
           (1:(40-max_exp))*(in_sample[length(in_sample)]-
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

#provide deflated wage data
dollar_cols<-c("salary","fringe","total_pay")
getSymbols("CPIAUCSL",src='FRED')
infl<-data.table(year=1996:2015,
                 index=CPIAUCSL[seq(from=as.Date('1995-10-01'),
                                    by='years',length.out=21)]/
                   as.numeric(CPIAUCSL['1995-10-01']),key="year")
teacher_data[infl,index:=index.CPIAUCSL]
teacher_data[,paste0(dollar_cols,"_real"):=lapply(.SD,function(x){x/teacher_data$index}),.SDcols=dollar_cols][,index:=NULL]
salary_scales[infl,index:=index.CPIAUCSL]
salary_scales[,paste0(dollar_cols,"_real"):=lapply(.SD,function(x){x/salary_scales$index}),.SDcols=dollar_cols][,index:=NULL]
rm(infl,dollar_cols)

write.csv(salary_scales,"wisconsin_salary_scales_imputed.csv",row.names=F)
salary_scales<-setkey(fread("wisconsin_salary_scales_imputed.csv"),year,agency,highest_degree,total_exp_floor)

## Spatial Data ####

wi_districts_elem_shp<-
  readShapeSpatial("/media/data_drive/gis_data/WI/wisconsin_elementary_districts_statewide.shp")
wi_districts_scdy_shp<-
  readShapeSpatial("/media/data_drive/gis_data/WI/wisconsin_secondary_districts_statewide.shp")

elem_nbhd<-nb2mat(poly2nb(wi_districts_elem_shp,row.names=wi_districts_elem_shp$GEOID10),style="B")
colnames(elem_nbhd)<-rownames(elem_nbhd)

scdy_nbhd<-nb2mat(poly2nb(wi_districts_scdy_shp,row.names=wi_districts_scdy_shp$GEOID10),style="B")
colnames(scdy_nbhd)<-rownames(scdy_nbhd)

### Need NCES Common Core Data to map polygon IDs to Agency ID
#### 2011-12 data (all but 9 districts)
ccd_id<-setnames(fread("/media/data_drive/common_core/district/universe_11_12_1a.txt",
                       select=c("LEAID","STID"),colClasses="character"
                       )[substr(LEAID,1,2)=="55",],c("leaid","agency"))
#### Add remaining 9 districts via CCD data in 1995-96
ccd_id<-
  setkey(rbindlist(list(
  ccd_id,setnames(data.table(read.fwf(
    "/media/data_drive/common_core/district/universe_95_96_1a.txt",
    widths=c(7,4)))[substr(V1,1,2)=="55",][V2 %in% teacher_data[!(agency %in% ccd_id$agency),
                                                                unique(agency)],],c("leaid","agency")))),leaid)

####Exclude districts not found in shapefile
elem_nbhd<-elem_nbhd[intersect(rownames(elem_nbhd),ccd_id$leaid),intersect(rownames(elem_nbhd),ccd_id$leaid)]
scdy_nbhd<-scdy_nbhd[intersect(rownames(scdy_nbhd),ccd_id$leaid),intersect(rownames(scdy_nbhd),ccd_id$leaid)]
####Rename rows from CCD LEA ID to Agency ID
rownames(elem_nbhd)<-colnames(elem_nbhd)<-ccd_id[.(colnames(elem_nbhd)),agency]
rownames(scdy_nbhd)<-colnames(scdy_nbhd)<-ccd_id[.(colnames(scdy_nbhd)),agency]
#***TURNS OUT ELEMENTARY AND SECONDARY ARE IDENTICAL--GET TO SIDESTEP THIS ISSUE FOR NOW***

####Get local average wages and local experience-dependent average discounted future pay
area_average<-function(pay_type="total_pay",aagency,yyear,hhighest_degree,ttotal_exp_floor=NULL){
  salary_scales[.(yyear[1],names(which(elem_nbhd[aagency[1],]!=0)),hhighest_degree[1],ttotal_exp_floor[1]),
                mean(get(pay_type),na.rm=T)]
}
setkey(teacher_data,agency)
teacher_data[.(rownames(elem_nbhd)),
             total_pay_loc_avg:=area_average("total_pay",agency,year,highest_degree),
             by=.(agency,year,highest_degree)]

#Grouping by agency, year, degree AND experience adds a lot to computation--paralellize by experience.
cl <- makeCluster(detectCores()); 
registerDoParallel(cl);
teacher_data_sub<-teacher_data[,.(agency,total_pay_future,year,highest_degree,total_exp_floor)]
clusterExport(cl,c("teacher_data_sub"),envir=environment());
clusterEvalQ(cl,library("data.table"));
tpf <- foreach(i = 1:30) %dopar% {
  teacher_data_sub[total_exp_floor==i,][.(rownames(elem_nbhd)),
      total_pay_loc_val:=area_average("total_pay_future",agency,year,highest_degree,total_exp_floor),
      by=.(agency,year,highest_degree,total_exp_floor)]
}
stopCluster(cl)
rm(teacher_data_sub,cl)
tpf<-setkey(do.call("rbind",tpf),year,agency,highest_degree,total_exp_floor)
setkey(teacher_data,year,agency,highest_degree,total_exp_floor)[tpf,total_pay_loc_val:=total_pay_loc_val]
rm(tpf)

teacher_data[,potential_earnings:=total_pay_loc_val-total_pay_future]

# Finally, some data analysis ####

plot_given<-function(yr,ag,hd){
  ag<-as.character(ag)
  hd<-as.character(hd)
  salary_scales[.(yr,ag,hd),plot(total_exp_floor,salary,type="l",col="black",lwd=3,xlab="Experience",ylab="$",
                                 ylim=range(salary_scales[.(yr,ag,hd),c("salary","total_pay"),with=F],
                                            teacher_data[.(yr,ag,hd),c("salary","total_pay"),with=F]))]
  salary_scales[.(yr,ag,hd),lines(total_exp_floor,total_pay,col="red",lwd=3)]
  teacher_data[.(yr,ag,hd),points(total_exp_floor,salary,col="black")]
  teacher_data[.(yr,ag,hd),points(total_exp_floor,total_pay,col="red",pch=3)]
}

#plotting the salary schedules for the 5 most populous agencies over the first 30 years
postscript('wisconsin_salary_tables_imputed_05_big5_base.ps')
par(mfrow=c(1,2))
matplot(1:40,dcast.data.table(salary_scales[.(2005,c("3619","3269","4620","2793","2289"),"4"),
                               !c("fringe","total_pay"),with=F],
                 total_exp_floor~agency,value.var="salary")[,!"total_exp_floor",with=F],
        xlab="Experience",ylab="$",main="Imputed Salary Schedules (BA)\n5 Biggest WI Districts, 2004-05",
        lty=1,type="l",col=c("black","red","blue","green","purple"),lwd=3)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)

matplot(1:40,dcast.data.table(salary_scales[.(2005,c("3619","3269","4620","2793","2289"),"5"),
                                            !c("fringe","total_pay"),with=F],
                              total_exp_floor~agency,value.var="salary")[,!"total_exp_floor",with=F],
        xlab="Experience",ylab="$",main="Imputed Salary Schedules (MA)\n5 Biggest WI Districts, 2004-05",
        lty=1,type="l",col=c("black","red","blue","green","purple"),lwd=3)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)
dev.off()

postscript('wisconsin_salary_tables_imputed_05_big5_total.ps')
par(mfrow=c(1,2))
matplot(1:40,dcast.data.table(salary_scales[.(2005,c("3619","3269","4620","2793","2289"),"4"),
                                            !c("fringe","salary"),with=F],
                              total_exp_floor~agency,value.var="total_pay")[,!"total_exp_floor",with=F],
        xlab="Experience",ylab="$",main="Imputed Total Pay Schedules (BA)\n5 Biggest WI Districts, 2004-05",
        lty=1,type="l",col=c("black","red","blue","green","purple"),lwd=3)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)

matplot(1:40,dcast.data.table(salary_scales[.(2005,c("3619","3269","4620","2793","2289"),"5"),
                                            !c("fringe","salary"),with=F],
                              total_exp_floor~agency,value.var="total_pay")[,!"total_exp_floor",with=F],
        xlab="Experience",ylab="$",main="Imputed Total Pay Schedules (MA)\n5 Biggest WI Districts, 2004-05",
        lty=1,type="l",col=c("black","red","blue","green","purple"),lwd=3)
legend("bottomright",legend=c("Milwaukee","Madison","Racine","Kenosha","Green Bay"),
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)
dev.off()

#now make some summary plots
##teacher wages, teacher experience, and proportion of 1st-year teachers
postscript('wage_exp_series_avg_1996-2015.ps')
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

#****\/ STILL NEEDS TO BE MODERNIZED \/***************
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