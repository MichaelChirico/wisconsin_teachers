#Wisconsin Data Exploration

# Package setup & Convenient Functions ####
rm(list=ls(all=T))
gc()
setwd("/home/michael/Desktop/research/Wisconsin Bargaining")
library(data.table)
library(cobs)
library(doParallel)
library(maptools)
library(spdep)
library(quantmod)
library(xtable)
library(xlsx)
library(nnet)
library(texreg)

# Convenient functions ####
create_quantiles<-function(x,num,right=F,include.lowest=T,na.rm=T){
  cut(x,breaks=quantile(x,probs=seq(0,1,by=1/num),na.rm=T),labels=1:num,right=right,include.lowest=include.lowest)
}

table2<-function(...,dig=NULL,prop=F,ord=F,pct=F){
  dots<-list(...)
  args<-names(dots) %in% names(formals(prop.table))
  tab<-if (prop) do.call(
    'prop.table',c(list(
      do.call('table',if (length(args)) dots[!args] else dots)),
      dots[args])) else do.call('table',list(...))
  if (ord) tab<-tab[order(tab)]
  if (pct) tab<-100*tab
  if (is.null(dig)) tab else round(tab,digits=dig)
}


print.xtable2<-function(...){
  #For pretty copy-pasting into LyX
  cat(capture.output(do.call('print.xtable',list(...))),sep="\n\n")
}

ntostr<-function(n,digits=2){
  paste0(ifelse(log10(n)<digits-1,
                substr(n+10^digits,2,digits+1),
                ifelse(log10(n)>=digits,
                       substr(n,nchar(n)-digits+1,nchar(n)),
                       n)))
}

pdf2<-function(...){
  graphics.off()
  dev.new()
  do.call('pdf',list(...))
  dev.set(which=dev.list()["RStudioGD"])
}

dev.off2<-function(){
  dev.copy(which=dev.list()["pdf"])
  invisible(dev.off(which=dev.list()["pdf"]))
}

# Data import ####
#   Raw data available here: http://lbstat.dpi.wi.gov/lbstat_newasr
#   (most available in fixed width; these were converted using
#   the Python file fixed_to_csv.py along with hand-made
#   variable name dictionaries XXdict.csv)
names<-c("first_name","last_name","nee")
full_data<-rbindlist(lapply(ntostr(95:114),function(x){
  #simultaneously read, concatenate all 20 years' data files
  fread(paste0("/media/data_drive/wisconsin/",x,"staff.csv"),
        drop=which(scan(file=paste0("/media/data_drive/wisconsin/",x,"staff.csv"),
                        what="",sep=",",nlines=1,quiet=T)=="filler"),
        colClasses=fread(paste0("/media/data_drive/wisconsin/",x,"dict.csv"),select=3)$V3)}),
  #fill=T is necessary because fields are not constant across time
  fill=T)[,year:=as.integer(substr(year_session,1,4))+1L
          #storage of some fields inconsistent across time; start unifying here
          ][year<2005,area:=paste0("0",area)
            #trim extraneous white space from names and force lowercase
            ][,(names):=lapply(.SD,function(x)gsub("^\\s+|\\s+$","",
                                                   gsub("\\b\\s{2,}\\b"," ",
                                                        tolower(x)))),
              .SDcols=names]

#Try to impute maiden name when it seems to have been stored in the last name
#  Specifically, when nee is empty, look for names with "(" or "-"; maiden names appear to be
#  stored between parentheses and before hyphens (not a perfect system); note that
#  full_data[gsub("\\s","",nee)=="",names(table(nee))] shows that when missing nee is stored as
#  a string, there is only one possible length of that string.
full_data[(is.na(nee)|nee=="")
          &grepl("\\(|-",last_name),nee:=
            regmatches(last_name,regexpr("(?<=\\().*?(?=\\)|$)|.*(?=-)",last_name,perl=T))]

#Try to get a "clean" version of first, last, and maiden names;
#  Delete *all* white space, initials
full_data[,paste0(names,"_clean"):=
            lapply(.SD,function(x){gsub("\\s|\\b[a-z]\\b","",
                                        gsub("[\\.,'\\-]","",x))}),
          .SDcols=names]
##Names like W C Fields were deleted; restore them
lapply(names,function(x)full_data[get(paste0(x,"_clean"))=="",
                                  paste0(x,"_clean"):=get(x)])
rm(names)
#Delete anyone who cannot possibly be identified below:
#  Namely, those who match another exactly on (cleaned) first/last name,
#  birth year and year of data
#  Also reset NA fringe values to 0, and remove anyone with a missing
#  year of birth
full_data<-full_data[,if (uniqueN(id)==1) .SD,
                     by=.(first_name_clean,last_name_clean,
                          birth_year,year)
                     ][is.na(fringe),fringe:=0]

#Will also use a different cleaned version of the first name--
#  Delete everything after the first space
full_data[,first_name2:=gsub("\\s.*","",first_name)]

#Data stored differently in 2011-12; need to append 0s for consistency
add0s<-c("agency","school","area")
full_data[year==2012,(add0s):=lapply(.SD,function(x)
  substr(paste0("000",x),nchar(x),nchar(x)+3)),.SDcols=add0s]; rm(add0s)
full_data[year==2012&nchar(position_code)==1,position_code:=paste0("0",position_code)]

#Again, 2012 already has experience defined correctly; other years express as 10ths
div10<-paste0(c("local","total"),"_exp")
full_data[year!=2012,(div10):=lapply(.SD,function(x)x/10),.SDcols=div10]; rm(div10)

#Reformat/create some variables
full_data[,c("birth_year","total_exp_floor","total_pay","agency_work_type","agency_hire_type"):=
            list(as.integer(birth_year),floor(total_exp),salary+fringe,
                 as.integer(agency_work_type),as.integer(agency_hire_type))][,age:=year-birth_year]
#Create ethnicity dummies, and separate the "main" ethnicities
ethnames<-c("black","hispanic","white")
ethcodes<-c("B","H","W")
setkey(full_data[,(ethnames):=lapply(ethcodes,function(x)ethnicity==x)],ethnicity); rm(ethnames,ethcodes)
full_data["B",ethnicity_main:="Black"]
full_data["H",ethnicity_main:="Hispanic"]
full_data["W",ethnicity_main:="White"]
full_data[,ethnicity_main:=factor(ethnicity_main)]

# Matching algorithm ####
#Initialize ID system & matching flags
setkey(full_data,id)[setkey(unique(full_data[year==1996,.(id)])[,teacher_id:=.I],id),
                     teacher_id:=ifelse(year==1996,i.teacher_id,NA)]
flags<-c("move_district","move_school","married",
         paste0("mismatch_",c("inits","exp")),"step")
full_data[year==1996,c("new_teacher",flags):=
            list(total_exp<2,F,F,F,F,F,0L)]

update_cols<-c("teacher_id",flags)

#Main matching function--given keys in previous years and key in current year,
#  match and assign flags as necessary; note that setting keys repeatedly is
#  quite (and increasingly, as more previous years are included) costly;
#  may be worth looking into using Pandas to speed up this process.
get_id<-function(yr,key_crnt,key_prev=key_crnt,
                 mdis,msch,mard,init,mexp,step){
  #Want to exclude anyone who is matched
  existing_ids<-full_data[.(yr),unique(na.omit(teacher_id))]
  #Get the most recent prior observation of all unmatched teachers
  unmatched<-
    full_data[.(1996:(yr-1))
              ][!teacher_id %in% existing_ids,
                #.N here gives the most recent observation
                #  of the teacher, in their "highest-intensity"
                #  position (b/c ordered by FTE)
                .SD[.N],by=teacher_id,
                .SDcols=c(key_prev,"teacher_id")
                #if .N>1, there is more than 1 teacher matched by
                #  keyfrom among the past teachers; 
                ][,if (.N==1L) .SD,keyby=key_prev
                  ][,(flags):=list(mdis,msch,mard,init,mexp,step)]
  #Merge, reset keys
  setkey(setkeyv(
    full_data,key_crnt)[year==yr&is.na(teacher_id),
                        (update_cols):=unmatched[.SD,update_cols,with=F]],
    year)

  #assign the ID and flags to all observations of matched teachers in the
  #  the current year (within-year, they can be identified by shared id)
  full_data[.(yr),(update_cols):=lapply(.SD,function(x)na.omit(x)[1]),
                                        by=id,.SDcols=update_cols]
}

setkey(full_data,year,full_time_equiv)

system.time(for (yy in 1997:2015){
  print(yy)
  #1) First match anyone who stayed in the same school
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  get_id(yy,c("first_name_clean","last_name_clean","birth_year","agency","school"),
         mdis=F,msch=F,mard=F,init=F,mexp=F,step=1L)
  #2) Loosen criteria--find within-district switchers
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY
  get_id(yy,c("first_name_clean","last_name_clean","birth_year","agency"),
         mdis=F,msch=T,mard=F,init=F,mexp=F,step=2L)
  #3) Loosen criteria--find district switchers
  #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR
  get_id(yy,c("first_name_clean","last_name_clean","birth_year"),
         mdis=T,msch=T,mard=F,init=F,mexp=F,step=3L)
  #4) Find anyone who appears to have gotten married
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  get_id(yy,c("first_name_clean","last_name_clean","birth_year","agency","school"),
            c("first_name_clean","nee_clean"      ,"birth_year","agency","school"),
         mdis=F,msch=F,mard=T,init=F,mexp=F,step=4L)
  #5) married and changed schools
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY
  get_id(yy,c("first_name_clean","last_name_clean","birth_year","agency"),
            c("first_name_clean","nee_clean"      ,"birth_year","agency"),
         mdis=F,msch=T,mard=T,init=F,mexp=F,step=5L)
  #6) married and changed districts
  #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR
  get_id(yy,c("first_name_clean","last_name_clean","birth_year"),
            c("first_name_clean","nee_clean"      ,"birth_year"),
         mdis=T,msch=T,mard=T,init=F,mexp=F,step=6L)
  #7) now match some stragglers with missing/included middle names & repeat above
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  get_id(yy,c("first_name2","last_name_clean","birth_year","agency","school"),
         mdis=F,msch=F,mard=F,init=T,mexp=F,step=7L)
  #8) stripped first name + school switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY
  get_id(yy,c("first_name2","last_name_clean","birth_year","agency"),
         mdis=F,msch=T,mard=F,init=T,mexp=F,step=8L)
  #9) stripped first name + district switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR
  get_id(yy,c("first_name2","last_name_clean","birth_year"),
         mdis=T,msch=T,mard=F,init=T,mexp=F,step=9L)
  #10) stripped first name + married
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY | SCHOOL ID
  get_id(yy,c("first_name2","last_name_clean","birth_year","agency","school"),
            c("first_name2","nee_clean"      ,"birth_year","agency","school"),
         mdis=F,msch=F,mard=T,init=T,mexp=F,step=10L)
  #11) stripped first name, married, school switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
  get_id(yy,c("first_name2","last_name_clean","birth_year","agency"),
            c("first_name2","nee_clean"      ,"birth_year","agency"),
         mdis=F,msch=T,mard=T,init=T,mexp=F,step=11L)
  #12) stripped first name, married, district switch
  #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
  get_id(yy,c("first_name2","last_name_clean","birth_year"),
            c("first_name2","nee_clean"      ,"birth_year"),
         mdis=T,msch=T,mard=T,init=T,mexp=F,step=12L)
  #13) finally, give up and assign new ids to new (read: unmatched) teachers
  current_max<-full_data[.(yy),max(teacher_id,na.rm=T)]
  new_ids<-
    setkey(full_data[year==yy&is.na(teacher_id),.(id=unique(id))
                     ][,add_id:=.I+current_max],id)
  setkey(setkey(full_data,id)[year==yy&is.na(teacher_id),
                              teacher_id:=new_ids[.SD,add_id]],year)
  #Picked up some stragglers by accident--worth checking if this can be improved
  #  But current mistakes are .09%
  full_data[teacher_id %in% full_data[.(1996:yy),uniqueN(id),by=teacher_id
                                      ][V1>yy-1995,teacher_id],teacher_id:=NA]
}); rm(yy,update_cols,flags,current_max,new_ids)

#Use last observation carried forward (LOCF) to synergize maiden names
mns<-paste0("nee",c("","_clean"))
full_data[,(mns):=lapply(.SD,na.locf,na.rm=F),by=teacher_id,.SDcols=mns]; rm(mns)

#Eliminate teachers *with at least some positions*
#  that don't satisfy some criteria/data cleanliness
#  - Agency Work Type: 4 is schools
#  - Months Employed (pre-2005) / Days of Contract (post-):
#      data-based reasonable minima chosen
#  - Salary: data-based reasonable minimum
#  - Non-Working Life Years: teachers should have been
#      at least 17 when they started working
#  - Highest Degree: Focus on Bachelor's/Master's holders
#  - Salary vs. Fringe : Fringe should not be higher than salary
#  - Fringe: Should be non-negative
#  - Agency Code: 99xx corresponds to administrative positions
#  - Total Experience: Ignore 31+ year teachers and 0-year teachers
#  - Area: Focus on math, reading, and all-purpose (elem.) teachers
#  - Position Code: 53 means full-time teacher
full_data<-setkey(full_data[!is.na(teacher_id)],teacher_id
                  )[.(full_data[agency_work_type==4
                                &ifelse(year<2005,months_employed>=875,
                                        days_of_contract>=175)
                                &salary>=15000&age-total_exp>17
                                &highest_degree %in% c("4","5")
                                &salary>fringe&fringe>=0
                                &substr(agency,1,2)!="99"
                                &total_exp_floor>0
                                &total_exp_floor<=31
                                &area %in% c("0050","0300","0400")
                                &position_code==53,unique(teacher_id)])]

#Now remove anyone who *still* has more than one position in any given year--
#  will be basically impossible to distinguish their position.
full_data<-
  setkey(full_data,teacher_id)[!.(unique(full_data[,if (.N>1) teacher_id,by=.(year,teacher_id)]$V1))]


#some full-sample variables:
## identify the "earliest" years for each teacher
##  (could help identify returners)
teacher_data[,is_first_year:=year==year[1],by=teacher_id]
## count number of job-to-job transitions per teacher
teacher_data[,count_moves:=sum(move),by=teacher_id]
## did the teacher certify?
teacher_data[,certified:=shift(highest_degree)[[1]]!=highest_degree,by=teacher_id]
## how many years of data do we have for each teacher?
teacher_data[,years_tracked:=.N,by=teacher_id]
## useful to track first and last recorded year for each teacher
teacher_data[,c("first_year","last_year"):=.(year[1],year[.N]),by=teacher_id]
## is my trajectory for this teacher censored by the data's beginning or end?
teacher_data[,paste0(c("left_","right_"),"censored"):=.(first_year==1996,last_year==2015),by=teacher_id]

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
teacher_data[,paste0(frwd_vars,"_next"):=shift(.SD,type="lead"),by=teacher_id,.SDcols=frwd_vars]
teacher_data[,paste0(frwd_vars,"_pl5"):=shift(.SD,n=5L,type="lead"),by=teacher_id,.SDcols=frwd_vars]; rm(frwd_vars)

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
teacher_data[,paste0(dollar_cols,"_real"):=lapply(.SD,function(x){x/teacher_data$index}),.SDcols=dollar_cols][,index:=NULL]
salary_scales[infl,index:=index.CPIAUCSL]
salary_scales[,paste0(dollar_cols,"_real"):=lapply(.SD,function(x){x/salary_scales$index}),.SDcols=dollar_cols][,index:=NULL]
rm(infl,dollar_cols)

write.csv(salary_scales,"wisconsin_salary_scales_imputed.csv",row.names=F)
salary_scales<-setkey(fread("wisconsin_salary_scales_imputed.csv"),year,agency,highest_degree,total_exp_floor)

# Spatial Data ####

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
ccd_id<-setnames(fread("/media/data_drive/common_core/district/universe_2011_12_1a.txt",
                       select=c("LEAID","STID"),colClasses="character"
)[substr(LEAID,1,2)=="55",],c("leaid","agency"))
#### Add remaining 9 districts via CCD data in 1995-96
ccd_id<-
  setkey(rbindlist(list(
    ccd_id,setnames(data.table(read.fwf(
      "/media/data_drive/common_core/district/universe_1995_96_1a.txt",
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

# District Demographic Data ####
### Common Core of Data
### ***SHOULD EXPAND TO OTHER YEARS BY AGGREGATING SCHOOL-LEVEL FILES***
ccd<-setkey(setnames(
  rbindlist(lapply(c("2010_11_2a","2011_12_1a","2012_13_1a"),
                   function(x){fread(paste0("/media/data_drive/common_core/",
                                            "district/universe_",x,".txt"),
                                     select=c("FIPST","STID","ULOCAL","HISP","BLACK","MEMBER"),
                                     colClasses=list(character=c("FIPST","STID"),
                                                     factor="ULOCAL",
                                                     integer=c("HISP","BLACK","MEMBER"))
                   )[FIPST=="55",][,FIPST:=NULL][,year:=2000+as.numeric(substr(x,6,7))]})),
  c("agency","ulocal","n_students","n_hispanic","n_black","year")),year,agency)[n_students>0,]

#Eliminate schools not present all 3 years
ccd<-ccd[!agency %in% ccd[,.N,by=agency][N<3,agency],]

ccd[,pct_hispanic:=n_hispanic/n_students
    ][,pct_hispanic_pctl:=ecdf(pct_hispanic)(pct_hispanic)]
ccd[,pct_black:=n_black/n_students
    ][,pct_black_pctl:=ecdf(pct_black)(pct_black)]
ccd[,urbanicity:=ifelse(substr(ulocal,1,1)=="1","City",
                        ifelse(substr(ulocal,1,1)=="2","Suburb",
                               ifelse(substr(ulocal,1,1)=="3","Town","Rural")))]

setkey(teacher_data,year,agency)[ccd,`:=`(pct_hispanic=i.pct_hispanic,
                                          pct_hispanic_pctl=i.pct_hispanic_pctl,
                                          pct_black=i.pct_black,
                                          pct_black_pctl=i.pct_black_pctl,
                                          urbanicity=i.urbanicity)]

setkey(teacher_data,year,agency_next)[ccd,`:=`(pct_hispanic_next=i.pct_hispanic,
                                               pct_hispanic_pctl_next=i.pct_hispanic_pctl,
                                               pct_black_next=i.pct_black,
                                               pct_black_pctl_next=i.pct_black_pctl,
                                               urbanicity_next=i.urbanicity)]

###School Testing Data

####WSAS Wisconsin Student Assessment System
####WKCE Wisconsin Knowledge and Concepts Examination
#### via: http://oea.dpi.wi.gov/assessment/data/WKCE/proficiency
#### Only use mathematics scores for now
wsas_data<-setkey(setnames(
  rbindlist(lapply(paste(ntostr(5:13),ntostr(6:14),sep="-"),
                   function(x){fread(paste0("/media/data_drive/wisconsin/",
                                            "wsas_certified_20",x,".csv"),na.strings="*"
                   )[GRADE_GROUP=="[All]"&DISTRICT_CODE!="0000"&
                       !TEST_RESULT %in% c("*","No WSAS")
                     &TEST_GROUP=="WKCE"&GROUP_BY=="All Students"
                     &TEST_SUBJECT=="Mathematics"
                     &TEST_RESULT_CODE %in% c(3,4),
                     ][,.(year=2000+as.numeric(substr(x,4,5)),
                          pct_prof_wsas=sum(as.numeric(PERCENT_OF_GROUP))),
                       by=DISTRICT_CODE][,pct_prof_wsas_pctl:=ecdf(pct_prof_wsas)(pct_prof_wsas)]})),
  "DISTRICT_CODE","stid"),year,stid)

setkey(teacher_data,year,agency
)[wsas_data,`:=`(pct_prof_wsas=i.pct_prof_wsas,
                 pct_prof_wsas_pctl=i.pct_prof_wsas_pctl)]

setkey(teacher_data,year,agency_next
)[wsas_data,`:=`(pct_prof_wsas_next=i.pct_prof_wsas,
                 pct_prof_wsas_pctl_next=i.pct_prof_wsas_pctl)]

####Now ACT scores
#### via: http://wise.dpi.wi.gov/wisedash_downloadfiles
#### Only look at Math for now
act_data<-setkey(setnames(
  rbindlist(lapply(paste(ntostr(7:13),ntostr(8:14),sep="-"),
                   function(x){fread(paste0("/media/data_drive/wisconsin/",
                                            "act_certified_20",x,".csv"),na.strings="*"
                   )[GRADE_GROUP=="[All]"&DISTRICT_CODE!="0000"&TEST_RESULT!="*"
                     &GROUP_BY=="All Students"&TEST_RESULT=="College ready"
                     &TEST_SUBJECT=="Mathematics",
                     ][,.(DISTRICT_CODE,year=2000+as.numeric(substr(x,4,5)),
                          pct_college_ready=
                            as.numeric(STUDENT_COUNT)/
                            as.numeric(GROUP_COUNT))
                       ][,pct_coll_ready_pctl:=ecdf(pct_college_ready)(pct_college_ready)]})),
  "DISTRICT_CODE","stid"),year,stid)

setkey(teacher_data,year,agency
)[act_data,`:=`(pct_college_ready=i.pct_college_ready,
                pct_coll_ready_pctl=i.pct_coll_ready_pctl)]

setkey(teacher_data,year,agency_next
)[act_data,`:=`(pct_college_ready_next=i.pct_college_ready,
                pct_coll_ready_pctl_next=i.pct_coll_ready_pctl)]

####Now AP scores
#### only look at Calc AB for now
ap_data<-setkey(setnames(
  rbindlist(lapply(paste(ntostr(6:13),ntostr(7:14),sep="-"),
                   function(x){fread(paste0("/media/data_drive/wisconsin/",
                                            "ap_certified_20",x,".csv"),na.strings="*"
                   )[DISTRICT_CODE!="0000"&TEST_SUBJECT=="Calculus AB"
                     &GROUP_BY=="All Students"&GRADE_GROUP=="[All]"&
                       !is.na(PERCENT_3_OR_ABOVE),
                     ][,.(DISTRICT_CODE,year=2000+as.numeric(substr(x,4,5)),PERCENT_3_OR_ABOVE)
                       ][,pct_3_or_above_pctl:=ecdf(PERCENT_3_OR_ABOVE)(PERCENT_3_OR_ABOVE)]})),
  c("DISTRICT_CODE","PERCENT_3_OR_ABOVE"),c("stid","pct_3_or_above")),year,stid)

setkey(teacher_data,year,agency
)[ap_data,`:=`(pct_3_or_above=i.pct_3_or_above,
               pct_3_or_above_pctl=i.pct_3_or_above_pctl)]


setkey(teacher_data,year,agency_next
)[ap_data,`:=`(pct_3_or_above_next=i.pct_3_or_above,
               pct_3_or_above_pctl_next=i.pct_3_or_above_pctl)]

####Finally HS completion
#### Only look at regular completion for now
#### Details on differences: http://lbstat.dpi.wi.gov/lbstat_datahsc
dropout_data<-setkey(setnames(
  rbindlist(lapply(paste(ntostr(9:12),ntostr(10:13),sep="-"),
                   function(x){fread(paste("/media/data_drive/wisconsin/",
                                           "hs_completion_certified_20",x,".csv",sep=""),na.strings="*"
                   )[GRADE_GROUP=="[All]"&DISTRICT_CODE!="0000"&GROUP_BY=="All Students"
                     &COMPLETION_STATUS=="Completed - Regular"&!is.na(STUDENT_COUNT),
                     ][,.(DISTRICT_CODE,year=2000+as.numeric(substr(x,4,5)),
                          pct_dropout=1-as.numeric(STUDENT_COUNT)/as.numeric(COHORT_COUNT))
                       ][,pct_dropout_pctl:=ecdf(pct_dropout)(pct_dropout)]})),
  "DISTRICT_CODE","stid"),year,stid)


setkey(teacher_data,year,agency
)[dropout_data,`:=`(pct_dropout=i.pct_dropout,
                    pct_dropout_pctl=i.pct_dropout_pctl)]

setkey(teacher_data,year,agency_next
)[dropout_data,`:=`(pct_dropout_next=i.pct_dropout,
                    pct_dropout_pctl_next=i.pct_dropout_pctl)]

### District-level economic data
###  via: http://nces.ed.gov/programs/edge/tables.aspx?ds=acs&y=2012
###   (All School Districts by State)
hh_income_data<-setkey(setkey(setnames(fread(
  paste0("/media/data_drive/wisconsin/",
         "wisconsin_median_income_by_district_08_12.csv"),
  select=c("GeoId","Estimate"),
  colClasses=list(character="GeoId",numeric="Estimate")),
  c("leaid","median_income_12"))[,leaid:=substr(leaid,8,14)],
  leaid)[ccd_id,agency:=as.character(i.agency)
         ][!is.na(median_income_12)&!is.na(agency),
           ][,leaid:=NULL][,med_income_pctl:=ecdf(median_income_12)(median_income_12)],
  agency)

setkey(teacher_data,agency
       )[hh_income_data,`:=`(median_income=i.median_income_12,
                             med_income_pctl=i.med_income_pctl)]

setkey(teacher_data,agency_next
       )[hh_income_data,`:=`(median_income_next=i.median_income_12,
                             med_income_pctl_next=i.med_income_pctl)]

# Reduced Form Analysis: Tables ####

#Plain one-way tables of # Career moves
t1<-teacher_data[,sum(move&!move_district),by=teacher_id][,table2(V1,pct=T,prop=T)]
t2<-teacher_data[,sum(move_district),by=teacher_id][,table2(V1,pct=T,prop=T)]
t1[setdiff(names(t2),names(t1))]=NA
t2[setdiff(names(t1),names(t2))]=NA
t<-rbind(t1,t2)[,paste0(0:6)]
rownames(t)<-c("Move School","Move District")
cat(capture.output(xtable(t,digits=1)),sep="\n\n")

#Compare my turnover data to results in Texas from Hanushek, Kain, O'Brien and Rivkin
setkey(teacher_data,year,agency,highest_degree,total_exp_floor)
cat(capture.output(print(xtable(matrix(unlist(cbind(
  c("Wisconsin","1 year",
    paste(c("2-3","4-6","7-11","12-21",">21"),"years"),
    "Texas","1 year",
    paste(c("2-3","4-6","7-11","12-21",">21"),"years")),
  rbind(as.data.table(rbind(rep(NA,4))),
        teacher_data[.(unique(year),"3619"),
                     .(move_school,move_district,quit,
                       exp_cut=cut(total_exp_floor,
                                   breaks=c(1,2,4,7,12,22,31),
                                   include.lowest=T,
                                   labels=c("1","2-3","4-6","7-11",
                                            "12-21",">21"),right=F))
                     ][,.(round(100*mean(!quit&!move_school,na.rm=T),1),
                          round(100*mean(move_school&!move_district,na.rm=T),1),
                          round(100*mean(move_district,na.rm=T),1),
                          round(100*mean(quit,na.rm=T),1)),by=exp_cut
                       ][,!"exp_cut",with=F],
        as.data.table(rbind(rep(NA,4),
                            #Numbers from Table A1
                            c(70.4,11.5,4.0,14.0),
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
                  cut(year,breaks=c(1996,2001,2006,2011,2016),
                      include.lowest=T,
                      labels=c("96-00","01-05","06-10","11-15"),right=F))
               ][,mean(move_district,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  ylab="% Changing Districts",xaxt="n",main="Change Districts",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##By School
par(mar=c(0,4.1,4.1,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[,.(total_exp_floor,move_school&!move_district,
                  cut(year,breaks=c(1996,2001,2006,2011,2016),
                      include.lowest=T,
                      labels=c("96-00","01-05","06-10","11-15"),right=F))
               ][,mean(V2,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  ylab="% Changing Schools",xaxt="n",main="Change Schools",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##Quit rates
par(mar=c(5.1,4.1,0,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[,.(total_exp_floor,quit,
                  cut(year,breaks=c(1996,2001,2006,2011,2016),
                      include.lowest=T,
                      labels=c("96-00","01-05","06-10","11-15"),right=F))
               ][,mean(quit,na.rm=T),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var="V1")[,!"total_exp_floor",with=F],
  xlab="Experience",ylab="% Leaving WI",
  main="\n Leaving WI Public Schools",
  type="l",lty=1,lwd=3,col=c("black","blue","red","green"))

##Degree Holdings
par(mar=c(5.1,4.1,0,2.1))
matplot(1:30,dcast.data.table(
  teacher_data[year!=1996,.(total_exp_floor,highest_degree,
                            cut(year,breaks=c(1996,2001,2006,2011,2016),
                                include.lowest=T,
                                labels=c("96-00","01-05","06-10","11-15"),right=F))
               ][,.(mean(highest_degree=="4",na.rm=T),
                    mean(highest_degree=="5",na.rm=T)),by=.(total_exp_floor,V3)],
  total_exp_floor~V3,value.var=c("V1","V2"))[,!"total_exp_floor",with=F],
  xlab="Experience",ylab="% Holding",
  main="\n Degree Holdings",
  type="l",lty=c(rep(1,4),rep(2,4)),lwd=3,col=rep(c("black","blue","red","green"),2))
legend("left",legend=c("MA","BA"),lty=c(2,1),lwd=3,bty="n",cex=.5)

par(mar=c(0,0,0,0))
plot(1,type="n",axes=F,xlab="",ylab="")
legend("top",legend=c("96-00","01-05","06-10","11-15"),lty=1,lwd=2,
       col=c("black","blue","red","green"),horiz=T,inset=0)
mtext("WI Turnover Moments",side=3,line=-1.5,outer=T)
dev.off2()

# Total turnover by year
pdf2("wisconsin_turnover_by_year.pdf")
matplot(1997:2015,teacher_data[year>1996,
                               .(sum(move),sum(move&!move_district),
                                 sum(move_district)),by=year][,!"year",with=F],
        type="l",lty=1,lwd=3,xlab="Year",ylab="# Moves",
        main="Number of Moves\nBy Year and Type")
legend("topleft",legend=c("Total","School","District"),lwd=3,lty=1,
       col=c("black","red","green"),cex=.7)
dev.off2()

#plotting the salary schedules for the 5 most populous agencies over the first 30 years
biggest_5<-sort(teacher_data[.(2015,unique(agency)),.N,by=agency][order(N)][(.N-4):.N,agency])
biggest_5_names<-c("Green Bay","Kenosha","Madison","Milwaukee","Racine")
rng<-salary_scales[.(2015,rep(biggest_5,2),rep(c("4","5"),5)),range(range(total_pay),range(salary))]
pdf2("wisconsin_salary_tables_imputed_15_big5_ba.pdf")
matplot(1:30,dcast.data.table(salary_scales[.(2015,biggest_5,"4"),
                                            c("agency","total_exp_floor","salary","total_pay"),with=F],
                              total_exp_floor~agency,value.var=c("salary","total_pay"))[,!"total_exp_floor",with=F],
        xlab="Experience",ylab="$",main="BA",ylim=rng,
        lty=c(rep(1,5),rep(2,5)),type="l",col=rep(c("black","red","blue","green","purple"),2),lwd=3)
legend("topleft",legend=biggest_5_names,bty="n",cex=.6,
       col=c("black","red","blue","green","purple"),lty=1,lwd=3)
legend("bottomright",legend=c("Total Pay","Base Pay"),cex=.6,
       lty=c(1,2),lwd=3,col="black",bty="n")
dev.off2()

pdf2("wisconsin_salary_tables_imputed_15_big5_ma.pdf")
matplot(1:30,dcast.data.table(salary_scales[.(2015,biggest_5,"5"),
                                            c("agency","total_exp_floor","salary","total_pay"),with=F],
                              total_exp_floor~agency,value.var=c("salary","total_pay"))[,!"total_exp_floor",with=F],
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
