#Wisconsin Teachers Project
#Background Data Clean-up, organization
#Michael Chirico
#August 25, 2015

# Package Setup, Convenient Functions ####
rm(list=ls(all=T))
gc()
setwd("/home/michael/Desktop/research/Wisconsin Bargaining")
gis.wd<-"/media/data_drive/gis_data/WI/"
cc.d.wd<-"/media/data_drive/common_core/district/"
act.wd<-"/media/data_drive/wisconsin/act/"
library(funchir)
library(data.table)
library(maptools)

# ACT data
## For 1995-96 - 2006-07, similar structure
act_96_07<-rbindlist(lapply(list.files(
  act.wd,pattern="act_[0-9]",full.names=T),
  fread,na.strings=c("--","*"),
  drop=c("grade","cesa","county","agency_key"))
  #Seems to be an entry for every school whether there
  #  were any ACT takers or not (with NAs); also
  #  drop Agency Type XX (Statewide) numbers until further notice
  )[!is.na(average_score_composite)&agency_type!="XX"]

act_08_14<-rbindlist(lapply(list.files(
  act.wd,pattern="act_c",full.names=T),
  fread,na.strings="*",drop=c("CESA","COUNTY")))[AGENCY_TYPE!=""]

# Spatial Data ####

wi_districts_elem_shp<-
  readShapeSpatial(gis.wd%+%"wisconsin_elementary_districts_statewide.shp")
wi_districts_scdy_shp<-
  readShapeSpatial(gis.wd%+%"wisconsin_secondary_districts_statewide.shp")

elem_nbhd<-nb2mat(poly2nb(wi_districts_elem_shp,
                          row.names=wi_districts_elem_shp$GEOID10),
                  style="B")
colnames(elem_nbhd)<-rownames(elem_nbhd)

scdy_nbhd<-nb2mat(poly2nb(wi_districts_scdy_shp,
                          row.names=wi_districts_scdy_shp$GEOID10),
                  style="B")
colnames(scdy_nbhd)<-rownames(scdy_nbhd)

### Need NCES Common Core Data to map polygon IDs to Agency ID
#### 2011-12 data (all but 9 districts)
ccd_id<-fread(cc.d.wd%+%"universe_2011_12_1a.txt",
              select=c("LEAID","STID"),colClasses="character"
              )[substr(LEAID,1,2)=="55",]
#### Add remaining 9 districts via CCD data in 1995-96
ccd_id<-
  setkey(rbindlist(list(
    ccd_id,setnames(setDT(read.fwf(
      cc.d.wd%+%"universe_1995_96_1a.txt",
      widths=c(7,4)))[substr(V1,1,2)=="55",
                      ][V2 %in% teacher_data[!(agency %in% ccd_id$agency),
                                             unique(agency)],],
      c("leaid","agency")))),leaid)

####Exclude districts not found in shapefile
elem_nbhd<-
  elem_nbhd[intersect(rownames(elem_nbhd),ccd_id$leaid),
            intersect(rownames(elem_nbhd),ccd_id$leaid)]
scdy_nbhd<-
  scdy_nbhd[intersect(rownames(scdy_nbhd),ccd_id$leaid),
            intersect(rownames(scdy_nbhd),ccd_id$leaid)]
####Rename rows from CCD LEA ID to Agency ID
rownames(elem_nbhd)<-colnames(elem_nbhd)<-ccd_id[.(colnames(elem_nbhd)),agency]
rownames(scdy_nbhd)<-colnames(scdy_nbhd)<-ccd_id[.(colnames(scdy_nbhd)),agency]
#***TURNS OUT ELEMENTARY AND SECONDARY ARE IDENTICAL--GET TO SIDESTEP THIS ISSUE FOR NOW***

####Get local average wages and local experience-dependent average discounted future pay
area_average<-function(pay_type="total_pay",aagency,yyear,hhighest_degree,ttotal_exp_floor=NULL){
  salary_scales[.(yyear[1],names(which(elem_nbhd[aagency[1],]!=0)),
                  hhighest_degree[1],ttotal_exp_floor[1]),
                mean(get(pay_type),na.rm=T)]
}

setkey(teacher_data,agency)
teacher_data[.(rownames(elem_nbhd)),
             total_pay_loc_avg:=area_average("total_pay",agency,year,highest_degree),
             by=.(agency,year,highest_degree)]

#Grouping by agency, year, degree AND experience
#  adds a lot to computation--paralellize by experience.
cl <- makeCluster(detectCores()); 
registerDoParallel(cl);
teacher_data_sub<-teacher_data[,.(agency,total_pay_future,year,highest_degree,total_exp_floor)]
clusterExport(cl,c("teacher_data_sub"),envir=environment());
clusterEvalQ(cl,library("data.table"));
tpf <- foreach(i = 1:30) %dopar% {
  teacher_data_sub[total_exp_floor==i,
                   ][.(rownames(elem_nbhd)),
                     total_pay_loc_val:=
                       area_average("total_pay_future",agency,year,
                                    highest_degree,total_exp_floor),
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
                   function(x){fread(cc.d.wd%+%paste0("universe_",x,".txt"),
                                     select=c("FIPST","STID","ULOCAL","HISP","BLACK","MEMBER"),
                                     colClasses=list(character=c("FIPST","STID"),
                                                     factor="ULOCAL",
                                                     integer=c("HISP","BLACK","MEMBER"))
                   )[FIPST=="55",][,FIPST:=NULL][,year:=2000L+as.integer(substr(x,6,7))]})),
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
