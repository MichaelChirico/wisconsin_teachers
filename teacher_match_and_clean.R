#!/usr/bin/env Rscript
#Wisconsin Teacher Project
#Teacher Data Matching, Cleaning
#Michael Chirico
#August 23, 2015

###############################################################################
#                             Package Setup                                   #
###############################################################################
rm(list = ls(all = TRUE))
gc()
external_path = '/media/data_drive/wisconsin/'
wds = c(data = paste0(external_path, 'teacher_raw_data/data_files'),
        keys = paste0(external_path, 'teacher_raw_data/'),
        write = external_path)
library(funchir)
library(data.table)
library(stringr)
library(readxl)
library(zoo)
library(RPushbullet)

###############################################################################
#                             Data Import                                     #
###############################################################################
#   see raw_data_cleaner for raw input sourcing & tidying
nnames = c("first_name", "last_name", "nee")
keys = fread(wds['keys'] %+% 'fwf_keys.csv', key = 'year',
             colClasses = c('character', 'character', 'integer', 'character'))
#drop the blanks which appear for the Excel files
keys = keys[!is.na(width)]
#progress bar
fls = list.files(wds["data"], pattern = "csv", full.names = TRUE)
message('Loading data...\n')
pb = txtProgressBar(0, length(fls))
full_data = #simultaneously read, concatenate all 20 years' data files
  rbindlist(lapply(fls, function(fl)
    fread({setTxtProgressBar(pb, match(fl, fls)); fl}, 
          drop = which(scan(file = fl, what = "", sep = "\t",
                            nlines = 1L, quiet = TRUE) == "filler"),
          #fill is necessary because fields are not constant across time
          colClasses = keys[.(gsub('[^0-9]', '', fl)), type],
          #one guy in 2014 has ?????? as his file number, which
          #  will prevent it from being read as integer unless
          #  we declare that as NA here
          na.strings = c('NA', '??????'))), fill = TRUE)
close(pb)

#Perpetual reminder: YYstaff.txt is the data for the (YY-1)-YY academic year;
#  year_session is encoded as YYYYR (so applies to Spring)
full_data[ , year := as.integer(substr(year_session, 1L, 4L))]
#storage of some fields inconsistent across time; start unifying here
full_data[year<2004, area := str_pad(area, width=4L, pad="0")]
#trim extraneous white space from names and force lowercase
#  patterns matched: - leading/trailing whitespace
#                    - extraneous whitespace between words
full_data[ , (nnames) := lapply(.SD, function(x) 
  gsub("^\\s+|\\s+$", "", gsub("\\b\\s{2,}\\b"," ", tolower(x)))),
  .SDcols = nnames]

#Try to impute maiden name when it seems to have been stored in the last name
#  Specifically, when nee is empty, look for names with "(" or "-";
#  maiden names appear to be stored between parentheses and before hyphens 
#  (not a perfect system); note that
#  full_data[gsub("\\s", "", nee) == "", names(table(nee))] 
#  shows that when missing nee is stored as a string, there is only one
#  possible length of that string. Also note that there is only one person 
#  in the data with both - and () in their name:
#    terese l demark-russo (uebe) (1998:000237414/1999:000315944)
#  and she has her maiden name stored as demark
#  As an alternative, define nee as the name coming after the hyphen:
full_data[(!nzchar(nee, keepNA = TRUE) & grepl("-", last_name)),
           c("nee", "nee2") := .(gsub("-.*", "", last_name),
                                 gsub(".*-", "", last_name))]
full_data[grepl('[()]', last_name),
          nee := gsub(".*\\(|\\).*", "", last_name)]
nnames = c(nnames, "nee2")

#Try to get a "clean" version of first, last, and maiden names by
#  deleting *all* white space, initials
##First, in first_name, add a space after the comma when it's missing
##  (causing problems & frequent--e.g., "lisa,a"->"lisa a")
full_data[grepl(",[^ ]", first_name), 
          first_name := gsub(",", " ", first_name, fixed = TRUE)]
full_data[ , paste0(nnames, "_clean") :=
            lapply(.SD, function(x) {
              gsub(paste0("\\s|", "(\\s|^)[a-z](\\s|$)|", 
                          "\\([^)]*(\\)|$)"), "",
                   gsub("[-.,'-]", "", x))}),
          .SDcols = nnames]
##Names like W C Fields were deleted; restore them
invisible(lapply(nnames, function(x)
  full_data[!nzchar(get(paste0(x, "_clean"))),
            paste0(x, "_clean") := get(x)]))
rm(nnames)

###############################################################################
#                             Data Errata                                     #
###############################################################################
#Incorporate errata cited here:
#  https://dpi.wi.gov/cst/data-collections/data-errata

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/Staff_CESA8_2015-2016_letter.pdf
##   Courtney Franz: Area _is_ 0810, should be 0825
full_data[year == 2016 & cesa == '08' & id == '000654132', area := '0825']

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/Staff_Elk_Mound_2015-2016_letter.pdf
##   Eric Wright (salary,fringe) _is_ (151189, 55637) should be (105664,52767)
full_data[year == 2016 & grepl('Elk Mound', district_name, fixed = TRUE) & 
            grepl('wright', last_name),
          c('salary', 'fringe') := .(105664, 52767)]

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Horicon_2015-2016.pdf
##   Insufficient information to identify the referenced teacher, or even
##     to know what should be fixed

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/Staff_Jefferson_2015_2016.pdf
##   School District of Jefferson (2702)
##   "...data reported, incorrectly included salary amounts from extra duties
##    and co-curriculars that some certified staff members received"
##   (unclear how to incorporate)

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/Staff_Marinett_2015_2016_letter.pdf
##  School District of Marinette (3311)
##  "...Michelle Ferm, was found to not be licensed for her current assignment.
##   This situation was unable to be rectified [and she] was terminated..."
##  Could just drop her, but presumably her data is otherwise useful/informative

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/
##   Staff_MPS_2015-2016%20Adminstrator%20Education%20Level%20Error.pdf
## Darienne Driver and Orlando Ramos both listed as Master's, but have doctorate
full_data[year == 2016 & id %in% c('000684557', '000697114'),
          highest_degree := '7']

## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/
##   Staff_2015-2016%20MPS%20Correction%20Letter%20Re-Teaching%20Years%20of%20Experience.pdf
##  Milwaukee Public Schools (3619) completely botched recording of experience.
## Spreadsheet link:
##   https://dpi.wi.gov/sites/default/files/imce/cst/xls/
##     Corrected%202015-2016%20Total%20Years%20of%20Experience%20%28for%20Errata%20Ltr%29.xlsx
##    * Corrections spreadsheet appears to be linked by file_number field 
##      (called DPI Entity Number in spreadsheet)
##    * Pending correspondence from Milwaukee's Donna Edwards, assuming
##      Local Exp 2015 (resp. Total) is "latent" and Local Exp 2015 Reported
##      is the "observed" version of this found in DPI files
##      (it appears to be just a rounded version of the "latent" measure)
mwk_ff = list.files(wds['keys'], pattern = '^Corrected.*xlsx', full.names = TRUE)
mwk_corr = read_excel(mwk_ff, sheet = 1L, skip = 2L,
                      col_names = c('mwk_id', 'last_name', 'first_name', 'salary',
                                    'file_number', 'wise_id', 'eff_date',
                                    rep('x', 4L), 'local_exp', 'total_exp'),
                      col_types = abbr_to_colClass('tntdsn', '312142'))
setDT(mwk_corr)
#IDs are zero-padded in the corrections file
mwk_corr[ , file_number := as.integer(file_number)]

#don't want to match NAs or any other year
full_data[year == 2016 & !is.na(file_number) & district == '3619',
          c('local_exp', 'total_exp') := 
            mwk_corr[.SD, .(x.local_exp, x.total_exp), on = 'file_number']]

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_StevenPoint_SD_2015-16.pdf
##   Jerome Gargulak (position, area) _is_ (64, 5000), should be (55, 0000)
full_data[year == 2016 & id == '000641542',
          c('position_code', 'area') := .('55', '0000')]

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Lodi_2014-2015.pdf
## Lodi Public Schools (3150)
## "...list of contractors used by Lodi Schools in the 2014-15 school year
##  who declined to provide... demographic information [and thus]
##  are not listed in our WiseStaff submission"
##  (unclear how to incorporate)

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Goodman-Armstrong_2014_2015.pdf
## Tracy Cassidy: unclear how to incorporate info about grade levels
##   as these errata are not distinguishable in the data
## Joleen Pahl: unclear which assignment is incorrect -- errata refer
##   to area 317, but this is not a valid code and not used with Joleen
##   (pending correspondence with Superintendent Hinkel)
## Dennis Christian position_code _is_ 52, but should be 
##   whatever's associated with "Administrative Assistant" (guessing 69)
## Patricia Christian: unsure what the error is?
## Richelle Jochem: unclear how to incorporate
full_data[year == 2015 & file_number == 91661, position_code := '69']

## https://dpi.wi.gov/sites/default/files/imce/cst/Manawa%20data%20errata%20letter.pdf
## Jill Seka: unclear if anything's wrong
## Julie VanderGrinten: unclear if anything's wrong
##   (she _does_ have one assignment as 86-0000)
## Mindi Wagner: erratum says she's 0405 and should be 0265, but she's 0250
## Judith Connelly: low_grade should be 1, not KG
## Jacob Abrahamson: unclear how to incorporate
## Jessica Hedtke & Holly Thontlin: 43-0000 should be 96-9883
full_data[year == 2015 & file_number == 728866, area := '0265']
full_data[year == 2015 & file_number == 693678,
          c('low_grade', 'low_grade_code') := .('01', '20')]
full_data[year == 2015 & file_number %in% c(809487, 209625),
          c('position_code', 'area') := .('96', '9883')]

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Menomonie_2014-2015.pdf
## Nathan McMahon: only licensed to teach grade 9
## Ryan Sterry: unclear the error, as there _is_ one assignment of 53-0291
## Harold Vlcek: only licensed to teach grade 9
## Philip Winegar: unclear the error, as there _is_ one assignment of 53-0220
## Willow Anderson: unclear which assignment is erroneous
## Jamie Richartz: unclear which assignment is erroneous
## Mary Snyder: unclear the error, as there _is_ one assignment of 53-0810
## Mary Henry: interpreting as 53 should be 64
full_data[year == 2015 & file_number %in% c(610056, 161164),
          c('high_grade', 'high_grade_code') := .('09', '52')]
full_data[year == 2015 & file_number == 132126 & position_code == '53',
          position_code := '64']

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Oconto%20Falls_2014-2015.pdf
##  Complete record creation for Cynthia R Cho (also found in several years prior)
full_data = 
  rbind(full_data,
        data.table(first_name = 'cynthia', last_name = 'cho',
                   file_number = 192807L, highest_degree = '5',
                   gender = 'F', race = 'W', local_exp = 35,
                   total_exp = 35, salary = 63104, full_time_equiv = 100,
                   final_contract = 'Y', birth_year = '1958',
                   fringe = 16763, district = '4074', bilingual = 'N',
                   district_hire = '4074', district_hire_type = '03',
                   days_of_contract = 187, category = '1',
                   year_session = '2015R', cesa = '08', county = '42',
                   county_name = 'Oconto County', ship_zip = '54154-1468',
                   district_name = 'Oconto Falls Public Sch Dist',
                   district_work_type = '04', mail_zip = '54154-1468',
                   school_mail_1 = '102 S Washington St',
                   school_mail_2 = 'Oconto Falls WI  54154-1468',
                   school_shipping_1 = '102 S Washington St',
                   school_shipping_2 = 'Oconto Falls WI  54154-1468',
                   mail_city = 'Oconto Falls', mail_state = 'WI',
                   ship_city = 'Oconto Falls', ship_state = 'WI',
                   school_name = 'Washington Middle', grade_level = '5',
                   low_grade = '07', low_grade_code = '44', year = 2015L,
                   telephone = '920-848-4463', admin_name = 'Louis Hobyan',
                   high_grade = '07', high_grade_code = '07',
                   first_name_clean = 'cynthia', last_name_clean = 'cho',
                   school = '0260', position_code = '53', subcontracted = 'N',
                   area = '0316', subcontracted = 'N', long_term_sub = 'N'),
        fill = TRUE)
        
## https://dpi.wi.gov/sites/default/files/imce/cst/pdf/Prairie%20Farm%20201508061437.pdf
##  Ariel Humpal: eliminate 53-0620 row, adjust FTE on 53-0605/53-0625 rows
##                to reflect erratum report
full_data[year == 2015 & file_number == 726878 & area == '0605',
          full_time_equiv := 60]
full_data[year == 2015 & file_number == 726878 & area == '0625',
          full_time_equiv := 40]
drop_idx = full_data[year == 2015 & file_number == 726878 & area == '0620', which = TRUE]
full_data = full_data[-drop_idx]

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Suring%20School%202014-2015.pdf
## Laura Lojpersberger: error doesn't actually appear to be in the data

## https://dpi.wi.gov/sites/default/files/imce/cst/Staff_Wauwatosa_2014-2015.pdf
## Cara Anderson: area 0515 -> 0506
## Marget Boyd: position 99 should be changed, but not clear what to
## Mary Butkus: typo in low grade/high grade
## Bernard Carreon: incorrect low grade
full_data[year == 2015 & file_number == 734594 & area == '0515',
          area := '0506']
full_data[year == 2015 & file_number == 34598,
          c('low_grade', 'high_grade') := '04']
full_data[year == 2015 & file_number == 626400,
          c('low_grade', 'low_grade_code') := .('06', '40')]

#Delete anyone who cannot possibly be identified below:
#  Namely, those who match another exactly on (cleaned) first/last name,
#  birth year and year of data
#  Also reset NA fringe values to 0, and remove anyone with a missing
#  year of birth
full_data = 
  full_data[ , if (uniqueN(id) == 1L) .SD,
             keyby = .(first_name_clean, last_name_clean,
                       birth_year, year)]
full_data[is.na(fringe), fringe := 0]

#Will also use a different cleaned version of the first name--
#  Delete everything after the first space
full_data[ , first_name2 := gsub("\\s.*", "", first_name)]

#Experience defined as 10ths of a year in raw data (except in Excel files)
div10 = c("local_exp", "total_exp")
full_data[year < 2015L, (div10) := lapply(.SD, `/`, 10), .SDcols = div10]; rm(div10)

#Reformat/create some variables
#  * birth_year, highest_degre --> integer
#  * total_exp_floor (floor of total experience)
#  * total_pay = salary + fringe
full_data[ , c("birth_year", "total_exp_floor",
               "total_pay", "highest_degree") :=
             .(as.integer(birth_year), floor(total_exp),
               salary+fringe, as.integer(highest_degree))]

#Create ethnicity dummies, and separate the "main" ethnicities
ethnames = c("black","hispanic","white")
ethcodes = c("B","H","W")
full_data[ , (ethnames) := lapply(ethcodes, function(x) ethnicity == x)]
full_data[ , ethnicity_main := 
             factor(ethnicity, levels = c('B', 'H', 'W'),
                    labels = c('Black', 'Hispanic', 'White'))]


###############################################################################
#                          Matching Algorithm                                 #
###############################################################################
#Initialize ID system & matching flags
yr0 = full_data[ , min(year)]
yrN = full_data[ , max(year)]
full_data[year == yr0, teacher_id := .GRP, by = id]
full_data[year == yr0,
          c("married", paste0("mismatch_", c("inits", "yob")), "step") := 
            .(FALSE, FALSE, FALSE, 0L)]

#Main matching function--given keys in previous years and key in current year,
#  match and assign flags as necessary; note that setting keys repeatedly is
#  quite (and increasingly, as more previous years are included) costly;
#  may be worth looking into using Pandas to speed up this process.
get_id = function(yr, key_prev, key_crnt = key_prev, step, ...){
  #figure out which columns are to be updated
  flags = c(names(list(...)), "step")
  update_cols = c("teacher_id", flags)
  #Want to exclude anyone who is matched
  existing_ids = full_data[.(yr), unique(na.omit(teacher_id))]
  #Get the most recent prior observation of all unmatched teachers
  unmatched = 
    full_data[.(yr0:(yr-1L))
              ][!teacher_id %in% existing_ids,
                #.N here gives the most recent observation
                #  of the teacher, in their "highest-intensity"
                #  position (b/c ordered by FTE)
                #use .SDcols to keep only merging columns around
                .SD[.N], by = teacher_id, .SDcols = key_prev
                #if .N>1, there will be more than 1 teacher matched by
                #  key_prev among the past teachers, so toss
                #  due to ambiguity
                ][ , if (.N==1L) .SD, keyby = key_prev
                  ][ , (flags) := list(..., step)]
  #Merge, reset keys
  setkeyv(full_data, key_crnt)
  full_data[year == yr & is.na(teacher_id),
            (update_cols) := unmatched[.SD, ..update_cols]]
  setkey(full_data, year)

  #assign the ID and flags to all observations of matched teachers in the
  #  the current year (within-year, they can be identified by shared id)
  full_data[.(yr), (update_cols) := 
              lapply(.SD, function(x) na.omit(x)[1L]),
            by = id, .SDcols = update_cols]
}

#add indices to hopefully facilitate the process
setindex(full_data, year)
#Used in Step 1, 4, 7
setindexv(full_data,
          c("first_name_clean", "last_name_clean",
            "birth_year", "district", "school"))
#Used in Step 2, 5, 8
setindexv(full_data,
          c("first_name_clean", "last_name_clean", "birth_year", "district"))
#Used in Step 3, 6, 9, 10
setindexv(full_data, c("first_name_clean", "last_name_clean", "birth_year"))
#Used in Step 4
setindexv(full_data,
          c("first_name_clean", "nee_clean",
            "birth_year", "district", "school"))
#Used in Step 5
setindexv(full_data,
          c("first_name_clean", "nee_clean", "birth_year", "district"))
#Used in Step 6
setindexv(full_data, 
          c("first_name_clean", "nee_clean", "birth_year"))
#Used in Step 7
setindexv(full_data,
          c("first_name_clean", "nee2_clean",
            "birth_year", "district", "school"))
#Used in Step 8
setindexv(full_data,
          c("first_name_clean", "nee2_clean", "birth_year", "district"))
setindexv(full_data, c("first_name_clean", "nee2_clean", "birth_year"))
#Used in Step 9
setindexv(full_data, c("first_name_clean", "nee2_clean", "birth_year"))
#Used in Step 10, 13, 16
setindexv(full_data,
          c("first_name2", "last_name_clean",
            "birth_year", "district", "school"))
#Used in Step 11, 14, 17
setindexv(full_data,
          c("first_name2", "last_name_clean", "birth_year", "district"))
#Used in Step 12, 15, 18
setindexv(full_data, c("first_name2", "last_name_clean", "birth_year"))
#Used in Step 13
setindexv(full_data,
          c("first_name2", "nee_clean", "birth_year", "district", "school"))
#Used in Step 14
setindexv(full_data, c("first_name2", "nee_clean", "birth_year", "district"))
#Used in Step 15
setindexv(full_data, c("first_name2", "nee_clean", "birth_year"))
#Used in Step 16
setindexv(full_data, 
          c("first_name2", "nee2_clean", "birth_year", "district", "school"))
#Used in Step 17
setindexv(full_data, c("first_name2", "nee2_clean", "birth_year", "district"))
#Used in Step 18
setindexv(full_data, c("first_name2", "nee2_clean", "birth_year"))
#Used in Step 19
setindexv(full_data,
          c("first_name_clean", "last_name_clean", "district", "school"))
#Used in Step 20
setindexv(full_data,
          c("first_name_clean", "last_name_clean", "district", "position_code"))
#Finally, sort by FTE to be sure the most-intensive position comes last
setkey(full_data, year, full_time_equiv)

system.time({
  for (yy in (yr0+1L):yrN){
    print(yy)
    #1) First match anyone who stayed in the same school
    #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | DISTRICT | SCHOOL ID
    get_id(yy, c("first_name_clean", "last_name_clean",
                 "birth_year", "district", "school"), step = 1L)
    #2) Loosen criteria--find within-district switchers
    #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name_clean", "last_name_clean",
                 "birth_year", "district"), step = 2L)
    #3) Loosen criteria--find district switchers
    #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR
    get_id(yy, c("first_name_clean", "last_name_clean", "birth_year"),
           step = 3L)
    #4) Find anyone who appears to have gotten married
    #MATCH ON:
    #  FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name_clean", "last_name_clean",
                 "birth_year", "district", "school"),
           c("first_name_clean", "nee_clean", 
             "birth_year", "district", "school"),
           married = TRUE, step = 4L)
    #5) married and changed schools
    #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name_clean", "last_name_clean", 
                 "birth_year", "district"),
           c("first_name_clean", "nee_clean",
             "birth_year", "district"),
           married = TRUE, step = 5L)
    #6) married and changed districts
    #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME | BIRTH YEAR
    get_id(yy, c("first_name_clean", "last_name_clean", "birth_year"),
           c("first_name_clean", "nee_clean", "birth_year"),
           married = TRUE, step = 6L)
    #7) Find marriages with maiden name assigned after the hyphen
    #MATCH ON:
    #  FIRST NAME | LAST NAME->MAIDEN NAME 2 | BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name_clean", "last_name_clean", 
                 "birth_year", "district", "school"),
           c("first_name_clean", "nee2_clean", 
             "birth_year", "district", "school"),
           married = TRUE, step = 7L)
    #8) married (maiden name post-hyphen) and changed schools
    #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME 2 | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name_clean", "last_name_clean", 
                 "birth_year", "district"),
           c("first_name_clean", "nee2_clean",
             "birth_year", "district"),
           married = TRUE, step = 8L)
    #9) married (maiden name post-hyphen) and changed districts
    #MATCH ON: FIRST NAME | LAST NAME->MAIDEN NAME 2 | BIRTH YEAR
    get_id(yy, c("first_name_clean", "last_name_clean", "birth_year"),
           c("first_name_clean", "nee2_clean", "birth_year"),
           married = TRUE, step = 9L)
    #10) now match some stragglers with
    #  missing/included middle names & repeat above
    #MATCH ON: 
    #  FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name2", "last_name_clean",
                 "birth_year", "district", "school"),
           mismatch_inits = TRUE, step = 10L)
    #11) stripped first name + school switch
    #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name2", "last_name_clean", "birth_year", "district"),
           mismatch_inits = TRUE, step = 11L)
    #12) stripped first name + district switch
    #MATCH ON: FIRST NAME (STRIPPED) | LAST NAME | BIRTH YEAR
    get_id(yy, c("first_name2", "last_name_clean", "birth_year"),
           mismatch_inits = TRUE, step = 12L)
    #13) stripped first name + married
    #MATCH ON:
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME |
    #                            BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name2", "last_name_clean",
                 "birth_year", "district", "school"),
           c("first_name2", "nee_clean",
             "birth_year", "district", "school"),
           married = TRUE, mismatch_inits = TRUE, step = 13L)
    #14) stripped first name, married, school switch
    #MATCH ON: 
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name2", "last_name_clean",
                 "birth_year", "district"),
           c("first_name2", "nee_clean",
             "birth_year", "district"),
           married = TRUE, mismatch_inits = TRUE, step = 14L)
    #15) stripped first name, married, district switch
    #MATCH ON:
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name2", "last_name_clean", "birth_year"),
           c("first_name2", "nee_clean", "birth_year"),
           married = TRUE, mismatch_inits = TRUE, step = 15L)
    #16) stripped first name + married (maiden name post-hyphen)
    #MATCH ON: 
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME 2 | 
    #                            BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name2", "last_name_clean",
                 "birth_year", "district", "school"),
           c("first_name2", "nee2_clean",
             "birth_year", "district", "school"),
           married = TRUE, mismatch_inits = TRUE, step = 16L)
    #17) stripped first name, married (maiden name post-hyphen), school switch
    #MATCH ON: 
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME 2 | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name2", "last_name_clean", "birth_year", "district"),
           c("first_name2", "nee2_clean", "birth_year", "district"),
           married = TRUE, mismatch_inits = TRUE, step = 17L)
    #18) stripped first name, married, district switch
    #MATCH ON:
    #  FIRST NAME (STRIPPED) | LAST NAME-> MAIDEN NAME 2 | BIRTH YEAR | AGENCY
    get_id(yy, c("first_name2", "last_name_clean", "birth_year"),
           c("first_name2", "nee2_clean", "birth_year"),
           married = TRUE, mismatch_inits = TRUE, step = 18L)
    #19) YOB noise: match anyone who stayed in the same school
    #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY | SCHOOL ID
    get_id(yy, c("first_name_clean", "last_name_clean", "district", "school"),
           mismatch_yob = TRUE, step = 19L)
    #20) YOB noise: within-district switchers (at the same position)
    #MATCH ON: FIRST NAME | LAST NAME | BIRTH YEAR | AGENCY | POSITION CODE
    get_id(yy, c("first_name_clean", "last_name_clean",
                 "district", "position_code"),
           mismatch_yob = TRUE, step = 20L)
    #21) finally, give up and assign new ids to new (read: unmatched) teachers
    current_max = full_data[.(yy), max(teacher_id, na.rm = TRUE)]
    new_ids = full_data[year==yy & is.na(teacher_id),
                        .(id = unique(id))][ , add_id := .I + current_max]
    setkey(new_ids, id)
    setkey(full_data, id)
    full_data[year==yy & is.na(teacher_id),
              teacher_id := new_ids[.SD, add_id]]
    setkey(full_data, year)
  }
}); rm(yy, current_max, new_ids)
pbPost('note', 'Matching Algorithm Completed')

###############################################################################
#                          Post-Match Cleanup                                 #
###############################################################################

#need to sort by FTE for adjustment to move_school below
setorder(full_data, teacher_id, year, full_time_equiv)
setkey(full_data, teacher_id, year)

#Some summary statistics about the measured career of each teacher
full_data[ , years_tracked := uniqueN(year), by = teacher_id]
full_data[ , c("first_year", "last_year") := 
             as.list(range(year)), by = teacher_id]
full_data[ , c("left_censored", "right_censored") :=
            .(first_year == yr0, last_year == yrN), by = teacher_id]

#Use last observation carried forward (LOCF) to synergize maiden names
mns = c('nee', 'nee_clean')
full_data[!nzchar(nee), nee := NA]
full_data[!nzchar(nee_clean), nee_clean := NA]
full_data[ , (mns) := lapply(.SD, na.locf, na.rm = FALSE),
           by = teacher_id, .SDcols = mns]; rm(mns)

#Correct ethnicity code for teachers with noisy assignment in two cases:
## 1: Ethnicity plain missing (==" ") for subset of observations
##      -Here, just replace missing by LOCF
full_data[ethnicity == " ", ethnicity := NA]
full_data[.(unique(teacher_id[is.na(ethnicity)])),
          c("ethnicity", "ethnicity_flag") :=
            .(na.locf(na.locf(ethnicity, na.rm = FALSE),
                      na.rm = FALSE, fromLast = TRUE), TRUE),
          by = teacher_id]
## 2: Single ethnicity violated on at most 30% of observations
##      -Here, overwrite with "dominant" ethnicity
full_data[full_data[ , if(uniqueN(ethnicity) > 1L) {
  if((tbl <- table2(ethnicity, prop = TRUE, ord = "dec"))[1L] >= .7) {
    names(tbl)[1L]}}, by = teacher_id],
  c("ethnicity", "ethnicity_flag") := .(i.V1, TRUE)]
## 3: Of all other individuals sharing a last name,
##    if at least 70% are of the same ethnicity,
##    assign this ethnicity to the multi-ethnic
ids = unique(full_data[ , if(uniqueN(ethnicity) > 1L ||
                             any(is.na(ethnicity))) 
  .(ln = last_name_clean), by = teacher_id])
setkey(ids, ln)
all_nms = 
  unique(
    full_data[!.(ids$teacher_id)
              ][last_name_clean %in% ids$ln,
                .(last_name_clean, teacher_id, ethnicity)],
    by = 'teacher_id'
  )[ , if (.N > 5L) {
    #What's the most frequent ethnicity for each last name?
    tbl = table2(ethnicity, prop = TRUE, ord = "dec")
    if (tbl[1L] >= .7) .(eth = names(tbl)[1L])},
    keyby = last_name_clean]
full_data[setkey(ids[all_nms], teacher_id),
          c("ethnicity", "ethnicity_flag") := .(eth, TRUE)]
rm(all_nms,ids)

#Fill in missing highest_degree
## Highest degree 1 not found in documentation, only appears 27 times
full_data[highest_degree == 1, highest_degree := NA]
##Use NA LOCF to replace highest degree, as with maiden names--
##  only allow "ratchet-up" of highest degree
full_data[.(unique(teacher_id[is.na(highest_degree)])),
          c("highest_degree", "highest_degree_flag") :=
            .(na.locf(highest_degree, na.rm = FALSE), TRUE),
          by = teacher_id]

#Correct noise in gender
##First, fill in missing gender via NA LOCF
full_data[gender == " ", gender := NA]
full_data[.(unique(teacher_id[is.na(gender)])),
          c("gender", "gender_flag") :=
            .(na.locf(na.locf(gender, na.rm = FALSE),
                      na.rm = FALSE, fromLast = TRUE), TRUE),
          by = teacher_id]
##Next, use ethnicity approach--if >=70% of observations agree,
##  assign "dominant" gender
full_data[full_data[ , if (uniqueN(gender) > 1L) {
  if((tbl <- table2(gender, prop = TRUE, ord = "dec"))[1L] >= .7) {
    names(tbl)[1L]}}, by = teacher_id],
  c("gender", "gender_flag") := .(i.V1, TRUE)]
##Finally, use empirical gender for the rest of the ambiguous
##  Look for at least 5 other people in the data with the
##  same first name--if at least 70% of those found have
##  the same gender, assign that gender.
ids = unique(full_data[ , if(uniqueN(gender) > 1L ||
                             any(is.na(gender)))
  .(fn = first_name2), by = teacher_id])
setkey(ids, fn)
all_nms = 
  unique(
    full_data[!.(ids$teacher_id)
              ][first_name2 %in% ids$fn,
                .(first_name2, teacher_id, gender)],
    by = "teacher_id"
  )[ , if (.N > 5L) {
    #What's the most frequent gender for each last name?
    tbl = table2(gender, prop = TRUE, ord = "dec")
    if(tbl[1L] >= .7) .(gend = names(tbl)[1L])}, keyby = first_name2]
full_data[setkey(ids[all_nms], teacher_id),
          c("gender", "gender_flag") := .(gend, TRUE)]
rm(all_nms,ids)

#Clean up school & district codes to
#  facilitate identifying switches
full_data[school=="    ", school := NA]
full_data[district == "0000", district := NA]
##Missing school particularly prevalent
##  among teachers who transition to 
##  being substitutes (but listed at the same district)
full_data[ , school_fill := na.locf(na.locf(school, na.rm = FALSE), 
                                    fromLast = TRUE, na.rm = FALSE),
           by = .(teacher_id, district)]
full_data[ , district_fill := na.locf(na.locf(district, na.rm = FALSE),
                                      fromLast = TRUE, na.rm = FALSE),
           by = .(teacher_id, district_hire)]

#Adding all lead/lag-based variables
shifts = c("school_fill", "district_fill", "married",
           "highest_degree", "position_code")
shift.s = c("sch", "dis", "mrd", "deg", "pos")
shifts2 = c("mv.sch", "mv.dis", "crt", "mv.pos")
shifts2.s = c("msc", "mds", "crt", "mps")

##Essential to focus on "main" positions only
##  (as defined by highest (or tied-for-highest) FTE
full_data_main = 
  unique(full_data[ , c("teacher_id", "year", shifts), with = FALSE],
         fromLast = TRUE)

#Get lags of all variables in shifts
full_data_main[ , paste0(shift.s, ".prv") :=
                  lapply(.SD, shift), by = teacher_id, .SDcols = shifts]

#get leads of all variables in shifts
full_data_main[ , paste0(shift.s, ".nxt") :=
                  lapply(.SD, shift, type = "lead"),
                by = teacher_id, .SDcols = shifts]

full_data_main[ , `:=`(mv.dis = district_fill != dis.prv,
                       mv.dis.nx = district_fill != dis.nxt,
                       #take care defining move school--
                       #  some teachers move to school
                       #  in new district with same school #
                       mv.sch = school_fill != sch.prv |
                         district_fill != dis.prv,
                       mv.sch.nx = school_fill != sch.nxt |
                         district_fill != dis.nxt,
                       #any increase in degree counted
                       #  as certification; not perfect,
                       #  but works well for 
                       #  bachelor's->masters
                       crt = highest_degree > deg.prv |
                         is.na(deg.prv) & !is.na(highest_degree) &
                         year != yr0,
                       crt.nx = highest_degree < deg.nxt |
                         is.na(highest_degree) & !is.na(deg.nxt),
                       #will be imperfect for teachers
                       #  holding positions whose
                       #  encoding changes over time,
                       #  but full-time teachers are always 53
                       mv.pos = position_code != pos.prv,
                       mv.pos.nx = position_code != pos.nxt)]

#needed to wait to define shifts on these
#  until the above were defined
full_data_main[ , paste0(shifts2.s, ".prv") :=
                  lapply(.SD, shift),
                by = teacher_id, .SDcols = shifts2]
#need this complicated approach to
#  define cumulative moves because
#  initial mv.sch is NA
full_data_main[ , mv.sch.cum := 
                  "[<-"(mv.sch, !is.na(mv.sch),
                        cumsum(na.omit(mv.sch))),
                by = teacher_id]
full_data_main[ , mv.sch.tot := 
                  max(max(mv.sch.cum, na.rm = TRUE), 0),
                by = teacher_id]
rm(list = ls(pattern = "shift"))

full_data[full_data_main,
          `:=`(school_prev_main = i.sch.prv,
               school_next_main = i.sch.nxt,
               district_prev_main = i.dis.prv,
               district_next_main = i.dis.nxt,
               move_school = i.mv.sch,
               move_school_next = i.mv.sch.nx,
               move_school_prev = i.msc.prv,
               move_district = i.mv.dis,
               move_district_next = i.mv.dis.nx,
               move_district_prev = i.mds.prv,
               cumulative_move_school = i.mv.sch.cum,
               total_move_school = i.mv.sch.tot,
               married_prev = i.mrd.prv,
               married_next = i.mrd.nxt,
               certified = i.crt,
               certified_next = i.crt.nx,
               certified_prev = i.crt.prv,
               move_position = i.mv.pos,
               move_position_next = i.mv.pos.nx,
               move_position_prev = i.mps.prv)]
rm(full_data_main)

#Synergize birth_year for those matched in the final two steps
#  Simply use the most common year of birth
#    In case of duplicates, use the birth year that
#      implies an age closest to the full data median age
#  Define age now that birth_year noise is eliminated
med_age = full_data[ , median(year - birth_year, na.rm = TRUE)]
full_data[.(full_data[ , if(any(mismatch_yob, na.rm = TRUE)) teacher_id,
                       by = teacher_id]$teacher_id),
          c("birth_year", "yob_flag") := {
            tbl = table2(birth_year, ord = "dec", useNA = "ifany")
            list(if(tbl[1L] > tbl[2L] || length(tbl) == 1L)
              as.integer(names(tbl)[1L])
              else {
                mxs = names(tbl == max(tbl))
                inds = birth_year %in% mxs
                mx.yrs = year[inds]
                mx.yob = birth_year[inds]
                mx.yob[which.min(abs(mx.yrs - mx.yob - med_age))]}, TRUE)},
          by = teacher_id]
full_data[ , age := year - birth_year]; rm(med_age)

#Add identifier for three common patterns of substitution
#  I) "Ease-in" period: begin career with some years as a sub
#  II) "Soft retirement" period: end career with some years as a sub
#  III) "Maternity" period: transition to subbing for some years mid-career
##I)
full_data[ , sub_ease_in :=
             all(unique(position_code[year == year[1L]]) == 43) &
             uniqueN(position_code) > 1L, by = teacher_id]
##II)
full_data[ , sub_soft_retd := 
             all(unique(position_code[year == year[.N]]) == 43) &
            uniqueN(position_code) > 1L & max(age, na.rm = TRUE) >= 50,
           by = teacher_id]
##III)
full_data[full_data[ , .(all_sub = all(position_code == 43)),
                     by = .(teacher_id, year)
                     ][ , .(sub_yr = any(all_sub) &! all(all_sub)),
                        keyby = teacher_id],
          sub_maternity := i.sub_yr &! (sub_ease_in | sub_soft_retd)]

#Add first/last observation and
#  new teacher/retirement or quit flags
full_data[ , c("first_obs", "last_obs") :=
            .(year == min(year), year == max(year)),
          by = teacher_id]
full_data[ , c("new_teacher", "quit_next") :=
             .(first_obs, last_obs)]
## Erase new_teacher and quit_next
##   in earliest and lastest years
full_data[(first_obs) & year == yr0, new_teacher := NA]
full_data[(last_obs) & year == yrN, quit_next := NA]

#Set NA to F, 0, or NA as appropriate, in three steps:
## I) All NA are F for these columns:
na.to.f = c("married", "mismatch_inits", "mismatch_yob",
            "ethnicity_flag", "highest_degree_flag",
            "gender_flag", "yob_flag", "certified", "move_position")
for (jj in na.to.f)
  set(full_data, which(is.na(full_data[[jj]])), jj, FALSE)
## II) All NA are 0 for these columns:
na.to.0 = c("step", "cumulative_move_school", "total_move_school")
for (jj in na.to.0)
  set(full_data, which(is.na(full_data[[jj]])), jj, 0L)
## III) Need to take care when _some_ NA should be NA, others F
### IIIa) NA when in the first year, F otherwise
na.to.f2 = paste0(c("married", "certified", "move_position"), "_prev")
for (jj in na.to.f2)
  full_data[is.na(get(jj)) & (first_obs), (jj) := FALSE]
### IIIb) NA when in the last year, F otherwise
na.to.f3 = gsub("prev", "next", na.to.f2)
for (jj in na.to.f2)
  full_data[is.na(get(jj)) & (last_obs), (jj) := FALSE]
rm(list = ls(pattern = "na.to")); rm(jj)

pbPost('note', 'Ex Post Cleanup Completed')

#Save cleaned data
fwrite(full_data, wds['write'] %+% "wisconsin_teacher_data_full.csv")
fwrite(full_data[ , .(names(full_data), sapply(.SD, class))],
       wds["write"] %+% "wisconsin_teacher_data_full_colClass.csv",
       col.names = FALSE)

#Save school-level information
#  including all variables that are
#   constant within district*school*year
sch_cols = 
  c("year", "district", "school", "district_name",
    "school_name", "grade_level", "cesa", "county",
    "district_work_type", "school_mail_" %+% 1L:3L,
    "school_shipping_" %+% 1L:3L,
    rep(c("mail_", "ship_"), each = 3L) %+% c("city", "state", "zip"),
    "telephone", "admin_name")
fwrite(unique(full_data[ , sch_cols, with = FALSE]),
       wds["write"] %+% "wisconsin_school_data_full.csv")
