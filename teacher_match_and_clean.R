#Wisconsin Teacher Project
#Teacher Data Matching, Cleaning
#Michael Chirico
#August 23, 2015

###############################################################################
#                   Package setup & Convenient Functions                      #
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
library(zoo)
library(RPushbullet)

###############################################################################
#                             Data import                                     #
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
          colClasses = keys[.(gsub('[^0-9]', '', fl)), type])), fill = TRUE)
close(pb)

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

#Experience defined as 10ths of a year in raw data
div10 = c("local_exp", "total_exp")
full_data[ , (div10) := lapply(.SD, `/`, 10), .SDcols = div10]; rm(div10)

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

#Save cleaned data before cutting down to the main sample
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
fwrite(unique(full_data[ , sch_cols]),
       wds["write"] %+% "wisconsin_school_data_full.csv")

###############################################################################
#                           Data Consolidation                                #
###############################################################################

#For keeping better track of effects of sample
# restrictions given any future code edits / sample revisions, etc.
counts_update = function(old_counts) {
  new_counts = full_data[ , c(.N, uniqueN(teacher_id))]
  cat("Decrease from ", old_counts[1L], " to ",
      new_counts[1L], " observations (difference of ",
      old_counts[1L]-new_counts[1L], "/",
      round(100*(1-new_counts[1L]/old_counts[1L])),
      "%);\n", "Decrease from ", old_counts[2L], " to ",
      new_counts[2L], " individuals (difference of ",
      old_counts[2L]-new_counts[2L], "/",
      round(100*(1-new_counts[2L]/old_counts[2L])), "%)\n", sep="")
  new_counts
}

counts = full_data[ , c(.N, uniqueN(teacher_id))]
cat(paste0("Step 0): ", counts[1L],
           " Observations, ", counts[2L],
           " Individuals\n"))

## 1) Position Code: 53 = full-time teacher
##    Eliminate 1,321,081 observations (41%) / 184,163 individuals (56%)
full_data = full_data[.(full_data[position_code==53, unique(teacher_id)])]
cat("Step 1):")
counts = counts_update(counts)

## 2) Area Code: 0050 (all-purpose elementary teachers)
##               0300 (English, typically middle/high school)
##               0400 (Mathematics, typically mid/high school)
##    Eliminate 930,321 observations (49%) / 66,778 individuals (46%)
full_data = full_data[.(full_data[area %in% c("0050","0300","0400"), 
                                  unique(teacher_id)])]
cat("Step 2):")
counts = counts_update(counts)

## 3) Highest Degree: 4 (Bachelor's Degree)
##                    5 (Master's Degree)
##    Eliminate 3,661 observations (0%) / 643 individuals (1%)
full_data = full_data[.(full_data[highest_degree %in% 4L:5L,
                                  unique(teacher_id)])]
cat("Step 3):")
counts = counts_update(counts)

## 4) District Code: 99xx are all CESA positions
##    Eliminate 561 observations (0%) / 44 individuals (0%)
full_data = full_data[.(full_data[substring(district, 1L, 2L) != "99",
                                  unique(teacher_id)])]
cat("Step 4):")
counts = counts_update(counts)

## 5) Total Experience: Eliminate teachers with
##   more than 30 and less than 1 (seems to
##   be a typo) years' experience
##   Eliminate 80,875 observations (9%) / 1707 individuals (2%)
full_data = full_data[total_exp >= 1 & total_exp <= 30]
cat("Step 5):")
counts = counts_update(counts)

## 6) Work District Type: 04 are regular public schools
##    Eliminate 5,779 observations (1%) / 1256 individuals (2%)
full_data = full_data[.(full_data[district_work_type == "04",
                                  unique(teacher_id)])]
cat("Step 6):")
counts = counts_update(counts)

## 7) Months Employed / Days of Contract
##    Through 2003-04, months used, days thereafter
##      *Eliminate those who never worked >=8.75 months
##      *Eliminate those who never worked >=175 days
##    Eliminate 3,354 observations (0%) / 838 individuals (1%)
full_data =
  full_data[ , if (any(months_employed[year <= 2004] >= 875, na.rm = TRUE) ||
                   any(days_of_contract[year > 2004] >= 175, na.rm = TRUE))
    .SD, by = teacher_id]
cat("Step 7):")
counts = counts_update(counts)

## 8) Instability in ethnicity/gender
##    Eliminate 1480 observations (1%) / 144 individuals (0%)
full_data = full_data[.(full_data[ , teacher_id[uniqueN(ethnicity) == 1L &
                                                  uniqueN(gender) == 1L],
                                 by = teacher_id]$V1)]
cat("Step 8):")
counts = counts_update(counts)

## 9) Category code: 1 are professional, regular education teachers
##    Eliminate 6230 observations (1%) / 422 individuals (1%)
full_data = full_data[.(full_data[category == "1", unique(teacher_id)])]
cat("Step 9):")
counts = counts_update(counts)

#Finally, write the data:
fwrite(full_data, wds["write"] %+% "wisconsin_teacher_data_matched.csv")
fwrite(full_data[ , .(names(full_data), sapply(.SD, class))],
       wds["write"] %+% "wisconsin_teacher_data_matched_colClass.csv",
       col.names=FALSE)
