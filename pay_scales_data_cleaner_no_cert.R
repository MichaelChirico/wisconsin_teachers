#!/usr/bin/env Rscript
#Wisconsin Teacher Project
#District-Level Pay Scales Data Cleaning (IGNORING CERTIFICATION)
#Michael Chirico
#March 29, 2017

###############################################################################
#                   Package Setup & Convenient Functions                      #
###############################################################################
rm(list = ls(all = TRUE))
gc()
data.path = "/media/data_drive/wisconsin/"
library(funchir)
library(data.table)

###############################################################################
#                        Data Import & Subset                                 #
###############################################################################

#Import full matched data created in wisconsin_teacher_data_cleaner.R;
#  impose sample restrictions to get rid of teachers bound to
#  cause noise in the contract fitting procedure, as well as teachers
#  with multiple positions by eliminating all but their highest-FTE positions
full_data = 
  fread(data.path %+% "wisconsin_teacher_data_full.csv",
        select = c("year", "highest_degree", "months_employed", "salary",
                   "fringe", "category", "position_code", "area",
                   "full_time_equiv", "days_of_contract", "total_exp_floor",
                   "teacher_id", "district_fill", "district_work_type"),
        #otherwise assumes integer and truncates
        colClasses = 
          list(character = c('area', 'district_fill')))

#Only want full-time teachers
#  (eliminate 1,924,483 = 46%)
#   *Mostly categorized as:
#   "Other Support Staff" [98], "Program Aide" [97],
#   "Short-term Substitute Teacher" [43], or in major
#   administrative/staff positions, e.g.
#   Athletic Coach [77]/Program Coordinator [64]/
#   Principal [51]/Speech-Language Pathologist [84]/
#   Plant Maintenance Personnel [72]/Guidance Counselor [54]/
#   Cafeteria Worker [73]/Clerical Support Staff [68])
full_data = full_data[position_code == 53L]

#Focusing on BA/MA scales; eliminate other degrees
#  (eliminate 21,872 = 1.34%)
full_data = full_data[highest_degree %in% 4L:5L]

#Category 1 - Professional, Regular Education
#  (eliminate 225,186 = 14.94%)
#  Vast majority dropped are 0: Professional, Special Education
full_data = full_data[category == 1L]

#Teachers whose total FTE load is not 100 within a year/district,
#  and who only taught at one district (some teachers listed as
#  full-time equivalent 100 at more than one district in a year;
#  such teachers are quite rare, and perhaps represent some
#  phasing in phenomenon--they tend to be very young)
#  (eliminate 152,176 = 10.94%)
fte100 =
  full_data[ , if (sum(full_time_equiv, na.rm = TRUE) == 100) TRUE,
             by = .(teacher_id, year, district_fill)
             ][ , if (uniqueN(district_fill) == 1L) .SD, 
                by = .(teacher_id, year)][ , .(teacher_id, year)]
full_data = full_data[fte100, on = c('teacher_id', 'year')]

rm(fte100)
#Eliminate all but the highest-FTE position (note -- this ordering
#  __shouldn't__ matter, because teachers should only be associated
#  with one salary in a given year, which is __almost__ without fail the case)
#  (eliminate 228,710 = 18%)
full_data = full_data[order(full_time_equiv), .SD[.N],
                      by = .(teacher_id, year)]

#Focus on Wisconsin Public Schools
#  (eliminate 14,222 = 1.4%)
full_data = full_data[district_work_type == '04']

#Despite efforts to focus on full-time teachers, some,
#  as per the months_employed construct (used pre-2003)
#  or the days_of_contract construct (used since), still enter
#  with what must be less than full-time contracts;
#  examining, e.g., boxplots of salary against this variable
#  suggest there is still a systemic deficiency in pay for
#  those working less than the "full year"; this increasing
#  relationship appears to stabilize between 8.75 and 10.5 months 
#  (or 175-195 days contracted), so eliminate those outside these bounds
#  (eliminate 14,284 = 1.4%)
full_data = 
  full_data[(months_employed %between% c(875, 1050) | year > 2003) &
              (days_of_contract %between% c(175, 195) | year <= 2003)]

#Seems unlikely a full-time teacher should be
#  making <10k in a year, so this should be eliminated as erroneous
#  (eliminate 807 = .08%)
full_data = full_data[salary + fringe >= 10000]

#Restrict focus to total experience (rounded down) between 1 & 30;
#  Total exp = 0 appears to be some sort of error? Very rare anyway.
#  There are nontrivial #s of teachers beyond 30 years, but 30 seems
#  as good a place as any (see robustness checks). box-plotting
#  pay vs. total experience, we see the series start to change around
#  25 (narrowing of IQR), suggesting some selection effects starting
#  to become important.
#  (eliminate 78,874 = 8%)
full_data = full_data[total_exp_floor %in% seq_len(30L)]

#Restrict subject areas
#  Most common excluded subject areas area:
#  [0910] - Health
#  [0002] - Academic Support - Teachers
#  [0940] - Academic Support - Non-Special Education Students
#  [0014] - Gifted & Talented
#  [0935] - At-Risk Tutor
#  [0001] - Non-teaching Time
#  [0952] - Alternative Education
#  (eliminate 22,577 = 2.5%)
full_data = full_data[substring(area, 2, 2) %in% 2:7 | area == '0050']

#Now, eliminate schools with insufficient coverage
yrds = c('year', 'district_fill')
setkeyv(full_data, yrds)

#Can't interpolate if there are only 2 or 3
#  unique experience cells represented
full_data[ , node_count_flag := uniqueN(total_exp_floor) < 7L, by = yrds]

#Nor if there are too few teachers
full_data[ , teach_count_flag := .N < 20L, by = yrds]

#Also troublesome when there is little variation in salaries like so:
full_data[ , sal_count_flag := uniqueN(salary) < 5L, by = yrds]
full_data[ , frn_count_flag := uniqueN(fringe) < 5L, by = yrds]

#Discard any variables hit by a flag
#  (eliminate 189,440 = 21.5%, almost all from teach_count_flag)
full_data = 
  full_data[!(node_count_flag | teach_count_flag | 
                sal_count_flag | frn_count_flag)]

#Discard variables not necessary for interpolation
n_teachers = full_data[ , uniqueN(teacher_id)]
full_data = 
  full_data[, .(year, district_fill, total_exp_floor, salary, fringe)]

fwrite(full_data, data.path %+% 'wisconsin_teacher_data_for_payscales_no_cert.csv')
