#Wisconsin Teachers Project
#Background Data Clean-up, organization
#Michael Chirico
#August 25, 2015

###############################################################################
#                             Package Setup                                   #
###############################################################################

rm(list = ls(all = TRUE))
gc()
cc.d.wd = "/media/data_drive/common_core/district/"
library(funchir)
library(data.table)

###############################################################################
#                           Demographic Data                                  #
###############################################################################

# Urbanicity (school-level, via CCD)


