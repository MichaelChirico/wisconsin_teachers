#Wisconsin Teacher Project
#Raw Teacher Data Check
#Michael Chirico
#August 23, 2015

rm(list=ls(all=T))
gc()
wds<-c(data="/media/data_drive/wisconsin/teacher_raw_data/data_files/",
       key="/media/data_drive/wisconsin/teacher_raw_data/fwf_keys/")

library(data.table)
library(funchir)

#Download and extract raw files to data folder
#  Raw files from here: http://lbstat.dpi.wi.gov/lbstat_newasr
urls<-readLines("~/Desktop/research/Wisconsin Bargaining/teacher_data_urls.txt")
sapply(urls,function(uu){
  tmp<-tempfile()
  download.file(uu,tmp)
  unzip(tmp,exdir=wds["data"])
  unlink(tmp)}); rm(urls)

#Remove pesky NUL characters
#  (having checked manually that,
#   as of Dec. 10, 2015,
#   each _pair_ of NUL characters
#   probably is supposed to
#   correspond to a _single_ space)
setwd(wds["data"])
for (fl in list.files(pattern="(?i)(txt|dat)")){
  r<-readBin(fl, raw(), file.info(fl)$size)
  #Remove BOM if it's there:
  if (any(r[1]==as.raw(0xef))){
    r<-r[-(1:3)]
  }
  #NUL corresponds to as.raw(0)
  if (any(r==as.raw(0))){
    idx <- which(r == as.raw(0))
    #only overwrite half with a space (as.raw(0x20))
    r[idx[seq(1,length(idx),by=2)]] <- as.raw(0x20)
    #exclude all remaining NUL and overwrite original file
    writeBin(r[-idx[seq(2,length(idx),by=2)]], fl)
  } else writeBin(r, fl)
}; rm(r)

#Now, fix all irregular files -- for the most part,
#  there are certain lines which appear to have been
#  stripped of trailing white space, so simply
#  augment these with enough blanks to make each file
#  flush on its edges.
for (fl in list.files()){
  wdth<-fread(wds["key"]%+%substr(fl,1,2)%+%"keys.csv")[,sum(V2)]
  #for exploring the files, use a variation on the following:
  #   fread(fl, header = FALSE, sep = "^", strip.white = FALSE
  #         )[,{wd<-nchar(V1); idx<-wd!=wdth
  #         table(wd)
  #         .(`Line #`=.I[idx],`Width`=wd[idx],
  #           `ID`=substr(V1[idx],1,9))}]
  write.table(
    fread(fl, header = FALSE, sep = "^", strip.white = FALSE,
          skip = if (grepl("14|13",fl)) 1L else 0L
          )[nchar(V1)<wdth,V1:=
              #simply append white space to ragged lines
              V1%+%sapply(V1,function(x)
                paste(rep(" ",wdth-nchar(x)),collapse=""))][],
    #overwrite file, mimicking original format as closely as possible
    file=fl,quote=F,row.names=F,col.names=F)
}

#Now, convert all fixed width files to .csv -- a one-time
#  high-cost operation which pays large dividends because
#  all future reading via fread is much faster
for (fl in list.files()){
  keys<-fread(wds["key"]%+%substr(fl,1,2)%+%"keys.csv")
  write.table(setnames(setDT(input.file(fl,formatter=dstrfw,
                            col_types=keys$V3,widths=keys$V2)),
           keys$V1),file=gsub("[.].*",".csv",fl),row.names=F,sep="\t")
}
