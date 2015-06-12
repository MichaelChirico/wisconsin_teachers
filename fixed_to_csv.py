import os
import csv
#Set correct directory
os.chdir('/home/michael/Desktop/research/Wisconsin Bargaining/')

#This file can be used to convert all of the fixed-width format teacher-level data files for Wisconsin
## (found here: http://lbstat.dpi.wi.gov/lbstat_newasr)
# to standard .csv format

def checkLength(ffile):
	"""
	Used to check that all lines in file have the same length (and so don't cause any issues below)
	"""
	with open(ffile,'r') as ff:
		firstrow=1
		troubles=0
		for rows in ff:
			if firstrow:
				length=len(rows)
				firstrow=0
			elif len(rows) != length:
				print rows
				print len(rows)
				troubles=1
	return troubles

def fixed2csv(infile,outfile,dictfile):
	"""
	This function takes a file name for a fixed-width dataset as input and 
	converts it to .csv format according to slices and column names specified in dictfile

	Parameters
	==========
		infile: string of input file name from which fixed-width data is to be read
			    e.g. 'fixed_width.dat'
	    outfile: string of output file name to which comma-separated data is to be saved
	    		 e.g. 'comma_separated.csv'
		dictfile: .csv-formatted dictionary file name from which to read the following:
				     * widths: field widths
				     * column names: names of columns to be written to the output .csv
			      column order must be: col_names,slices,types
	"""
	with open(dictfile,'r') as dictf:
		fieldnames = ("col_names","widths","types") #types used in R later
		ddict = csv.DictReader(dictf,fieldnames)
		slices=[]
		colNames=[]
		wwidths=[]
		for rows in ddict:
			wwidths.append(int(rows['widths'])) #Python 0-based, must subtract 1
			colNames.append(rows['col_names'])
		offset = 0
		for w in wwidths:
			slices.append(slice(offset,offset+w))
			offset+=w
	with open(infile,'r') as fixedf:
		with open(outfile,'w') as csvf:
			csvfile=csv.writer(csvf)
			csvfile.writerow(colNames)
			for rows in fixedf:
				csvfile.writerow([rows[s] for s in slices])


#Specify in list format all of the files to be converted, as well as names for the output files and the corresponding dictionary files
file_list =['95staff.txt','96staff.txt','97staff.txt','98staff.txt','99STAFF.DAT',
            '00staff.dat','01staff.dat','02staff.txt','03staff.txt','04staff.dat',
            '05staff.txt','06staff.txt','07staff.txt','08STAFF.TXT','09STAFF.TXT',
            '10STAFF.TXT','12STAFF.txt','13staff.txt','14staff.txt']

outfile_list = ['95staff.csv','96staff.csv','97staff.csv','98staff.csv','99staff.csv',
                '00staff.csv','01staff.csv','02staff.csv','03staff.csv','04staff.csv',
                '05staff.csv','06staff.csv','07staff.csv','08staff.csv','09staff.csv',
                '10staff.csv','12staff.csv','13staff.csv','14staff.csv']

dict_list = ['95dict.csv','96dict.csv','97dict.csv','98dict.csv','99dict.csv',
             '00dict.csv','01dict.csv','02dict.csv','03dict.csv','04dict.csv',
             '05dict.csv','06dict.csv','07dict.csv','08dict.csv','09dict.csv',
             '10dict.csv','12dict.csv','13dict.csv','14dict.csv']

for ii in range(len(file_list)):
	if checkLength(file_list[ii])==0:
		fixed2csv(file_list[ii],outfile_list[ii],dict_list[ii])
		print 'File ' + file_list[ii] + ' converted.'
	else:
		print 'Re-check '+ file_list[ii]+' for errors.'

