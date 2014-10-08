#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from io import open
import json
import pymongo
import re
from os import listdir, walk
from os.path import isfile, join, splitext


# where is all the data being stored?
client = MongoClient('mongodb://localhost:27017/')
db = client['articles']
collection = db['damianjeroen']


def insert_lexisnexis(pathwithlnfiles,recursive):
	"""
	Usage: insert_lexisnexis(pathwithlnfiles,recursive)
	pathwithlnfiles = path to a directory where lexis nexis output is stored
	recursive: TRUE = search recursively all subdirectories, but include only files ending on .txt
	           FALSE = take ALL files from directory supplied, but do not include subdirectories
	"""
	tekst={}
	byline={}
	section={}
	length={}
	loaddate={}
	language={}
	pubtype={}
	journal={}
	if recursive:
		alleinputbestanden=[]
		for path, subFolders, files in walk(pathwithlnfiles):
			for f in files:
				if isfile(join(path,f)) and splitext(f)[1].lower()==".txt":
					alleinputbestanden.append(join(path,f))
	else:
		alleinputbestanden = [ f for f in listdir(pathwithlnfiles) if isfile(join(pathwithlnfiles,f)) ]
	artikel=0
	for bestand in alleinputbestanden:
		with open(bestand,"r") as f:
			i=0
			for line in f:
				i=i+1
				#print "Regel",i,": ", line
				line=line.replace("\r","")
				if line=="\n":
					continue
				matchObj=re.match(r"\s+(\d+) of (\d+) DOCUMENTS",line)
				if matchObj:
					artikel+=1
					tekst[artikel]=""
					continue
				if line.startswith("BYLINE"):
					byline[artikel]=line.replace("BYLINE: ","").rstrip("\n")
				elif line.startswith("SECTION"):
					 section[artikel]=line.replace("SECTION: ","").rstrip("\n")
				elif line.startswith("LENGTH"):
					length[artikel]=line.replace("LENGTH: ","").rstrip("\n").rstrip(" woorden")
				elif line.startswith("LOAD-DATE"):
					loaddate[artikel]=line.replace("LOAD-DATE: ","").rstrip("\n")
				elif line.startswith("LANGUAGE"):
					language[artikel]=line.replace("LANGUAGE: ","").rstrip("\n")
				elif line.startswith("PUBLICATION-TYPE"):
					pubtype[artikel]=line.replace("PUBLICATION-TYPE: ","").rstrip("\n")
				elif line.startswith("JOURNAL-CODE"):
					journal[artikel]=line.replace("JOURNAL-CODE: ","").rstrip("\n")
				elif line.lstrip().startswith("Copyright ") or line.lstrip().startswith("All Rights Reserved"):
					pass
				else:
					 tekst[artikel]=tekst[artikel]+line
	print "Er zijn",artikel,"artikelen."
	krantenlijst=[]
	lengteartikelen=[]
	datumlijst=[]

	for i in range(artikel):
		krantenlijst.append(journal[i+1])
		lengteartikelen.append(length[i+1])
		datumlijst.append(loaddate[i+1])

	outputdata=zip(krantenlijst,lengteartikelen, datumlijst, sentimentlijst)
	writer=CsvUnicodeWriter(open("overzicht.csv","wb"))
	writer.writerows(outputdata)











def main():
	print "Hoi."
	# insertlexisnexis("/Users/damain/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen_Dataverzameling project Bedrijven in het Nieuws/Unix-LF_zonderBOM",TRUE)
	
if __name__ == "__main__":
	main()
	