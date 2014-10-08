#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from io import open
import json
from pymongo import MongoClient, Connection
import re
from os import listdir, walk
from os.path import isfile, join, splitext

import argparse



# where is all the data being stored?

databasename="articles"
collectionname="damianjeroen"

client = MongoClient('mongodb://localhost:27017/')
db = client[databasename]
collection = db[collectionname]


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
				# print "Regel",i,": ", line
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
	print "Done!",artikel,"articles added."
	"""
	print len(journal)
	print len(loaddate)
	print len(section)
	print len(language)
	print len(byline)
	print len(length)
	print len(tekst)
	"""

	for i in range(artikel):
		art={"source":journal[i+1],"date":loaddate[i+1],"section":section[i+1],"language":language[i+1],"length":length[i+1],"text":tekst[i+1],"from-database":"lexisnexis"}
		article_id=collection.insert(art)
		# "byline":byline[i+1] vervalt, werkt kennelijk niet altijd




def main():
        parser=argparse.ArgumentParser(description="This program is part of VETTE NAAM BEDENKEN PLUS VERWIJZING NAAR ONS PAPER. UITLEG GEVEN HOE HET WERKT.")
        group=parser.add_mutually_exclusive_group()
        group.add_argument("--delete_all", help="Deletes everything in your database (!!!)",action="store_true")
        group.add_argument("--overview", help="Give an overview of the data stored in the database",action="store_true")
        group.add_argument("--insert_ln", help="Inserts LexisNexis articles. Name the folder with the input data after --insert_ln",)
        parser.add_argument("--recursive", help="Indicates that all subfolders are processed as well", action="store_true")
        # parser.add_argument("folder", help = "The folder in which data to be inserted is stored", nargs="?")

        args = parser.parse_args()

        if args.delete_all:
                #print "NOG NIET GEIMPLEMENTEERD"
                print "Do you REALLY want to ERASE the whole collection",collectionname,"within the database",databasename,"?"
                cont=raw_input('Type "Hell, yeah!" and hit Return if you want to continue: ')
                if cont=="Hell, yeah!":
                        c=Connection()
                        c[databasename].drop_collection(collectionname)
                        print "Done. RIP",collectionname
                else:
                        print "OK, you changed your mind. Fine."
                
        if args.overview:
                #alles=collection.find()
                #for artikel in alles:
                #        print artikel["date"],artikel["source"]
                overview1=collection.aggregate([ {"$group" :  {"_id" : "$source", "number": {"$sum":1}}}])
                #print overview1["result"]
                for combi in overview1["result"]:
                        print combi["_id"],"\t",combi["number"]

        if args.insert_ln:
                print "Starting to insert",args.insert_ln
                print "Including subfolders?",args.recursive
                print "This can take some time..."
                insert_lexisnexis(args.insert_ln,args.recursive)
                        # insert_lexisnexis("/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen_Dataverzameling project Bedrijven in het Nieuws/Unix-LF_zonderBOM/2. AD",True)
	



if __name__ == "__main__":
        main()
	
