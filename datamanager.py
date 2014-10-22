#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import division
from io import open
import json
from pymongo import MongoClient, Connection
import re, sys, unicodedata
from os import listdir, walk
from os.path import isfile, join, splitext
import ConfigParser
import argparse
from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords

# read config file and set up MongoDB
config = ConfigParser.RawConfigParser()
config.read('config.conf')
replacementlistfile=config.get('files','replacementlist')
stopwordsfile=config.get('files','stopwords')
replacementlistlastnamesfile=config.get('files','replacementlistlastnames')
databasename=config.get('mongodb','databasename')
collectionname=config.get('mongodb','collectionname')
collectionnamecleaned=config.get('mongodb','collectionnamecleaned')
client = MongoClient(config.get('mongodb','url'))
db = client[databasename]
collection = db[collectionname]
collectioncleaned = db[collectionnamecleaned]



# hier wordt de functie voor het vervangen van leestekens gedefinieerd. Ze worden ERUIT gehaald, niet vervangen door spaties, en dat is juist wat we willen: willem-alexander --> willemalexander
tbl = dict.fromkeys(i for i in xrange(sys.maxunicode) if unicodedata.category(unichr(i)).startswith('P'))
def remove_punctuation(text):
        return text.translate(tbl)




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
                                elif line.lstrip().startswith("AD/Algemeen Dagblad") or line.lstrip().startswith("De Telegraaf") or line.lstrip().startswith("Trouw") or line.lstrip().startswith("de Volkskrant") or line.lstrip().startswith("NRC Handelsblad") or line.lstrip().startswith("Metro") or line.lstrip().startswith("Spits"):
                                        pass
				else:
					 tekst[artikel]=tekst[artikel]+line
	print "Done!",artikel,"articles added."

	if not len(journal) == len(loaddate) == len(section) == len(language) == len(byline) == len(length) == len(tekst):
		print "!!!!!!!!!!!!!!!!!!!!!!!!!"
		print "Ooooops! Not all articles seem to have data for each field. These are the numbers of fields that where correctly coded (and, of course, they should be equal to the number of articles, which they aren't in all cases."
		print "journal",len(journal)
		print "loaddate",len(loaddate)
		print "section",len(section)
		print "language",len(language)
		print "byline",len(byline)
		print "length",len(length)
		print "tekst",len(tekst)
		print "!!!!!!!!!!!!!!!!!!!!!!!!!"
		print 
		print "Anyhow, we're gonna proceed and set those invalid fields to 'NA'. However, you should be aware of this when analyzing your data!"
		

	else:
		print "No missing values encountered."
	
	for i in range(artikel):
		try:
			art_source=journal[i+1]
		except:
			art_source="NA"
		try:
			art_date=loaddate[i+1]
		except:
			art_date="NA"
		try:
			art_section=section[i+1]
		except:
			art_section="NA"
		try:
			art_language=language[i+1]
		except:
			art_language="NA"
		try:
			art_length=length[i+1]
		except:
			art_length="NA"
		try:
			art_text=tekst[i+1]
		except:
			art_text="NA"
		try:
			art_byline=byline[i+1]
		except:
			art_byline="NA"
		art={"source":art_source.lower(),"date":art_date,"section":art_section.lower(),"language":art_language.lower(),"length":art_length,"text":art_text,"byline":art_byline,"from-database":"lexisnexis"}
		article_id=collection.insert(art)



def clean_database():
        
        '''
        TODO
        ERVOOR ZORGEN DAT JE DE FUNCTIES (1) VERVANGEN (2) LOWERCASE (3) LEESTEKENS (4) STEMMING/STOPWORD REMOVAL AFZONDERLIJK KAN OPROEPEN. VIA ARGPARSE BIJVOORBEELD OF CONFIG.CONF
        NIET ALLEEN STOPWOORDEN, MAAR OOK CIJFERS ERUIT

  
        NU WORDT EERST VERVANGEN, DAARNA LOWERCASE GEDAAN. CHECKEN OF WE DIT WILLEN BLIJVEN DOEN. VOOR: NAUWKEURIGER. TEGEN: Van der Laan WORDT NIET GEVONDEN DOOR van der Laan. MAAR DAT KUNNEN WE OOK APART OPLOSSEN (GEDAAN!)
      
        '''
        
        # make sure that there is no old cleaned collection
        c=Connection()
        c[databasename].drop_collection(collectionnamecleaned)
                       

        # get info on what has to be replaced
        with open(replacementlistfile,mode="r",encoding="utf-8") as fi:
                repldict=json.load(fi)
        # spaties etc escapen voor de REGEXPs
        repldict = dict((re.escape(k), v) for k, v in repldict.iteritems())
        # pattern = re.compile("|".join(repldict.keys()))
        # we want whole words only, therefore we use the following regexp adding the word boundries \b:
        pattern = re.compile("\\b|\\b".join(repldict.keys()))        
        

        # hetzelfde nog een keer voor achternamen
        with open(replacementlistlastnamesfile,mode="r",encoding="utf-8") as fi:
                repldict2=json.load(fi)
        repldict2 = dict((re.escape(k), v) for k, v in repldict2.iteritems())
        pattern2 = re.compile("\\b|\\b".join(repldict2.keys()))        
        pattern2_fullname = re.compile("\\b|\\b".join(repldict2.values()))        

        # processing articles one by one
        allarticles=collection.find()
        aantal=collection.count()
        i=0
        for art in allarticles:
                i+=1
                print "\r",i,"/",aantal," or ",int(i/aantal*100),"%",
                sys.stdout.flush()
                # newlines moeten eruit (vandaar .replace) om "ABN\nAmro" te kunnen vervangen. Maar nu verdwijnen ook alinea's (\n\n), we zouden kunnen overwegen hier een onderscheid te maken. Maar hoeft nu even niet.
                thisart=art["text"].replace("\n"," ")
                #print thisart
                # functie 1: vervangen nav vervanglijstke
                thisart=pattern.sub(lambda m: repldict[re.escape(m.group(0))], thisart)
                # TODO: CHECKEN WAT ER GEBEURT ALS WE EN "KLM AIR FRACNE" EN "AIR FRANCE" ALS ZOEKTERM HEBBEN
                # OPLOSSING: Prioriteiten toekennen. MISSCHIEN TWEE LIJSTJES MAKEN, EERST DE ENE DOEN (KLM OPEN vervangen door KLM_OPEN), daarna de algemene (KLM door KLM_Air_France)

                # functie 1b: als iemand een keer met z'n volledige naam genoemd wordt, ook de volgende keren dat alleen z'n achternaam wordt genoemd deze vervangen
                #print thisart
                for naam in pattern2_fullname.findall(thisart):
                        #print naam,"mentioned, so we're also gonna replace",
                        varianten=[k for k,v in repldict2.items() if v==naam]
                        for naam in varianten:
                                #print naam,"by",repldict2[naam]
                                # DE .lower()s hebben we toegevoegd om het van der Laan/Van der Laan probleem op te lossen
                                thisart=re.sub("\\b"+naam.lower()+"\\b",repldict2[naam],thisart.lower())
                #print thisart

				
                                
                '''
                # functie 2: lowercase
                thisart=thisart.lower()

                # functie 3: leestekens weg
                thisart=remove_punctuation(thisart)


		# stemming wordt verplaatst naar analysis.py zodat het kan worden toegepast indien nodig, maar weggelaten indien niet gewenst

                # functie 4: Stemming and stopwords removal
                # dit zouden de nltk-stopwords zijn, maar wij gebruiken onze eigen lijst
                #stops=set(stopwords.words("dutch"))
                stops=[line.strip() for line in open(stopwords,mode="r",encoding="utf-8")]

                stemmer= SnowballStemmer("dutch")
                tas=thisart.split()
                thisart=""
                for word in tas:
                        if word not in stops:
                                thisart=" ".join((thisart,stemmer.stem(word)))
                
                '''

                # functies 2/3/(4): lowercase, leestekens weg, stopwords en cijfers eruit, maar GEEN stemming
                
                thisart=remove_punctuation(thisart.lower())
                #print thisart
                stops=[line.strip() for line in open(stopwordsfile,mode="r",encoding="utf-8")]
                tas=thisart.split()
                thisart=""
                for woord in tas:
                        if (woord not in stops) and (not woord.isdigit()):
                                thisart=" ".join([thisart,woord])
                # replace original text with  modified text and put the whole item in the cleaned collection 
                art["text"] = thisart
                article_id=collectioncleaned.insert(art)

                #if i > 10:
                #        print "we stoppen ff voor het gemak na 11 artikelen"
                #        break





def main():
        parser=argparse.ArgumentParser(description="This program is part of VETTE NAAM BEDENKEN PLUS VERWIJZING NAAR ONS PAPER. UITLEG GEVEN HOE HET WERKT.")
        group=parser.add_mutually_exclusive_group()
        group.add_argument("--overview", help="Give an overview of the data stored in the collection",action="store_true")
        group.add_argument("--overview_cleaned", help="Give an overview of the data stored in the cleaned collection",action="store_true")
        group.add_argument("--insert_ln", help="Inserts LexisNexis articles. Name the folder with the input data after --insert_ln",)
        group.add_argument("--clean", help="Creates a cleaned version of your collection by removing punctuation and replacing words as specified in the configuration files.", action="store_true")
        group.add_argument("--delete_all", help="Deletes everything in your database (!!!)",action="store_true")
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

        if args.overview_cleaned:
                overview1=collectioncleaned.aggregate([ {"$group" :  {"_id" : "$source", "number": {"$sum":1}}}])
                for combi in overview1["result"]:
                        print combi["_id"],"\t",combi["number"]


        if args.insert_ln:
                print "Starting to insert",args.insert_ln
                print "Including subfolders?",args.recursive
                print "This can take some time..."
                insert_lexisnexis(args.insert_ln,args.recursive)
        
        if args.clean:
                clean_database()
                

                        # insert_lexisnexis("/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen_Dataverzameling project Bedrijven in het Nieuws/Unix-LF_zonderBOM/2. AD",True)
	



if __name__ == "__main__":
        main()
	
