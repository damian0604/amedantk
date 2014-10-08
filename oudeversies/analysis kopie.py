#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import division
from io import open
import json
from pymongo import MongoClient, Connection
import argparse
import ConfigParser
from collections import Counter, defaultdict
import sys
from nltk.stem import SnowballStemmer
from itertools import combinations

# read config file and set up MongoDB
config = ConfigParser.RawConfigParser()
config.read('config.conf')
dictionaryfile=config.get('files','dictionary')
networkoutputfile=config.get('files','networkoutput')
databasename=config.get('mongodb','databasename')
collectionname=config.get('mongodb','collectionname')
collectionnamecleaned=config.get('mongodb','collectionnamecleaned')
client = MongoClient(config.get('mongodb','url'))
db = client[databasename]
collection = db[collectionname]
collectioncleaned = db[collectionnamecleaned]




def frequencies_nodict():
    '''
    returns a counter object of word frequencies
    '''

    # .replace is nodig omdat we ervan uitgaan dat alle spaties in de tekst door _ zijn vervangen (met de clean-functie)
    #knownwords = set([line.strip().replace(" ","_").lower() for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    # we moeten deze woorden natuurlijk ook stemmen als we collection_cleaned hebben gestemd
    stemmer=SnowballStemmer("dutch")
    knownwords = set([stemmer.stem(line.strip().replace(" ","_").lower()) for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    
    all=collectioncleaned.find({},{"text": 1, "_id":0})
    aantal=all.count()
    #print all[50]["text"]
    unknown=[]
    i=0
    for item in all:
       i+=1
       print "\r",i,"/",aantal," or ",int(i/aantal*100),"%",
       sys.stdout.flush()
                
       unknown+=[woord for woord in item["text"].split() if woord not in knownwords]
    # print unknown

    c=Counter(unknown)

    return c



def frequencies():
    '''
    returns a counter object of word frequencies
    '''

    # .replace is nodig omdat we ervan uitgaan dat alle spaties in de tekst door _ zijn vervangen (met de clean-functie)
    #knownwords = set([line.strip().replace(" ","_").lower() for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    # we moeten deze woorden natuurlijk ook stemmen als we collection_cleaned hebben gestemd
    stemmer=SnowballStemmer("dutch")
    knownwords = set([stemmer.stem(line.strip().replace(" ","_").lower()) for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    
    all=collectioncleaned.find({},{"text": 1, "_id":0})
    aantal=all.count()
    #print all[50]["text"]
    c=Counter()
    i=0
    for item in all:
       i+=1
       print "\r",i,"/",aantal," or ",int(i/aantal*100),"%",
       sys.stdout.flush()
       c.update([woord for woord in item["text"].split()])         
    return c






def coocnet(n,minedgeweight):
    ''' 
    n = top n words
    minedgeweight = minimum number of co-occurances (=edgeweight) to be included
    '''


    '''
    TODO
    
    GIVE THE OPTION TO DETERMINE WHAT HAS TO BE INCLUDED BASED ON LOGLIKELIHOOD ETC INSTEAD OF RAW FREQUENCIES
    '''
    
    cooc=defaultdict(int)
    
    print "Determining the",n,"most frequent words...\n"
    c=frequencies()
    topnwords=set([a for a,b in c.most_common(n)])
   
    all=collectioncleaned.find({},{"text": 1, "_id":0})
    aantal=all.count()
    
    print "\n\nDetermining the cooccurrances of these words with a minimum cooccurance of",minedgeweight,"...\n"
    i=0
    for item in all:
        i+=1
        print "\r",i,"/",aantal," or ",int(i/aantal*100),"%",
        words=item["text"].split()
        wordsfilterd=[w for w in words if w in topnwords]		
        uniquecombi = set(combinations(wordsfilterd,2))
        for a,b in uniquecombi:
            if (b,a) in cooc:
                continue
            else:
                if a!=b:
                    cooc[(a,b)]+=1


    with open(networkoutputfile,mode="w",encoding="utf-8") as f:
        f.write("nodedef>name VARCHAR, width DOUBLE\n")
        algenoemd=[]
        verwijderen=[]
        for k in cooc:
                if cooc[k]<minedgeweight:
                        verwijderen.append(k)
                else:
                        if k[0] not in algenoemd:
                                f.write(k[0]+","+str(c[k[0]])+"\n")
                                algenoemd.append(k[0])
                        if k[1] not in algenoemd:
                                f.write(k[1]+","+str(c[k[1]])+"\n")
                                algenoemd.append(k[1])
        for k in verwijderen:
                del cooc[k]

        f.write("edgedef>node1 VARCHAR,node2 VARCHAR, weight DOUBLE\n")
        for k, v in cooc.iteritems():
                regel= ",".join(k)+","+str(v)
                f.write(regel+"\n")

    print "\nDone. Network file written to",networkoutputfile
    



















def main():
    parser=argparse.ArgumentParser("This program is part of VETTE NAAM BEDENKEN EN ZO VERDER")
    group=parser.add_mutually_exclusive_group()
    group.add_argument("--frequencies",help="List the N most common words")
    group.add_argument("--frequencies_nodict",help="List the N most common words, but only those which are NOT in the specified dictionary (i.e., list all non-dutch words)")
    group.add_argument("--network",help="Create .gdf network file to visualize word-cooccurrances of the N1 most frequently used words with a minimum edgeweight of N2. E.g.: --network 200 50",nargs=2)

    args=parser.parse_args()

    if args.frequencies:
        c=frequencies()
        for k,v in c.most_common(int(args.frequencies)):
            print v,"\t",k
            # willen we de woorden nog opslaan? zo ja, iets toevoegen zoals hieronder. of met set() als we de duplicaten eruit willen halen
            '''
            with open(outputbestand,"w", encoding="utf-8") as f:
            for woord in belangrijk:
            f.write(woord+"\n")
            '''



    if args.frequencies_nodict:
        c=frequencies_nodict()
        for k,v in c.most_common(int(args.frequencies_nodict)):
            print v,"\t",k
            # willen we de woorden nog opslaan? zo ja, iets toevoegen zoals hieronder. of met set() als we de duplicaten eruit willen halen
            '''
            with open(outputbestand,"w", encoding="utf-8") as f:
            for woord in belangrijk:
            f.write(woord+"\n")
            '''

    if args.network:
        coocnet(int(args.network[0]),int(args.network[1]))



if __name__ == "__main__":
    main()
