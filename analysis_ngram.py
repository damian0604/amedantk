#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import division
from io import open
import json
from pymongo import MongoClient, Connection, TEXT
import argparse
import ConfigParser
from collections import Counter, defaultdict
import sys
from nltk.stem import SnowballStemmer
from itertools import combinations, chain
import ast
from numpy import log
from gensim import corpora, models, similarities
from nltk.util import ngrams as nltkngram


# read config file and set up MongoDB
config = ConfigParser.RawConfigParser()
config.read('config.conf')
dictionaryfile=config.get('files','dictionary')
networkoutputfile=config.get('files','networkoutput')
lloutputfile=config.get('files','loglikelihoodoutput')
databasename=config.get('mongodb','databasename')
collectionname=config.get('mongodb','collectionname')
collectionnamecleaned=config.get('mongodb','collectionnamecleaned')
client = MongoClient(config.get('mongodb','url'))
db = client[databasename]
collection = db[collectionname]
collectioncleaned = db[collectionnamecleaned]



print "Ensure that the articles are properly indexed..."
collectioncleaned.ensure_index([("text", TEXT)], cache_for=300,default_language="nl",language_override="nl")
print "Done building index."



def frequencies_nodict():
    '''
    returns a counter object of word frequencies
    '''

    # .replace is nodig omdat we ervan uitgaan dat alle spaties in de tekst door _ zijn vervangen (met de clean-functie)
    #knownwords = set([line.strip().replace(" ","_").lower() for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    # we moeten deze woorden natuurlijk ook stemmen als we collection_cleaned hebben gestemd
    #stemmer=SnowballStemmer("dutch")
    #knownwords = set([stemmer.stem(line.strip().replace(" ","_").lower()) for line in open(dictionaryfile,mode="r",encoding="utf-8")])
    
    knownwords = set([line.strip().replace(" ","_").lower() for line in open(dictionaryfile,mode="r",encoding="utf-8")])

    all=collectioncleaned.find(subset,{"text": 1, "_id":0})
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
    
    all=collectioncleaned.find(subset,{"text": 1, "_id":0})
    aantal=all.count()
    # print all[50]["text"]
    c=Counter()
    i=0
    for item in all:
       i+=1
       print "\r",i,"/",aantal," or ",int(i/aantal*100),"%",
       sys.stdout.flush()
       c.update([woord for woord in nltkngram(item["text"].split(),n=2)])         
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
   
    all=collectioncleaned.find(subset,{"text": 1, "_id":0})
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
    





def llcompare(corpus1,corpus2,llbestand):
	# using the same terminology as the cited paper:
	# a = freq in corpus1
	# b = freq in corpus2
	# c = number of words corpus1
	# d = number of words corpus2
	# e1 = expected value corpus1
	# e2 = expected value corpus2

	c = len(corpus1)
	d = len(corpus2)
	ll={}
	
	for word in corpus1:
		a=corpus1[word]
		try:
			b=corpus2[word]
		except KeyError:
			b=0
		e1 = c * (a + b) / (c + d)
		e2 = d * (a + b) / (c + d)
		# llvalue=2 * ((a * log(a/e1)) + (b * log(b/e2)))
		# if b=0 then (b * log(b/e2)=0 and NOT nan. therefore, we cannot use the formula above
		if a==0:
			part1=0
		else:
			part1=a * log(a/e1)
		if b==0:
			part2=0
		else:		
			part2=b * log(b/e2)
		llvalue=2*(part1 + part2)
		ll[word]=llvalue
	
	for word in corpus2:
		if word not in corpus1:
			a=0
			b=corpus2[word]
			e2 = d * (a + b) / (c + d)
			llvalue=2 * (b * log(b/e2))
			ll[word]=llvalue

	with open(llbestand, mode='w', encoding="utf-8") as f:
            f.write("ll,word,freqcorp1,freqcorp2\n")
            for word,value in sorted(ll.iteritems(), key=lambda (word,value): (value, word), reverse=True):
                    print value,word
                    try:
                            freqcorp1=corpus1[word]
                    except KeyError:
                            freqcorp1=0
                    try:
                            freqcorp2=corpus2[word]
                    except KeyError:
                            freqcorp2=0
                    f.write(str(value)+","+word+","+str(freqcorp1)+","+str(freqcorp2)+"\n")



def ll():
    corpus1=frequencies()
    print len(corpus1)
    global subset
    subsetbak=subset
    subset={}
    corpus2=frequencies()
    print len(corpus2)
    subset=subsetbak
    llcompare(corpus1,corpus2,lloutputfile)




def lda(ntopics,minfreq):
    c=frequencies()
    all=collectioncleaned.find(subset,{"text": 1, "_id":0})
    
    texts =[[word for word in item["text"].split()] for item in all]
    
    texts =[[word for word in text if c[word]>=minfreq] for text in texts]
    
    # Create Dictionary.
    id2word = corpora.Dictionary(texts)

    # Creates the Bag of Word corpus.
    mm =[id2word.doc2bow(text) for text in texts]
    # Trains the LDA models.
    lda = models.ldamodel.LdaModel(corpus=mm, id2word=id2word, num_topics=ntopics, update_every=1, chunksize=10000, passes=1)
    # Prints the topics.
    for top in lda.print_topics(): 
        print "\n",top









def main():
    parser=argparse.ArgumentParser("This program is part of VETTE NAAM BEDENKEN EN ZO VERDER")
    group=parser.add_mutually_exclusive_group()
    group.add_argument("--frequencies",help="List the N most common words")
    group.add_argument("--frequencies_nodict",help="List the N most common words, but only those which are NOT in the specified dictionary (i.e., list all non-dutch words)")
    group.add_argument("--lda",help="Perform a Latent Diriclet Allocation analysis with N topics",nargs=2)
    group.add_argument("--ll",help="Compare the loglikelihood of the words within the subset with the whole dataset",action="store_true")
    group.add_argument("--network",help="Create .gdf network file to visualize word-cooccurrances of the N1 most frequently used words with a minimum edgeweight of N2. E.g.: --network 200 50",nargs=2)
    group.add_argument("--search", help="Perform a simple search, no further options possible. E.g.:  --search hema")
    parser.add_argument("--subset", help="Use MongoDB-style .find() filter in form of a Python dict. E.g.:  --subset=\"{'source':'de Volkskrant'}\" or --subset=\"{'\\$text':{'\\$search':'hema'}}\" or a combination of both: --subset=\"{'\\$text':{'\\$search':'hema'},'source':'de Volkskrant'}\"")
    # parser.add_argument("--search", help="Use MongoDB-style text search in form of a Python dict. E.g.:  --subset \"{'\\$text':{'\\$search':'hema'}}\"")
    



    '''
    TODO TEXT SEARCH
    ---search
    FILTEREN OP ZOEKTERMEN
    http://blog.mongodb.org/post/52139821470/integrating-mongodb-text-search-with-a-python-app
    '''


    args=parser.parse_args()

    global subset
    if not args.subset:
        subset={}
    else:
        try:
            subset=ast.literal_eval(args.subset)
        except:
            print "You specified an invalid filter!"
            sys.exit()

        if type(subset) is not dict:
            print "You specified an invalid filter!"
            sys.exit()


        print "Analysis will be based on a dataset filterd on",subset


    if args.search:
        query=db.command('text',collectionnamecleaned,search=args.search, language="nl")
        print "Finished with search,",len(query["results"]),"matching articles found."
        print "Some stats:",query["stats"]
        print "relevance\tsource\tdate"
        for results in query["results"]:
            print results["score"],"\t",results["obj"]["source"],"\t",results["obj"]["date"]


    if args.ll:
        ll()

    if args.lda:
        lda(int(args.lda[0]),int(args.lda[1]))


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
