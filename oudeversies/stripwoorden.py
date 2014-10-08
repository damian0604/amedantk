#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from io import open
from collections import Counter
from nltk.stem import SnowballStemmer


import unicodedata
import sys

# functie om leestekens te verwijderen
tbl = dict.fromkeys(i for i in xrange(sys.maxunicode) if unicodedata.category(unichr(i)).startswith('P'))
def remove_punctuation(text):
    return text.translate(tbl)


inputbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/alleartikelen_kort_processed.txt"
outputbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/tekstyondernederlandsewoorden.txt"
nlwoordenbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/OpenTaal-210G-basis-gekeurd.txt"

# TODO: PLUS: CASE-specifiek bestand van filterwoorden (bijvoorbeeld lexis-nexis termen)


# .replace is nodig omdat we ervan uitgaan dat alle spaties in de tekst door _ zijn vervangen
nlwoorden = set([line.strip().replace(" ","_").lower() for line in open(nlwoordenbestand,mode="r",encoding="utf-8")])
tekstwoorden=[]


lines=[line.rstrip("\n").split() for line in open(inputbestand,mode="r",encoding="utf-8")]
print "Klaar met lezen!"
# TODO: EVTL snowballstemmer vervangen door een geavanceerder algoritme door bijvoorbeeld de zinnen te parsen en dan zelfstandige naamwoorden (meervoud->enkelvoud) anders te behandelen dan werkwoorden (-->INFINITIVE)
stemmer = SnowballStemmer("dutch")
tekstwoorden=[stemmer.stem(remove_punctuation(item).lower()) for sublist in lines for item in sublist]
print "Klaar met flattenen!"

belangrijk=[woord for woord in tekstwoorden if woord not in nlwoorden]
#met set zou het veel sneller gaan, maar dan kunnen we niet meer tellen.
#belangrijk=set(tekst.split()) - set(nlwoorden)

with open(outputbestand,"w", encoding="utf-8") as f:
	for woord in belangrijk:
		f.write(woord+"\n")

c=Counter(belangrijk)

for k,v in c.most_common(500):
	print v,"\t",k
