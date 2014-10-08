#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from io import open
import json
import re

# waar is het vervanglijstje opgeslagen?
vlbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/vervanglijstje.json"


# in welk bestand moeten de uitdrukkingen worden vervangen?
inbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/alleartikelen_kort.txt"

# waar moet het resultaat opgeslagen worden?
outbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/alleartikelen_kort_processed.txt"


with open(vlbestand,mode="r",encoding="utf-8") as fi:
	repldict=json.load(fi)

text=open(inbestand,mode="r",encoding="utf-8").read()


# spaties etc escapen voor de REGEXPs
repldict = dict((re.escape(k), v) for k, v in repldict.iteritems())
# pattern = re.compile("|".join(repldict.keys()))
# we want whole words only, therefore we use the following regexp adding the word boundries \b:
pattern = re.compile("\\b|\\b".join(repldict.keys()))
# print "\\b|\\b".join(repldict.keys())


print "Dit gaat ff duren..."
text = pattern.sub(lambda m: repldict[re.escape(m.group(0))], text)

with open(outbestand,mode="w",encoding="utf-8") as fo:
	fo.write(text)

print "PUT YOUR COCKTAILS DOWN, YOU'RE READY!"