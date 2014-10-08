#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from io import open
import json

# TODO

'''
DEELS HIER, DEELS IN vervang.py
VERVANGPROGRAMMA
- koppeltekens
- é ä etc --> e a ae (meerdere spellingsvarianten toestaan) (functie voor alles!)
- uitzonderingen als E.ON (EON, eon,...)?? --> uitzonderingenlijstje

spaties
McDonald's
H & M --> H&M
Heineken Holding(s) --> ??
Reckitt Benckiser Group
& Co --> strippen!
Banco Santander --> Santander
Hyundai Heavy Industries
'''



# bestanden specificeren
# bestand met (alle) Nederlandse woorden, één per regel
nlwoordenbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/OpenTaal-210G-basis-gekeurd.txt"
# bestanden met namen (voornaam achternaam, één per regel)
naambestanden=["/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/politici_voornaam_achternaam.txt"]
# own tab-sperated files with two columns (first column original, second replacement)
ownreplacements=["/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/vervanglijstje.tab"]

# own tab-sperated files with two columns (first column original, second replacement) - special for lastnames
ownreplacements_lastnames=["/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/vervanglijstje_speciaal_menselijke achternamen.tab"]



# lijst met bedrijven 
lijstenmetbedrijven=["/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/lijst_bedrijven_500meestinvloedrijkinNL.txt","/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/input/lijst_bedrijven_multinationals_top2000.txt"]


# Waar de output opslaan?
outputbestand="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/vervanglijstje.json"

# een nog een outputbestand, nu voor achternamen (afzonderlijk)
outputbestand2="/Users/damian/Dropbox/uva/onderzoeksprojecten_lopend/2014-damianjeroen/3. Nieuwprogramma/output/vervanglijstje_achternamen.json"






def replacespaces(listwithwords):
	repldict={}
	wordswithspace=[woord for woord in listwithwords if woord.find(" ")>-1]
	for woord in wordswithspace:
		repldict[woord]=woord.replace(" ","_")
	return repldict
	
	
def replacenames(naambestanden):
	repldict={}
	names=[]
	for fname in naambestanden:
		naamtemp=[line.strip() for line in open(fname,mode="r",encoding="utf-8")]
		names+=naamtemp
	for naam in names:
		# first, just the same as with space removal
		repldict[naam]=naam.replace(" ","_")
		# BUT we are also going to assume that there is just one person with the same last name (TRICKY, I KNOW, HAS TO BE IMPROVED):
		naamsplit=naam.split()
		if naamsplit[-2].lower() not in ["van", "de","den","der"]:
			# TODO:
			# HIER WORDT "De Vries" VERVANGEN DOOR "Henk_de_Vries"
			# DIT GAAT NATUURLIJK NIET ALTIJD OP, ZEKER MET VAAK VOORKOMENDE NAMEN
			# OPLOSSING: IN HETZELFDE ARTIKEL OPZOEKEN OVER WELKE DE VRIES HET GAAT
			# IDEE: EERSTE KEER ZAL IE WEL MET ZN VOLLEDIGE NAAM GENOEMD WORDEN
			repldict[naamsplit[-1]]=naam.replace(" ","_")
		else:
			repldict[" ".join(naamsplit[-2:])]=naam.replace(" ","_")
	return repldict

def replacebedrijf(lijstenmetbedrijven):
	repldict={}
	names=[]
	for fname in lijstenmetbedrijven:
		naamtemp=[line.strip() for line in open(fname,mode="r",encoding="utf-8")]
		names+=naamtemp
	for naam in names:
		# first, just the same as with space removal, ook McDonald's --> McDonalds
		repldict[naam]=naam.replace(" ","_").replace("'","")
		# evtl later iets toevoegen om "Holding", "Group" etc te vervangen (?) 
	return repldict


def replaceown(inputfiles):
	repldict={}
	for fname in inputfiles:
                print fname
		with open(fname,mode="r",encoding="utf-8") as fi:
			for line in fi:
				bothcolumns=line.strip().split("\t")
				repldict[bothcolumns[0]]=bothcolumns[1]
	return repldict
	

def main():
	complrepldict={}
	
        # STAP 1: VASTE UITDRUKKINGEN ('s ochtends --> 's_ochtends)
        alldutchwords=[line.strip() for line in open(nlwoordenbestand,mode="r",encoding="utf-8")]
	complrepldict.update(replacespaces(alldutchwords))

        # STAP 2: EIGEN VERVANGLIJSTJE (namen, bedrijven, ...), zelf aangemaakt tab-seperated file
        complrepldict.update(replaceown(ownreplacements))
	

        # ANDERE OPTIES:
        # complrepldict.update(replacenames(naambestanden))
	# complrepldict.update(replacebedrijf(lijstenmetbedrijven))

	with open(outputbestand,mode="w",encoding="utf-8") as fo:
		fo.write(unicode(json.dumps(complrepldict,ensure_ascii=False)))
	
	print "YOU'RE READY!"
	


        
        # STAP 2b: WE DOEN HET NOG EEN KEER, WANT WE WILLEN NOG EEN VERVANGLIJSTJE DAT WE ALLEEN GAAN TOEPASSEN ALS NAMEN AL EEN KEER ZIJN GENOEMD; 
        
        complrepldict2={}
        complrepldict2.update(replaceown(ownreplacements_lastnames))
	

        # ANDERE OPTIES:
        # complrepldict.update(replacenames(naambestanden))
	# complrepldict.update(replacebedrijf(lijstenmetbedrijven))

	with open(outputbestand2,mode="w",encoding="utf-8") as fo:
		fo.write(unicode(json.dumps(complrepldict2,ensure_ascii=False)))
	
	print "YOU'RE READY!"





	
if __name__ == "__main__":
	main()
	
