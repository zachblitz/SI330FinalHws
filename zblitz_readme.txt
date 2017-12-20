- First, on the file preprocess.py, I eliminated the preamble and portable lines by slicing the original les mis text and wrote those lines to a new file called lesmis.txt

- Then, I converted each sentence to single lines, and wrote that to lesmissentences.txt

- Next, on mrbigram.py, I first retrieved all of the bigrams and made them lower case. If any of the words in the tuple are in stopwords, it continues thus getting rid of the tuple.

- When you ran the program, the tuples were printing alphabetically. In order to get the numbers first, I used zfill(5) as a solution.

- Lastly, adding | tail -50 at the end of the command line in terminal gets the top 50 bigrams excluding those containing common stopwords.

Here is the final output after running it through Hadoop:

"00039"	["one", "knows"]
"00039"	["chapter", "iv"]
"00040"	["following", "day"]
"00040"	["first", "time"]
"00040"	["little", "girl"]
"00040"	["young", "girls"]
"00041"	["one", "side"]
"00041"	["five", "hundred"]
"00042"	["father", "madeleine"]
"00043"	["took", "place"]
"00044"	["said", "marius"]
"00044"	["chapter", "iii"]
"00044"	["la", "chanvrerie"]
"00044"	["human", "race"]
"00045"	["great", "deal"]
"00045"	["said", "jean"]
"00046"	["long", "time"]
"00047"	["first", "place"]
"00047"	["grape", "shot"]
"00047"	["chapter", "ii"]
"00049"	["turned", "round"]
"00050"	["low", "voice"]
"00050"	["grave", "digger"]
"00052"	["petit", "picpus"]
"00053"	["caught", "sight"]
"00054"	["rue", "plumet"]
"00059"	["wine", "shop"]
"00059"	["rue", "du"]
"00059"	["le", "maire"]
"00060"	["every", "one"]
"00060"	["years", "ago"]
"00064"	["every", "day"]
"00065"	["hundred", "francs"]
"00067"	["rue", "saint"]
"00068"	["thousand", "francs"]
"00070"	["madame", "magloire"]
"00070"	["good", "god"]
"00074"	["old", "woman"]
"00076"	["young", "girl"]
"00078"	["rue", "des"]
"00078"	["young", "man"]
"00080"	["de", "l"]
"00088"	["one", "would"]
"00102"	["one", "day"]
"00111"	["monsieur", "le"]
"00113"	["de", "la"]
"00173"	["old", "man"]
"00184"	["rue", "de"]
"00234"	["let", "us"]
"01107"	["jean", "valjean"]


 