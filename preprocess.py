import nltk
import itertools


#Here I am eliminating the preamble and postable lines - Part 1
les_mis_lines = "".join(open("original_les_mis.txt").readlines()[659:-837])
les_mis = open("lesmis.txt", "w")
les_mis.write(les_mis_lines)

#Here I am converting the sentences to single lines - Part 2
sentence_retreiver = nltk.data.load('tokenizers/punkt/english.pickle')
line = '\n'.join(sentence_retreiver.tokenize(les_mis_lines.strip().replace("\n"," ")))

les_mis_sentences = open("lesmissentences.txt", "w")
les_mis_sentences.write(line)
