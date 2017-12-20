import nltk
from nltk.tokenize import RegexpTokenizer
from mrjob.job import MRJob
from mrjob.step import MRStep

#worked with Nate Wellek 

tokenizer = RegexpTokenizer('\w+')


stop_words = ('their', 'she', 'did', 'not', 'needn', 'have', 'all', 'a', 'has', 'between', 'shouldn', 'where', 'these', 'had', 'ours', 'who', 'further', 'does', 's', 't', 'are', 'isn', 'should', 'both', 'against', 'll', 're', 'can', 'that', 'few', 'out', 'no', 'hers', 'myself', 'but', 'at', 'too', 'once', 'the', 'there', 'o', 'this', 'down', 'in', 'some', 'and', 'weren', 'we', 'own', 'into', 'don', 'other', 'him', 'during', 'himself', 'having', 'them', 'why', 'ain', 'each', 'it', 'when', 'were', 'will', 'mightn', 'very', 'aren', 'am', 'mustn', 'they', 'ourselves', 'only', 'd', 'or', 'than', 'if', 'itself', 'from', 'i', 'being', 'her', 'me', 'after', 'yourselves', 'more', 'yours', 'through', 'those', 'of', 'you', 'doesn', 'about', 'to', 'y', 'your', 'doing', 'just', 'herself', 'now', 'wouldn', 'its', 'been', 'under', 'hadn', 'wasn', 'above', 'any', 'nor', 'over', 'because', 'on', 'shan', 'themselves', 've', 'off', 'while', 'then', 'how', 'so', 'until', 'most', 'our', 'up', 'is', 'yourself', 'was', 'what', 'before', 'which', 'same', 'again', 'didn', 'haven', 'ma', 'be', 'do', 'with', 'won', 'm', 'couldn', 'whom', 'my', 'theirs', 'below', 'such', 'for', 'his', 'an', 'by', 'hasn', 'as', 'here', 'he')

class MRRatingsCounter(MRJob):
  
	def steps(self):	
		return [MRStep(mapper = self.map_bigrams, reducer = self.bigram_reducer), MRStep(reducer=self.sort_50_bigrams)]

	def map_bigrams(self, _, line):
		word_list = tokenizer.tokenize(line) 
		word_list_2 = []
		for word in word_list:
			word_list_2.append(word.lower())
		bigrams = nltk.bigrams(word_list_2)
		for bigram in bigrams:
			if bigram[0] in stop_words:	
				continue
			if bigram[1] in stop_words:
				continue
			yield (bigram, 1)

	def bigram_reducer(self, key, values):
		yield str(sum(values)).zfill(5), key

	def sort_50_bigrams(self, count, key):
		for item in key:
			yield count, item

if __name__ == '__main__':
	MRRatingsCounter.run()