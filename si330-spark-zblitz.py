import json
import math
import si330_helper
from pyspark import SparkContext
from si330_helper import convert_dict_to_tuples

#worked with Jake Kreinik and Nate Wellek


input_file = sc.textFile('/var/si330f17/yelp_academic_dataset_review.json')
step_1a = input_file.map(lambda line:json.loads(line))
step_1b = step_1a.flatMap(lambda x: convert_dict_to_tuples(x))
step_2b1 = step_1b.filter(lambda x : x[0] >= 4)
step_2c1 = step_1b.filter(lambda x : x[0] <= 2)

step_2a2 = step_1b.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)
step_2b2 = step_2b1.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)
step_2c2 = step_2c1.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)

all_review_word_count = step_2a2.map(lambda x: (1, x[1])).reduceByKey(lambda x,y:x+y).collect()[0][1]
pos_review_word_count = step_2b2.map(lambda x: (1, x[1])).reduceByKey(lambda x,y:x+y).collect()[0][1]
neg_review_word_count = step_2c2.map(lambda x: (1, x[1])).reduceByKey(lambda x,y:x+y).collect()[0][1]

freq_words = step_2a2.filter(lambda x: x[1] > 5000)

step_3pos = freq_words.join(step_2b2)
step_3neg = freq_words.join(step_2c2)

positive_words = step_3pos.map(lambda x:(x[0], math.log(float(x[1][1])/pos_review_word_count) - math.log(float(x[1][0])/all_review_word_count)))
negative_words = step_3neg.map(lambda x:(x[0], math.log(float(x[1][1])/neg_review_word_count) - math.log(float(x[1][0])/all_review_word_count)))

sorted_positive_words = positive_words.sortBy(lambda x:x[1], ascending = False)
sorted_negative_words = negative_words.sortBy(lambda x:x[1], ascending = False)

sorted_positive_words.saveAsTextFile('/user/zblitz/output-positive/part-00000')
sorted_negative_words.saveAsTextFile('/user/zblitz/output-negative/part-00000')








