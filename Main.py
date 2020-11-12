from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType
from itertools import *
import sys
import gc

from operator import truediv

conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
       .set('spark.driver.memory', '8G')
       .set("spark.driver.host", "127.0.0.1"))

sc = SparkContext(conf = conf)
#print(sc._conf.get('spark.driver.memory'))
spark = SparkSession(sc)

def frequent(support):
    rawPurchases = sc.textFile("datas/T10I4D100K.dat")
    total = rawPurchases.count()

    words = rawPurchases.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b)
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: (x[1]/total)*100 >= support ) #Remove retour à la ligne

    wordsFreq = wordCounts.map(lambda x: x[0]) # get the item for the pair (item, count)
    pair = sc.parallelize(list(combinations(wordsFreq.collect(), 2))) # Compute all the pairs of items possible

    pairDatas = pair.collect()
    dataSet = set(pairDatas)
    wordsFreqList = wordsFreq.collect()

    purchases = rawPurchases.map(lambda x: x.split(" "))

    pairCount = purchases.filter(lambda x: len(list(set(x) & set(wordsFreqList))) >= 2)
    #print("step 1 : "+str(pairCount.count()))
    pairCount = pairCount.map(lambda x: list(combinations(x, 2)))
    #print("step 2 : "+str(pairCount.count()))
    pairCount = pairCount.map(lambda x: list(set(x) & dataSet))
    #print("step 3 : "+str(pairCount.count()))
    pairCount = pairCount.flatMap(lambda x: x)
    #print("step 4 : "+str(pairCount.count()))
    #pairCount = pairCount.filter(lambda x: x in pairDatas)
    #print("step 4 : "+str(pairCount.count()))
    pairCount = pairCount.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b)
    #print("step 5 : "+str(pairCount.count()))
    pairCount = pairCount.filter(lambda x: (x[1]/total)*100 >= support) 

    print(pairCount.count())


if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)

