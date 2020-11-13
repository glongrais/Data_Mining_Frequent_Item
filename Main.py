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
    purchases = rawPurchases.map(lambda x: x.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b)
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: (x[1]/total)*100 >= support ) #Remove retour à la ligne

    wordsFreq = wordCounts.sortByKey(True)
    wordsFreq = wordsFreq.map(lambda x: x[0]) # get the item for the pair (item, count)
    # pair = sc.parallelize(list(combinations(wordsFreq.collect(), 2))) # Compute all the pairs of items possible

    # pairDatas = pair.collect()
    # dataSet = set(pairDatas)
    # wordsFreqList = wordsFreq.collect()
    # wordsFreqSet = set(wordsFreqList)

    # pairCount = purchases.filter(lambda x: len(list(set(x) & wordsFreqSet)) >= 2)
    # pairCount = pairCount.map(lambda x: list(combinations(x, 2)))
    # pairCount = pairCount.map(lambda x: list(set(x) & dataSet))
    # pairCount = pairCount.flatMap(lambda x: x)
    # pairCount = pairCount.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b)
    # pairCount = pairCount.filter(lambda x: (x[1]/total)*100 >= support) 

    # print(pairCount.count())

    tupleCount = wordCounts
    index = 2

    combinations(wordsFreq.collect(), 4)

    while tupleCount.count() > 0:
        print(index)
        tupleFreq = tupleCount.map(lambda x: x[0]) # get the item for the pair (item, count)
        tupleFreqSet = set(tupleFreq.collect())
        tuplePrec = sc.parallelize(list(combinations(wordsFreq.collect(), index))) # Compute all the pairs of items possible
        if index > 2: 
            tuplePrec = tuplePrec.filter(lambda x: list(set(combinations(list(x), (index - 1))) & tupleFreqSet))

        tuplePrecDatas = tuplePrec.collect()
        dataSet = set(tuplePrecDatas)
        wordsFreqList = wordsFreq.collect()
        wordsFreqSet = set(wordsFreqList)

        tupleCount = purchases.filter(lambda x: len(list(set(x) & wordsFreqSet)) >= index)
        tupleCount = tupleCount.map(lambda x: list(combinations(x, index)))
        tupleCount = tupleCount.map(lambda x: list(set(x) & dataSet))
        tupleCount = tupleCount.flatMap(lambda x: x)
        tupleCount = tupleCount.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b)
        tupleCount = tupleCount.filter(lambda x: (x[1]/total)*100 >= support) 

        index += 1



if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)

