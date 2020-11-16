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

def rules(RDD, total, index, s):
    lastComputeTuple = RDD[index-1].collect()
    for tuples in lastComputeTuple:
        valueTuples = tuples[1]
        for i in range (index-1, 1, -1):
            combination = combinations(tuples[0], i)
            for A in combination:
                B = list(set(tuples[0])-set(A))
                B.sort()
                print(str(A)+ " -> " + str(B))

                indexA = len(A)-1
                indexB = len(B)-1
                valueA, valueB = 0, 0
                if len(A) == 1:
                    valueA = RDD[indexA].filter(lambda x: x[0] == A[0]).first()[1]
                else:
                    valueA = RDD[indexA].filter(lambda x: x[0] == A).first()[1]
                
                if len(B) == 1:
                    valueB = RDD[indexB].filter(lambda x: x[0] == B[0]).first()[1]
                else:
                    valueB = RDD[indexB].filter(lambda x: x[0] == tuple(B)).first()[1]
                
                confidence = valueTuples/valueA
                if confidence >= s:
                    print("confidence = "+str(confidence))
                    print("interest = "+str(confidence - valueB/total))
                



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

    RDD = [wordCounts]

    while tupleCount.count() > 0:
        print(index)
        tupleCountRules = tupleCount

        tupleFreq = tupleCount.map(lambda x: x[0]) # get the item for the pair (item, count)
        #####
        
        #####
        tupleFreqSet = set(tupleFreq.collect())
       
        if index == 2:
            explodeTuple = tupleFreqSet
        else:
            explodeTuple = tupleFreq.reduce(lambda x,y: set(x) | set(y))
        wordsFreq = wordsFreq.filter(lambda x: x in explodeTuple)
        ##
        
        ##

        ##
        # tuplePrec = sc.parallelize(list(combinations(wordsFreq.collect(), index))) # Compute all the pairs of items possible
        # if index > 2: 
        #     tuplePrec = tuplePrec.filter(lambda x: list(set(combinations(list(x), (index - 1))) & tupleFreqSet))
        ##

        wordsFreqList = wordsFreq.collect()
        if index == 2:
            tuplePrec = tupleFreq.map(lambda x: [[x,i] for i in wordsFreqList])
        else:
            tuplePrec = tupleFreq.map(lambda x: [list(x) + [i] for i in wordsFreqList])
        tuplePrec = tuplePrec.map(lambda x: [sorted(i) for i in x])
        tuplePrec = tuplePrec.flatMap(lambda x: x)
        tuplePrec = tuplePrec.map(lambda x: tuple(x))

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

        RDD.append(tupleCount)
        print(tupleCount.count())

        rules(RDD, total, index, 0.9)

        index += 1





if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)

