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
spark = SparkSession(sc)


#Generate the rules 
def rules(RDD, total, s):
    RDDDict = []
    for rdd in RDD:
        RDDDict.append(rdd.collectAsMap())
    index = 1
    for rdd in RDDDict[1:]:
        for key in rdd.keys():
            for i in range (index, 1, -1):
                combination = combinations(key, i)
                for A in combination:
                    B = list(set(key)-set(A))
                    B.sort()

                    indexA = len(A)-1
                    indexB = len(B)-1
                    valueA, valueB = 0, 0
                    if len(A) == 1:
                        valueA = RDDDict[indexA][A[0]]
                    else:
                        valueA = RDDDict[indexA][A]
                    
                    if len(B) == 1:
                        valueB = RDDDict[indexB][B[0]]
                    else:
                        valueB = RDDDict[indexB][tuple(B)]
                    
                    confidence = rdd[key]/valueA
                    if confidence >= s:
                        print(str(A)+ " -> " + str(B))
                        print("confidence = "+str(confidence))
                        print("interest = "+str(abs(confidence - valueB/total)))
        index += 1
                


#Generate frequent tuples
def frequent(support):
    rawPurchases = sc.textFile("datas/T10I4D100K.dat") # Get the datas
    total = rawPurchases.count() # 

    words = rawPurchases.flatMap(lambda line: line.split(" "))
    purchases = rawPurchases.map(lambda x: x.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b) # Count frequent individual item
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: (x[1]/total)*100 >= support ) #Remove items below support

    wordsFreq = wordCounts.sortByKey(True)
    wordsFreq = wordsFreq.map(lambda x: x[0]) # get the item for the pair (item, count)

    tupleCount = wordCounts
    index = 2  
    print(1)
    print(tupleCount.count())
    RDD = [wordCounts]

    while tupleCount.count() > 0:
        print(index)
        tupleCountRules = tupleCount

        tupleFreq = tupleCount.map(lambda x: x[0]) # get the item for the pair (item, count)
   
        tupleFreqSet = set(tupleFreq.collect())
       
        if index == 2:
            explodeTuple = tupleFreqSet
        else:
            explodeTuple = tupleFreq.reduce(lambda x,y: set(x) | set(y))
        wordsFreq = wordsFreq.filter(lambda x: x in explodeTuple)
        
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
        tupleCount = tupleCount.map(lambda x : sorted(x))
        tupleCount = tupleCount.map(lambda x: list(combinations(x, index)))
        tupleCount = tupleCount.map(lambda x: list(set(x) & dataSet))
        tupleCount = tupleCount.flatMap(lambda x: x)
        tupleCount = tupleCount.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b)
        tupleCount = tupleCount.filter(lambda x: (x[1]/total)*100 >= support) 

        if tupleCount.count() > 0:
            RDD.append(tupleCount)
        print(tupleCount.count())

        index += 1
    
    rules(RDD, total, 0.9)





if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)

