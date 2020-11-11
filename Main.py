from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType
import sys


sc = SparkContext('local[*]')
spark = SparkSession(sc)

def frequent(support):
    rawPurchases = sc.textFile("datas/T10I4D100K.dat")
    total = rawPurchases.count()
    words = rawPurchases.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b)
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: (x[1]/total)*100 >= support ) #Remove retour à la ligne

    pair = wordCounts.map(lambda x: x[0]) # get the item for the pair (item, count)
    pair = pair.cartesian(pair) # Compute all the pairs of items possible
    pair = pair.filter(lambda x: x[0] != x[1]) # Remove pairs (x, x)
    pair = pair.map(lambda x: x if x[0] < x[1] else (x[1], x[0])) # Order elements in pair  : [(1,2), (2,1)] -> [(1,2), (1,2)]
    pair = pair.distinct() # Remove duplicated pairs

    pairDatas = pair.collect()

    purchases = rawPurchases.map(lambda x: x.split(" "))

    pairCount = sc.emptyRDD()

    datas = purchases.collect()

    index = 0
    for i in datas:
        print(index)
        tmp = sc.parallelize(i)
        tmp = tmp.cartesian(tmp)
        tmp = tmp.filter(lambda x: x in pairDatas)
        tmp = tmp.map(lambda pair: (pair, 1)).reduceByKey(lambda a,b: a + b)
        pairCount = pairCount.union(tmp)
        index +=1

    pairCount = pairCount.reduceByKey(lambda a,b: a + b)
    pairCount = pairCount.filter(lambda x: (x[1]/total)*100 >= support )

    print(wordCounts.collect())
    print(pair.collect())
    print(pairCount.collect())






if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)

