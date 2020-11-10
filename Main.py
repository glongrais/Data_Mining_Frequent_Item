from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer
import sys


sc = SparkContext('local[*]')
spark = SparkSession(sc)

def frequent(support):
    rawPurchases = sc.textFile("datas/T10I4D100K.dat")
    #print(rawPurchases.collect())
    total = rawPurchases.count()
    words = rawPurchases.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b)
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: x[1]/total >= support ) #Remove retour à la ligne

    purchases = rawPurchases.map(lambda x: x.split(" "))

    pair = wordCounts.map(lambda x: x[0]) # get the item for the pair (item, count)
    pair = pair.cartesian(pair) # Compute all the pairs of items possible
    pair = pair.filter(lambda x: x[0] != x[1]) # Remove pairs (x, x)
    pair = pair.map(lambda x: x if x[0] < x[1] else (x[1], x[0])) # Order elements in pair  : [(1,2), (2,1)] -> [(1,2), (1,2)]
    pair = pair.distinct() # Remove duplicated pairs

    print(wordCounts.collect())
    print(pair.collect())






if sys.argv[1] == "Frequent":
    support = float(sys.argv[2]) #if we call compare, perform compare function for jaccard similarity 
    frequent(support)
else:    
    frequent(0.05)





#df = spark.read.format("csv").option("delimiter", "\t").load("datas/T10I4D100K.dat").withColumnRenamed("_c0","Basket")
#df.show(truncate=100)

#tokenizer = Tokenizer(inputCol="Basket", outputCol="Purchase")
#df = tokenizer.transform(df)
#print(df.agg(countDistinct("Purchase")))