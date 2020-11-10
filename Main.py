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
    words= rawPurchases.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
    wordCounts = wordCounts.filter(lambda x: len(x[0])>=1 ) #Remove retour à la ligne
    wordCounts = wordCounts.filter(lambda x: x[1]/total >= support ) #Remove retour à la ligne

    purchases = rawPurchases.map(lambda x: x.split(" "))
    print(purchases.collect())






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