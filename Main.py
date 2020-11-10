from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer

sc = SparkContext('local[*]')
spark = SparkSession(sc)

df = spark.read.format("csv").load("datas/T10I4D100K.dat").withColumnRenamed("_c0","Basket")
df.show(truncate=100)

tokenizer = Tokenizer(inputCol="Basket", outputCol="Purchase")
df = tokenizer.transform(df)
df.show(truncate=100)
print(df.select('Purchase').collect()[0][0])
print(type(df.select('Purchase').collect()[0][0]))
#print(df.agg(countDistinct("Purchase")))