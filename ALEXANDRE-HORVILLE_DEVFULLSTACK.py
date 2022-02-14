#!/usr/bin/env python
# coding: utf-8


#Lunch session of spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys




def saveDfToCSV(df,csv_name) :
    df.repartition(1).write.mode("overwrite").format("csv").option("header", "true").save("./save/" + csv_name)




#Parse args to csv1 = ApplePrices ; csv2 = CurrencyConversion    
[csv1, csv2] = sys.argv[-2:] or ["ApplePrices.csv", "CurrencyConversion.csv"]




spark = SparkSession.builder.appName('abc').getOrCreate()




#Init df's 
df_apple = spark.read.csv(csv1,header=True)
df_currency = spark.read.csv(csv2,header=True)




#Convert Current Currency into USD currency
df1 = df_apple.join(df_currency , df_apple["Currency"] ==  df_currency["ISO_4217"] ,"inner")
df1 = df1.withColumn("USD_PRICE",round(col("Price")/col("Dollar_To_Curr_Ratio") , 2))

#We drop the surplus cols
cols = ("Price","Currency","ISO_4217" , "Dollar_To_Curr_Ratio")
df1 = df1.drop(*cols)




#Prep of moyennePrix
df_usa = df1.select("*").where( col("Country") == "United States" )
df_usa = df_usa.withColumnRenamed("Model_name", "Model_name_usa")
df_usa = df_usa.withColumnRenamed("USD_PRICE", "USD_PRICE_USA")
df_usa = df_usa.withColumnRenamed("Country", "Country_USA")

df_join_usa = df1.join(df_usa , df_usa["Model_name_usa"] ==  df1["Model_name"] ,"inner")
df_join_usa = df_join_usa.drop("Model_name_usa")
df_join_usa = df_join_usa.drop("Country_USA")

df_join_usa = df_join_usa.groupBy("Country").avg("USD_PRICE" , "USD_PRICE_USA")


#Calc of moyennePrix.csv
df_join_usa = df_join_usa.withColumnRenamed("avg(USD_PRICE_USA)", "avg(USA)")

df_moyenne = df_join_usa.withColumn( "delta('Country - USA')%" , round( (col("avg(USD_PRICE)") - col("avg(USA)"))/col("avg(USA)") * 100, 2 ) )
df_moyenne = df_moyenne.sort(col("delta('Country - USA')%").desc())

saveDfToCSV(df_moyenne , "moyennePrix")


#calc of coutTotal.csv
df_total = df1.groupBy("Country").sum("USD_PRICE").select("Country" , round("sum(USD_PRICE)" , 2))
saveDfToCSV(df_total , "coutTotal")



#ListProduit
df_listProduit = df1.dropDuplicates(["Model_name"])

#Drop useless cols
cols = ("Country" , "USD_PRICE")
df_listProduit = df_listProduit.drop(*cols)
saveDfToCSV(df_listProduit , "listeProduit")




#AirpodsPro.csv
df_airPodsPro = df1.select("Country" , "USD_PRICE").where(df1["Model_name"] == "AirPods Pro")
df_airPodsPro = df_airPodsPro.sort(col("USD_PRICE").asc())

saveDfToCSV(df_airPodsPro , "airpodsPro")



