#!/usr/bin/env python
# coding: utf-8

# # Projet

# ## Imports & lecture des fichiers
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('abc').getOrCreate()

df1 = spark.read.format("csv").option("header", "true").option(
    "inferSchema", "true").load('ApplePrices.csv')

df2 = spark.read.format("csv").option("header", "true").option(
    "inferSchema", "true").load('CurrencyConversion.csv')

df1.show()

df2.show()

# ## Join the dataframes et conversion
join_express = df1["Currency"] == df2["ISO_4217"]
joined = df1.join(df2, join_express, "inner")

joined_and_converted_prices = joined.withColumn(
    "Dollar Prices", round(expr("Price / Dollar_To_Curr_Ratio"), 2))
joined_and_converted_prices.show()

joined_and_converted_prices = joined_and_converted_prices.drop(
    "ISO_4217").drop("Dollar_To_Curr_Ratio")
joined_and_converted_prices.show(1000)

# # Ecart à la moyenne
avg_prices = joined_and_converted_prices.groupBy("Country").agg(avg(col("Dollar Prices"))).sort("Country").select(
    "Country", round("`avg(Dollar Prices)`", 2)) .withColumnRenamed("round(avg(Dollar Prices), 2)", "Moyenne")
avg_prices.show(100)

usa_average = avg_prices.where(
    col("Country") == "United States").select("Moyenne").collect()[0][0]
usa_average = str(usa_average)
moyennePrix = avg_prices.withColumn("Ecart à la moyenne (%)", round(expr(
    f"(abs(Moyenne) - {usa_average})/ {usa_average} * 100"), 2)).sort(desc("Ecart à la moyenne (%)"))
moyennePrix.show(100)
moyennePrix.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("moyennePrix")

# ## Somme totale
cout_total = joined_and_converted_prices.groupBy("Country").agg({"Dollar Prices": "sum"}).select("Country", round(
    "sum(Dollar Prices)", 2)).withColumnRenamed("round(sum(Dollar Prices), 2)", "Somme des produits").sort(desc("sum(Dollar Prices)"))
cout_total.show(1000)
cout_total.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("cout_total")

# ## Liste des produits
listeProduit = df1.dropDuplicates(["Model_name"]).sort("Model_name")
listeProduit.show(1000)
listeProduit.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("listeProduit")

# ## Pays le moins cher pour acheter les AirPods Pro
airpodsPro = joined_and_converted_prices.filter(
    "Model_name == 'AirPods Pro'").sort(asc("Dollar Prices")).limit(1)
airpodsPro.show()

airpodsPro.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("airpodsPro")
