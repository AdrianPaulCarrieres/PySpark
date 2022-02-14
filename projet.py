#!/usr/bin/env python
# coding: utf-8

# # Projet

# ## Arg lecture
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import sys

[csv1, cvs2] = sys.argv[-2:]

# ## Imports & reading files
spark = SparkSession.builder.appName('abc').getOrCreate()

df1 = spark.read.format("csv").option("header", "true").option(
    "inferSchema", "true").load(csv1)

df2 = spark.read.format("csv").option("header", "true").option(
    "inferSchema", "true").load(cvs2)

df1.show()

df2.show()

# ## Join the dataframes & conversion
# Firt we need to create a join expression on our two dataframe on the sole link they share: the currency/ISO_4217 designation of the currency.
# That will give us a new dataframe with line by line comparison between the original price in each country and the dollar to currency ratio.
join_express = df1["Currency"] == df2["ISO_4217"]
joined = df1.join(df2, join_express, "inner")

# We will then create a new column containing the converted dollar price for each product.
joined_and_converted_prices = joined.withColumn(
    "Dollar Prices", round(expr("Price / Dollar_To_Curr_Ratio"), 2))
joined_and_converted_prices.show()

# A little cleaning of the duplicates/useless columns later.
joined_and_converted_prices = joined_and_converted_prices.drop(
    "ISO_4217").drop("Dollar_To_Curr_Ratio")
joined_and_converted_prices.show(1000)

# ## That average question
# For comprehension and handling I decided to clean (drop/rename) columns of my dataframe.
# The idea will be create a USA products only dataframe and then to join on the model name column to get line by line a comparison between a product in a country and its price in the United States.
cleaned_version = joined_and_converted_prices.drop("Price").drop("Currency")
cleaned_version.show()

usa = joined_and_converted_prices.filter("Country = 'United States'").drop("Country").drop("Currency").drop(
    "Price").withColumnRenamed("Dollar Prices", "USA_Prices").withColumnRenamed("Model_name", "Model")
usa.show()

join_express = cleaned_version["Model_name"] == usa["Model"]
joined = cleaned_version.join(usa, join_express, "inner").drop("Model")
joined.show()

avg_prices = joined.groupBy("Country").avg("Dollar Prices", "USA_PRICES").sort("Country").select("Country", round("`avg(Dollar Prices)`", 2), round(
    "`avg(USA_PRICES)`", 2)) .withColumnRenamed("round(avg(Dollar Prices), 2)", "Moyenne").withColumnRenamed("round(avg(USA_PRICES), 2)", "Moyenne aux USA")
avg_prices.show(100)

moyennePrix = avg_prices.withColumn("Ecart entre la moyenne et celle des USA(%)", round(expr(
    f"(abs(Moyenne) - `Moyenne aux USA`)/ `Moyenne aux USA` * 100"), 2)).sort(desc("Ecart entre la moyenne et celle des USA(%)"))
moyennePrix.show(100)

moyennePrix.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("moyennePrix")

# ## Sum
total_count = joined_and_converted_prices.groupBy("Country").agg({"Dollar Prices": "sum"}).select("Country", round(
    "sum(Dollar Prices)", 2)).withColumnRenamed("round(sum(Dollar Prices), 2)", "Somme des produits").sort(desc("sum(Dollar Prices)"))
total_count.show(1000)
total_count.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("cout_total")

# ## Liste des produits
product_list = df1.dropDuplicates(["Model_name"]).sort("Model_name")
product_list.show(1000)
product_list.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("listeProduit")

# ## Pays le moins cher pour acheter les AirPods Pro
airpodsPro = joined_and_converted_prices.filter(
    "Model_name == 'AirPods Pro'").sort(asc("Dollar Prices")).limit(1)
airpodsPro.show()

airpodsPro.repartition(1).write.mode("overwrite").format(
    "csv").option("header", "true").save("airpodsPro")
