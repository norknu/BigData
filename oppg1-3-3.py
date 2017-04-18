# -*- coding: UTF-8 -*- 
#Test app for spark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import re

sc = SparkContext("local", "Test App")
cols = {}

sqlContext = SQLContext(sc)



def run():

	df = sqlContext.read.load('airbnb_datasets/listings_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')
	#df = sqlContext.read.csv('airbnb_datasets/listings_us.csv')

	#res = sqlContext.sql("SELECT * FROM ")
	#res.show()
	#pdf.write.parquet("listnings.parquet")

	#perquetFile = spark.read.perquet("listnings.parquet")

	start_time = time.time()
	avg_price(df)
	avg_price_city(df)
	#avg_price_per_room_city(df)
	#num_review_per_month_city(df)
	#estimated_nights_booked_city(df)
	#estimated_money_spent_city(df)
	#estimated_nights_booked_city_sql(df)

	print("---%s sec ---" % (time.time() - start_time))


#A
def avg_price(df):
	to_float_udf = udf(to_float, DoubleType())
	out = df.withColumn("price", to_float_udf(df["price"]))
	out = out.select(avg('price'))
	
	#out.write.format("com.databricks.spark.csv").save("aribnb_datasets/test3.csv")
	out.write.option("header", "true").csv("airbnb_datasets/test4.csv", 'append')

def avg_price_city(df):
	to_float_udf = udf(to_float, DoubleType())
	out = df.withColumn("price", to_float_udf(df["price"]))
	out = out.groupBy('city').agg(avg('price').alias('Avg price per city'))

	#out.write.format("com.databricks.spark.csv").save("aribnb_datasets/test3.csv")
	out.write.option("header", "true").csv("airbnb_datasets/test4.csv", 'append')
	#out.write.table(test3, "airbnb_datasets/test3.csv", sep = "\t", col.names = T, append = T)

#B
def avg_price_per_room_city(df):
	to_float_udf = udf(to_float, DoubleType())
	out = df.withColumn("price", to_float_udf(df["price"]))
	return out.groupBy('city', 'room_type').agg(avg('price').alias('Avg price per city')).show()

#C
def num_review_per_month_city(df):
	return df.groupBy('city').agg(avg('reviews_per_month')).show()

#D
def estimated_nights_booked_city(df):
	#mangler bare aa gange med (1/0.7)*3*12
	y = (1/0.7)*3*12
	#to_float_udf = udf(to_float, DoubleType())
	#out = df.withColumn("reviews_per_month", to_float_udf(df["reviews_per_month"])).groupBy('city').sum('reviews_per_month'*y)
	return df.select(col("city"), (y*col("reviews_per_month").cast('float')).alias('nights_booked'))\
		.groupBy('city').round(sum('nights_booked'),2).show()

#E 
def estimated_money_spent_city(df):
	y = (1/0.7)*3*12
	to_float_udf = udf(to_float, DoubleType())
	out = df.withColumn("price", to_float_udf(df["price"]))
	return out.select(col("city"), (y*col("price")*col("reviews_per_month").cast('float')).alias('money_earned'))\
		.groupBy('city').sum('money_earned').show()
	
#Need this function to handle empty strings
def to_float(string):
	try:
		return float(string.replace(',', '').replace('$', ''))
	except ValueError, e:
		return 0


run()