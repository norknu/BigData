# -*- coding: UTF-8 -*- 
#Test app for spark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
#from pyspark.sql import function as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

import re

sc = SparkContext("local", "Test App")
cols = {}

sqlContext = SQLContext(sc)

def run():

	df = sqlContext.read.load('Data/listings_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')

	dfcal = sqlContext.read.load('Data/calendar_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true').sample(False, 0.01, 7)

	#num_listings_per_host(df)
	#print(more_than_one_list(df))
	top_three(df, dfcal)


#A group by host_id, count listnings. Ans: 1.26
def num_listings_per_host(df):
	return df.groupBy('host_id').count().agg(avg('count')).show()

#B number of hosts with more than 1 listning / total hosts. Ans: 14.55%
def more_than_one_list(df):
	#Creates df with host_ids with more than 1 listings
	num_1 = df.groupBy('host_id').count().filter("count >= 2").count()
	#Number of all host_ids
	num_all = df.groupBy('host_id').count().count()
	print("Prosentandel av host_id's med flere enn 1 listing:")
	return (float(num_1)/float(num_all))*100

#C 
def top_three(df, dfcal):
#Join calendar and listings on listing_id, select host_id, listing_id, price and available. 
	temp = dfcal.filter("available == 'f'")\
	.select(col('listing_id'), col('available'))\
	.groupBy('listing_id')\
	.agg(count('available').alias("Num of taken rooms"))

	to_float_udf = udf(to_float, DoubleType())
	df = df.withColumn("price", to_float_udf(df["price"]))

	temp2 = df.select(col('host_id'), col('city'), col('id'), col('price'))

	joined = temp.join(temp2, temp2.id == temp.listing_id)

	tem = joined.select(col('host_id'), col('city'), (col('Num of taken rooms')*col('price')).alias('income'))\
		.groupBy('city', 'host_id').sum('income').sort(desc('sum(income)'))
	#Have to print top 3 in each city
	w = Window.partitionBy('city').orderBy(desc('sum(income)'))

	out = tem.withColumn("rn", row_number().over(w)).where(col("rn")<4).select('city', 'sum(income)')
	#.filter("row_number == 1").drop("row_number")

	return out.show()


#Need this function to handle empty strings
def to_float(string):
	try:
		return float(string.replace(',', '').replace('$', ''))
	except ValueError, e:
		return 0

run()
