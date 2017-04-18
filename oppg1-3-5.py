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

	df = sqlContext.read.load('airbnb_datasets/listings_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')

	dfrev = sqlContext.read.load('airbnb_datasets/reviews_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')

	#dfcal = sqlContext.read.load('airbnb_datasets/calendar_us_sam.csv',
	#					 format='com.databricks.spark.csv',
	#					 header='true',
	#					 delimiter="\t",
	#					 inferSchema='true')

	#top_three_reviewers(df, dfrev)
	top_guest(df, dfrev)

#A Svarene stemmer med de fleste av anders sine, men ikke alle
def top_three_reviewers(df, dfrev):
	
	temp = dfrev.select(col('listing_id'), col('reviewer_id'), col('reviewer_name'))
	temp2 = df.select(col('city'), col('id'))

	joined = temp.join(temp2, temp2.id == temp.listing_id)
	joined2 = joined.groupBy('city', 'reviewer_id', 'reviewer_name').agg(count('id').alias("num_reviews"))

	df = joined2.select(col('city'), col('reviewer_id'), col('reviewer_name'), col('num_reviews')).orderBy('city', desc('num_reviews'))
	
	w = Window.partitionBy('city').orderBy(desc('num_reviews'))

	out = df.withColumn("rn", row_number().over(w)).where(col("rn")<4).select('city', 'num_reviews', 'reviewer_id', 'reviewer_name')

	return out.show()

#B

def top_guest(df, dfrev):

	to_float_udf = udf(to_float, DoubleType())
	out = df.withColumn("price", to_float_udf(df["price"]))

	temp = dfrev.select(col('listing_id'), col('reviewer_id'), col('reviewer_name'))
	temp2 = df.select(col('id'), col('price'))

	joined = temp.join(temp2, temp2.id == temp.listing_id, 'inner')
	#Antar at hver review_id (altså gjest) har i gjennomsnitt overnattet i 3 netter
	joined2 = joined.groupBy('reviewer_id', 'reviewer_name').agg(sum(col('price')*3).alias("amount_spent")).sort(desc('amount_spent'))

	return joined2.show(1)


#Float
def to_float(string):
	try:
		return float(string.replace(',', '').replace('$', ''))
	except ValueError, e:
		return 0

run()