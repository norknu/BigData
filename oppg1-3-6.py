# -*- coding: UTF-8 -*- 
#Test app for spark
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
#from pyspark.sql import function as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import re


sc = SparkContext("local", "Test App")
cols = {}

sqlContext = SQLContext(sc)


def run():

	#data = load_data_array("airbnb_datasets/listings_us.csv")
	#header = data.first()
	#data = data.filter(lambda line: line != header)
	#set_cols(header)

	df = sqlContext.read.load('airbnb_datasets/listings_us.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')

	dfn = sqlContext.read.load('airbnb_datasets/neighborhood_test.csv',
						 format='com.databricks.spark.csv',
						 header='true',
						 delimiter="\t",
						 inferSchema='true')

	#dfgeo = sqlContext.read.json('airbnb_datasets/neighbourhoods.geojson')
	#geo = json.load('airbnb_datasets/neighbourhoods.geojson')


	#neighboorhood_match(df, dfn)
	distinct_amenities(df, dfn)

def load_data_array(path):
	raw_data = sc.textFile(path).cache()
	sample = raw_data.sample(False, 1, 12345)

	return sample.map(lambda line: line.split('\t'))

def set_cols(header):
	i = 0
	for el in header:
		cols[str(el)] = i
		i+=1

#A
#def neighboorhood_match(df, dfn):
#	temp = df.select(col('id'), col('longitude'), cal('latitude'))
#	pass

#def compare_coord(coord, poly):
	#returerer neighborhood
#pass	

#B 
def distinct_amenities(df, dfn):
	temp = df.select(col('id').alias('id_list'), col('amenities'))
	temp2 = dfn.select(col('id'), col('neighbourhood'))

	joined = temp.join(temp2, temp2.id == temp.id_list).drop('id_list', 'id')
	#.groupBy('neighbourhood').distinct('amenities')
	#.select(col('id')groupBy('neighbourhood')
		#, 'amenities').agg(col('id'))
	out = joined.groupBy('neighbourhood').agg(countDistinct('amenities').alias('num_amenities'))

	return out.show()



run()