import sys
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('extract interesting amenities').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

amenities_schema = types.StructType([
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
    types.StructField('timestamp', types.TimestampType()),
    types.StructField('amenity', types.StringType()),
    types.StructField('name', types.StringType()),
    # types.StructField('tags', types.ArrayType(types.StringType())), # not sure which of these works
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()))
])

def print_shape(dataframe):
    print("(" + str(dataframe.count()) + ", " + str(len(dataframe.columns)) + ")")
    
    
def extract_wikidata_id(tags):
    # Input: tags field of amenities
    if tags == {}:
        return None
    for tag in tags:
        if "wikidata" in tag:
            return tags[tag]
    return None

def main(in_directory):
    # Load data
    amenities = spark.read.json(in_directory, schema=amenities_schema)
    # print("amenities:")
    # print_shape(amenities)
    # amenities.show()
    
    extract_wiki_tag_udf = functions.udf(lambda x:extract_wikidata_id(x), returnType=types.StringType())
    amenities = amenities.withColumn("wikidata_id", extract_wiki_tag_udf(amenities.tags))
    # print_shape(amenities)
    # amenities.show()
    
    amenities = amenities.na.drop()
    # print_shape(amenities)
    # amenities.show()
    
    unique_wikidata_ids = amenities.groupBy('wikidata_id').count()
    print("unique_wiki_id:")
    print_shape(unique_wikidata_ids)
    unique_wikidata_ids.show()
    
    threshold = 150;
    filtered_based_on_percentile = unique_wikidata_ids.filter(col("count")>=threshold)
    # filtered_based_on_percentile = unique_wikidata_ids.groupb("percent_rank", functions.percent_rank())
    filtered_based_on_percentile = filtered_based_on_percentile.sort(col("count").desc());
    filtered_based_on_percentile = filtered_based_on_percentile.withColumnRenamed('wikidata_id','filtered_id')
    # print_shape(filtered_based_on_percentile)
    # filtered_based_on_percentile.show()
    
    join_data = amenities.join(filtered_based_on_percentile, amenities.wikidata_id == filtered_based_on_percentile.filtered_id).select('lat', 'lon', 'amenity', 'name', 'count', 'wikidata_id')
    print("vancouver_interesting_things:")
    print_shape(join_data)
    join_data.show()
    
    # name = amenities.groupBy('name').count()
    # print_shape(name)
    # name.show()
    
    # print_shape(amenities)
    # amenities.show()
    
    # unique_wikidata_ids = unique_wikidata_ids.na.drop()
    # # unique_wikidata_ids = unique_wikidata_ids.drop('count')
    # unique_wikidata_ids = unique_wikidata_ids.withColumnRenamed('count', 'wikidata_occurrences')
    
    # # Get list of unique amenity names
    # amenities_only = amenities.select(amenities.columns[3])
    # print("amenities_only:")
    # print_shape(amenities_only)
    # amenities_only.show()

    # unique_amenities = amenities_only.dropDuplicates()
    # print("unique_amenities:")
    # print_shape(unique_amenities)
    # unique_amenities.show()

    # This list of values is in the 100s so safe to coalesce
    join_data.toPandas().to_csv("vancouver-interesting-things.csv")

if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)