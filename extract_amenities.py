import sys
import pandas as pd
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('extract amenities OSM').getOrCreate()
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

def main(in_directory, out_directory):
    amenities = spark.read.json(in_directory, schema=amenities_schema)
    print("amenities:")
    print_shape(amenities)
    amenities.show()
    
    # Get list of unique amenity names
    amenities_only = amenities.select(amenities.columns[3])
    print("amenities_only:")
    print_shape(amenities_only)
    amenities_only.show()

    unique_amenities = amenities_only.dropDuplicates()
    print("unique_amenities:")
    print_shape(unique_amenities)
    unique_amenities.show()

    # This list of values is in the 100s so safe to coalesce
    unique_amenities.coalesce(1).write.csv(out_directory, mode='overwrite')

    

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)