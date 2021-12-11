import sys
import pandas as pd
import utility_functions as uf
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('load extracted amenities').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

amenities_schema = types.StructType([
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
    types.StructField('timestamp', types.TimestampType()),
    types.StructField('amenity', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType()))
])


def main(in_directory, out_directory, amenities_to_drop):
    # Code will load only the amenities we care about
    amenities_to_drop = spark.read.csv(amenities_to_drop).withColumnRenamed('_c0','amenity')

    all_amenities = spark.read.json(in_directory, schema=amenities_schema)

    # Drop amenities not in interesting_amenities
    filtered_amenities = all_amenities.join(amenities_to_drop, ['amenity'], "left_outer") \
        .where(amenities_to_drop.amenity.isNull()) \
        .select(all_amenities.columns)

    filtered_amenities.write.mode('overwrite').json(out_directory)

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    amenities_to_drop = sys.argv[3]
    main(in_directory, out_directory, amenities_to_drop)