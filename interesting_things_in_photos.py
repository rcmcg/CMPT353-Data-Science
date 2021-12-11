import sys
import math
import utility_functions as uf
import geopy.distance
from tkinter.filedialog import askopenfilenames, Tk
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
from image_parser.ImageParser import ImageParser

spark = SparkSession.builder.appName('interesting things in photos').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

amenities_schema = types.StructType([
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
    types.StructField('timestamp', types.TimestampType()),
    types.StructField('amenity', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('tags', types.MapType(types.StringType(), types.StringType())),
    types.StructField('wikidata_id', types.StringType()),
    types.StructField('wikidata_occurrences', types.LongType()),
    types.StructField('count_sitelinks', types.LongType()),
    types.StructField('count_sitelinks_rel_score', types.FloatType()),
    types.StructField('tags_length', types.LongType()),
    types.StructField('interest_score', types.FloatType())
])

nearby_interesting_schema = types.StructType([
    types.StructField('amenity_lat', types.FloatType()),
    types.StructField('amenity_lon', types.FloatType()),
    types.StructField('name', types.StringType()),
    types.StructField('interest_score', types.FloatType()),
    types.StructField('photo_lat', types.FloatType()),
    types.StructField('photo_lon', types.FloatType())
])

def get_image_filepaths():
    filenames = askopenfilenames()
    return filenames

def is_amenity_within_range(photo_lat, photo_lon, amenity_lat, amenity_lon, valid_range_metres):
    distance_metres = compute_distance_metres(photo_lat, photo_lon, amenity_lat, amenity_lon)
    if distance_metres <= valid_range_metres:
        return distance_metres
    else:
        return -1.0

def compute_distance_metres(photo_lat, photo_lon, amenity_lat, amenity_lon):
    distance_metres = geopy.distance.distance((photo_lat, photo_lon),(amenity_lat, amenity_lon)).m
    return distance_metres


def main(in_directory, out_filename):
    valid_range_metres = 500
    image_parser = ImageParser()

    amenities = spark.read.json(in_directory, schema=amenities_schema)

    is_amenity_within_range_udf = functions.udf(
        lambda photo_lat, photo_lon, amenity_lat, amenity_lon, valid_range_metres: is_amenity_within_range(photo_lat, photo_lon, amenity_lat, amenity_lon, valid_range_metres), 
        returnType=types.FloatType())

    nearby_interesting = spark.createDataFrame([], nearby_interesting_schema)

    image_filepaths = get_image_filepaths()
    for image_filepath in image_filepaths:
        image_coordinates = image_parser.read_image_get_coordinates(image_filepath)
        if image_coordinates == (-1, -1):
            print("ERROR: " + image_filepath + " does not have GPS coordinates in its EXIF metadata")
            continue
        nearby_amenities = amenities.withColumn('photo_lat', lit(image_coordinates[0]))
        nearby_amenities = nearby_amenities.withColumn('photo_lon', lit(image_coordinates[1]))
        nearby_amenities = nearby_amenities.withColumn('valid_range', lit(valid_range_metres))
        nearby_amenities = nearby_amenities.withColumn('distance_photo_to_amenity', is_amenity_within_range_udf(
            nearby_amenities.photo_lat, nearby_amenities.photo_lon,
            nearby_amenities.lat, nearby_amenities.lon, nearby_amenities.valid_range))
        nearby_amenities = nearby_amenities.filter(nearby_amenities.distance_photo_to_amenity != -1.0)
        nearby_amenities = nearby_amenities.orderBy('interest_score', ascending=False)

        # Remove columns and rows to union with nearby_interesting for export
        max_interest_score = nearby_amenities.agg({'interest_score': 'max'}).collect()[0]['max(interest_score)']
        nearby_amenities = nearby_amenities.filter(nearby_amenities.interest_score == max_interest_score)
        nearby_amenities = nearby_amenities.drop('timestamp','amenity','tags','wikidata_id','wikidata_occurrences','count_sitelinks','count_sitelinks_rel_score',
            'tags_length', 'valid_range', 'distance_photo_to_amenity')
        nearby_amenities = nearby_amenities.withColumnRenamed('lat', 'amenity_lat')
        nearby_amenities = nearby_amenities.withColumnRenamed('lon', 'amenity_lon')
        nearby_interesting = nearby_interesting.union(nearby_amenities)

    # DF should be small so safe to put in memory
    nearby_interesting.toPandas().to_csv(out_filename)
    return

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_filename = sys.argv[2]
    main(in_directory, out_filename)