import sys
import utility_functions as uf
from pyspark.sql import SparkSession, functions, types
from image_parser.ImageParser import ImageParser

spark = SparkSession.builder.appName('give_amenities_score').getOrCreate()
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

def main(in_directory, out_directory, image_filepath):
    # Start with input as one image for now, add support for any number of images
    amenities = spark.read.json(in_directory, schema=amenities_schema)
    uf.print_dataframe("amenities after loading", amenities)

    imageParser = ImageParser()
    image_coordinates = imageParser.read_image_get_coordinates(image_filepath)
    print(image_coordinates)

    return

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    image_filepath = sys.argv[3]
    main(in_directory, out_directory, image_filepath)