import sys
import pandas as pd
import utility_functions as uf
import time
from wikidata_api.WikidataAPI import WikidataAPI
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('append wikidata info').getOrCreate()
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

def extract_wikidata_id(tags):
    if tags == {}:
        return None
    for tag in tags:
        if "wikidata" in tag:
            return tags[tag]
    return None


def main(in_directory, out_directory):
    api = WikidataAPI()

    # Load amenities
    amenities = spark.read.json(in_directory, schema=amenities_schema)

    extract_wikidata_id_udf = functions.udf(lambda x:extract_wikidata_id(x), returnType=types.StringType())
    amenities = amenities.withColumn("wikidata_id", extract_wikidata_id_udf(amenities.tags))

    unique_wikidata_ids = amenities.groupBy('wikidata_id').count()
    unique_wikidata_ids = unique_wikidata_ids.na.drop()
    unique_wikidata_ids = unique_wikidata_ids.withColumnRenamed('count', 'wikidata_occurrences')
    
    ids = unique_wikidata_ids.rdd.collect()
    wikidata_sitelink_counts = {}
    for id in ids:
        wikidata_sitelink_counts[id[0]] = api.get_count_sitelinks(id[0])
        if len(ids) > 100:
            time.sleep(1) # Slow down API calls

    # Convert dictionary to dataframe by first converting dictionary to list of lists
    # https://stackoverflow.com/questions/37584077/convert-a-standard-python-key-value-dictionary-list-to-pyspark-data-frame
    listOfLists = list(map(list, wikidata_sitelink_counts.items()))
    wikidata_sitelink_counts_df = spark.createDataFrame(listOfLists, ["wikidata_id", "count_sitelinks"])

    # Append information to unique_wikidata_ids
    unique_wikidata_ids = unique_wikidata_ids.join(wikidata_sitelink_counts_df.hint('broadcast'), unique_wikidata_ids.wikidata_id == wikidata_sitelink_counts_df.wikidata_id) \
        .drop(wikidata_sitelink_counts_df.wikidata_id)
    unique_wikidata_ids.cache() 

    # Join unique_wikidata_ids to amenities and keep column count_sitelinks
    amenities = amenities.join(unique_wikidata_ids.hint('broadcast'), amenities.wikidata_id == unique_wikidata_ids.wikidata_id, "left").drop(unique_wikidata_ids.wikidata_id)
    amenities.write.mode('overwrite').json(out_directory)
    return

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)