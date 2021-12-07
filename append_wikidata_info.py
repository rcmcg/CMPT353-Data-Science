import sys
import pandas as pd
import utility_functions as uf
import time
from wikidata_api.WikidataAPI import WikidataAPI
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

def extract_wikidata_id(tags):
    # Input: tags field of amenities
    if tags == {}:
        return None
    for tag in tags:
        if "wikidata" in tag:
            return tags[tag]
    return None


def main(in_directory, out_directory):
    api = WikidataAPI()
    # Purpose of this file is to analyze {amenities}\{boring amenities} and give each amenity a score
    # Want to analyze: How "complete an entry is".
    # Does it have a name? Maybe that should be dropped in load_extracted_amenities.py
    # What kind of data can we get from wikidata?

    # Load amenities
    amenities = spark.read.json(in_directory, schema=amenities_schema)
    # uf.print_dataframe("amenities", amenities)

    # Add wikidata id as a column (want to make a call for each wikidata id, not each row with a wikidata entry)
    # amenities = amenities.withColumn("wikidata_id", amenities.tags['brand:wikidata'].isNotNull())
    # Add an intermediate column
    # Column has the key of the first tag that has wikidata in the name
    extract_wikidata_id_udf = functions.udf(lambda x:extract_wikidata_id(x), returnType=types.StringType())
    # amenities = amenities.withColumn("wikidata_id", amenities.tags['brand:wikidata'])
    amenities = amenities.withColumn("wikidata_id", extract_wikidata_id_udf(amenities.tags))
    # print("amenities after load and adding column")
    # uf.print_dataframe("amenities", amenities)

    # get list of unique wikidata_id's
    unique_wikidata_ids = amenities.groupBy('wikidata_id').count()
    unique_wikidata_ids = unique_wikidata_ids.na.drop()
    # unique_wikidata_ids = unique_wikidata_ids.drop('count')
    unique_wikidata_ids = unique_wikidata_ids.withColumnRenamed('count', 'wikidata_occurrences')
    # unique_wikidata_ids.show()
    # print("Unique_wikidata_ids after groupBy from amenities")
    # uf.print_dataframe("unique_wikidata_ids", unique_wikidata_ids)

    # Append sitelinks count to unique_wikidata_ids
    # count_sitelinks_udf = functions.udf(lambda x:api.get_count_sitelinks(x), returnType=types.LongType())
    # unique_wikidata_ids = unique_wikidata_ids.withColumn('count_sitelinks', count_sitelinks_udf(unique_wikidata_ids['wikidata_id']))
    # Above line seems buggy
    # Wasn't buggy at all, problem was grabbing data from Wikidata. 
    
    # Instead, create a dictionary of QID, count_sitelinks pairs, convert that to a DF, then append that to the dataframe. See if that works better
    ids = unique_wikidata_ids.rdd.collect()
    wikidata_sitelink_counts = {}
    # print("Entering for loop")
    # print("total entries: " + str(len(ids)))
    for id in ids:
        wikidata_sitelink_counts[id[0]] = api.get_count_sitelinks(id[0])
        if len(ids) > 100:
            time.sleep(1) # Slow down API calls
    # print(wikidata_sitelink_counts)

    # Convert dictionary to dataframe by first converting dictionary to list of lists
    # https://stackoverflow.com/questions/37584077/convert-a-standard-python-key-value-dictionary-list-to-pyspark-data-frame
    listOfLists = list(map(list, wikidata_sitelink_counts.items()))
    wikidata_sitelink_counts_df = spark.createDataFrame(listOfLists, ["wikidata_id", "count_sitelinks"])
    # print("wikidata_sitelink_counts_df after creating dataframe from dictionary filled by API calls")
    # uf.print_dataframe("wikidata_sitelink_counts_df", wikidata_sitelink_counts_df)

    # Append information to unique_wikidata_ids
    unique_wikidata_ids = unique_wikidata_ids.join(wikidata_sitelink_counts_df.hint('broadcast'), unique_wikidata_ids.wikidata_id == wikidata_sitelink_counts_df.wikidata_id) \
        .drop(wikidata_sitelink_counts_df.wikidata_id)
    unique_wikidata_ids.cache() 
    # print("unique_wikidata_ids after joining the sitelink_counts from the API calls")
    # uf.print_dataframe("unique_wikidata_ids", unique_wikidata_ids)

    # Join unique_wikidata_ids to amenities and keep column count_sitelinks
    amenities = amenities.join(unique_wikidata_ids.hint('broadcast'), amenities.wikidata_id == unique_wikidata_ids.wikidata_id, "left").drop(unique_wikidata_ids.wikidata_id)
    amenities.cache()
    # print("amenities after joining unique_wikidata_ids")
    # uf.print_dataframe("amenities", amenities)

    # amenities = amenities.orderBy(amenities.count_sitelinks.desc())
    # uf.print_dataframe("amenities", amenities)

    # amenities.drop('tags').coalesce(1).write.csv("amenities") # For testing
    amenities.write.mode('overwrite').json(out_directory)
    return

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)