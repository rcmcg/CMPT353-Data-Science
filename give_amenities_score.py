import sys
import utility_functions as uf
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit, struct

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
    types.StructField('count_sitelinks', types.LongType())
])

def calculate_interest_score(row):
    min_score = 0.0
    score_multiplier = 1.0
    wikidata_sitelinks_boost = 0.0
    if row.wikidata_occurrences is not None:
        if row.wikidata_occurrences == 1:   # Unique wikidata entries should be boosted
            min_score = 4.0
        elif row.wikidata_occurrences >= 1: # Likely part of a chain of amenities, but still boost since has a wikidata entry
            min_score = 2.0

    if row.amenity in ['restaurant', 'cafe']: # Amenities we're not as interested in, but not boring enough to completely exclude
        score_multiplier = 0.5
    else: 
        score_multiplier = 2.0
    if row.count_sitelinks_rel_score is not None:
        wikidata_sitelinks_boost = calc_count_sitelinks_rel_score_boost(row.min_count_sitelinks_rel_score, row.max_count_sitelinks_rel_score, row.count_sitelinks_rel_score)    

    tags_length_boost = calc_tag_length_boost(row.min_tags_length, row.max_tags_length, row.tags_length)
    tags_entry_boost = calc_tag_entry_boost(row.tags)

    ret_value = score_multiplier * (min_score + wikidata_sitelinks_boost + tags_length_boost + tags_entry_boost)
    return ret_value

def calc_count_sitelinks_rel_score_boost(min_rel_score, max_rel_score, rel_score): 
    return rel_score / (max_rel_score - min_rel_score)

def calc_tag_length_boost(min_tags_length, max_tags_length, tags_length):
    return (tags_length/(max_tags_length-min_tags_length))*2

def calc_tag_entry_boost(tags):
    ret_boost = 0
    for tag in tags:
        if tag in ['tourism', 'historic']:
            ret_boost += 1
    return ret_boost

def main(in_directory, out_directory):
    amenities = spark.read.json(in_directory, schema=amenities_schema)

    # Add a column for relative count_sitelinks score
    average_count_sitelinks = amenities.agg({'count_sitelinks': 'avg'}).collect()[0]['avg(count_sitelinks)']
    amenities = amenities.withColumn('count_sitelinks_rel_score', amenities.count_sitelinks / average_count_sitelinks)

    # Calculate max and min count_sitelinks_rel_score
    max_count_sitelinks_rel_score = amenities.agg({'count_sitelinks_rel_score': 'max'}).collect()[0]['max(count_sitelinks_rel_score)']
    min_count_sitelinks_rel_score = amenities.agg({'count_sitelinks_rel_score': 'min'}).collect()[0]['min(count_sitelinks_rel_score)']

    # Calculate max and min length of tags
    extract_length_tags_udf = functions.udf(lambda tags: len(tags), returnType=types.IntegerType())
    amenities = amenities.withColumn('tags_length', extract_length_tags_udf(amenities.tags))
    max_tags_length = amenities.agg({'tags_length': 'max'}).collect()[0]['max(tags_length)']
    min_tags_length = amenities.agg({'tags_length': 'min'}).collect()[0]['min(tags_length)'] 

    # Add score column
    calculate_interest_score_udf = functions.udf(
        lambda min_count_sitelinks_rel_score_col, max_count_sitelinks_rel_score_col, count_sitelinks_rel_score_col, wikidata_occurrences_col, amenity_col, tags_col: 
        calculate_interest_score(min_count_sitelinks_rel_score_col, max_count_sitelinks_rel_score_col, count_sitelinks_rel_score_col, wikidata_occurrences_col, amenity_col, tags_col), 
        returnType=types.DoubleType())
    calculate_interest_score_udf = functions.udf(
        lambda row: calculate_interest_score(row), returnType=types.DoubleType())
    amenities = amenities.withColumn('min_count_sitelinks_rel_score', lit(min_count_sitelinks_rel_score))
    amenities = amenities.withColumn('max_count_sitelinks_rel_score', lit(max_count_sitelinks_rel_score))
    amenities = amenities.withColumn('min_tags_length', lit(min_tags_length))
    amenities = amenities.withColumn('max_tags_length', lit(max_tags_length))
    amenities = amenities.withColumn('interest_score', calculate_interest_score_udf(struct([amenities[x] for x in amenities.columns])))
    amenities = amenities.drop('min_count_sitelinks_rel_score')
    amenities = amenities.drop('max_count_sitelinks_rel_score')
    amenities = amenities.drop('min_tags_length')
    amenities = amenities.drop('max_tags_length')

    amenities.write.mode('overwrite').json(out_directory)
    return

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)