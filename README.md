# cmpt353-project

The final project for CMPT353.

Dependencies for the project:
- exif
- lxml (if you'd like to use the code in .\provided\code)
- pandas
- spark
- numpy
- geopy
- tkinter
- matplotlib

## How to get results for the first presented problem, "Given a collection of geotagged photos, can we create a model that attempts to find the most interesting thing in the photographs?"
CD into the root of the repository and run the following commands
- spark-submit load_extracted_amenities.py provided\amenities-vancouver.json.gz temp-dir vancouver-amenities-to-drop.csv
- spark-submit append_wikidata_info.py temp-dir temp-dir-2
- spark-submit give_amenities_score.py temp-dir-2 temp-dir-3
- spark-submit interesting_things_in_photos.py temp-dir-3 output_filename.csv

The output_filename contains coordinates for the model's best guess of the most interesting amenity in the photo along with the amenities name and the photo's coordinates.

## If I was planning a tour of the city (by walking/biking/driving), where should I go? Are there paths that take me past an interesting variety of things?
CD into the root of the repository and run the following commands
- spark-submit get_interesting_things.py provided/amenities-vancouver.json.gz
- python3 get_paths_to_interesting_things.py vancouver-interesting-things.csv

The paths.csv file contains the recommended paths to a variety of "interesting things", assuming that a person can visit at most 3 locations in a day.
