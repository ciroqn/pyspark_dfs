# Analysing the traffic to Wiki pages via open source info

from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession\
.builder\
.config('spark.app.name', 'wiki_clickstreams')\
.getOrCreate()

# Sample data
sample_clickstream_counts = [
    ["other-search", "Hanging_Gardens_of_Babylon", "external", 47000],
    ["other-empty", "Hanging_Gardens_of_Babylon", "external", 34600],
    ["Wonders_of_the_World", "Hanging_Gardens_of_Babylon", "link", 14000],
    ["Babylon", "Hanging_Gardens_of_Babylon", "link", 2500]
]

# Create RDD from sample data
clickstream_counts_rdd = spark.sparkContext.parallelize(sample_clickstream_counts)

# Create a DataFrame from the RDD of sample clickstream counts (col names are give in teh list arg.
clickstream_sample_df = clickstream_counts_rdd.toDF(['source_page', 'target_page', 'link_category', 'link_count'])

# Display the DataFrame to the notebook
clickstream_sample_df.show(5, truncate=False)


############# Inspect raw clickstream data ##############

# Read the target directory (`./cleaned/clickstream/`) into a DataFrame (`clickstream`). NOTE: cleaned ---> clickstream folder/files NOT in this directory.
clickstream = spark.read\
.option('header', True)\
.option('delimiter', '\t')\
.option('inferSchema', True)\
.csv('./cleaned/clickstream/')

# Display the DataFrame to the notebook
clickstream.show(5, truncate=False)

# Display the schema (i.e dtypes) of the `clickstream` DataFrame columns.
clickstream.printSchema()
