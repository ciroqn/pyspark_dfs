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

# Create a DataFrame from the RDD of sample clickstream counts (col names are give in the list arg)
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

# Display the schema (i.e dtypes) of the `clickstream` DataFrame columns
clickstream.printSchema()

# Drop 'language_code' column since the analysis is focused on English
clickstream = clickstream.drop('language_code')

# Display the first few rows of the DataFrame
clickstream.show(5)

# Rename `referrer` and `resource` to `source_page` and `target_page`
clickstream = clickstream\
.withColumnRenamed('referrer', 'source_page')\
.withColumnRenamed('resource', 'target_page')
  
# Display the first few rows of the DataFrame
clickstream.show(5, truncate=False)

# Display the new schema
clickstream.printSchema()

######## Filter data ########

# Create a temporary view in the metadata for this `SparkSession` 
clickstream.createOrReplaceTempView('clickstream')

# Filter and sort the DataFrame using PySpark DataFrame methods
clickstream\
.filter(clickstream.target_page == 'Hanging_Gardens_of_Babylon')\
.select('*')\
.orderBy('click_count', ascending=False)\
.show(truncate=False)

# Alternatively, filter and sort the DataFrame using SQL
query = """SELECT *
FROM clickstream
WHERE target_page = 'Hanging_Gardens_of_Babylon'
ORDER BY click_count
"""

spark.sql(query).show(truncate=False)

# Aggregate the DataFrame using PySpark DataFrame Methods: here, we want to know where the traffic is coming from
clickstream\
.select(['link_category', 'click_count'])\
.groupBy('link_category')\
.sum()\
.orderBy('sum(click_count)', ascending=False)\
.show(truncate=False)

# Aggregate the DataFrame using SQL (same as the above)
query = """SELECT link_category, SUM(click_count)
FROM clickstream
GROUP BY 1
ORDER BY 2 DESC
"""

spark.sql(query).show(truncate=False)

