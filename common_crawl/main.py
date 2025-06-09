# Import required modules
from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession \
    .builder \
    .getOrCreate()

# Get SparkContext
sc = spark.sparkContext

# Read Domains CSV File into an RDD (NOT included in directory)
common_crawl_domain_counts = sc.textFile('./crawl/cc-main-limited-domains.csv')

# e.g.
# ['367855\t172-in-addr\tarpa\t1',
#  '367856\taddr\tarpa\t1',
#  '367857\tamphic\tarpa\t1',
#  '367858\tbeta\tarpa\t1',
#  '367859\tcallic\tarpa\t1',
#  '367860\tch\tarpa\t1',
#  '367861\td\tarpa\t1',
#  '367862\thome\tarpa\t7',
#  '367863\tiana\tarpa\t1',
#  '367907\tlocal\tarpa\t1']

# Display first few domains from the RDD
common_crawl_domain_counts.take(10)

def fmt_domain_graph_entry(entry):
    """
    Formats a Common Crawl domain graph entry. Extracts the site_id, 
    top-level domain (tld), domain name, and subdomain count as seperate items.
    """

    # Split the entry on delimiter ('\t') into site_id, domain, tld, and num_subdomains
    site_id, domain, tld, num_subdomains = entry.split('\t')        
    return int(site_id), domain, tld, int(num_subdomains)


# Apply `fmt_domain_graph_entry` to the raw data RDD
formatted_host_counts = common_crawl_domain_counts.map(lambda x: fmt_domain_graph_entry(x))
# Display the first few entries of the new RDD
formatted_host_counts.take(10)

# function that returns the number of subdomains from each entry in a RDD.
def extract_subdomain_counts(entry):
    """
    Extract the subdomain count from a Common Crawl domain graph entry.
    """
    
    # Split the entry on delimiter ('\t') into site_id, domain, tld, and num_subdomains
    site_id, domain, tld, num_subdomains = entry.split('\t')
    
    # return ONLY the num_subdomains
    return int(num_subdomains)


# Apply `extract_subdomain_counts` to the raw data RDD
host_counts = common_crawl_domain_counts.map(lambda x: extract_subdomain_counts(x))

# Display the first few entries
host_counts.take(10)

# e.g. ---> [1, 1, 1, 1, 1, 1, 1, 7, 1, 1]

# Reduce the RDD to a single value, the sum of subdomains, with a lambda function
# as the reduce function
total_host_counts = host_counts.reduce(lambda x, y: x + y)

# Display result count
print(total_host_counts)

# Stop the sparkContext and the SparkSession in order to analyse data with SparkSQL
spark.stop()

############# SparkSQL ##############

# Create a new SparkSession
spark = SparkSession\
.builder\
.getOrCreate()

# Read the target file into a DataFrame. There are NO headers in the DF, but use the default headers given by Spark for now.
common_crawl = spark.read\
.option('delimiter', '\t')\
.option('inferSchema', True)\
.csv('./crawl/cc-main-limited-domains.csv')

# Display the DataFrame
common_crawl.show(5, truncate=False)

# Rename the DataFrame's columns 
common_crawl = common_crawl.withColumnRenamed('_c0', 'site_id')\
.withColumnRenamed('_c1', 'domain')\
.withColumnRenamed('_c2', 'top_level_domain')\
.withColumnRenamed('_c3', 'num_subdomains')

# Display the first few rows of the DataFrame and the new schema
common_crawl.show(5, truncate=False)
common_crawl.printSchema()
