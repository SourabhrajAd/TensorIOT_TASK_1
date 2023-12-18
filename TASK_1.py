# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, date_format, unix_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Path to your JSON file
json_file_path = "C:\SPARK\Video_Games.json"

# Read the JSON file and create a DataFrame
df = spark.read.json(json_file_path)

# Dropping the "style" column due to inconsistent JSON syntax
df = df.drop("style")

# Find the item with the least rating
df_least_rating = df.orderBy("overall")
least_rating_item = df_least_rating.first()
df_least_rating.show(1)
print("Item with the least rating:", least_rating_item)

# Find the item with the most rating
df_most_rating = df.orderBy(col("overall").desc())
most_rating_item = df_most_rating.first()
df_most_rating.show(1)
print("Item with the most rating:", most_rating_item)

# Add a new column 'review_length' representing the length of each review
df_with_length = df.withColumn("review_length", length(col("reviewText")))

# Find the item with the longest review
df_longest_review = df_with_length.orderBy(col("review_length").desc())
longest_review_item = df_longest_review.first()
df_longest_review.show(1)
print("Item with the longest review:", longest_review_item)

# Convert 'reviewTime' to a timestamp and format the date as MM-DD-YYYY
df_formatted_date = (
    df_with_length
    .withColumn("reviewTimestamp", unix_timestamp(col("reviewTime"), "MM dd, yyyy").cast("timestamp"))
    .withColumn("formattedReviewTime", date_format(col("reviewTimestamp"), "MM-dd-yyyy"))
)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Show the DataFrame with formatted dates
df_formatted_date.show()

# Path to save the Parquet file
parquet_file_path = "C:\SPARK\OUTPUT_FILES\test1"

# Write the DataFrame to Parquet
df_formatted_date.write.parquet(parquet_file_path, mode="overwrite")
