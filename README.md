# Analyzing-Movie-Ratings-with-Apache-Spark

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
import matplotlib.pyplot as plt

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

# Load the dataset into a Spark DataFrame
data_file = "movie_ratings.csv"
df = spark.read.csv(data_file, header=True, inferSchema=True)

# Display the schema and first few rows of the DataFrame
df.printSchema()
df.show(5)

# Check for missing values
df.select([count(col).alias(col) for col in df.columns]).show()

# Drop any rows with missing values
df = df.dropna()

# Convert the DataFrame to Pandas for visualization
top_rated_movies_pd = top_rated_movies.toPandas()

# Create a bar plot for top-rated movies
plt.figure(figsize=(10, 6))
plt.bar(top_rated_movies_pd["movieId"], top_rated_movies_pd["avg_rating"], color='blue')
plt.xlabel('Movie ID')
plt.ylabel('Average Rating')
plt.title('Top 10 Highest-Rated Movies')
plt.xticks(rotation=45)
plt.show()

