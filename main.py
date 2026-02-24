from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Hide the messy Spark INFO logs in the console
spark.sparkContext.setLogLevel("WARN")

# Load datasets
listening_history = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
song_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Convert timestamp column properly
listening_history = listening_history.withColumn("timestamp", to_timestamp("timestamp"))

# ---------------------------------------------------------
# Task 1: User Favorite Genres
# Added 'listen_count' to show why it's their favorite
# ---------------------------------------------------------
user_favorite_genres = listening_history.join(song_metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .agg(count("*").alias("listen_count")) \
    .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("listen_count")))) \
    .filter(col("rank") == 1) \
    .select("user_id", col("genre").alias("favorite_genre"), "listen_count")

# ---------------------------------------------------------
# Task 2: Average Listen Time
# Shows the user_id and their average duration
# ---------------------------------------------------------
average_listen_time = listening_history.groupBy("user_id") \
    .agg(round(avg("duration_sec"), 2).alias("avg_listen_time_sec"))

# ---------------------------------------------------------
# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
# Shows user_id, the genre, and their calculated loyalty score
# ---------------------------------------------------------
genre_stats = listening_history.join(song_metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .agg(sum("duration_sec").alias("genre_duration"))

total_time_window = Window.partitionBy("user_id")

top_10_loyalty_scores = genre_stats \
    .withColumn("total_user_duration", sum("genre_duration").over(total_time_window)) \
    .withColumn("loyalty_score", round(col("genre_duration") / col("total_user_duration"), 4)) \
    .orderBy(desc("loyalty_score")) \
    .limit(10) \
    .select("user_id", "genre", "loyalty_score")

# ---------------------------------------------------------
# Task 4: Identify users who listen between 12 AM and 5 AM
# Grouped to show the user_id and exactly how many songs they played late at night
# ---------------------------------------------------------
late_night_listeners = listening_history \
    .withColumn("hour", hour("timestamp")) \
    .filter((col("hour") >= 0) & (col("hour") < 5)) \
    .groupBy("user_id") \
    .agg(count("*").alias("late_night_play_count"))

# ---------------------------------------------------------
# Print Results Cleanly
# ---------------------------------------------------------
print("\n" + "="*40)
print("Task 1: User Favorite Genres")
print("="*40)
user_favorite_genres.show(10, truncate=False)

print("\n" + "="*40)
print("Task 2: Average Listen Time")
print("="*40)
average_listen_time.show(10, truncate=False)

print("\n" + "="*40)
print("Task 3: Top 10 Genre Loyalty Scores")
print("="*40)
top_10_loyalty_scores.show(truncate=False)

print("\n" + "="*40)
print("Task 4: Late Night Listeners")
print("="*40)
late_night_listeners.show(10, truncate=False)

spark.stop()