# Music Streaming Analysis Using Spark Structured APIs

## Overview

This project leverages Apache Spark's Structured APIs (DataFrames and Window functions) to analyze a simulated music streaming dataset. The analysis extracts user behavior metrics, including favorite genres, average listening times, genre loyalty scores, and late-night streaming patterns.

## Dataset Description

The analysis relies on two primary datasets:

* **1. `listening_logs.csv`**: Contains streaming event records.
  * `user_id`: Unique identifier for the listener.
  * `song_id`: Unique identifier for the track played.
  * `timestamp`: The date and time the stream occurred.
  * `duration_sec`: The length of time the user listened to the song, in seconds.

* **2. `songs_metadata.csv`**: Contains track details.
  * `song_id`: Unique identifier for the track.
  * `title`: Track name.
  * `artist`: Artist name.
  * `genre`: The musical genre of the track.
  * `mood`: The emotional category of the track.

## Repository Structure

```text
├── datasets/
│   ├── listening_logs.csv
│   └── songs_metadata.csv
├── datagen.py               # Script to simulate/generate input data
├── main.py                  # Main PySpark analysis script
├── output.txt               # Text file containing the console output of all tasks
└── README.md

```

## Tasks and Outputs

This pipeline performs four distinct analytical tasks, outputting the resulting DataFrames directly to the console:

1. **Task 1: User Favorite Genres** - Calculates the total play count per genre for each user and identifies their #1 most-played genre.

```text
========================================
Task 1: User Favorite Genres
========================================
+--------+--------------+------------+
|user_id |favorite_genre|listen_count|
+--------+--------------+------------+
|user_1  |Pop           |4           |
|user_10 |Classical     |3           |
|user_100|Rock          |3           |
|user_11 |Hip-Hop       |4           |
|user_12 |Jazz          |8           |
|user_13 |Rock          |4           |
|user_14 |Jazz          |4           |
|user_15 |Classical     |3           |
|user_16 |Hip-Hop       |4           |
|user_17 |Classical     |4           |
+--------+--------------+------------+
only showing top 10 rows

```

2. **Task 2: Average Listen Time** - Aggregates the total listening duration per user and calculates their average listen time per track in seconds.

```text
========================================
Task 2: Average Listen Time
========================================
+-------+-------------------+
|user_id|avg_listen_time_sec|
+-------+-------------------+
|user_58|205.45             |
|user_94|178.25             |
|user_73|146.4              |
|user_85|209.92             |
|user_14|133.08             |
|user_56|202.38             |
|user_22|149.45             |
|user_68|161.0              |
|user_86|168.58             |
|user_97|139.13             |
+-------+-------------------+
only showing top 10 rows

```

3. **Task 3: Genre Loyalty Scores** - Calculates a custom metric (time spent on a specific genre divided by the total time spent listening) and ranks the global top 10 highest loyalty scores.

```text
========================================
Task 3: Top 10 Genre Loyalty Scores
========================================
+-------+---------+-------------+
|user_id|genre    |loyalty_score|
+-------+---------+-------------+
|user_75|Jazz     |0.8671       |
|user_39|Hip-Hop  |0.8281       |
|user_28|Classical|0.7387       |
|user_68|Classical|0.7275       |
|user_30|Classical|0.6917       |
|user_96|Classical|0.6734       |
|user_74|Rock     |0.5857       |
|user_45|Jazz     |0.5856       |
|user_4 |Classical|0.5809       |
|user_88|Hip-Hop  |0.5797       |
+-------+---------+-------------+

```

4. **Task 4: Late Night Listeners** - Filters the dataset to identify users who actively stream music between 12:00 AM and 5:00 AM.

```text
========================================
Task 4: Late Night Listeners
========================================
+-------+---------------------+
|user_id|late_night_play_count|
+-------+---------------------+
|user_58|3                    |
|user_94|6                    |
|user_14|2                    |
|user_56|5                    |
|user_22|4                    |
|user_68|1                    |
|user_86|3                    |
|user_97|5                    |
|user_65|6                    |
|user_47|2                    |
+-------+---------------------+
only showing top 10 rows

```

## Prerequisites

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

**1. Python 3.x:**

* Verify installation:

```bash
python3 --version

```

**2. PySpark:**

* Install using pip:

```bash
pip install pyspark

```

**3. Apache Spark:**

* Ensure Spark is installed locally.
* Verify installation by running:

```bash
spark-submit --version

```

## Execution Instructions

**Running Locally**

1. **Generate the Input:**

```bash
python3 datagen.py

```

2. **Execute the Analysis:**
Run the main script using `spark-submit`.

```bash
spark-submit main.py

```

3. **Capture the Outputs:** The results will print directly to the console. You can review the provided `output.txt` file in this repository to see the captured results of the executed queries.

## Known Issues and Resolutions

* **Error: `java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.**`
* *Cause:* Running Apache Spark on Windows requires a Hadoop environment setup (`winutils.exe`) to handle local file system write operations. Attempting to use `.write.csv()` without this configured results in a crash.
* *Resolution:* Bypassed Spark's local file writer by using `.show()` to print the resulting DataFrames to the console and saving the standard output to a simple `.txt` file (`output.txt`).


* **Error: JDK incompatibility of JDK 25.x.x.x**
* *Resolution:* Installed JDK 17.x.x.x, which fixed the error caused by the newer JDK version.


* **Messy Console Logs during `spark-submit**`
* *Cause:* Spark prints internal `INFO` level logs to the console by default, burying the DataFrame outputs.
* *Resolution:* Added `spark.sparkContext.setLogLevel("WARN")` directly after initializing the SparkSession to suppress non-critical logs and keep the output clean.