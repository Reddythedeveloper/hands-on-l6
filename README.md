# Music Streaming Analysis Using Spark Structured APIs

## Overview

This project leverages Apache Spark's Structured APIs (DataFrames and Window functions) to analyze a simulated music streaming dataset. The analysis extracts user behavior metrics, including favorite genres, average listening times, genre loyalty scores, and late-night streaming patterns.

## Dataset Description

The analysis relies on two primary datasets:

* **`listening_logs.csv`**: Contains streaming event records.
* `user_id`: Unique identifier for the listener.
* `song_id`: Unique identifier for the track played.
* `timestamp`: The date and time the stream occurred.
* `duration_sec`: The length of time the user listened to the song, in seconds.


* **`songs_metadata.csv`**: Contains track details.
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
2. **Task 2: Average Listen Time** - Aggregates the total listening duration per user and calculates their average listen time per track in seconds.
3. **Task 3: Genre Loyalty Scores** - Calculates a custom metric (time spent on a specific genre divided by the total time spent listening) and ranks the global top 10 highest loyalty scores.
4. **Task 4: Late Night Listeners** - Filters the dataset to identify users who actively stream music between 12:00 AM and 5:00 AM.

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
* **Error: JDK incompatibility of JDK 25.x.x.x
* *Resolution:* Installed JDK 17.x.x.x, it fixed error causing with the JDK.

* **Messy Console Logs during `spark-submit**`
* *Cause:* Spark prints internal `INFO` level logs to the console by default, burying the DataFrame outputs.
* *Resolution:* Added `spark.sparkContext.setLogLevel("WARN")` directly after initializing the SparkSession to suppress non-critical logs and keep the output clean.