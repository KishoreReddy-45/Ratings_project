from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# ==================================================
# Create Spark Session
# ==================================================

spark = (
    SparkSession.builder
    .appName("IMDB Reporting")
    .getOrCreate()
)

# ==================================================
# Read CSV Files
# ==================================================

shows_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("all-series-ep-average.csv")
)

seasons_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("top-seasons-full.csv")
)

episodes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("all-episode-ratings.csv")
)

# ==================================================
# Data Cleaning
# ==================================================

shows_df = (
    shows_df
    .withColumn(
        "rating_count",
        regexp_replace(col("Rating Count"), ",", "").cast("int")
    )
    .drop("Rating Count")
    .withColumnRenamed("Code", "code")
    .withColumnRenamed("Title", "title")
    .withColumnRenamed("Rating", "rating")
    .withColumnRenamed("Rank", "rank")
    .withColumnRenamed("Rating Mean", "rating_mean")
)

seasons_df = (
    seasons_df
    .withColumnRenamed("Key", "season_key")
    .withColumnRenamed("Title", "title")
    .withColumnRenamed("Season", "season_number")
    .withColumnRenamed("Rating Mean", "rating_mean")
    .withColumnRenamed("Number of Episodes", "number_of_episodes")
)

episodes_df = (
    episodes_df
    .withColumnRenamed("id_code", "id_code")
    .withColumnRenamed("Season", "season_number")
    .withColumnRenamed("Episode", "episode_number")
    .withColumnRenamed("Rating", "rating")
    .withColumnRenamed("Code", "code")
)

# ==================================================
# Register Views
# ==================================================

shows_df.createOrReplaceTempView("shows")
seasons_df.createOrReplaceTempView("seasons")
episodes_df.createOrReplaceTempView("episodes")

# ○ Display all shows (include the show title) with a rating less than or equal to 6
# ■ Which shows from this data had more than 1 season?

print("\n================ REPORT 1 =================")

spark.sql("""
SELECT
    title,
    rating
FROM shows
WHERE rating <= 6
ORDER BY rating DESC
""").show(truncate=False)

print("\n========== REPORT 1A ==========")

spark.sql("""
SELECT
    s.title,
    s.rating,
    COUNT(DISTINCT se.season_number) AS season_count
FROM shows s
JOIN seasons se
ON s.title = se.title
WHERE s.rating <= 6
GROUP BY s.title, s.rating
HAVING COUNT(DISTINCT se.season_number) > 1
ORDER BY season_count DESC
""").show(truncate=False)

# Display the show(s) that have the highest rating count and the lowest rank
print("\n================ REPORT 2: Highest Rating Count and Lowest Rank =================")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MAX(rating_count) FROM shows)
)
SELECT
    title,
    rating_count,
    rank
FROM target_show
ORDER BY rank ASC
LIMIT 1
""").show(truncate=False)

print("\n========== REPORT 2A: Episode Count ==========")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MAX(rating_count) FROM shows)
    ORDER BY rank ASC
    LIMIT 1
)
SELECT
    t.title,
    COUNT(DISTINCT e.id_code) AS total_episodes
FROM target_show t
JOIN episodes e
ON t.code = e.code
GROUP BY t.title
""").show(truncate=False)

print("\n========== REPORT 2B: Season Count ==========")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MAX(rating_count) FROM shows)
    ORDER BY rank ASC
    LIMIT 1
)
SELECT
    t.title,
    COUNT(DISTINCT se.season_number) AS total_seasons
FROM target_show t
JOIN seasons se
ON t.title = se.title
GROUP BY t.title
""").show(truncate=False)

# Display the show(s) that have the lowest rating count and the highest rank

print("\n================ REPORT 3: Lowest Rating Count and Highest Rank =================")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MIN(rating_count) FROM shows)
)
SELECT
    title,
    rating_count,
    rank
FROM target_show
ORDER BY rank DESC
LIMIT 1
""").show(truncate=False)

print("\n========== REPORT 3A: Episode Count ==========")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MIN(rating_count) FROM shows)
    ORDER BY rank DESC
    LIMIT 1
)
SELECT
    t.title,
    COUNT(DISTINCT e.id_code) AS total_episodes
FROM target_show t
JOIN episodes e
ON t.code = e.code
GROUP BY t.title
""").show(truncate=False)

print("\n========== REPORT 3B: Season Count ==========")

spark.sql("""
WITH target_show AS (
    SELECT *
    FROM shows
    WHERE rating_count =
        (SELECT MIN(rating_count) FROM shows)
    ORDER BY rank DESC
    LIMIT 1
)
SELECT
    t.title,
    COUNT(DISTINCT se.season_number) AS total_seasons
FROM target_show t
JOIN seasons se
ON t.title = se.title
GROUP BY t.title
""").show(truncate=False)

spark.stop()