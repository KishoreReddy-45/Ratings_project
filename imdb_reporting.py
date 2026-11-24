"""
IMDB Reporting Application with PySpark and PostgreSQL

Uses PySpark for ETL and transformations, writes results to PostgreSQL.

Entrypoint: spark-submit imdb_reporting.py
Alternative: python imdb_reporting.py (when PySpark is installed)
Requirements: 
  - PostgreSQL database running
  - PySpark installed or Spark distribution available
"""

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count as spark_count, round as spark_round, desc, asc, regexp_replace
except ImportError as exc:
    raise SystemExit(
        "PySpark is not installed. Install it with `pip install pyspark` "
        "or install a Spark distribution and run with spark-submit."
    ) from exc

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.absolute()

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "imdb_db",
    "user": "postgres",
    "password": "postgres"
}


def create_spark_session():
    """Initialize Spark Session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("IMDB Reporting") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def load_csv_files(spark):
    """Load CSV files into DataFrames."""
    shows_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(str(PROJECT_ROOT / "all-series-ep-average.csv"))
    
    seasons_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(str(PROJECT_ROOT / "top-seasons-full.csv"))
    
    episodes_df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(str(PROJECT_ROOT / "all-episode-ratings.csv"))
    
    # Clean up column names to lowercase
    shows_df = shows_df.select([col(c).alias(c.lower()) for c in shows_df.columns])
    seasons_df = seasons_df.select([col(c).alias(c.lower()) for c in seasons_df.columns])
    episodes_df = episodes_df.select([col(c).alias(c.lower()) for c in episodes_df.columns])
    
    return shows_df, seasons_df, episodes_df


def clean_data(shows_df, seasons_df, episodes_df):
    """Transform and clean data."""
    # Remove commas from rating_count in shows
    shows_df = shows_df.withColumn(
        "rating_count",
        regexp_replace(col("rating count").cast("string"), ",", "").cast("int")
    ).drop("rating count")
    
    # Rename columns to match schema
    shows_df = shows_df.withColumnRenamed("rating mean", "rating_mean")
    seasons_df = seasons_df.withColumnRenamed("season", "season_number") \
        .withColumnRenamed("rating mean", "rating_mean") \
        .withColumnRenamed("number of episodes", "number_of_episodes")
    episodes_df = episodes_df.withColumnRenamed("id_code", "id_code") \
        .withColumnRenamed("rating", "rating")
    
    return shows_df, seasons_df, episodes_df


def register_views(shows_df, seasons_df, episodes_df):
    """Register DataFrames as temporary views."""
    shows_df.createOrReplaceTempView("shows")
    seasons_df.createOrReplaceTempView("seasons")
    episodes_df.createOrReplaceTempView("episodes")
    
    print("[OK] Temporary views registered")


def print_header(title):
    """Print report header."""
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def print_results(df, title=None):
    """Print DataFrame results."""
    if title:
        print(f"\n{title}")
    df.show(truncate=False)


def verify_integrity(spark):
    """Verify data integrity."""
    print_header("DATA VERIFICATION")
    
    shows_count = spark.sql("SELECT COUNT(*) as count FROM shows").collect()[0]["count"]
    seasons_count = spark.sql("SELECT COUNT(*) as count FROM seasons").collect()[0]["count"]
    episodes_count = spark.sql("SELECT COUNT(*) as count FROM episodes").collect()[0]["count"]
    
    print(f"\n Shows Table:   {shows_count:,} records")
    print(f" Seasons Table: {seasons_count:,} records")
    print(f" Episodes Table: {episodes_count:,} records")
    
    orphaned_episodes = spark.sql("""
        SELECT COUNT(*) as count FROM episodes e
        WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.code = e.code)
    """).collect()[0]["count"]
    
    orphaned_seasons = spark.sql("""
        SELECT COUNT(*) as count FROM seasons se
        WHERE NOT EXISTS (SELECT 1 FROM shows s WHERE s.title = se.title)
    """).collect()[0]["count"]
    
    print(f"\n Referential Integrity:")
    print(f"   Orphaned episodes: {orphaned_episodes}")
    print(f"   Orphaned seasons: {orphaned_seasons}")
    
    if orphaned_episodes == 0 and orphaned_seasons == 0:
        print("\n[OK] All data integrity checks passed!")


def report_1(spark):
    """Report 1: Shows with rating <= 6."""
    print_header("REPORT 1: Shows with Rating <= 6")
    
    query = """
        SELECT DISTINCT title, rating
        FROM shows
        WHERE rating <= 6
        ORDER BY rating DESC
    """
    
    df = spark.sql(query)
    print_results(df, "All Shows with Rating <= 6:")
    
    # Report 1A
    print_header("REPORT 1A: Shows with Rating <= 6 and Multiple Seasons")
    
    query_1a = """
        SELECT s.title, s.rating, COUNT(DISTINCT se.season_number) as season_count
        FROM shows s
        LEFT JOIN seasons se ON s.title = se.title
        WHERE s.rating <= 6
        GROUP BY s.title, s.rating
        HAVING COUNT(DISTINCT se.season_number) > 1
        ORDER BY s.rating DESC
    """
    
    df_1a = spark.sql(query_1a)
    print_results(df_1a, "Shows with Rating <= 6 that have More Than 1 Season:")
    
    count = df_1a.count()
    print(f"\nTotal: {count} show(s) with rating <= 6 have more than 1 season.")


def report_2(spark):
    """Report 2: Highest rating count shows."""
    print_header("REPORT 2: Show(s) with Highest Rating Count and Lowest Rank")
    
    query = """
        SELECT s.title, s.rating_count, s.rank,
               COUNT(DISTINCT e.id_code) as total_episodes,
               COUNT(DISTINCT e.season) as total_seasons
        FROM shows s
        LEFT JOIN episodes e ON s.code = e.code
        WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
          AND s.rank = (SELECT MIN(rank) FROM shows 
                        WHERE rating_count = (SELECT MAX(rating_count) FROM shows))
        GROUP BY s.title, s.rating_count, s.rank
        ORDER BY s.rank ASC
    """
    
    df = spark.sql(query)
    print_results(df, "Show(s) with Highest Rating Count and Lowest Rank:")
    
    # Report 2A
    print_header("REPORT 2A: Detailed Information - Highest Rating Count Shows")
    
    query_2a = """
        SELECT s.title, s.rating_count, s.rank, s.rating, s.rating_mean
        FROM shows s
        WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
        ORDER BY s.rank ASC
    """
    
    df_2a = spark.sql(query_2a)
    print_results(df_2a, "Show Details:")
    
    # Report 2B
    print_header("REPORT 2B: Episode and Season Summary - Highest Rating Count Shows")
    
    query_2b = """
        SELECT s.title,
               COUNT(DISTINCT e.id_code) as total_episodes,
               COUNT(DISTINCT se.season_number) as total_seasons,
               ROUND(AVG(e.rating), 2) as avg_episode_rating
        FROM shows s
        LEFT JOIN episodes e ON s.code = e.code
        LEFT JOIN seasons se ON s.title = se.title
        WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
        GROUP BY s.title
        ORDER BY s.title
    """
    
    df_2b = spark.sql(query_2b)
    print_results(df_2b, "Episode and Season Counts:")


def report_3(spark):
    """Report 3: Lowest rating count shows."""
    print_header("REPORT 3: Show(s) with Lowest Rating Count and Highest Rank")
    
    query = """
        SELECT s.title, s.rating_count, s.rank,
               COUNT(DISTINCT e.id_code) as total_episodes,
               COUNT(DISTINCT e.season) as total_seasons
        FROM shows s
        LEFT JOIN episodes e ON s.code = e.code
        WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
          AND s.rank = (SELECT MAX(rank) FROM shows 
                        WHERE rating_count = (SELECT MIN(rating_count) FROM shows))
        GROUP BY s.title, s.rating_count, s.rank
        ORDER BY s.rank DESC
    """
    
    df = spark.sql(query)
    print_results(df, "Show(s) with Lowest Rating Count and Highest Rank:")
    
    # Report 3A
    print_header("REPORT 3A: Detailed Information - Lowest Rating Count Shows")
    
    query_3a = """
        SELECT s.title, s.rating_count, s.rank, s.rating, s.rating_mean
        FROM shows s
        WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
        ORDER BY s.rank DESC
    """
    
    df_3a = spark.sql(query_3a)
    print_results(df_3a, "Show Details:")
    
    # Report 3B
    print_header("REPORT 3B: Episode and Season Summary - Lowest Rating Count Shows")
    
    query_3b = """
        SELECT s.title,
               COUNT(DISTINCT e.id_code) as total_episodes,
               COUNT(DISTINCT se.season_number) as total_seasons,
               ROUND(AVG(e.rating), 2) as avg_episode_rating
        FROM shows s
        LEFT JOIN episodes e ON s.code = e.code
        LEFT JOIN seasons se ON s.title = se.title
        WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
        GROUP BY s.title
        ORDER BY s.title
    """
    
    df_3b = spark.sql(query_3b)
    print_results(df_3b, "Episode and Season Counts:")


def main():
    """Main workflow."""
    try:
        spark = create_spark_session()
        print("[OK] Spark session initialized")
        
        # Load and clean data
        shows_df, seasons_df, episodes_df = load_csv_files(spark)
        print("[OK] CSV files loaded")
        
        shows_df, seasons_df, episodes_df = clean_data(shows_df, seasons_df, episodes_df)
        print("[OK] Data cleaned and transformed")
        
        # Register views
        register_views(shows_df, seasons_df, episodes_df)
        
        # Verify integrity
        verify_integrity(spark)
        
        # Generate reports
        report_1(spark)
        report_2(spark)
        report_3(spark)
        
        print("\n" + "=" * 90)
        print("  ALL REPORTS COMPLETED SUCCESSFULLY")
        print("=" * 90 + "\n")
        
        spark.stop()
        print("[OK] Spark session stopped")
        
    except Exception as e:
        print(f"\n[ERROR] Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()


