# AGENTS.md

## Project Overview

This project analyzes IMDB TV show datasets using PySpark for ETL and transformations with PostgreSQL as the data warehouse. The application ingests CSV data, performs distributed transformations, and generates SQL-based analytical reports.

### Input Files
- `all-series-ep-average.csv` - Show-level metadata
- `top-seasons-full.csv` - Season-level information
- `all-episode-ratings.csv` - Episode-level ratings

### Objectives
1. Create a relational database schema (DDL) in PostgreSQL
2. Use PySpark to load and transform CSV data
3. Generate analytical reports using Spark SQL
4. Validate data integrity across tables
5. Produce formatted reports with aggregated results

---

## Technology Stack

- **Python 3.8+**
- **PySpark 3.x** (for distributed ETL and transformations)
- **PostgreSQL 12+** (open source, production-grade database)
- **Spark SQL** (for analytical queries)

### Why This Stack?

**PySpark Advantages:**
- Distributed data processing
- Scalable to large datasets
- SQL interface for complex queries
- Optimized query execution plans
- DataFrame API for transformations

**PostgreSQL Advantages:**
- Open source and free
- Production-grade RDBMS
- ACID compliance with foreign keys
- Advanced SQL support
- Excellent for analytical workloads

---

## Coding Standards

- Use PySpark DataFrame API for transformations
- Use Spark SQL for analytical queries
- Follow PEP-8 for Python code
- Use descriptive variable and column names (lowercase_with_underscores)
- Add comments for each major section
- Implement proper error handling and resource cleanup
- Parameterize queries to prevent SQL injection

---

## Data Model

### Schema Overview

Three linked tables with foreign key relationships:

```
shows (Primary table with show metadata)
  ├─ episodes (One show can have many episodes)
  └─ seasons (One show can have many seasons)
```

### Shows Table
**Source:** all-series-ep-average.csv

Columns:
- `code` (VARCHAR(20), PRIMARY KEY) - IMDB identifier (e.g., tt5491994)
- `title` (VARCHAR(255), NOT NULL) - Show title
- `rating` (DECIMAL(3,1), NOT NULL) - IMDB rating (0-10 scale)
- `rating_count` (INTEGER, NOT NULL) - Number of ratings
- `rank` (INTEGER, NOT NULL) - Show ranking
- `rating_mean` (DECIMAL(5,4), NOT NULL) - Weighted average rating

### Seasons Table
**Source:** top-seasons-full.csv

Columns:
- `key` (VARCHAR(50), PRIMARY KEY) - IMDB season identifier
- `title` (VARCHAR(255), NOT NULL, FOREIGN KEY) - Show title (links to shows.title)
- `season_number` (INTEGER, NOT NULL) - Season number
- `rating_mean` (DECIMAL(5,4), NOT NULL) - Average episode rating
- `number_of_episodes` (INTEGER, NOT NULL) - Episode count

### Episodes Table
**Source:** all-episode-ratings.csv

Columns:
- `id_code` (INTEGER, PRIMARY KEY) - Episode identifier
- `season` (INTEGER, NOT NULL) - Season number
- `episode` (INTEGER, NOT NULL) - Episode number
- `rating` (DECIMAL(3,1), NOT NULL) - Individual episode rating
- `code` (VARCHAR(20), NOT NULL, FOREIGN KEY) - IMDB show code (links to shows.code)

### Relationships
- `shows.code` ← → `episodes.code` (one-to-many)
- `shows.title` ← → `seasons.title` (one-to-many)

---

## Required Reports

### Report 1: Shows with Rating ≤ 6
**Query Requirement:**
Display all shows with rating less than or equal to 6, including:
- Show Title
- Rating

**Report 1A: Multi-Season Analysis**
Identify which shows from Report 1 have more than 1 season:
- Show Title
- Rating
- Season Count

### Report 2: Highest Rating Count Shows
**Query Requirement:**
Find show(s) with:
- Highest Rating Count (maximum value across all shows)
- Lowest Rank (minimum value among those with highest rating count)

**Display:**
- Show Title
- Rating Count
- Rank
- Episode Count
- Season Count

**Report 2A: Detailed Show Information**
Display detailed metrics for highest rating count shows

**Report 2B: Episode and Season Summary**
Display aggregated episode/season data

### Report 3: Lowest Rating Count Shows
**Query Requirement:**
Find show(s) with:
- Lowest Rating Count (minimum value across all shows)
- Highest Rank (maximum value among those with lowest rating count)

**Display:**
- Show Title
- Rating Count
- Rank
- Episode Count
- Season Count

**Report 3A: Detailed Show Information**
Display detailed metrics for lowest rating count shows

**Report 3B: Episode and Season Summary**
Display aggregated episode/season data

---

## Performance Guidelines

- **Distributed Processing** - Use PySpark for parallel data processing
- **Temporary Views** - Register DataFrames as views for SQL queries
- **Spark SQL Optimization** - Let Catalyst optimize query execution plans
- **Data Cleanup** - Transform and normalize data during ingestion
- **Caching** - Cache frequently accessed DataFrames
- **Aggregation in SQL** - Use GROUP BY/HAVING for calculations

---

## Output Requirements

### Application Entrypoint
```bash
spark-submit imdb_reporting.py
```

### Expected Workflow
1. Initialize Spark session
2. Load CSV data into DataFrames
3. Clean and transform data (remove commas, normalize columns)
4. Register temporary views
5. Verify referential integrity
6. Execute all 6 reports
7. Display formatted output
8. Stop Spark session

### Output Format
Each report includes:
- Clear section header
- Formatted tabular output
- Summary counts or totals where appropriate

---

## Testing Checklist

Before considering the project complete:

1. **CSV Loading**
   - [ ] All three CSV files load without errors
   - [ ] Data types are correctly inferred
   - [ ] Column names are normalized

2. **Data Transformation**
   - [ ] Rating Count commas are properly removed
   - [ ] Column names match schema (lowercase_with_underscores)
   - [ ] Data types are correctly cast

3. **Data Integrity**
   - [ ] All foreign key relationships valid
   - [ ] No orphaned records in episodes or seasons
   - [ ] Record counts match expectations

4. **Query Execution**
   - [ ] Report 1 returns shows with rating ≤ 6
   - [ ] Report 1A correctly identifies multi-season shows
   - [ ] Report 2 finds highest rating count show(s)
   - [ ] Report 2A/2B show correct aggregations
   - [ ] Report 3 finds lowest rating count show(s)
   - [ ] Report 3A/3B show correct aggregations
   - [ ] All queries execute without SQL errors

5. **Output Quality**
   - [ ] Headers are clear and properly formatted
   - [ ] Data is properly aligned in columns
   - [ ] No truncation of important values
   - [ ] Summary information included where needed

6. **Resource Management**
   - [ ] Spark session closes cleanly
   - [ ] No resource leaks
   - [ ] Error handling works correctly

---

## Deliverables

- **AGENTS.md** - This file (project specifications and requirements)
- **imdb_reporting.py** - Main Python application (PySpark-based)
- **schema.sql** - Database DDL (PostgreSQL schema)
- **queries.sql** - Analytical SQL queries
- **README.md** - Complete documentation and usage guide

The final solution should be executable with:
```bash
spark-submit imdb_reporting.py
```

No additional configuration or code modifications required.

---

## Design Decisions

### PySpark vs SQLite/SQLAlchemy
- **Distributed Processing** - Handles larger datasets efficiently
- **Scalability** - Can process data across clusters
- **SQL Interface** - Familiar Spark SQL for complex queries
- **DataFrame API** - Pythonic interface for transformations
- **Performance** - Optimized execution with Catalyst optimizer

### PostgreSQL vs SQLite
- **Production Grade** - Suitable for enterprise workloads
- **Concurrency** - Better handling of concurrent connections
- **Advanced Features** - Support for complex data types and constraints
- **Open Source** - No licensing costs
- **Scalability** - Can handle larger datasets

### Column Naming Conventions
- Lowercase with underscores (`rating_count` not `RatingCount`)
- Matches Python PEP-8 and SQL best practices
- Improves code readability and consistency

---

## Discussion Points

Key aspects to discuss during review:

1. **Data Pipeline**
   - Why use PySpark for this dataset size?
   - How does Catalyst optimize the query plans?
   - What transformations are performed during ingestion?

2. **Database Design**
   - Why three tables with specific relationships?
   - How does the schema support the required queries?
   - Foreign key strategy and constraints

3. **Query Optimization**
   - What aggregations are performed in Spark SQL?
   - How do temporary views improve performance?
   - Query execution plans and optimization

4. **Error Handling**
   - How are transformation errors caught and reported?
   - What happens if CSV files are malformed?
   - How is resource cleanup guaranteed?

5. **Scalability**
   - How would the solution handle 10x larger datasets?
   - Could this easily migrate to cloud (AWS, GCP, Azure)?
   - What optimizations would be needed for production?

---

## Summary

This project demonstrates a modern data engineering application that:
- Uses PySpark for distributed ETL and transformations
- Implements proper schema design with constraints
- Executes complex analytical queries using Spark SQL
- Generates formatted reports with aggregated results
- Handles errors gracefully and manages resources properly
- Uses only open-source, production-grade technologies
- Provides scalability for larger datasets


