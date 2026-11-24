-- IMDB Analysis Queries
-- SQL queries to generate analytical reports

-- ============================================================================
-- REPORT 1: All shows with rating ≤ 6
-- ============================================================================
SELECT DISTINCT s.title, s.rating
FROM shows s
WHERE s.rating <= 6
ORDER BY s.rating DESC;

-- ============================================================================
-- REPORT 1A: Shows with rating ≤ 6 that have more than 1 season
-- ============================================================================
SELECT s.title, s.rating, COUNT(DISTINCT se.season_number) as season_count
FROM shows s
LEFT JOIN seasons se ON s.title = se.title
WHERE s.rating <= 6
GROUP BY s.title, s.rating
HAVING COUNT(DISTINCT se.season_number) > 1
ORDER BY s.rating DESC;

-- ============================================================================
-- REPORT 2: Show(s) with highest rating count and lowest rank
-- ============================================================================
SELECT s.title, s.rating_count, s.rank,
       COUNT(DISTINCT e.id_code) as total_episodes,
       COUNT(DISTINCT e.season) as total_seasons
FROM shows s
LEFT JOIN episodes e ON s.code = e.code
WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
  AND s.rank = (SELECT MIN(rank) FROM shows 
                WHERE rating_count = (SELECT MAX(rating_count) FROM shows))
GROUP BY s.title, s.rating_count, s.rank
ORDER BY s.rank ASC;

-- ============================================================================
-- REPORT 2A: Detailed information for highest rating count shows
-- ============================================================================
SELECT s.title, s.rating_count, s.rank, s.rating, s.rating_mean
FROM shows s
WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
ORDER BY s.rank ASC;

-- ============================================================================
-- REPORT 2B: Episode and season summary for highest rating count shows
-- ============================================================================
SELECT s.title,
       COUNT(DISTINCT e.id_code) as total_episodes,
       COUNT(DISTINCT se.season_number) as total_seasons,
       ROUND(AVG(e.rating), 2) as avg_episode_rating
FROM shows s
LEFT JOIN episodes e ON s.code = e.code
LEFT JOIN seasons se ON s.title = se.title
WHERE s.rating_count = (SELECT MAX(rating_count) FROM shows)
GROUP BY s.title
ORDER BY s.title;

-- ============================================================================
-- REPORT 3: Show(s) with lowest rating count and highest rank
-- ============================================================================
SELECT s.title, s.rating_count, s.rank,
       COUNT(DISTINCT e.id_code) as total_episodes,
       COUNT(DISTINCT e.season) as total_seasons
FROM shows s
LEFT JOIN episodes e ON s.code = e.code
WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
  AND s.rank = (SELECT MAX(rank) FROM shows 
                WHERE rating_count = (SELECT MIN(rating_count) FROM shows))
GROUP BY s.title, s.rating_count, s.rank
ORDER BY s.rank DESC;

-- ============================================================================
-- REPORT 3A: Detailed information for lowest rating count shows
-- ============================================================================
SELECT s.title, s.rating_count, s.rank, s.rating, s.rating_mean
FROM shows s
WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
ORDER BY s.rank DESC;

-- ============================================================================
-- REPORT 3B: Episode and season summary for lowest rating count shows
-- ============================================================================
SELECT s.title,
       COUNT(DISTINCT e.id_code) as total_episodes,
       COUNT(DISTINCT se.season_number) as total_seasons,
       ROUND(AVG(e.rating), 2) as avg_episode_rating
FROM shows s
LEFT JOIN episodes e ON s.code = e.code
LEFT JOIN seasons se ON s.title = se.title
WHERE s.rating_count = (SELECT MIN(rating_count) FROM shows)
GROUP BY s.title
ORDER BY s.title;
