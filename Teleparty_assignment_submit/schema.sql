

-- ============================================================================
-- Shows Table
-- ============================================================================
-- Contains metadata about TV shows, including their IMDB ratings and rankings
-- Source: all-series-ep-average.csv
CREATE TABLE IF NOT EXISTS shows (
    code VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    rating DECIMAL(3, 1) NOT NULL,
    rating_count INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    rating_mean DECIMAL(5, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Seasons Table
-- ============================================================================
-- Contains information about individual seasons of shows
-- Source: top-seasons-full.csv
CREATE TABLE IF NOT EXISTS seasons (
    key VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL REFERENCES shows(title),
    season_number INTEGER NOT NULL,
    rating_mean DECIMAL(5, 4) NOT NULL,
    number_of_episodes INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Episodes Table
-- ============================================================================
-- Contains detailed information about individual episodes
-- Source: all-episode-ratings.csv
CREATE TABLE IF NOT EXISTS episodes (
    id_code INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,
    episode INTEGER NOT NULL,
    rating DECIMAL(3, 1) NOT NULL,
    code VARCHAR(20) NOT NULL REFERENCES shows(code),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

