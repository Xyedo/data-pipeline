-- Fact Tables

-- Watch History Fact
CREATE TABLE IF NOT EXISTS  playground.fact_watch_history (
    watch_sk UInt64,
    watch_id String,
    user_sk UInt64,
    user_id String,
    movie_sk UInt64,
    movie_id String,
    watch_date Date,
    device_type String,
    watch_duration_minutes Nullable(Float64),
    progress_percentage Nullable(Float64),
    action String,
    quality String,
    location_country String,
    is_download Bool,
    user_rating Nullable(UInt8),
    load_timestamp DateTime DEFAULT now(),
    source_file String DEFAULT 'watch_history.csv'
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(watch_date, watch_id)
ORDER BY (watch_date, user_sk, movie_sk)
PRIMARY KEY (watch_date, user_sk, movie_sk);

-- watch_date, user_sk, movies_sk

-- Search Logs Fact
CREATE TABLE IF NOT EXISTS  playground.fact_search_logs (
    search_sk UInt64,
    search_id String,
    user_sk UInt64,
    user_id String,
    search_query String,
    search_date Date,
    results_returned Nullable(UInt32),
    clicked_result_position Nullable(UInt16),
    device_type String,
    search_duration_seconds Nullable(Float64),
    had_typo Bool,
    used_filters Bool,
    location_country String,
    load_timestamp DateTime DEFAULT now(),
    source_file String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(search_date)
ORDER BY (search_date, user_sk)
PRIMARY KEY (search_date, user_sk);

-- Recommendation Logs Fact
CREATE TABLE IF NOT EXISTS  playground.fact_recommendations (
    recommendation_sk UInt64,
    recommendation_id String,
    user_sk UInt64,
    user_id String,
    movie_sk UInt64,
    movie_id String,
    recommendation_date Date,
    recommendation_type String,
    recommendation_score Nullable(Float64),
    was_clicked Bool,
    position_in_list UInt16,
    device_type String,
    time_of_day String,
    algorithm_version Nullable(String),
    load_timestamp DateTime DEFAULT now(),
    source_file String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(recommendation_date)
ORDER BY (recommendation_date, user_sk, movie_sk)
PRIMARY KEY (recommendation_date, user_sk, movie_sk);

-- Reviews Fact
CREATE TABLE IF NOT EXISTS  playground.fact_reviews (
    review_sk UInt64,
    review_id String,
    user_sk UInt64,
    user_id String,
    movie_sk UInt64,
    movie_id String,
    rating UInt8,
    review_date Date,
    device_type String,
    is_verified_watch Bool,
    helpful_votes Nullable(UInt32),
    total_votes Nullable(UInt32),
    review_text Nullable(String),
    sentiment String,
    sentiment_score Nullable(Float64),
    load_timestamp DateTime DEFAULT now(),
    source_file String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(review_date)
ORDER BY (review_date, user_sk, movie_sk)
PRIMARY KEY (review_date, user_sk, movie_sk);