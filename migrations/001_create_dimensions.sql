-- Dimension Tables

-- Users Dimension
CREATE TABLE IF NOT EXISTS playground.dim_users (
    user_sk UInt64,
    user_id String,
    email String,
    first_name String,
    last_name String,
    age Nullable(UInt64),
    gender Nullable(String),
    country String,
    state_province String,
    city String,
    subscription_plan String,
    subscription_start_date Date,
    is_active Bool,
    monthly_spend Nullable(Float64),
    primary_device String,
    household_size Nullable(UInt64),
    created_at DateTime,
    load_timestamp DateTime DEFAULT now(),
    source_file String
) ENGINE = MergeTree()
ORDER BY user_sk
PRIMARY KEY user_sk;

-- Movies Dimension
CREATE TABLE IF NOT EXISTS  playground.dim_movies (
    movie_sk UInt64,
    movie_id String,
    title String,
    content_type String,
    genre_primary String,
    genre_secondary Nullable(String),
    release_year UInt16,
    duration_minutes Float64,
    rating String,
    language String,
    country_of_origin String,
    imdb_rating Nullable(Float64),
    production_budget Nullable(Float64),
    box_office_revenue Nullable(Float64),
    number_of_seasons Nullable(UInt16),
    number_of_episodes Nullable(UInt16),
    is_netflix_original Bool,
    added_to_platform Date,
    content_warning Bool,
    load_timestamp DateTime DEFAULT now(),
    source_file String
) ENGINE = MergeTree()
ORDER BY movie_sk
PRIMARY KEY movie_sk;