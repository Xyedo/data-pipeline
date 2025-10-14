from prefect import flow, task
from prefect.cache_policies import NO_CACHE
import polars as pl
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
import os
from datetime import datetime
import dotenv


class NetflixData:
    def __init__(
        self,
        movies: pl.DataFrame,
        recommendation_log: pl.DataFrame,
        reviews: pl.DataFrame,
        search_log: pl.DataFrame,
        users: pl.DataFrame,
        watch_history: pl.DataFrame,
    ):
        self.movies = movies
        self.recommendation_log = recommendation_log
        self.reviews = reviews
        self.search_log = search_log
        self.users = users
        self.watch_history = watch_history


@task
def get_clickhouse_client():
    """Create ClickHouse client connection"""
    client = get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "local"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "local"),
        database=os.getenv("CLICKHOUSE_DATABASE", "playground"),
    )
    return client


@task
def read_data():
    movies = pl.read_csv("dataset/movies.csv")
    recommendation_log = pl.read_csv("dataset/recommendation_logs.csv")
    reviews = pl.read_csv("dataset/reviews.csv")
    search_log = pl.read_csv("dataset/search_logs.csv")
    users = pl.read_csv("dataset/users.csv")
    watch_history = pl.read_csv("dataset/watch_history.csv")

    return NetflixData(
        movies, recommendation_log, reviews, search_log, users, watch_history
    )


@task
def create_dimension_user(data: NetflixData):
    users = data.users.with_columns(
        pl.col("user_id").alias("user_sk").str.strip_prefix("user_").cast(pl.Int64),
        pl.col("subscription_start_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f"),
        pl.col("age").map_elements(lambda x: None if x < 0 else x).cast(pl.UInt64),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("users.csv").alias("source_file"),
    )
    return users


@task
def create_dimension_movie(data: NetflixData):
    movies = data.movies.with_columns(
        pl.col("movie_id").alias("movie_sk").str.strip_prefix("movie_").cast(pl.Int64),
        pl.col("added_to_platform").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("movies.csv").alias("source_file"),
    )
    return movies


@task
def create_fact_watch(data: NetflixData):
    watch_history = data.watch_history.with_columns(
        pl.col("session_id").alias("watch_sk").str.strip_prefix("session_").cast(pl.Int64), 
        pl.col("session_id").alias("watch_id"),
        pl.col("user_id").str.strip_prefix("user_").cast(pl.Int64).alias("user_sk"),
        pl.col("movie_id").str.strip_prefix("movie_").cast(pl.Int64).alias("movie_sk"),
        pl.col("watch_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("watch_history.csv").alias("source_file"),
    )
    
    return watch_history.drop("session_id")


@task
def create_fact_search(data: NetflixData):
    search_log =  data.search_log.with_columns(
        pl.col("search_id").alias("search_sk").str.strip_prefix("search_").cast(pl.Int64),
        pl.col("user_id").str.strip_prefix("user_").cast(pl.Int64).alias("user_sk"),
        pl.col("search_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("search_logs.csv").alias("source_file"),
    )

    return search_log


@task
def create_fact_recommendation(data: NetflixData):
    recommendation_log = data.recommendation_log.with_columns(
        pl.col("recommendation_id").alias("recommendation_sk").str.strip_prefix("rec_").cast(pl.Int64),
        pl.col("user_id").str.strip_prefix("user_").cast(pl.Int64).alias("user_sk"),
        pl.col("movie_id").str.strip_prefix("movie_").cast(pl.Int64).alias("movie_sk"),
        pl.col("recommendation_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("recommendation_logs.csv").alias("source_file"),
    )

    return recommendation_log


@task
def create_fact_review(data: NetflixData):
    reviews = data.reviews.with_columns(pl.col("review_id").alias("review_sk"))
    reviews = reviews.with_columns(
        pl.col("review_sk").str.strip_prefix("review_").cast(pl.Int64),
        pl.col("user_id").str.strip_prefix("user_").cast(pl.Int64).alias("user_sk"),
        pl.col("movie_id").str.strip_prefix("movie_").cast(pl.Int64).alias("movie_sk"),
        pl.col("review_date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.lit(datetime.now()).alias("load_timestamp"),
        pl.lit("reviews.csv").alias("source_file"),
    )
    return reviews


@task(cache_policy=NO_CACHE)
def insert_to_clickhouse(client: Client, df: pl.DataFrame, table_name: str):
    data = df.to_dicts()
    # 
    if data:
        key_order = list(data[0].keys())
        datas = [[d[k] for k in key_order] for d in data]
        client.insert(
              table=table_name,
              data=datas,
              column_names=key_order,
          )
        print(f"Inserted {len(data)} rows into {table_name}")
    else:
        print(f"No data to insert into {table_name}")

    return len(data)


@flow(name="api_to_star_schema", log_prints=True)
def data_pipeline():
    dotenv.load_dotenv()

    data = read_data()
    pl.Config.set_tbl_cols(-1)

    dim_user = create_dimension_user(data)
    dim_movie = create_dimension_movie(data)

    fact_watch = create_fact_watch(data)
    fact_search = create_fact_search(data)
    fact_recommendation = create_fact_recommendation(data)
    fact_review = create_fact_review(data)

    client = get_clickhouse_client()

    insert_to_clickhouse(client, dim_user, "dim_users")
    insert_to_clickhouse(client, dim_movie, "dim_movies")

    insert_to_clickhouse(client, fact_watch, "fact_watch_history")
    insert_to_clickhouse(client, fact_search, "fact_search_logs")
    insert_to_clickhouse(client, fact_recommendation, "fact_recommendations")
    insert_to_clickhouse(client, fact_review, "fact_reviews")

    return {
        "dim_users": len(dim_user),
        "dim_movies": len(dim_movie),
        "fact_watch_history": len(fact_watch),
        "fact_search_logs": len(fact_search),
        "fact_recommendations": len(fact_recommendation),
        "fact_reviews": len(fact_review),
    }


if __name__ == "__main__":
    data_pipeline()
