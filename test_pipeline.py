#!/usr/bin/env python3
"""
Simple test script to verify the pipeline and data quality
"""
import clickhouse_connect
import os
from datetime import datetime
import dotenv

def get_clickhouse_client():
    """Create ClickHouse client connection"""
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8123)),
        username=os.getenv('CLICKHOUSE_USER', 'local'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'local'),
        database=os.getenv('CLICKHOUSE_DATABASE', 'playground')
    )

def test_table_counts(client):
    """Test that all tables have data"""
    tables = [
        'dim_users',
        'dim_movies', 
        'fact_watch_history',
        'fact_search_logs',
        'fact_recommendations',
        'fact_reviews'
    ]
    
    print("=== Table Row Counts ===")
    for table in tables:
        try:
            result = client.query(f"SELECT COUNT(*) FROM {table}")
            count = result.result_rows[0][0]
            print(f"{table}: {count:,} rows")
        except Exception as e:
            print(f"{table}: ERROR - {e}")
    print()

def test_data_quality(client):
    """Run basic data quality checks"""
    print("=== Data Quality Checks ===")
    
    # Check for null user_sk in facts
    checks = [
        ("Null user_sk in watch history", "SELECT COUNT(*) FROM fact_watch_history WHERE user_sk IS NULL"),
        ("Null movie_sk in watch history", "SELECT COUNT(*) FROM fact_watch_history WHERE movie_sk IS NULL"),
        ("Invalid dates in watch history", "SELECT COUNT(*) FROM fact_watch_history WHERE watch_date > today()"),
        ("Duplicate users", "SELECT COUNT(*) - COUNT(DISTINCT user_sk) FROM dim_users"),
        ("Duplicate movies", "SELECT COUNT(*) - COUNT(DISTINCT movie_sk) FROM dim_movies"),
    ]
    
    for check_name, query in checks:
        try:
            result = client.query(query)
            count = result.result_rows[0][0]
            status = "✓ PASS" if count == 0 else f"✗ FAIL ({count})"
            print(f"{check_name}: {status}")
        except Exception as e:
            print(f"{check_name}: ERROR - {e}")
    print()

def test_sample_queries(client):
    """Test sample analytical queries"""
    print("=== Sample Analytical Queries ===")
    
    queries = [
        ("Top 5 movies by watch time", """
            SELECT 
                m.title,
                SUM(w.watch_duration_minutes) as total_minutes
            FROM fact_watch_history w
            JOIN dim_movies m ON w.movie_sk = m.movie_sk
            GROUP BY m.title
            ORDER BY total_minutes DESC
            LIMIT 5
        """),
        ("Users by subscription plan", """
            SELECT 
                subscription_plan,
                COUNT(*) as user_count
            FROM dim_users
            GROUP BY subscription_plan
            ORDER BY user_count DESC
        """),
        ("Daily watch activity (last 7 days)", """
            SELECT 
                watch_date,
                COUNT(*) as sessions,
                SUM(watch_duration_minutes) as total_minutes
            FROM fact_watch_history
            WHERE watch_date >= today() - INTERVAL 7 DAY
            GROUP BY watch_date
            ORDER BY watch_date DESC
        """)
    ]
    
    for query_name, query in queries:
        try:
            print(f"\n{query_name}:")
            result = client.query(query)
            for row in result.result_rows[:5]:  # Show first 5 rows
                print(f"  {row}")
        except Exception as e:
            print(f"  ERROR: {e}")
    print()

def main():
    dotenv.load_dotenv()
    """Run all tests"""
    print(f"Testing ClickHouse Data Pipeline - {datetime.now()}")
    print("=" * 50)
    
    try:
        client = get_clickhouse_client()
        print(f"✓ Connected to ClickHouse at {os.getenv('CLICKHOUSE_HOST', 'localhost')}")
        
        test_table_counts(client)
        test_data_quality(client)
        test_sample_queries(client)
        
        print("✓ All tests completed!")
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())