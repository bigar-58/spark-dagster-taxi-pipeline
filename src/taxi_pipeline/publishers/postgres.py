from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame

from taxi_pipeline.config import PostgresSettings

DAILY_ZONE_COLUMNS: tuple[str, ...] = (
    "pickup_date",
    "pickup_location_id",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "trip_count",
    "gross_revenue_amount",
    "fare_revenue_amount",
    "total_tip_amount",
    "avg_trip_distance",
    "avg_trip_duration_minutes",
    "avg_fare_per_mile",
    "card_payment_share",
    "cash_payment_share",
    "source_batch_count",
    "latest_source_ingested_at"
)

HOURLY_DEMAND_COLUMNS: tuple[str, ...] = (
    "pickup_date",
    "pickup_hour",
    "trip_count",
    "distinct_pickup_zones",
    "gross_revenue_amount",
    "fare_revenue_amount",
    "total_tip_amount",
    "avg_trip_distance",
    "avg_trip_duration_minutes",
    "card_payment_share",
    "cash_payment_share",
    "source_batch_count",
    "latest_source_ingested_at"
)


@dataclass(frozen=True)
class PublishResult:
    run_id: str
    date_from: date
    date_to: date
    daily_zone_row_count: int
    hourly_demand_row_count: int
    

def require_columns(df: DataFrame, required_columns: tuple[str, ...], dataset_name: str) -> None:
    missing_columns = sorted(set(required_columns) - set(df.columns))
    
    if missing_columns:
        raise ValueError(f"{dataset_name} is missing columns: {missing_columns}")
    

def copy_dataframe(
    connection: psycopg.Connection,
    df: DataFrame,
    *,
    schema_name: str,
    table_name: str,
    source_columns: tuple[str, ...],
    publish_run_id: str
) -> int:
    target_columns = (*source_columns, "publish_run_id")
    
    copy_stmt = sql.SQL(
        "COPY {} ({}) FROM STDIN"
    ).format(
        sql.Identifier(schema_name, table_name),
        sql.SQL(",").join(sql.Identifier(column) for column in target_columns)
    )
    
    rows_written = 0
    
    with connection.cursor() as cursor:
        with cursor.copy(copy_stmt) as copy:
            for row in df.select(*source_columns).toLocalIterator():
                copy.write_row((*tuple(row), publish_run_id))
                rows_written += 1
    
    return rows_written

def record_publish_started(
    settings: PostgresSettings,
    *,
    run_id: str,
    started_at: datetime
) -> None:
    with psycopg.connect(**settings.connection_kwargs()) as connection:
        connection.execute(
            """
            INSERT INTO audit.gold_publish_runs (
                run_id,
                status,
                started_at
            )
            VALUES (%s, 'running', %s)
            """,
            (run_id, started_at)
        )
        
def record_publish_failed(
    settings: PostgresSettings,
    *,
    run_id: str,
    error: Exception
) -> None:
    with psycopg.connect(**settings.connection_kwargs()) as connection:
        connection.execute(
            """
            UPDATE audit.gold_publish_runs
            SET
                status = 'failed',
                completed_at = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                datetime.now(timezone.utc),
                str(error)[:2000],
                run_id,
            )
        )
    
def publish_gold_marts(
    daily_zone_df: DataFrame,
    hourly_demand_df: DataFrame,
    *, 
    settings: PostgresSettings,
    run_id: str
) -> PublishResult:
    """Publish both daily and hourly metrics gold marts"""
    
    #Check that we have all required columns in the data frames
    require_columns(daily_zone_df, DAILY_ZONE_COLUMNS, "daily zone metrics")
    require_columns(hourly_demand_df, HOURLY_DEMAND_COLUMNS, "hourly demand metrics")
    
    
    started_at = datetime.now(timezone.utc)
    
    record_publish_started(settings, run_id=run_id, started_at=started_at)
    
    # TO-DO update to avoid full table load for data marts.
    try:
        with psycopg.connect(**settings.connection_kwargs()) as connection:
            with connection.transaction():
                connection.execute(
                    """
                    TRUNCATE TABLE
                        staging.daily_zone_metrics,
                        staging.hourly_demand_metrics
                    """
                )
                
                daily_count = copy_dataframe(
                    connection,
                    daily_zone_df,
                    schema_name="staging",
                    table_name="daily_zone_metrics",
                    source_columns=DAILY_ZONE_COLUMNS,
                    publish_run_id=run_id
                )
                
                hourly_count = copy_dataframe(
                    connection,
                    hourly_demand_df,
                    schema_name="staging",
                    table_name="hourly_demand_metrics",
                    source_columns=HOURLY_DEMAND_COLUMNS,
                    publish_run_id=run_id
                )
                
                if daily_count == 0 or hourly_count == 0:
                    raise ValueError("Gold marts must both contain at least one row")
                
                
                #Check if any duplicates exist in the copied dataset
                duplicate_daily = connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM staging.daily_zone_metrics
                        GROUP BY pickup_date, pickup_location_id
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()
                
                duplicate_hourly = connection.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM staging.hourly_demand_metrics
                        GROUP BY pickup_date, pickup_hour
                        HAVING COUNT(*) > 1
                    )
                    """
                ).fetchone()
                
                
                if duplicate_daily and duplicate_daily[0]:
                    raise ValueError("Staged daily-zone metrics contain duplicate keys")
                
                if duplicate_hourly and duplicate_hourly[0]:
                    raise ValueError("Staged hourly metrics contain duplicate keys")
                
                date_window = connection.execute(
                    """
                    SELECT
                        MIN(pickup_date),
                        MAX(pickup_date)
                    FROM (
                        SELECT pickup_date
                        FROM staging.daily_zone_metrics

                        UNION ALL

                        SELECT pickup_date
                        FROM staging.hourly_demand_metrics
                    ) AS publish_dates
                    """
                ).fetchone()
                
                if not date_window or not date_window[0] or not date_window[1]:
                    raise ValueError("Unable to find a complete date range for staged data")
                
                
                
                date_from, date_to = date_window

                # Delete existing data in gold marts for staged date range/window
                connection.execute(
                    """
                    DELETE FROM mart.daily_zone_metrics
                    WHERE pickup_date BETWEEN %s AND %s
                    """,
                    (date_from, date_to)
                )

                connection.execute(
                    """
                    DELETE FROM mart.hourly_demand_metrics
                    WHERE pickup_date BETWEEN %s AND %s
                    """,
                    (date_from, date_to)
                )

                # Re-insert/update the staged date-range's data in the gold-mart.
                connection.execute(
                    """
                    INSERT INTO mart.daily_zone_metrics
                    SELECT *
                    FROM staging.daily_zone_metrics
                    """
                )

                connection.execute(
                    """
                    INSERT INTO mart.hourly_demand_metrics
                    SELECT *
                    FROM staging.hourly_demand_metrics
                    """
                )
                
                
                completed_at = datetime.now(timezone.utc)

                connection.execute(
                    """
                    UPDATE audit.gold_publish_runs
                    SET
                        status = 'succeeded',
                        completed_at = %s,
                        date_from = %s,
                        date_to = %s,
                        daily_zone_row_count = %s,
                        hourly_demand_row_count = %s
                    WHERE run_id = %s
                    """,
                    (
                        completed_at,
                        date_from,
                        date_to,
                        daily_count,
                        hourly_count,
                        run_id,
                    ),
                )
        return PublishResult(
            run_id=run_id,
            date_from=date_from,
            date_to=date_to,
            daily_zone_row_count=daily_count,
            hourly_demand_row_count=hourly_count
        )
    except Exception as error:
        record_publish_failed(settings, run_id=run_id, error=error)
        raise
        