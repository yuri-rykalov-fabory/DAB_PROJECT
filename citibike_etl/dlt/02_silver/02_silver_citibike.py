import dlt
from pyspark.sql.functions import col, unix_timestamp, to_date


@dlt.table(
    comment="Silver layer: cleaned and enriched Citi Bike data"
)
def silver_jc_citibike():
    # Read the Bronze DLT table
    df_bronze = dlt.read("bronze_jc_citibike")

    # Compute duration, extract date, then select the final columns
    df_silver = (
        df_bronze
        .withColumn(
            "trip_duration_mins",
            (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60
        )
        .withColumn(
            "trip_start_date",
            to_date(col("started_at"))
        )
        .select(
            "ride_id",
            "trip_start_date",
            "started_at",
            "ended_at",
            "start_station_name",
            "end_station_name",
            "trip_duration_mins"
        )
    )

    return df_silver