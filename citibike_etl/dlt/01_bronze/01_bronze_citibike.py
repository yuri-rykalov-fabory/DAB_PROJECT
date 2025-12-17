import dlt
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType


catalog = spark.conf.get("catalog")

schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True), 
    StructField("start_station_id", StringType(), True),   
    StructField("end_station_name", StringType(), True), 
    StructField("end_station_id", StringType(), True), 
    StructField("start_lat", DecimalType(), True), 
    StructField("start_lng", DecimalType(), True), 
    StructField("end_lat", DecimalType(), True), 
    StructField("end_lng", DecimalType(), True), 
    StructField("member_casual", StringType(), True), 
])

@dlt.table(
    comment="Bronze layer: raw Citi Bike data with ingest metadata"
)
def bronze_jc_citibike():
    df = (
        spark.read
             .schema(schema)
             .csv(f"/Volumes/{catalog}/00_landing/source_citybike_data/JC-202503-citibike-tripdata.csv",
                  header=True)
    )
    return df