"""Data queries"""
import pyspark.sql.functions as F
import data

def delayed_routes(df):
    """Find routes with the longest average delays"""
    return (df
        # Select origin/destination columns and the total delay
        .selectExpr("origin as origin_code", 
                    "origin.name as origin_name",
                    "dest as dest_code",
                    "destination.name as dest_name",
                    "total_delay")
        # Group by origin/destination columns
        .groupBy("origin_code", "origin_name", "dest_code", "dest_name")
        # Calculate average delay, and count flights
        .agg(F.avg("total_delay").alias("avg_delay"), 
             F.count(F.lit(1)).alias("total_flights"))
        # Sort by highest average delay
        .sort(F.desc("avg_delay")))


def airport_type_stats(df):
    """Calculate statistics across the airport types of the origin airport"""
    return (df
        # Select airport type of origin airport, delay columns, and cancellation column
        .selectExpr(*["origin.type as airport_type",
                    "cancelled",
                    "total_delay"] +
                    data.DELAY_COLUMNS)
        # Group by airport type
        .groupBy("airport_type")
        # Calculate statistics: Count flights, cancelled flights, average minutes of various delay types
        .agg(*[
            F.count(F.lit(1)).alias("total_flights"),
            F.sum("cancelled").alias("cancelled_flights"),
            F.avg("total_delay").alias("avg_total_delay")
            ] + 
            [F.avg(col).alias(f"avg_{col}") for col in data.DELAY_COLUMNS])
        # Calculate cancellation rate
        .withColumn("cancellation_rate", F.col("cancelled_flights") / F.col("total_flights"))
        # Sort by highest number of flights
        .sort(F.asc("total_flights")))


def elevation_stats(df):
    """Calculate statistics across elevation levels (0-999 feet, 1000-1999 feet etc.)"""
    return (df
        .withColumn('elevation_level', elevation_level()(df['destination.elevation_ft']))
        .groupBy("elevation_level")
        .agg(*[
            F.count(F.lit(1)).alias("total_flights")] + 
            list(map(F.avg, data.DELAY_COLUMNS + ["total_delay"])))
        .sort(F.asc("elevation_level")))


def elevation_level():
    """Return a user defined function to convert elevation in feet into elevation group in thousands of feet"""
    return F.udf(lambda x: str(int(x/1000)*1000))
