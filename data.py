"""Data access functions"""
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, DateType, DoubleType


"""Schema for the flights CSV files"""
FLIGHTS_SCHEMA = StructType() \
    .add("fl_date", DateType(), False) \
    .add("origin", StringType(), False) \
    .add("origin_city_name", StringType(), False) \
    .add("origin_state_abr", StringType(), False) \
    .add("dest", StringType(), False) \
    .add("dest_city_name", StringType(), False) \
    .add("dest_state_abr", StringType(), False) \
    .add("crs_dep_time", StringType(), False) \
    .add("dep_time", StringType(), True) \
    .add("taxi_out", DoubleType(), True) \
    .add("wheels_off", StringType(), True) \
    .add("wheels_on", StringType(), True) \
    .add("taxi_in", DoubleType(), True) \
    .add("crs_arr_time", StringType(), False) \
    .add("arr_time", StringType(), True) \
    .add("cancelled", DoubleType(), False) \
    .add("cancellation_code", StringType(), True) \
    .add("crs_elapsed_time", DoubleType(), False) \
    .add("actual_elapsed_time", DoubleType(), True) \
    .add("air_time", DoubleType(), True) \
    .add("flights", DoubleType(), False) \
    .add("distance", DoubleType(), False) \
    .add("carrier_delay", DoubleType(), True) \
    .add("weather_delay", DoubleType(), True) \
    .add("nas_delay", DoubleType(), True) \
    .add("security_delay", DoubleType(), True) \
    .add("late_aircraft_delay", DoubleType(), True) \
    .add("empty_column", StringType(), True)


"""List of delay columns in the flight data"""
DELAY_COLUMNS = ["carrier_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]


def load_airports(spark):
    """Load airports from JSON file"""
    return spark.read.json('s3a://atr-udacity-dend/airport-codes.json', prefersDecimal=True)


def preprocess_airports(df):
    """Preprocess airports"""
    return df.where(df.iso_country == 'US') \
        .dropna(subset=['iata_code']) \
        .dropDuplicates(subset=['iata_code'])


def load_flights(spark):
    """Load flights from CSV files"""
    return spark.read.csv('s3a://atr-udacity-dend/flightdata/2019/*.csv', header=True, schema=FLIGHTS_SCHEMA)


def preprocess_flights(df):
    """Preprocess flights"""
    return df \
        .withColumn("cancelled_bool", F.col("cancelled") == 1) \
        .withColumn("total_delay", sum(df[col] for col in DELAY_COLUMNS))


def join(flights, airports):
    """Join flights with airport data using the origin and destination airport code"""
    origin_airports = airports.alias("origin")
    destination_airports = airports.alias("destination")
    return flights \
        .join(origin_airports, F.col("origin") == F.col("origin.iata_code"), 'inner') \
        .join(destination_airports, F.col("dest") == F.col("destination.iata_code"), 'inner')
