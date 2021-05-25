"""Main script"""
from pyspark.sql import SparkSession
import config, pipeline


def create_spark_session():
    """Get or create the SparkSession object, including the hadoop-aws package."""
    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("FlightDataAnalyzer") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def main():
    """Main function: Create a Spark session runs the pipeline"""
    config.configure()
    spark = create_spark_session()
    pipeline.run(spark)


if __name__ == "__main__":
    main()
