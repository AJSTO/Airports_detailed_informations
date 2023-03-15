from pyspark.sql import SparkSession
from jobs import airports_job


def main():
    """
    Main function executed by spark-submit command.
    Creating Spark Session with connection to Bigquery.
    """

    spark = SparkSession.builder \
        .appName('Airports_aggregates_per_day') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar') \
        .getOrCreate()
    airports_job._run_job(spark)


if __name__ == "__main__":
    main()
    
