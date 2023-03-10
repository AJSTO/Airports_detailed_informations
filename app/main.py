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

# in cloudshell:
# gcloud dataproc jobs submit pyspark --jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar --cluster cluster-adam --region europe-central2 gs://dataproc-staging-europe-central2-305196457839-jazljgsi/notebooks/jupyter/job.py

# in terminal dataproc:
# spark-submit --jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar home/dataproc/flights_agg/main.py