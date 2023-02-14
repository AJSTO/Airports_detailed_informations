import json
from jobs import served_passengers, depature_delay, bookings, arrival_airports, flight_occupancy, occupancy_per_fare
from pyspark.sql import SparkSession


def main():
    """ Main function excecuted by spark-submit command"""
    with open("config.json", "r") as config_file:
        _config = json.load(config_file)

    spark = SparkSession.builder \
        .appName('xd') \
        .config("spark.executor.extraClassPath", _config.get('postgres_driver_path')) \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .master('local[2]') \
        .config("parentProject", _config.get("biquery_project")) \
        .getOrCreate()
    # TO CHANGE THIS SHIT
    # Before next computation cache need to be cleared
    spark.catalog.clearCache()
    served_passengers.run_job(spark, _config)


if __name__ == "__main__":
    main()

# TO RUN PYSPARK JOB via console:
#  pytest && spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job served_passengers

'''
to create dataset in bigquery:
date string nullable
unit_id required
sum_of_infections nullable
'''
