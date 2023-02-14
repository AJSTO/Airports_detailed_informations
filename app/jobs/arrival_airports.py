from pyspark.sql.functions import col, format_string, date_format, format_number
import pyspark


def _extract_flights_data(spark, config):
    """ Load flights data to dataframe from PosgreSQL """
    # URL connection to postgres database
    url = f'jdbc:postgresql://' \
          f'{config.get("postgres_host")}:' \
          f'{config.get("postgres_port")}/' \
          f'{config.get("postgres_db")}'

    return spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('driver', 'org.postgresql.Driver') \
        .option('dtable', 'tickets') \
        .option('user', config.get('postgres_username')) \
        .option('query', 'SELECT * FROM flights') \
        .option('password', config.get('postgres_password')) \
        .load()


def _extract_airports_data(spark, config):
    """ Load airports data to dataframe from PosgreSQL """
    # URL connection to postgres database
    url = f'jdbc:postgresql://' \
          f'{config.get("postgres_host")}:' \
          f'{config.get("postgres_port")}/' \
          f'{config.get("postgres_db")}'

    return spark.read \
        .format('jdbc') \
        .option('url', url) \
        .option('driver', 'org.postgresql.Driver') \
        .option('dtable', 'tickets') \
        .option('user', config.get('postgres_username')) \
        .option('query', 'SELECT * FROM airports_data') \
        .option('password', config.get('postgres_password')) \
        .load()


def _transform_data(
        flights_df, airports_data_df,
):
    """ Create daily sum of flights to each arrival airport """
    # Joining tables
    flights_to_arrival = flights_df.join(
        airports_data_df, flights_df.arrival_airport == airports_data_df.airport_code,
        "leftouter"
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(flights_to_arrival['airport_name'], '"')
    flights_to_arrival = flights_to_arrival.withColumn(
        'arrival_airport', split_col.getItem(3)
    )
    # Date to format DD-MM-YYYY
    flights_to_arrival = flights_to_arrival.withColumn(
        "date", date_format(col('scheduled_arrival'), "dd-MM-yyyy")
    )
    flights_to_arrival = flights_to_arrival.groupBy('arrival_airport', 'date') \
        .count()
    # Rename aggregates passengers column
    flights_to_arrival = flights_to_arrival.withColumnRenamed(
        "count", "num_of_flights"
    )
    flights_to_arrival = flights_to_arrival.select(
        'date', 'arrival_airport', 'num_of_flights'
    )

    return flights_to_arrival


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_flights_to_airport_per_day'))


def run_job(spark, config):
    """ Run aggregate daily sum of flights to each arrival airport  """
    _load_data(config,
               _transform_data(
                   _extract_flights_data(spark, config),
                   _extract_airports_data(spark, config),
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job arrival_airports\
