from pyspark.sql.functions import col, format_string, date_format, format_number
import pyspark


def _extract_airports_data(spark, config):
    """ Load airports data to dataframe from PosgreSQL """
    # URL connection to postgres database
    url = f'jdbc:postgresql://' \
          f'{config.get("postgres_host")}:' \
          f'{config.get("postgres_port")}/' \
          f'{config.get("postgres_db")}'

    return (
        spark.read \
            .format('jdbc') \
            .option('url', url) \
            .option('driver', 'org.postgresql.Driver') \
            .option('dtable', 'tickets') \
            .option('user', config.get('postgres_username')) \
            .option('query', 'SELECT * FROM airports_data') \
            .option('password', config.get('postgres_password')) \
            .load()
    )


def _extract_ticket_flights_data(spark, config):
    """ Load tickets flights info data to dataframe from PosgreSQL """
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
        .option('query', 'SELECT * FROM ticket_flights') \
        .option('password', config.get('postgres_password')) \
        .load()


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


def _transform_data(airports_data_df, ticket_flights_df, flights_df):
    """ Create aggregate of number of served passengers per airport per day """
    # Selecting interesting columns from all neccessary tables
    airport_info = airports_data_df.select('airport_code', 'airport_name')
    ticket_info = ticket_flights_df.select('flight_id', 'ticket_no')
    flights_info = flights_df.select('scheduled_departure', 'flight_id', 'departure_airport')
    # Summing number of tickets per flight
    tickets_per_flight = ticket_info.groupBy('flight_id') \
        .count()
    # Joining tables
    passengers_sum = flights_info.join(
        airport_info, airport_info.airport_code == flights_info.departure_airport, "leftouter"
    )
    passengers_sum = passengers_sum.join(
        tickets_per_flight, passengers_sum.flight_id == tickets_per_flight.flight_id, "leftouter"
    )
    # Dropping columns
    passengers_sum = passengers_sum.drop('flight_id', 'airport_code', 'departure_airport')
    # Date to format DD-MM-YYYY
    passengers_sum = passengers_sum.withColumn(
        "date", date_format(col('scheduled_departure'), "dd-MM-yyyy")
    )
    # Aggregate passengers per day per airport
    passengers_sum = passengers_sum.groupBy('airport_name', 'date') \
        .sum()
    # Rename aggregates passengers column
    passengers_sum = passengers_sum.withColumnRenamed(
        "sum(count)", "num_of_passengers"
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(passengers_sum['airport_name'], '"')
    passengers_sum = passengers_sum.withColumn('airport', split_col.getItem(3))
    # Change order of columns
    passengers_aggregate = passengers_sum.select('date', 'airport', 'num_of_passengers')

    return passengers_aggregate


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_passengers_per_airport_day'))


def run_job(spark, config):
    """ Run aggregate of number of served passengers per airport per day job """
    _load_data(config,
               _transform_data(
                   _extract_airports_data(spark, config),
                   _extract_ticket_flights_data(spark, config),
                   _extract_flights_data(spark, config)
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job served_passengers