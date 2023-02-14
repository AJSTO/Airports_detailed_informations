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


def _transform_data(flights_df, airports_data_df):
    """ Create aggregate of daily average of depatures delay per airport """
    # Dropping NaT values of depatures and arrivals
    flight_info = flights_df.dropna(
        how='any',
        thresh=None,
        subset=('scheduled_departure', 'actual_departure'),
    )
    # Joining tables
    delay_per_airport = flight_info.join(
        airports_data_df,
        flight_info.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(delay_per_airport['airport_name'], '"')
    delay_per_airport = delay_per_airport.withColumn('airport', split_col.getItem(3))
    # Selecting only necessary info
    delay_per_airport = delay_per_airport.select(
        'scheduled_departure', 'actual_departure', 'airport',
    )
    # Date to format DD-MM-YYYY
    delay_per_airport = delay_per_airport.withColumn(
        "date", date_format(col('scheduled_departure'), "dd-MM-yyyy")
    )
    # Count delay per flight
    delay_per_airport = delay_per_airport.withColumn('delay',
                                                     delay_per_airport['actual_departure'] - delay_per_airport[
                                                         'scheduled_departure']
                                                     )
    delay_per_airport = delay_per_airport.select('airport', 'date', 'delay')
    delay_per_airport = delay_per_airport.withColumn(
        "delay_seconds",
        delay_per_airport['delay'].cast('integer')
    )
    # Count mean delay per day per airport per second
    delay_per_airport = delay_per_airport.groupBy('airport', 'date') \
        .mean()
    # Convert to minutes
    delay_per_airport = delay_per_airport.withColumn(
        'avg_delay_min',
        delay_per_airport['avg(delay_seconds)'] / 60
    )
    '''
    delay_per_airport = delay_per_airport.withColumn(
        "avg_delay_in_minutes",
        format_number("avg_delay_min", 2)
    )
    '''
    # Instead of function used above, because format_number gives string
    delay_per_airport = delay_per_airport.withColumnRenamed(
        'avg_delay_min', 'avg_delay_in_minutes'
    )
    delay_per_airport = delay_per_airport.select(
        'date', 'airport', 'avg_delay_in_minutes'
    )

    return delay_per_airport


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_depature_per_airport_day'))


def run_job(spark, config):
    """ Run aggregate of daily average of depatures delay per airport """
    _load_data(config,
               _transform_data(
                   _extract_flights_data(spark, config),
                   _extract_airports_data(spark, config),
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job depature_delay
