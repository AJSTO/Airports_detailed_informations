from pyspark.sql.functions import col, format_string, date_format, format_number
import pyspark


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


def _extract_aircrafts_data(spark, config):
    """ Load aircrafts data to dataframe from PosgreSQL """
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
        .option('query', 'SELECT * FROM aircrafts_data') \
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


def _extract_seats_data(spark, config):
    """ Load seats data to dataframe from PosgreSQL """
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
        .option('query', 'SELECT * FROM seats') \
        .option('password', config.get('postgres_password')) \
        .load()


def _transform_data(
        ticket_flights_df, aircrafts_data_df, flights_df, airports_data_df, seats_df
):
    """ Create daily average flight occupancy per airport """
    # Count tickets per flight
    tickets_in_flight = ticket_flights_df.groupBy('flight_id') \
        .count()
    # Prepare column before merge
    tickets_in_flight = tickets_in_flight.withColumnRenamed(
        "count", "num_of_passengers"
    )
    # Seat numbers per aircraft
    seats = seats_df.withColumnRenamed(
        "aircraft_code", "aircraft_c"
    )
    seats_per_aircraft = seats.groupBy('aircraft_c') \
        .count()
    seats_per_aircraft = seats_per_aircraft.join(
        aircrafts_data_df,
        seats_per_aircraft.aircraft_c == aircrafts_data_df.aircraft_code,
        "leftouter"
    )
    # Prepare column before merge
    seats_per_aircraft = seats_per_aircraft.withColumnRenamed(
        "count", "num_of_seats"
    )
    # Joining tables
    avg_occupancy = flights_df.join(
        tickets_in_flight,
        flights_df.flight_id == tickets_in_flight.flight_id,
        "leftouter"
    )
    avg_occupancy = avg_occupancy.join(
        seats_per_aircraft,
        avg_occupancy.aircraft_code == seats_per_aircraft.aircraft_code,
        "leftouter"
    )
    avg_occupancy = avg_occupancy.join(
        airports_data_df,
        avg_occupancy.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(avg_occupancy['airport_name'], '"')
    avg_occupancy = avg_occupancy.withColumn('airport', split_col.getItem(3))
    # Selecting needed columns
    avg_occupancy = avg_occupancy.select(
        'airport', 'scheduled_departure', 'num_of_seats', 'num_of_passengers'
    )
    # Count average occupancy per flight per day in %
    avg_occupancy = avg_occupancy.withColumn('occupancy_%',
                                             (avg_occupancy['num_of_passengers'] / avg_occupancy['num_of_seats']) * 100
                                             )
    # Date to format DD-MM-YYYY
    avg_occupancy = avg_occupancy.withColumn(
        "date", date_format(col('scheduled_departure'), "dd-MM-yyyy")
    )
    # Counting average occupancy per day per airport
    avg_occupancy = avg_occupancy.groupBy('airport', 'date') \
        .mean()
    avg_occupancy = avg_occupancy.withColumn(
        "avg_occupancy_percent",
        format_number("avg(occupancy_%)", 2)
    )
    # Select columns to final aggregate
    avg_occupancy = avg_occupancy.select('date', 'airport', 'avg_occupancy_percent')

    return avg_occupancy


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_flight_occupancy_per_airport_day'))


def run_job(spark, config):
    """ Run aggregate daily average flight occupancy per airport """
    _load_data(config,
               _transform_data(
                   _extract_ticket_flights_data(spark, config),
                   _extract_aircrafts_data(spark, config),
                   _extract_flights_data(spark, config),
                   _extract_airports_data(spark, config),
                   _extract_seats_data(spark, config),
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job flight_occupancy