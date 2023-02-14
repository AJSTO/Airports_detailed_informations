from pyspark.sql.functions import col, format_string, date_format, format_number
import pyspark


def _extract_bookings_data(spark, config):
    """ Load bookings data to dataframe from PosgreSQL """
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
        .option('query', 'SELECT * FROM bookings') \
        .option('password', config.get('postgres_password')) \
        .load()


def _extract_tickets_data(spark, config):
    """ Load tickets info data to dataframe from PosgreSQL """
    # URL connection to postgres database
    url = f'jdbc:postgresql://' \
          f'{config.get("postgres_host")}:' \
          f'{config.get("postgres_port")}/' \
          f'{config.get("postgres_db")}'

    return spark.read\
        .format('jdbc')\
        .option('url',url)\
        .option('driver', 'org.postgresql.Driver')\
        .option('dtable', 'tickets')\
        .option('user', config.get('postgres_username')) \
        .option('query', 'SELECT * FROM tickets')\
        .option('password', config.get('postgres_password')) \
        .load()


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
        bookings_df, tickets_df, ticket_flights_df, flights_df, airports_data_df
):
    """ Create daily average booking per airport """
    # Joining tables
    book_per_airport = bookings_df.join(
        tickets_df, bookings_df.book_ref == tickets_df.book_ref,
        "leftouter"
    )
    book_per_airport = book_per_airport.drop(
        'passenger_id', 'passenger_name', 'contact_data'
    )
    book_per_airport = book_per_airport.join(
        ticket_flights_df, book_per_airport.ticket_no == ticket_flights_df.ticket_no,
        "leftouter"
    )
    book_per_airport = book_per_airport.drop(
        'fare_conditions', 'amount',
    )
    book_per_airport = book_per_airport.join(
        flights_df, book_per_airport.flight_id == flights_df.flight_id,
        "leftouter"
    )
    book_per_airport = book_per_airport.drop(
        'actual_arrival', 'actual_departure', 'status', 'aircraft_code',
        'scheduled_departure', 'scheduled_arrival',
    )
    book_per_airport = book_per_airport.join(
        airports_data_df,
        book_per_airport.departure_airport == airports_data_df.airport_code,
        "leftouter",
    )
    # Selecting only needed columns
    book_per_airport = book_per_airport.select(
        'total_amount', 'book_date', 'airport_name'
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(book_per_airport['airport_name'], '"')
    book_per_airport = book_per_airport.withColumn('airport', split_col.getItem(3))
    # Date to format DD-MM-YYYY
    book_per_airport = book_per_airport.withColumn(
        "date", date_format(col('book_date'), "dd-MM-yyyy")
    )
    # Aggregate bookings per day per airport
    book_per_airport = book_per_airport.groupBy('airport', 'date') \
        .count()
    # Rename aggregates count of bookings column
    book_per_airport = book_per_airport.withColumnRenamed(
        "count", "num_of_bookings"
    )
    # Set in order
    book_per_airport = book_per_airport.select('date', 'airport', 'num_of_bookings')

    return book_per_airport


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_booking_per_airport_day'))


def run_job(spark, config):
    """ Run aggregate of daily booking per airport """
    _load_data(config,
               _transform_data(
                   _extract_bookings_data(spark, config),
                   _extract_tickets_data(spark, config),
                   _extract_ticket_flights_data(spark, config),
                   _extract_flights_data(spark, config),
                   _extract_airports_data(spark, config),
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job bookings