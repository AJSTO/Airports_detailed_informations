from pyspark.sql.functions import col, format_string, date_format, format_number
import pyspark


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
        seats_df, ticket_flights_df, flights_df, airports_data_df
):
    """ Create daily average of seats occupied per fare conditions per airport """
    # Seat numbers per fare conditions per aircraft
    seats = seats_df.withColumnRenamed(
        "aircraft_code", "aircraft_c"
    )
    seats_per_aircraft = seats.groupBy('aircraft_c', 'fare_conditions') \
        .count()
    # Filtering
    seats_comfort = seats_per_aircraft.filter(
        seats_per_aircraft['fare_conditions'] == 'Comfort'
    )
    seats_bussines = seats_per_aircraft.filter(
        seats_per_aircraft['fare_conditions'] == 'Business'
    )
    seats_economy = seats_per_aircraft.filter(
        seats_per_aircraft['fare_conditions'] == 'Economy'
    )
    seats_comfort = seats_comfort.withColumnRenamed(
        'count', 'seats_comfort'
    ).drop('fare_conditions')
    seats_bussines = seats_bussines.withColumnRenamed(
        'count', 'seats_bussines'
    ).drop('fare_conditions').withColumnRenamed(
        'aircraft_c', 'aircraft_c1'
    )
    seats_economy = seats_economy.withColumnRenamed(
        'count', 'seats_economy'
    ).drop('fare_conditions').withColumnRenamed(
        'aircraft_c', 'aircraft_c2'
    )
    seats = seats_comfort.join(
        seats_bussines,
        seats_comfort.aircraft_c == seats_bussines.aircraft_c1,
        "right"
    )
    seats = seats.drop('aircraft_c')
    seats = seats.join(
        seats_economy,
        seats.aircraft_c1 == seats_economy.aircraft_c2,
        "right"
    )
    seats = seats.drop('aircraft_c1')
    seats = seats.select('aircraft_c2', 'seats_economy', 'seats_bussines', 'seats_comfort')

    # Passengers per fare conditions per aircraft
    passengers = ticket_flights_df.groupBy('flight_id', 'fare_conditions') \
        .count()
    # Filtering
    passengers_comfort = passengers.filter(passengers['fare_conditions'] == 'Comfort')
    passengers_bussines = passengers.filter(passengers['fare_conditions'] == 'Business')
    passengers_economy = passengers.filter(passengers['fare_conditions'] == 'Economy')
    passengers_comfort = passengers_comfort.withColumnRenamed(
        'count', 'passengers_comfort'
    ).drop('fare_conditions')
    passengers_bussines = passengers_bussines.withColumnRenamed(
        'count', 'passengers_bussines'
    ).drop('fare_conditions').withColumnRenamed(
        'flight_id', 'flight_id1'
    )
    passengers_economy = passengers_economy.withColumnRenamed(
        'count', 'passengers_economy'
    ).drop('fare_conditions').withColumnRenamed(
        'flight_id', 'flight_id2'
    )
    passengers = passengers_comfort.join(
        passengers_bussines,
        passengers_comfort.flight_id == passengers_bussines.flight_id1,
        "leftouter"
    )
    passengers = passengers.join(
        passengers_economy,
        passengers.flight_id == passengers_economy.flight_id2,
        "leftouter"
    )
    passengers = passengers.select(
        'flight_id2', 'passengers_comfort', 'passengers_bussines', 'passengers_economy',
    )
    # Now joining prepared passengers and seats tables with flights tables
    occupancy_per = flights_df.join(
        passengers,
        passengers.flight_id2 == flights_df.flight_id,
        "leftouter"
    )
    occupancy_per = occupancy_per.join(
        seats,
        occupancy_per.aircraft_code == seats.aircraft_c2,
        "leftouter"
    )
    # Counting occupancy per flight in each airport
    occupancy_per = occupancy_per.withColumn(
        'occ_eco',
        occupancy_per['passengers_economy'] / occupancy_per['seats_economy']
    )
    occupancy_per = occupancy_per.withColumn(
        'occ_bus',
        occupancy_per['passengers_bussines'] / occupancy_per['seats_bussines']
    )
    occupancy_per = occupancy_per.withColumn(
        'occ_com',
        occupancy_per['passengers_comfort'] / occupancy_per['seats_comfort']
    )
    occupancy_per = occupancy_per.select(
        'scheduled_departure', 'departure_airport', 'occ_eco', 'occ_bus', 'occ_com'
    )
    # Adding info about airport
    occupancy_per = occupancy_per.join(
        airports_data_df,
        occupancy_per.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    # Airport name to english name
    split_col = pyspark.sql.functions.split(occupancy_per['airport_name'], '"')
    occupancy_per = occupancy_per.withColumn('airport', split_col.getItem(3))
    # Date to format DD-MM-YYYY
    occupancy_per = occupancy_per.withColumn(
        "date", date_format(col('scheduled_departure'), "dd-MM-yyyy")
    )
    # Only needed columns
    occupancy_per = occupancy_per.select(
        'date', 'airport', 'occ_eco', 'occ_bus', 'occ_com'
    )
    # Counting average occupancy per day per airport per fare conditions
    occupancy_per = occupancy_per.groupBy('date', 'airport') \
        .mean()
    occupancy_per = occupancy_per.withColumn(
        'avg_eco',
        occupancy_per['avg(occ_eco)'] * 100
    )
    occupancy_per = occupancy_per.withColumn(
        "avg_occ_economy_percent",
        format_number("avg_eco", 2)
    )
    occupancy_per = occupancy_per.withColumn(
        'avg_bus',
        occupancy_per['avg(occ_bus)'] * 100
    )
    occupancy_per = occupancy_per.withColumn(
        "avg_occ_bussines_percent",
        format_number("avg_bus", 2)
    )
    occupancy_per = occupancy_per.withColumn(
        'avg_com',
        occupancy_per['avg(occ_com)'] * 100
    )
    occupancy_per = occupancy_per.withColumn(
        "avg_occ_comfort_percent",
        format_number("avg_com", 2)
    )
    # Final table
    occupancy_per = occupancy_per.select(
        'date', 'airport', 'avg_occ_economy_percent', 'avg_occ_bussines_percent', 'avg_occ_comfort_percent'
    )

    return occupancy_per


def _load_data(config, transformed_df):
    """ Save aggregates to Bigquery """
    transformed_df.write.format("bigquery") \
        .option("credentialsFile", config.get('source_credentials')) \
        .option("writeMethod", "direct") \
        .mode('append') \
        .save(config.get('short_seats_occupied_per_fare_conditions'))


def run_job(spark, config):
    """ Run aggregate daily average of seats occupied per fare conditions per airport """
    _load_data(config,
               _transform_data(
                   _extract_seats_data(spark, config),
                   _extract_ticket_flights_data(spark, config),
                   _extract_flights_data(spark, config),
                   _extract_airports_data(spark, config),
               )
            )

# spark-submit --packages org.postgresql:postgresql:42.5.1 --jars bq_con/spark-3.1-bigquery-0.28.0-preview.jar  --files config.json main.py --job occupancy_per_fare