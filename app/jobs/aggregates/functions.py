import pyspark
from pyspark.sql.functions import col, format_string, format_number, to_date


def num_of_flights(flights_df, airports_data_df):
    """
    Creating aggregate for number of flights per day per airport.

    Parameters
    ----------
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of number of flights per day per airport.
    """
    num_of_flights_df = flights_df.join(
        airports_data_df,
        flights_df.departure_airport == airports_data_df.airport_code,
        "leftouter",
    )
    split_col = pyspark.sql.functions.split(num_of_flights_df['airport_name'], '"')
    num_of_flights_df = num_of_flights_df.withColumn('airport', split_col.getItem(3))
    num_of_flights_df = num_of_flights_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    num_of_flights_df = num_of_flights_df.groupBy('airport', 'date') \
        .count()
    num_of_flights_df = num_of_flights_df.withColumnRenamed(
        "count", "num_of_flights"
    )
    num_of_flights_df = num_of_flights_df.select('date', 'airport', 'num_of_flights')

    return num_of_flights_df


def bookings(
        bookings_df, tickets_df, ticket_flights_df, flights_df, airports_data_df
):
    """
    Creating aggregate for sum of made bookings per day per airport.

    Parameters
    ----------
    bookings_df : object
        Extracted table of bookings from Bigquery converted to Pyspark Dataframe.
    tickets_df : object
        Extracted table of tickets from Bigquery converted to Pyspark Dataframe.
    ticket_flights_df : object
        Extracted table of ticket flights from Bigquery converted to Pyspark Dataframe.
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of sum of made bookings per day per airport.
    """
    bookings_aggregate_df = bookings_df.join(
        tickets_df,
        bookings_df.book_ref == tickets_df.book_ref,
        'leftouter'
    ).drop(
        'passenger_id', 'passenger_name', 'contact_data'
    )
    bookings_aggregate_df = bookings_aggregate_df.join(
        ticket_flights_df,
        bookings_aggregate_df.ticket_no == ticket_flights_df.ticket_no,
        'leftouter'
    ).drop(
        'fare_conditions', 'amount',
    )
    bookings_aggregate_df = bookings_aggregate_df.join(
        flights_df,
        bookings_aggregate_df.flight_id == flights_df.flight_id,
        'leftouter'
    ).drop(
        'actual_arrival', 'actual_departure', 'status', 'aircraft_code',
        'scheduled_departure', 'scheduled_arrival',
    )
    bookings_aggregate_df = airports_data_df.join(
        bookings_aggregate_df,
        airports_data_df.airport_code == bookings_aggregate_df.departure_airport,
        "leftouter",
    )
    bookings_aggregate_df = bookings_aggregate_df.select(
        'total_amount', 'book_date', 'airport_name'
    )
    split_col = pyspark.sql.functions.split(bookings_aggregate_df['airport_name'], '"')
    bookings_aggregate_df = bookings_aggregate_df.withColumn('airport', split_col.getItem(3))
    bookings_aggregate_df = bookings_aggregate_df.withColumn(
        "date", to_date(col('book_date'), "dd-MM-yyyy")
    )
    bookings_aggregate_df = bookings_aggregate_df.groupBy('airport', 'date') \
        .count()
    bookings_aggregate_df = bookings_aggregate_df.withColumnRenamed(
        "count", "num_of_bookings"
    )
    bookings_aggregate_df = bookings_aggregate_df.select('date', 'airport', 'num_of_bookings')

    return bookings_aggregate_df


def departure_delay(
        flights_df, airports_data_df
):
    """
    Creating aggregate of average departure delay per day per airport.

    Parameters
    ----------
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of aggregate of average departure delay per day per airport.
    """
    departure_delay_aggregate_df = flights_df.select(
        'scheduled_departure', 'actual_departure', 'departure_airport',
    )
    departure_delay_aggregate_df = departure_delay_aggregate_df.dropna(
        how='any',
        thresh=None,
        subset=('scheduled_departure', 'actual_departure'),
    )
    departure_delay_aggregate_df = departure_delay_aggregate_df.join(
        airports_data_df,
        departure_delay_aggregate_df.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    split_col = pyspark.sql.functions.split(departure_delay_aggregate_df['airport_name'], '"')
    departure_delay_aggregate_df = departure_delay_aggregate_df.withColumn('airport', split_col.getItem(3))
    departure_delay_aggregate_df = departure_delay_aggregate_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    departure_delay_aggregate_df = departure_delay_aggregate_df.withColumn(
        'delay_seconds',
        departure_delay_aggregate_df['actual_departure'].cast('integer') - departure_delay_aggregate_df[
            'scheduled_departure'].cast('integer')
    )
    departure_delay_aggregate_df = departure_delay_aggregate_df.groupBy('airport', 'date') \
        .mean()
    departure_delay_aggregate_df = departure_delay_aggregate_df.withColumn(
        'avg_delay_in_minutes',
        departure_delay_aggregate_df['avg(delay_seconds)'] / 60
    )
    departure_delay_aggregate_df = departure_delay_aggregate_df.select(
        'date', 'airport', 'avg_delay_in_minutes'
    )

    return departure_delay_aggregate_df


def flight_occupancy(
        ticket_flights_df, aircrafts_data_df, flights_df, airports_data_df, seats_df, tickets_df
):
    """
    Creating aggregate of average flight occupancy per airport per day.

    Parameters
    ----------
    ticket_flights_df : object
        Extracted table of ticket flights from Bigquery converted to Pyspark Dataframe.
    aircrafts_data_df : object
        Extracted table of aircrafts from Bigquery converted to Pyspark Dataframe.
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.
    seats_df : object
        Extracted table of seats_per_aircraft_df from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of aggregate of average flight occupancy per airport per day.
    """
    tickets_per_flight_df = ticket_flights_df.groupBy('flight_id') \
        .count()
    tickets_per_flight_df = tickets_per_flight_df.withColumnRenamed(
        "count", "num_of_passengers"
    )
    seats_per_aircraft_df = seats_df.groupBy('aircraft_code') \
        .count()
    seats_per_aircraft_df = seats_per_aircraft_df.join(
        aircrafts_data_df, ['aircraft_code']
    )
    seats_per_aircraft_df = seats_per_aircraft_df.withColumnRenamed(
        "count", "num_of_seats"
    )
    flight_occupancy_aggregate_df = flights_df.join(
        tickets_per_flight_df,
        flights_df.flight_id == tickets_per_flight_df.flight_id,
        'leftouter'
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.join(
        seats_per_aircraft_df,
        flight_occupancy_aggregate_df.aircraft_code == seats_per_aircraft_df.aircraft_code,
        "leftouter"
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.join(
        airports_data_df,
        flight_occupancy_aggregate_df.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    split_col = pyspark.sql.functions.split(flight_occupancy_aggregate_df['airport_name'], '"')
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn('airport', split_col.getItem(3))
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.select(
        'airport', 'scheduled_departure', 'num_of_seats', 'num_of_passengers'
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.groupBy('airport', 'date') \
        .sum('num_of_seats', 'num_of_passengers')
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn(
        'avg_occupancy_percent',
        (flight_occupancy_aggregate_df['sum(num_of_passengers)'] / flight_occupancy_aggregate_df[
            'sum(num_of_seats)']) * 100
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.select('date', 'airport', 'avg_occupancy_percent')

    return flight_occupancy_aggregate_df


def flight_occupancy_per_fare(
        seats_df, ticket_flights_df, flights_df, airports_data_df
):
    """
    Creating aggregate of average flight occupancy per fare conditions per airport per day.

    Parameters
    ----------
    seats_df : object
        Extracted table of ticket num_of_seats_per_aircraft_df from Bigquery converted to Pyspark Dataframe.
    ticket_flights_df : object
        Extracted table of ticket flights from Bigquery converted to Pyspark Dataframe.
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of aggregate of average flight occupancy per fare conditions per airport per day.
    """
    tickets_per_flight_df = ticket_flights_df.groupBy('flight_id') \
        .count()
    tickets_per_flight_df = tickets_per_flight_df.withColumnRenamed(
        "count", "num_of_passengers"
    )
    seats_per_aircraft_df = seats_df.groupBy('aircraft_code') \
        .count()
    seats_per_aircraft_df = seats_per_aircraft_df.join(
        aircrafts_data_df, ['aircraft_code']
    )
    seats_per_aircraft_df = seats_per_aircraft_df.withColumnRenamed(
        "count", "num_of_seats"
    )
    flight_occupancy_aggregate_df = flights_df.join(
        tickets_per_flight_df,
        flights_df.flight_id == tickets_per_flight_df.flight_id,
        'leftouter'
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.join(
        seats_per_aircraft_df,
        flight_occupancy_aggregate_df.aircraft_code == seats_per_aircraft_df.aircraft_code,
        "leftouter"
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.join(
        airports_data_df,
        flight_occupancy_aggregate_df.departure_airport == airports_data_df.airport_code,
        "leftouter"
    )
    split_col = pyspark.sql.functions.split(flight_occupancy_aggregate_df['airport_name'], '"')
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn('airport', split_col.getItem(3))
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.select(
        'airport', 'scheduled_departure', 'num_of_seats', 'num_of_passengers'
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.groupBy('airport', 'date') \
        .sum('num_of_seats', 'num_of_passengers')
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.withColumn(
        'occupancy_percent',
        (flight_occupancy_aggregate_df['sum(num_of_passengers)'] / flight_occupancy_aggregate_df[
            'sum(num_of_seats)']) * 100
    )
    flight_occupancy_aggregate_df = flight_occupancy_aggregate_df.select('date', 'airport', 'occupancy_percent')

    return flight_occupancy_aggregate_df


def flight_occupancy_per_fare(
        seats_df, ticket_flights_df, flights_df, airports_data_df
):
    """
    Creating aggregate of average flight occupancy per fare conditions per airport per day.

    Parameters
    ----------
    seats_df : object
        Extracted table of ticket num_of_seats_per_aircraft_df from Bigquery converted to Pyspark Dataframe.
    ticket_flights_df : object
        Extracted table of ticket flights from Bigquery converted to Pyspark Dataframe.
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of aggregate of average flight occupancy per fare conditions per airport per day.
    """
    num_of_seats_per_aircraft_df = seats_df.groupBy('aircraft_code', 'fare_conditions') \
        .count()
    num_of_seats_economy_per_aircraft_df = num_of_seats_per_aircraft_df.filter(
        num_of_seats_per_aircraft_df['fare_conditions'] == 'Economy'
    )
    num_of_seats_business_per_aircraft_df = num_of_seats_per_aircraft_df.filter(
        num_of_seats_per_aircraft_df['fare_conditions'] == 'Business'
    )
    num_of_seats_comfort_per_aircraft_df = num_of_seats_per_aircraft_df.filter(
        num_of_seats_per_aircraft_df['fare_conditions'] == 'Comfort'
    )
    num_of_seats_economy_per_aircraft_df = num_of_seats_economy_per_aircraft_df.withColumnRenamed(
        'count', 'num_of_seats_economy_per_aircraft'
    ).drop('fare_conditions')
    num_of_seats_business_per_aircraft_df = num_of_seats_business_per_aircraft_df.withColumnRenamed(
        'count', 'num_of_seats_business_per_aircraft'
    ).drop('fare_conditions')
    num_of_seats_business_per_aircraft_df = num_of_seats_business_per_aircraft_df.withColumnRenamed(
        'aircraft_code', 'aircraft_code_b'
    )
    num_of_seats_comfort_per_aircraft_df = num_of_seats_comfort_per_aircraft_df.withColumnRenamed(
        'count', 'num_of_seats_comfort_per_aircraft'
    ).drop('fare_conditions')
    num_of_seats_comfort_per_aircraft_df = num_of_seats_comfort_per_aircraft_df.withColumnRenamed(
        'aircraft_code', 'aircraft_code_c'
    )
    num_of_seats_per_aircraft_df = num_of_seats_economy_per_aircraft_df.join(
        num_of_seats_comfort_per_aircraft_df,
        num_of_seats_economy_per_aircraft_df.aircraft_code == num_of_seats_comfort_per_aircraft_df.aircraft_code_c,
        'left'
    )
    num_of_seats_per_aircraft_df = num_of_seats_per_aircraft_df.join(
        num_of_seats_business_per_aircraft_df,
        num_of_seats_per_aircraft_df.aircraft_code == num_of_seats_business_per_aircraft_df.aircraft_code_b,
        'left'
    )
    num_of_seats_per_aircraft_df = num_of_seats_per_aircraft_df.select(
        'aircraft_code',
        'num_of_seats_economy_per_aircraft',
        'num_of_seats_business_per_aircraft',
        'num_of_seats_comfort_per_aircraft'
    )
    passengers_per_flight_df = ticket_flights_df.groupBy('flight_id', 'fare_conditions') \
        .count()
    passengers_per_flight_in_economy_df = passengers_per_flight_df.filter(
        passengers_per_flight_df['fare_conditions'] == 'Economy'
    )
    passengers_per_flight_in_business_df = passengers_per_flight_df.filter(
        passengers_per_flight_df['fare_conditions'] == 'Business'
    )
    passengers_per_flight_in_comfort_df = passengers_per_flight_df.filter(
        passengers_per_flight_df['fare_conditions'] == 'Comfort'
    )
    passengers_per_flight_in_economy_df = passengers_per_flight_in_economy_df.withColumnRenamed(
        'count', 'passengers_per_flight_in_economy'
    ).drop('fare_conditions')
    passengers_per_flight_in_business_df = passengers_per_flight_in_business_df.withColumnRenamed(
        'count', 'passengers_per_flight_in_business'
    ).drop('fare_conditions')
    passengers_per_flight_in_business_df = passengers_per_flight_in_business_df.withColumnRenamed(
        'flight_id', 'flight_id_b'
    )
    passengers_per_flight_in_comfort_df = passengers_per_flight_in_comfort_df.withColumnRenamed(
        'count', 'passengers_per_flight_in_comfort'
    ).drop('fare_conditions')
    passengers_per_flight_in_comfort_df = passengers_per_flight_in_comfort_df.withColumnRenamed(
        'flight_id', 'flight_id_c'
    )
    passengers_per_flight_df = passengers_per_flight_in_economy_df.join(
        passengers_per_flight_in_comfort_df,
        passengers_per_flight_in_economy_df.flight_id == passengers_per_flight_in_comfort_df.flight_id_c,
        'left'
    )
    passengers_per_flight_df = passengers_per_flight_df.join(
        passengers_per_flight_in_business_df,
        passengers_per_flight_df.flight_id == passengers_per_flight_in_business_df.flight_id_b,
        'left'
    )
    passengers_per_flight_df = passengers_per_flight_df.select(
        'flight_id',
        'passengers_per_flight_in_comfort',
        'passengers_per_flight_in_business',
        'passengers_per_flight_in_economy',
    )
    flight_occupancy_per_fare_aggregate_df = flights_df.join(
        passengers_per_flight_df,
        flights_df.flight_id == passengers_per_flight_df.flight_id,
        'fullouter'
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.join(
        num_of_seats_per_aircraft_df, ['aircraft_code']
    )
    flight_occupancy_per_fare_aggregate_df = airports_data_df.join(
        flight_occupancy_per_fare_aggregate_df,
        airports_data_df.airport_code == flight_occupancy_per_fare_aggregate_df.departure_airport,
        "leftouter"
    )
    split_col = pyspark.sql.functions.split(flight_occupancy_per_fare_aggregate_df['airport_name'], '"')
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.withColumn(
        'airport', split_col.getItem(3)
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.groupBy('date', 'airport') \
        .sum(
        'passengers_per_flight_in_economy', 'num_of_seats_economy_per_aircraft',
        'passengers_per_flight_in_business', 'num_of_seats_business_per_aircraft',
        'passengers_per_flight_in_comfort', 'num_of_seats_comfort_per_aircraft'
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.withColumn(
        'avg_occ_economy_percent',
        (flight_occupancy_per_fare_aggregate_df['sum(passengers_per_flight_in_economy)'] /
         flight_occupancy_per_fare_aggregate_df['sum(num_of_seats_economy_per_aircraft)']) * 100
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.withColumn(
        'avg_occ_business_percent',
        (flight_occupancy_per_fare_aggregate_df['sum(passengers_per_flight_in_business)'] /
         flight_occupancy_per_fare_aggregate_df['sum(num_of_seats_business_per_aircraft)']) * 100
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.withColumn(
        'avg_occ_comfort_percent',
        (flight_occupancy_per_fare_aggregate_df['sum(passengers_per_flight_in_comfort)'] /
         flight_occupancy_per_fare_aggregate_df['sum(num_of_seats_comfort_per_aircraft)']) * 100
    )

    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare_aggregate_df.select(
        'date', 'airport', 'avg_occ_economy_percent', 'avg_occ_business_percent', 'avg_occ_comfort_percent'
    )

    return flight_occupancy_per_fare_aggregate_df


def served_passengers(
        airports_data_df, ticket_flights_df, flights_df
):
    """
    Creating aggregate of sum of served passengers per depature airport per day.

    Parameters
    ----------
    airports_data_df : object
        Extracted table of airports from Bigquery converted to Pyspark Dataframe.
    ticket_flights_df : object
        Extracted table of ticket flights from Bigquery converted to Pyspark Dataframe.
    flights_df : object
        Extracted table of flights from Bigquery converted to Pyspark Dataframe.

    Returns
    -------
    Pyspark Dataframe
        Table of aggregate of sum of served passengers per depature airport per day.
    """
    tickets_per_flight_df = ticket_flights_df.groupBy('flight_id') \
        .count()
    served_passengers_aggregate_df = flights_df.join(
        airports_data_df,
        airports_data_df.airport_code == flights_df.departure_airport, "leftouter"
    )
    served_passengers_aggregate_df = served_passengers_aggregate_df.join(
        tickets_per_flight_df,
        served_passengers_aggregate_df.flight_id == tickets_per_flight_df.flight_id,
        'leftouter'

    )
    served_passengers_aggregate_df = served_passengers_aggregate_df.drop(
        'flight_id', 'airport_code', 'departure_airport'
    )
    served_passengers_aggregate_df = served_passengers_aggregate_df.withColumn(
        "date", to_date(col('scheduled_departure'), "dd-MM-yyyy")
    )
    served_passengers_aggregate_df = served_passengers_aggregate_df.groupBy('airport_name', 'date') \
        .sum()
    served_passengers_aggregate_df = served_passengers_aggregate_df.withColumnRenamed(
        "sum(count)", "num_of_passengers"
    )
    split_col = pyspark.sql.functions.split(served_passengers_aggregate_df['airport_name'], '"')
    served_passengers_aggregate_df = served_passengers_aggregate_df.withColumn('airport', split_col.getItem(3))

    served_passengers_aggregate_df = served_passengers_aggregate_df.select('date', 'airport', 'num_of_passengers')

    return served_passengers_aggregate_df