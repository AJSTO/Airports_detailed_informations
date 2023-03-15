from jobs.aggregates.functions import num_of_flights, bookings, departure_delay
from jobs.aggregates.functions import flight_occupancy, flight_occupancy_per_fare, served_passengers


def _extract_table(spark, table_name):
    """
    Extract airports database tables to dataframe from Bigquery.
    Universal function to get table from given table name.

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables.

    Returns
    -------
    Pyspark Dataframe
        Extracted table depending on given table_name from Bigquery.
    """   
    return (
        spark.read \
            .format('com.google.cloud.spark.bigquery') \
            .option('table', f'flights_database.{table_name}') \
            .load()
    )


def _transform_data(
        aircrafts_data_df,
        airports_data_df,
        bookings_df,
        flights_df,
        tickets_df,
        ticket_flights_df,
        seats_df
):
    """
    Create aggregates for each airport per day.
    Creating 5 types of aggregates:
    - Sum of made bookings per airport per day;
    - Average departure delay per airport per day;
    - Average flight occupancy per airport per day;
    - Average flight occupancy depending on fare conditions per airport per day;
    - Number of passengers served per airport per day;
    - Number of flights per day per airport;

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables.

    Returns
    -------
    Pyspark Dataframe
        Table with 6 aggregates per airport per day.
    """
    bookings_aggregate_df = bookings(
        bookings_df,
        tickets_df,
        ticket_flights_df,
        flights_df,
        airports_data_df,
    )
    departure_delay_aggregate_df = departure_delay(
        flights_df,
        airports_data_df
    )
    flight_occupancy_aggregate_df = flight_occupancy(
        ticket_flights_df,
        aircrafts_data_df,
        flights_df,
        airports_data_df,
        seats_df,
        tickets_df
    )
    flight_occupancy_per_fare_aggregate_df = flight_occupancy_per_fare(
        seats_df,
        ticket_flights_df,
        flights_df,
        airports_data_df
    )
    served_passengers_aggregate_df = served_passengers(
        airports_data_df,
        ticket_flights_df,
        flights_df
    )
    num_of_flights_df = num_of_flights(
        flights_df,
        airports_data_df,
    )
    # Joining all aggregates into one Pyspark Dataframe
    aggregation_table = flight_occupancy_aggregate_df.join(
        flight_occupancy_per_fare_aggregate_df,
        on=['date', 'airport'],
        how='left'
    ).drop(flight_occupancy_per_fare_aggregate_df.date).drop(flight_occupancy_per_fare_aggregate_df.airport)
    aggregation_table = aggregation_table.join(
        bookings_aggregate_df,
        on=['date', 'airport'],
        how='left'
    ).drop(bookings_aggregate_df.date).drop(bookings_aggregate_df.airport)
    aggregation_table = aggregation_table.join(
        departure_delay_aggregate_df,
        on=['date', 'airport'],
        how='left'
    ).drop(departure_delay_aggregate_df.date).drop(departure_delay_aggregate_df.airport)
    aggregation_table = aggregation_table.join(
        served_passengers_aggregate_df,
        on=['date', 'airport'],
        how='left'
    ).drop(served_passengers_aggregate_df.date).drop(served_passengers_aggregate_df.airport)
    aggregation_table = aggregation_table.join(
        num_of_flights_df,
        on=['date', 'airport'],
        how='left'
    ).drop(num_of_flights_df.date).drop(num_of_flights_df.airport)

    return aggregation_table


def _load_data(aggregation_table):
    """
    Saving created table of aggregates per airport per day to Google Bigquery table.
    Mode of saving data - append. Writing pyspark Dataframe directly Bigquery table.
    Table contains columns:
    - Date;
    - Airport name;
    - Sum of made bookings per airport per day;
    - Average departure delay per airport per day;
    - Average flight occupancy per airport per day;
    - Average flight occupancy depending on fare conditions per airport per day;
    - Number of passengers served per airport per day;

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables.
    """
    aggregation_table.write.format("com.google.cloud.spark.bigquery") \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", "exampleersdfsdfsdf") \
        .save('flights_database.aggregates')


def _run_job(spark):
    """
    Running ETL job for getting daily aggregate per airport.

    Parameters
    ----------
    spark : object
        SparkSession built in main.py.
    config : json object
        Json file with information about Bigquery project, dataset, tables.
    """
    _load_data(
        _transform_data(
            _extract_table(spark, 'aircrafts_data'),
            _extract_table(spark, 'airports_data'),
            _extract_table(spark, 'bookings'),
            _extract_table(spark, 'flights'),
            _extract_table(spark, 'tickets'),
            _extract_table(spark, 'ticket_flights_v1'),
            _extract_table(spark, 'seats')
        )
    )