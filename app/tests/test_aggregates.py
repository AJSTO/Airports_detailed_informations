from datetime import datetime

import pytest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from jobs import airports_job


class TestAggregates:
    """
    This class is used to test generating of the aggregates.
    """

    @pytest.fixture
    def spark_session(self):
        """
        Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            SparkSession: A SparkSession object with the application name "testing_agg".
        """
        return (
            SparkSession.builder.appName('testing_agg').getOrCreate()
        )

    @pytest.fixture
    def bookings_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for booking table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        '000004', datetime.strptime('2017-06-13 08:25:00', '%Y-%m-%d %H:%M:%S'), 55800.00
                    ),
                    (
                        '00000F', datetime.strptime('2017-06-13 08:20:00', '%Y-%m-%d %H:%M:%S'), 265700.00
                    ),
                    (
                        '0000010', datetime.strptime('2017-06-13 08:15:00', '%Y-%m-%d %H:%M:%S'), 50900.00
                    ),
                ],
                [
                    "book_ref", "book_date", "total_amount",
                ]
            )
        )

    @pytest.fixture
    def tickets_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for tickets table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        '0005435981740', '000004', '2761 658276', 'IRINA KOZLOVA',
                        '{"email": "kozlova_i-031980@postgrespro.ru", "phone": "+70555875224"}'
                    ),
                    (
                        '0005435981741', '00000F', '3786 388525', 'ANDREY ZAYCEV',
                        '{"phone": "+70451935914"}'
                    ),
                    (
                        '0005435981742', '0000010', '7879 114492', 'NINA LUKYANOVA',
                        '{"phone": "+70326667306"}'
                    ),
                ],
                [
                    'ticket_no', 'book_ref', 'passenger_id', 'passenger_name', 'contact_data'
                ]
            )
        )

    @pytest.fixture
    def ticket_flights_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for ticket flights table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        '0005435981740', 14789, 'Economy', 6700.00
                    ),
                    (
                        '0005435981741', 14789, 'Economy', 12200.00
                    ),
                    (
                        '0005435981742', 14789, 'Economy', 14800.00
                    )
                ],
                [
                    'ticket_no', 'flight_id', 'fare_conditions', 'amount'
                ]
            )
        )

    @pytest.fixture
    def flights_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for flights table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        14789, 'PG0403', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                        datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                        datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                        datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                    ),
                ],
                [
                    "flight_id", "flight_no", "scheduled_departure", "scheduled_arrival", "departure_airport",
                    "arrival_airport", "status", "aircraft_code", "actual_departure", "actual_arrival",
                ]
            )
        )

    @pytest.fixture
    def airports_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for airports table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        'DME', '{"en": "Domodedovo International Airport", "ru": "Домодедово"}',
                        '{"en": "Moscow", "ru": "Москва"}',
                        (37.90629959106445, 55.40879821777344), 'Europe/Moscow'
                    ),
                    (
                        'LED', '{"en": "Pulkovo Airport", "ru": "Пулково"}',
                        '{"en": "St. Petersburg", "ru": "Санкт-Петербург"}',
                        (30.262500762939453, 59.80030059814453), 'Europe/Moscow'
                    ),
                ],
                [
                    "airport_code", "airport_name", "city", "coordinates", "timezone",
                ]
            )
        )

    @pytest.fixture
    def aircrafts_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for aircrafts table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        '321', '{"en": "Airbus A321-200", "ru": "Аэробус A321-200"}', 5600
                    )
                ],
                [
                    'aircraft_code', 'model', 'range'
                ]
            )
        )

    @pytest.fixture
    def seats_test_data(self, spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for seats table.

        Parameters
        ----------
        spark_session : fixture
            Pytest fixture that creates a SparkSession object for testing purposes.

        Returns
        -------
            DataFrame: A PySpark DataFrame.
        """
        return (
            spark_session.createDataFrame(
                [
                    (
                        '321', '2A', 'Economy',
                    ),
                    (
                        '321', '3B', 'Economy',
                    ),
                    (
                        '321', '8A', 'Economy',
                    )
                ],
                [
                    "aircraft_code", "seat_no", "fare_conditions"
                ]
            )
        )

    def test_aggregation(
            self,
            spark_session,
            aircrafts_test_data,
            airports_test_data,
            bookings_test_data,
            flights_test_data,
            tickets_test_data,
            ticket_flights_test_data,
            seats_test_data
    ):
        """
        Pytest function to test the "_transform_data" function in the "airports_per_day_agg" module,
        to ensure that all aggregations are counted properly.

        Parameters
        ----------
            spark_session: A fixture that provides a SparkSession object for testing Spark code.
            aircrafts_test_data: A fixture that provides aircrafts test data for the Spark DataFrame.
            airports_test_data: A fixture that provides airports test data for the Spark DataFrame.
            bookings_test_data: A fixture that provides bookings test data for the Spark DataFrame.
            flights_test_data: A fixture that provides flights test data for the Spark DataFrame.
            tickets_test_data: A fixture that provides tickets test data for the Spark DataFrame.
            ticket_flights_test_data: A fixture that ticket flights provides test data for the Spark DataFrame.
            seats_test_data: A fixture that provides seats test data for the Spark DataFrame.

        Returns
        -------
            None
        """
        expected_data = pd.DataFrame(
            {
                "date": [datetime.strptime("2017-06-13", "%Y-%m-%d").date()],
                "airport": ['Domodedovo International Airport'],
                "avg_occupancy_percent": [100.0],
                "avg_occ_economy_percent": [100.0],
                "avg_occ_business_percent": [np.float64(None)],
                "avg_occ_comfort_percent": [np.float64(None)],
                "num_of_bookings": [3],
                "avg_delay_in_minutes": [4.0],
                "num_of_passengers": [3],
                "num_of_flights": [1]
            }
        )

        real_data = airports_job._transform_data(
            aircrafts_test_data,
            airports_test_data,
            bookings_test_data,
            flights_test_data,
            tickets_test_data,
            ticket_flights_test_data,
            seats_test_data
        ).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)

        spark_session.stop()
