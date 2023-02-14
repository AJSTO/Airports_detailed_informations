from jobs import arrival_airports
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime


class TestArrivalAirports:
    def test_aggregation_arrival_airports(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        flights_test_data = spark_session.createDataFrame(
            [
                (
                    1, 'PG0403', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    2, 'PG0404', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    3, 'PG0405', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
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

        airports_test_data = spark_session.createDataFrame(
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

        expected_data = spark_session.createDataFrame(
            [
                ("13-06-2017", 'Pulkovo Airport', 3),
            ],
            ["date", "arrival_airport", "num_of_flights"],
        ).toPandas()

        real_data = arrival_airports._transform_data(flights_test_data, airports_test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)