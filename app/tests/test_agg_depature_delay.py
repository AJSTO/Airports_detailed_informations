from jobs import depature_delay
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
                    14789, 'PG0403', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    87989, 'PG0404', datetime.strptime('2017-06-15 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-15 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-15 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-15 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    78177, 'PG0405', datetime.strptime('2017-06-17 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-17 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-17 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-17 11:24:00', '%Y-%m-%d %H:%M:%S'),
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
                ("17-06-2017", 'Domodedovo International Airport', '4.00'),
                ("15-06-2017", 'Domodedovo International Airport', '4.00'),
                ("13-06-2017", 'Domodedovo International Airport', '4.00'),
            ],
            ["date", "airport", "avg_delay_in_minutes"],
        ).toPandas()

        real_data = depature_delay._transform_data(
            flights_test_data,
            airports_test_data
        ).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)