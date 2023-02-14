from jobs import occupancy_per_fare
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime


class TestArrivalAirports:
    def test_aggregation_arrival_airports(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        seats_test_data = spark_session.createDataFrame(
            [
                (
                    '319', '2A', 'Economy',
                ),
                (
                    '319', '3B', 'Business',
                ),
                (
                    '319', '8A', 'Comfort',
                )
            ],
            [
                "aircraft_code", "seat_no", "fare_conditions"
            ]
        )

        ticket_flights_test_data = spark_session.createDataFrame(
            [
                (
                    '0005435981740', 14789, 'Economy', 6700.00
                ),
                (
                    '0005435981741', 14789, 'Business', 12200.00
                ),
                (
                    '0005435981742', 14789, 'Comfort', 14800.00
                )
            ],
            [
                'ticket_no', 'flight_id', 'fare_conditions', 'amount'
            ]
        )

        flights_test_data = spark_session.createDataFrame(
            [
                (
                    14789, 'PG0403', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '319',
                    datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    87989, 'PG0404', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
                    datetime.strptime('2017-06-13 10:29:00', '%Y-%m-%d %H:%M:%S'),
                    datetime.strptime('2017-06-13 11:24:00', '%Y-%m-%d %H:%M:%S'),
                ),
                (
                    78177, 'PG0405', datetime.strptime('2017-06-13 10:25:00', '%Y-%m-%d %H:%M:%S'),
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
                ("13-06-2017", 'Domodedovo International Airport', '100.00', '100.00', '100.00'),
            ],
            [
                "date", "airport", "avg_occ_economy_percent", "avg_occ_bussines_percent", "avg_occ_comfort_percent",
            ],
        ).toPandas()

        real_data = occupancy_per_fare._transform_data(
            seats_test_data,
            ticket_flights_test_data,
            flights_test_data,
            airports_test_data
        ).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)