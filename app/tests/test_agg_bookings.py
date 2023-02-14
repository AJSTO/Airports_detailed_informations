from jobs import bookings
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime


class TestArrivalAirports:
    def test_aggregation_arrival_airports(self):
        spark_session = SparkSession.builder \
            .appName('testing_agg').getOrCreate()
        bookings_test_data = spark_session.createDataFrame(
            [
                (
                    '000004',datetime.strptime('2016-08-13 14:40:00', '%Y-%m-%d %H:%M:%S'), 55800.00
                ),
                (
                    '00000F', datetime.strptime('2017-07-05 02:12:00', '%Y-%m-%d %H:%M:%S'), 265700.00
                ),
                (
                    '0000010', datetime.strptime('2017-01-08 17:45:00', '%Y-%m-%d %H:%M:%S'), 50900.00
                ),
            ],
            [
                "book_ref", "book_date", "total_amount",
            ]
        )
        tickets_test_data = spark_session.createDataFrame(
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

        ticket_flights_test_data = spark_session.createDataFrame(
            [
                (
                    '0005435981740', 14789, 'Economy', 6700.00
                ),
                (
                    '0005435981741', 87989, 'Economy', 12200.00
                ),
                (
                    '0005435981742', 78177, 'Economy', 14800.00
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
                    datetime.strptime('2017-06-13 11:20:00', '%Y-%m-%d %H:%M:%S'), 'DME', 'LED', 'Arrived', '321',
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
                ("08-01-2017", 'Domodedovo International Airport', 1),
                ("13-08-2016", 'Domodedovo International Airport', 1),
                ("05-07-2017", 'Domodedovo International Airport', 1),
            ],
            ["date", "airport", "num_of_bookings"],
        ).toPandas()

        real_data = bookings._transform_data(
            bookings_test_data,
            tickets_test_data,
            ticket_flights_test_data,
            flights_test_data,
            airports_test_data
        ).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=True)