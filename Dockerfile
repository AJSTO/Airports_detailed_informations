FROM postgres:11.18-alpine

ENV POSTGRES_PASSWORD flights
ENV POSTGRES_DB flights
ENV POSTGRES_USER flights

COPY flights_database.sql /docker-entrypoint-initdb.d/
