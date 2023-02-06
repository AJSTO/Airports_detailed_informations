FROM postgres:11.18-alpine

ENV POSTGRES_PASSWORD flights
ENV POSTGRES_DB flights
ENV POSTGRES_USER flights

COPY flights_database.sql /docker-entrypoint-initdb.d/

# When you are in folder with Dockerfile
# docker build -t flights_db .
# docker run -d -p 5432:5432 --name flights_db_container flights_db
# Now docker container with database should be running
