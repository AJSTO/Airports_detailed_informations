## 👨‍💻 Built with
<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" /> <img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/> <img src="https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white" /> <img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" /> <img src="https://img.shields.io/badge/Numpy-777BB4?style=for-the-badge&logo=numpy&logoColor=white" /> 
<img src="https://miro.medium.com/max/1400/1*5C4UQznqEiN3D6Xutlgwlg.png" width="100" height="27,5" />
<img src="https://www.devagroup.pl/blog/wp-content/uploads/2022/10/logo-Google-Looker-Studio.png" width="100" height="27,5" />
<img src="https://www.scitylana.com/wp-content/uploads/2019/01/Hello-BigQuery.png" width="100" height="27,5" />
<img src="https://lh6.googleusercontent.com/2jkuIwxrds9QFLoBN5Rh1-uLgt5ukWTmGjXj_8TluJTdb9jYZ2su50b0vM_zU7cqn3y7xf5MRNjrrXUVQtZ-xetqMVgGrQBivhurxhTyM0ElzaSANRvEftTcW7edTVmb7UhJZ0Tj" width="100" height="27,5" />

##  Descripction about project

### ℹ️Project info

In this project I used data which was found on website:

[PostgresPro - Demonstration Database](https://postgrespro.com/community/demodb)

Project was based on using Pyspark on Google Dataproc to process a data and Bigquery to keep database of flights and created aggregates.
Before using Pyspark there was done data analyse in jupyter notebook.
The aim of the project was to catch informations about airports. So there were created aggregates for each airport.
Created aggregates for each airport per day:
- Sum of made bookings per airport per day;
- Average departure delay per airport per day;
- Average flight occupancy per airport per day;
- Average flight occupancy depending on fare conditions per airport per day;
- Number of passengers served per airport per day;
- Number of flights per day per airport;

The obtained data were loaded into a table in google bigquery and then visualized using Looker Studio.
In addition, the Pyspark application received tests that were written in pytest.

## 🔎 Looker Studio
Link to generated report in looker :

[Airports detailed informations](https://lookerstudio.google.com/reporting/4563da3e-7863-41ed-aaa2-63478c5d5a53)
![IMG LOOKER](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/gif-looker.gif)

## 🛬Database infromation:
Link to database:

[demo-big-en.zip](https://edu.postgrespro.com/demo-big-en.zip) - (232 MB) — flight data for one year (DB size is about 2.5 GB).

Firstly i created locally PostgreSQL database. For this reason i used created Dockerfile which is saved in this repo. Before building image and run container you should download sql file from link above and put it in building context.

#### ⚙️ Run PostgreSQL container locally:

Build image:
```bash
  $ docker build -t flights_db .
```

Run container:
```bash
  $ docker run -d -p 5432:5432 --name flights_db_container flights_db
```

But the aim of the project was using Pyspark on Dataproc, so I migrated database into Bigquery. 

More informations about: [Connecting PostgreSQL to BigQuery: 2 Easy Methods](https://hevodata.com/blog/postgresql-to-bigquery-data-migration/)

## Schema of the database:

![IMG SCHEMA](https://repo.postgrespro.ru/doc//std/10.23.1/en/html/demodb-bookings-schema.svg)

#### [Schema description](https://postgrespro.com/docs/postgrespro/10/apjs03.html)

## 🌲 Project tree
```bash
.
├── Dockerfile # docker file to create container image of PosgreSQL database
├── .gitignore
├── README.md
├── Flights_data_analyse.ipynb # jupyter notebook with analyse
├── postgresql-42.5.1.jar # jar file in case you want to work with Pyspark and PostgreSQL locally
└── app
    ├── jobs # pyspark job folder
    │   ├── __init__.py
    │   ├── airports_job.py # pyspark job
    │   └── aggregates 
    │       ├── __init__.py
    │       └── functions.py # aggregation functions module
    ├── main.py
    ├── conftest.py
    └── tests
        ├── pytest.ini
        └── test_aggregates.py # tests for pyspark application

```

##  📊Data visualisation
All analyse is located in: *Flights_data_analyse.ipynb*

### Exemplary graphs:
![X](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/connection.png)
![X](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/delay.png)
![X](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/passengers.png)
![X](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/seats.png)

##  📚Created aggregates

- Sum of made bookings per airport per day;
- Average departure delay per airport per day;
- Average flight occupancy per airport per day;
- Average flight occupancy depending on fare conditions per airport per day;
- Number of passengers served per airport per day;
- Number of flights per day per airport;

![Table of aggregates](https://github.com/AJSTO/Airports_detailed_informations/blob/master/img/aggregates_table.png)

## ⚙️ Run Pyspark via Dataproc
- Clone the project
Informations about: [Submit a job via Dataproc](https://cloud.google.com/dataproc/docs/guides/submit-job)

Command to run Pyspark via Cloudshell:
```bash
  $  gcloud dataproc jobs submit pyspark --jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar --cluster YOUR_CLUSTER_NAME --region REGION_NAME gs://PATH/TO/YOUR/FILE/MAIN.PY
```
Command to run Pyspark via Dataproc terminal:
```bash
  $  spark-submit --jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar home/PATH/TO/YOUR/FILE/MAIN.PY
```

_______________________________________________________________________
## ⚙️ Run Pyspark locally
If you want to run Pyspark locally:
- Clone the project
- Download bigquery jar and put it in project directory
- Replace jar localisation to your localisation of jar file
- Go to the app folder in project directory:
Type in CLI:
```bash
  $  spark-submit --jars path/to/file/spark-3.1-bigquery-0.28.0-preview.jar --files main.py --job airports_job
```
