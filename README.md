## 👨‍💻 Built with
<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" /> <img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/> <img src="https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white" /> <img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" /> <img src="https://img.shields.io/badge/Numpy-777BB4?style=for-the-badge&logo=numpy&logoColor=white" /> 
<img src="https://miro.medium.com/max/1400/1*5C4UQznqEiN3D6Xutlgwlg.png" width="100" height="27,5" />
<img src="https://cdn-images-1.medium.com/max/1000/1*-7Ro7fO__wwWz0iL9tucHQ.png" width="100" height="27,5" />
<img src="https://www.devagroup.pl/blog/wp-content/uploads/2022/10/logo-Google-Looker-Studio.png" width="100" height="27,5" />
<img src="https://www.scitylana.com/wp-content/uploads/2019/01/Hello-BigQuery.png" width="100" height="27,5" />

##  Descripction about project

### ℹ️Project info

This project is created to catch information about reported covid infections in Poland. 

Getting information from government site: [gov.pl - Zakażenia z powodu COVID-19](https://dane.gov.pl/pl/dataset/2582/resource/41901/table).

The aim of the project was to use pyspark to process data from a csv file.
Before using pyspark, data analysis was done using jupyter notebook.
For analysing data was created container with jupyter notebook.
Pyspark was launched locally. 
Pyspark was used to get the sum of infections reported on a given day for the voivodship and the poviat.
The obtained data were loaded into a table in google bigquery and then visualized using Looker studio.
In addition, the Pyspark application received tests that were written in pytest

## 🗒️Table of COVID aggregates:
![IMG SCHEMA](https://i.ibb.co/jHQb0xN/Zrzut-ekranu-2023-02-19-o-23-33-31.png)
![IMG TABLE](https://i.ibb.co/0htdfYv/Zrzut-ekranu-2023-02-19-o-23-32-13.png)

## 🌲 Project tree
```bash
.
├── Dockerfile # docker file to create container image
├── requirements.txt # requirements to create image
├── README.md
├── analyse_COVID_infections.ipynb # jupyter notebook with analyse
└── app
    ├── bigquery_connection
    │   ├── credentials.json # your credential key from bigquery should be here
    │   └── spark-3.1-bigquery-0.28.0-preview.jar # you need to put jar file here
    ├── config.json # informations about project
    ├── conftest.py
    ├── data
    │   └── covid_infections.csv
    ├── jobs
    │   ├── __init__.py
    │   └── covid_aggregates.py # pyspark job to count aggregates
    ├── main.py
    └── tests
        ├── pytest.ini
        └── test_aggregation_infections_per_day.py # tests for pyspark application

```
## 🔑 Setup 

To run properly this project you should set variables in files: 
### ./app/config.json:
{
  "app_name": "YOUR_APP_NAME",
  "source_data_path": "./data/covid_infections.csv", # download CSV file from link in Project info and paste it here
  "source_credentials": "bigquery_connection/credentials.json", # you should put your json key here and name it 'credentials.json'
  "bigquery_table_path": "BIGQUERY-ID-PROJECT.DATASET_NAME.TABLE_NAME",
  "bigquery_table_path_short": "DATASET_NAME.TABLE_NAME",
  "bigquery_project": "BIGQUERY-ID-PROJECT"
}

## ⚙️ Run analyse container:

Build image:
```bash
  $ docker build -t covid_analyse .
```

Run container:
```bash
  $ docker run --name covid_analyse -p 8888:8888 covid_analyse
```

##  📊Data visualisation
**To start notebook you should type in your browser:**
```bash
  localhost:8888
```

Next choose a file 🗒️analyse_COVID_infections.ipynb and run all cells to see data analyse.

Examples of charts:
- Infections per day;
- Total infections per month;
- Total infections per age category;
- Infections divided by age categories;


## ⚙️ Run Pyspark locally
- Clone the project
- Go to the app folder in project directory:
Type in CLI:
```bash
  $  spark-submit --jars bigquery_connection/spark-3.1-bigquery-0.28.0-preview.jar --files config.json main.py --job covid_aggregates
```
## ⚙️ Run Pyspark via Dataproc
[Submit a job via Dataproc](https://cloud.google.com/dataproc/docs/guides/submit-job)

## 🔎 Looker Studio
Link to generated report in looker for Covid epidemic in Poland:

[Covid empidemic in Poland - years 2021-2022](https://lookerstudio.google.com/reporting/2be2f844-a75d-40b8-bf1f-cc44913e3a87)
![IMG LOOKER](https://i.postimg.cc/52F9rkD5/Zrzut-ekranu-2023-03-9-o-17-31-55.png)
