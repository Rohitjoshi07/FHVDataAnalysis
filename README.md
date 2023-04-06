# For Hire Vehicle Data Analysis: Data Engineerin Project
This repo contains the final project implemented for the [Data Engineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) course.

## Introduction
The aim of this project is to analyse the hire-taxi data in newy york city for 2022. The project is developed intended to analyse the FHV(For hi
re-vehicle) data and to get the answer of few questions as below:
1) Different providers offering services in tax-hiring and their market share?
2) Distribution of taxi hiring based on each month of 2022 and drill down based on service providers?


## Dataset
The [NYC Taxi For Hire Vehicle(FHV) Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset is used. This dataset is updated monthly.

Details include information about the time, location and descriptive categorizations of the trip records for FHV taxi high volume data. To know more about the dataset click [Here](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf).

## Tools

The following components were utilized to implement the required solution:
* Data Ingestion: Data extracted using python requests module using [NYC Taxi data internal API](https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet)
* Infrastructure as Code: Terraform
* Workflow orchestration: Airflow
* Data Lake: Google Cloud Storage
* Data Warehouse: Google BigQuery
* Data Transformation: Spark via Google Dataproc
* Reporting: Google Looker Studio

### Architecture
![](images/architecture.png)


## Dashboard

![](images/report.png)

It can also be viewed at this [link](https://lookerstudio.google.com/s/tslbaH39mBY).
