# Loan modelling
Demo of creditworthy assessment models for FinTech companies.

## Description
This project focuses on examplary workflow of creditworthy assessment models. It is implemented using Apache Airflow, Docker, python and postgresql as tech stack. The core of the demo is a singular DAG utilizing DockerOperator and project-specific docker images published on DockerHub. Their blueprints are available in `/docker-images` folder.

Processed data is divided into two star schemas - train and test. Each schema contains the same tables meant for test and train data coming from Kaggle repository. 

## Data
Test data is fetched from [Kaggle repository](https://www.kaggle.com/datasets/vikasukani/loan-eligible-dataset). 

## Project structure
- `airflow` - folder containing Airflow assets
- `docker-images` - folder containing blueprints for the images used in the original dag and published on dockerhub
- `docker-compose.yaml` - original Airflow-provided local compose of Airflow infra. Expanded with one more postgresql container to serve as data lake and data warehouse.

- `postgres.env.example` - postgres credentials config example
- `kaggle.json.example` - kaggle credentials config example
- `_kubernetes` - folder holding an attempt in deploying a local airflow cluster on minikube. It also containes a version of the dag using KubenetesPodOperator instead of DockerOperator (which is used in the original one under `airflow` folder).

## Docker images used in the main DAG
All images are placed in `/docker-images` folder. All of them follow the concept of copy-pasting the source code and additional env files into the container and execute appropriate scripts on container start.

#### h4sio/database_creator3.1
Creates necessary database, schema, tables and their definitions with SQLAlchemy. Precisely - a schema for "test" and "train" with the same tables definitions within it.

#### h4sio/dataset_puller3.1
Downloads Kaggle dataset and temporarily saves it. Adds a timestamp to the pulled dataset to allow incremental ingestion. Timestamp is passed between tasks through Xcom.

Raw dataset is ingested into simplistic train and test data lakes - default, public schema of the same postgres instance. 

#### h4sio/data_modifier3.1
Script ingesting data from the lakehouse into the dedicated schemas in test and train warehouse tables in postgresql instance. It is meant as the transformation step, where column names are unified, duplicates dropped, data types optimized. Most importantly it normalizes the raw data in dimenstion and fact tables. Populates 3 dimension tables and one fact table per schema and connects them with foreign keys. 

## DAG
#### loan_predictor.py
Main DAG mimicking the whole demo workflow with Docker operators. 
1. Sets up a database programmatically. ([database_creator](#h4siodatabase_creator31))
2. Downloads two CSV files from Kaggle using a Dockerized Python script and saves the results to postgresql lake. ([dataset_puller](#h4siodataset_puller31))
3. Transforms the data into a star schema. ([data_modifier](#h4siodata_modifier31))
4. Writes the data to the database into train and test tables. ([data_modifier](#h4siodata_modifier31))

## Configs
- Additional kaggle config to use the SDK for pulling the dataset.
- Additional postgresql config to set a predefined user.
