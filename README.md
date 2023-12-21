# Airflow Spotify Project

This project establishes an ETL pipeline designed to extract data from my recently played Spotify songs and store them in a storage system for subsequent analysis. It was developed for educational purposes, providing a hands-on experience with fundamental Data Engineering tools.


## Technologies Used
- **Apache Airflow**
- **Python**
- **Docker**
- **Linux**
- **Git**

# Project Details
In more detail, this project utilizes the ETL pipeline to extract my recently played songs through the Spotify API, transform the data, and incorporate it into a Postgres SQL database functioning as a Data Warehouse.

To facilitate this process, we employed Airflow as a data flow orchestrator tool. To run this locally, Linux is required, and we use the Docker Desktop application for Windows. This application runs a Linux Subsystem on Windows, enabling the building and deployment of any application in an environment.

### Docker Setup

To establish a multi-container environment, we create a docker-compose.yml file to define the settings for each container:

- postgres: This container comprises a Postgres database (based on a predefined Postgres Docker image) with specific environment variable settings.
- Webserver: This container includes Airflow (based on a predefined Airflow image) and depends on the successful creation of the Postgres database.

### DAG Setup

The program consists of a DAG with two PythonOperator tasks that runs daily at night:

- Fetch_and_create_tables: Fetches the data from the last day through the Spotify API and creates tables based on a predefined schema if they don't already exist.
- Transform_and_load_data: Transforms the data according to analysis requirements and loads it into the Data Warehouse.

For the DAG to function properly, users need to configure the **authentication tokens**.

## Future Extensions

For future extensions, consider implementing a type 2 changing dimension in the Data Warehouse to save historical data. Additionally, incorporating an Airflow sensor to check Spotify API status before execution could enhance reliability. 
Furthermore, analysis using tools like PowerBI and Tableau, along with the implementation of Machine Learning or Deep Learning techniques, could lead to the development of a recommendation system using a comprehensive music dataset.
