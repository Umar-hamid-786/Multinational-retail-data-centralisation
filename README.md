# multinational-retail-data-centralisation406
## Table of Contents, if the README file is long

- [Overview of project](#overview)
- [Project Aim](#project-structure)
- [Project structure](#project-structure)
- [What did I learn and what I found challenging](#challenge)
- [Installation and Usage instructions](#installation)
- [Future Enhancements](#future-enhancements)
- [License information](#license)



## Overview of project

### Project Problem
The project aims to tackle a problem faced by a multinational company that sells various goods across the globe. However, since their sales data is spread across many different data sources, the company data sources are not easily accessed by company employees for analysis. In an effort to become more data-driven, the company would like to make its sales data accessible from one centralised location. 

### Project Aim
 The project aims to centralize and standardize sales data from multiple sources into a single PostgreSQL database. This database serves as the single source of truth for the company's sales data, enabling easy access and analysis for data-driven decision-making. The project involved data extraction, cleaning, transformation, and loading into a structured database, followed by querying to generate key business metrics.

## Project structure

### 1. Data Extraction, Cleaning and Uploading. 
Data was sourced from multiple platforms and formats, including databases from AWS through S3, PDF files, and APIs.
A data cleaning process was implemented to handle missing values, inconsistent formats, and duplicate entries. Each table was unique in their mistakes, hence various cleaning techinques were applied when necessary.

### 2. Data Loading
Cleaned and structured data was loaded into the PostgreSQL  database 'Sales_data'. A PostgreSQL database was chosen for its robustness and scalability.

### 3. Database Setup and Schema Design
The database schema was carefully designed to normalize the data and ensure data integrity. Key tables include:
Dim_users: Contains records of all users
Dim_stores: Details of each store, including location and contact information.
Dim_products: Information about the products sold, including categories and prices.
Dim_card_details: Data on customers card details. 
Dim_date_times: Data on every purchase
Orders_table: A single source of truth to create a relation between every table listed for analysis. 
Each table has been designed with appropriate data types to ensure efficient storage and querying.

### 4. Data Analysis and Querying
The centralized database allows for comprehensive querying to derive insights and metrics. For example:
Number of stores and their respective countries?
Which months produced the largest amount of sales?
How many sales are from web?
Percentages of sales from each store type?
Which month produced the highest amount of sales?
Maximum staff headcount?
Which store sells the most in a particular region?
How quickly are consecutive sales?

## What did I learn and what I found challenging

### Simple Data Engineering
I built my first fundemental ETL pipeline wherein I extracted data from various sources, cleaned them using pandas and various other libaries and finally loaded them into a relational database. The most difficult part was setting up the engine to connect Vscode to Postgresql as many errors occured in this process, namely connecting the database itself. This gave me insight into data engineering practices and techinques and have become more confident in creation of future ETL pipelines.

### SQL

I also improved upon my current SQL knowledge and how to deal with more difficult queries, namely using JOIN and GROUP BY commands. I have become more confident in using most commands in SQL. 

### Python

My ability to debug certain code in python i.e., using error handling within my code, applying file handling code and using various python commands has increased before this project.

# #Future Enhancements
Data Visualization: Integration with BI tools like Tableau or Power BI for visual data analysis.
Automated ETL Pipeline: Automating the extraction, transformation, and loading processes to update the database with new data regularly.
Advanced Analytics: Implementing predictive analytics and machine learning models to forecast sales trends and customer behavior.

## Installation and Usage instructions

Download the files from GITHUB and the files into a single folder which can then be opened in VSCODE. 
Run code from the main files, which has imported every other file. 
Ensure that the correct database is connected and ensure that you have correctly inputted the correct database details. 
Once you have the tables in postgresql, run the SQL files individually to cast the correct data types to the table columns. 

## File structure of the project
The file structure include .py, .csv, .yaml, .json and finally .sql. 

## License information

Apache License 2.0.

