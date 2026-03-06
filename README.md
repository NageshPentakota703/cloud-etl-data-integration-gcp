**Cloud ETL & Data Integration Solution on Google Cloud:**\
End-to-end cloud data integration pipeline built on Google Cloud Platform (GCP) to ingest, transform, and analyze large-scale structured datasets.
This project demonstrates the design and implementation of a cloud-based ETL architecture integrating weather data with large-volume traffic violation datasets to support structured analytics and reporting.

**Project Overview:**\
Organizations often collect data from multiple sources but struggle to integrate them into a unified system for analytics.
This project builds a cloud-based ETL pipeline to:
- Ingest structured datasets from multiple sources
- Transform and clean raw datasets
- Load integrated data into a relational cloud database
- Enable SQL-based analytical queries
The solution was implemented using Google Cloud services and Python-based ETL workflows.

**Architecture Overview:**\
The project architecture consists of four primary layers:

*Data Sources*
- Weather dataset (Excel)
- Washington DC moving violations dataset (CSV files, 1.7M+ records)

*Compute Layer*
- Google Compute Engine Virtual Machine
- Python ETL scripts

*Data Storage Layer*
- Google Cloud SQL (MySQL)

*Analytics Layer*
- SQL queries for analytical insights

**Architecture Diagram:**
<img width="2220" height="1021" alt="image" src="https://github.com/user-attachments/assets/5e2faf57-2609-42c6-8562-7a1efa1162f1" />

*Example:*\
Weather Data + Moving Violations CSV -> Python ETL (Compute Engine VM) -> Google Cloud SQL (MySQL) -> SQL Queries & Analytics

**Technology Stack:**

*Cloud Platform*
- Google Cloud Platform (GCP)

*Infrastructure*
- Google Compute Engine
- Google Cloud SQL (MySQL)

*Programming*
- Python

*Data Processing*
- ETL Pipelines
- Data Cleaning & Transformation

*Analytics*
- SQL Queries

**Dataset Description:**\
*Two datasets were integrated:*

*Weather Dataset*
- Daily weather records, ~450 rows, Excel format

*Moving Violations Dataset*
- Traffic violations data, Monthly CSV files, 1.7M+ records\
The datasets were integrated using date relationships to enable combined analysis.

**ETL Pipeline:**\
*The ETL pipeline performs the following steps:*

*Extract*
- Read weather data from Excel files
- Read moving violation data from CSV files

*Transform*
- Standardize column formats
- Convert date fields
- Clean missing values
- Remove duplicate records

*Load*
- Upload transformed datasets to Google Cloud SQL
- Use chunk-based loading to efficiently process large files

**Data Validation:**

*To ensure data accuracy:*
- Row counts were validated after loading
- Duplicate prevention implemented through primary keys
- Data consistency verified through SQL queries

**Sample Analytical Queries:**

*Example analyses performed:*
- Number of traffic violations by agency
- Average number of tickets by weekday
- Average violations by hour of day
- Ticket trends during rainy days
- Average fine amount for speeding violations

The SQL queries used for these analyses are available in the /sql folder.

**Screenshots:**

- *VM Machine*
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/b2b9d355-70f5-476e-a061-96a6e7956772" />

- *Transforming & Loading the Data into Google Cloud SQL ( MySql )*
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/759dd2f8-5d81-4cec-8b47-35d437bb62c7" />

- *SQL Query Performed in MySql*
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/7c0d6bfc-73a4-4d7e-a360-2db1c404f758" />

- *SQL Query Performed in MySql*
<img width="1800" height="900" alt="image" src="https://github.com/user-attachments/assets/6f7b88d3-3a1b-4e01-b4d3-fe7f5a51aafd" />

**Key Learning Outcomes:**

*This project demonstrates practical experience with:*
- Cloud data architecture design
- Building ETL pipelines for large datasets
- Cloud SQL database design
- Data integration across multiple datasets
- SQL-based analytical workflows
- Deploying data solutions on Google Cloud

**Future Improvements:**

*Potential enhancements include:*
- Automating ETL workflows using scheduling tools
- Migrating analytics workloads to BigQuery
- Implementing real-time data ingestion pipelines
- Building dashboards using BI tools such as Looker or Tableau

**Author:**\
Nagesh Pentakota\
Master of Business Analytics — University of Dayton\
LinkedIn: https://www.linkedin.com/in/nagesh-pentakota/
