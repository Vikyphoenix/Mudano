# Mudano
This repo is to have the code and relevant items for Mudano's assignment


Problem Satement:

As part of this assignment, you would write a Python script to retrieve publicly available
data and perform analysis on that data using SQL.

Publicly available data is available from the World Bank. One dataset which contains
information about countries is available through World Bank API and its documentation 1
provides all the necessary details on how to use it. Another dataset which includes Gross
Domestic Product (GDP) data in CSV format is available for download from World Bank 2
Data Catalog.

Objectives

1. Write Python script to retrieve countries’ data from a World Bank Country API as
well as GDP data from World Bank Data Catalog.
2. Store all data in a database; ideally PostgreSQL locally.
○ Model data in a way to be able to perform data analysis (see below).
3. Write SQL queries for Analysis as per requirement

1 https://datahelpdesk.worldbank.org/knowledgebase/articles/898590-api-country-queries (http://api.worldbank.org/v2/country?format=json)

2 https://datacatalog.worldbank.org/dataset/global-economic-prospects (./input/GEPData.csv)

-----------------------------------------------------------------------------------------------
Tasks completed as part of the Assignment:
------------------------------------------
BUILT A PYTON ETL APPLICATION
1. Pulled data from both the above data sources using Python (./scripts/pullDataSources.py)
2. Have performed cleansing and schema modification  so that the data can be stored in Postgres DB (./scripts/App.py)
3. Have loaded the pandas dataframes formed from the above datasets into Postgres (./scripts/pushToDb.py.py)
4. Performed required SQL analyis on top of the data stored in the Postgres Tables (./analysis/ANALYSIS.sql)

Value Adds:
-----------
1. Have transformed normalized the country data to store as a Datawarehouse architechture
so that the data can be used by downstream apps like BI dashboards in a better way
2. Created a schema diagram for reference to infer the tables created in Postgres (./analysis/postgres_schema_design.pptx)

Analysis
--------
Interesting facts from the dataset:

a. We can see a significant decrease in the GDP of many countries in 2020 due to COVID

b. The GDP dataset doesnot contain the data for all the countries in the Country dataset, So the corresponding analysis is done for the matching countries

c. With the data provided, I was able to build a small Datawarehouse having a STAR schema which can be used for easy integration with downstream applications

Challenges:

a. While reading the GCP CSV in pandas, it had some unknow column containing null values, so have to cleanse it.

b. Had to change schema as per naming standards so as to maintain proper schema in postgres and to insert data into the table.
Also could have directly created the table out of the pandas data frame but that would result in unconventional naming of tables and columns in the db.

c. In real time scenario, the  password  for the DB could be retrieved from a Key Management Store(KMS) in a secure way, also not using getpass() as it would not work in IDE. So, getting it as input from the command line as of now.

d. The GDP dataset has some missing GDP values which had to be filtered out for certain analysis.



