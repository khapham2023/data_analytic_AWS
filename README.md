1/ Introduction
This is a typical Analytical Data Engineering Project. Data is ingested from several data sources and populated into the Snowflake data warehouse. In the wareahouse, after a couple of data transformation process i made the data ready for BI usage. Then the BI tool Metabase was connected to the data warehouse to generate different dashboards and reports. 

2/ About Data
This dataset is the dataset from TPCDS. TPCDS is a famous dataset for database testing. The business background of the dataset is Retail Sales. The data contains the sales records from website and Catalog. And also the inventory level of each item in each warehouse. In addition to these, there are 15 dimensional tables those contain the information of customer, warehouse, items, etc. This entire dataset is not stored in one place, instead, the datset was splited into 2 parts:
. RDS: all the tables except for the inventory tables were stored in the Postgre DB in AWS RDS. The tables will be refreshed everyday and updated with the newest data so for the sales data, so in order to get the newest data i need to run ETL process every day. 
. S3 bucket: The single Inventory table is stored in S3 bucket, every day there will be a new file contain the newest data dumped into the S3 bucket. However, be aware that the inventory table usually only recordthe inventory data at the end of each week.  
    
Please see the Readme.docx for more detail of the project
