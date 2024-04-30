
# An end-to-end data pipeline using Azure on Formula F1 dataset

The project is an application project focused on building an end-to-end data pipeline using Azure platform. The objective is to design, implement and optimize a robust architecture that ingests, process analyze and visualize Formula f1 Ergast dataset to derive meaningful insights.

## Data Source

The project utilizes Formula 1 racing data obtained from the Ergest Developer API. The API provides various csv files including circuits, races, constructors, drivers, results, pitstops, lap times, and qualifying data.

https://ergast.com/mrd/db/#csv


![image](https://github.com/psundari/spark_learning/assets/112127625/d44ec791-9efe-489b-945c-41265a91175d)


## Project Resources :

1. **Azure Data Lake Gen2**: Used for hierarchical storage.
2. **Azure Databricks**: Used for data ingestion, transformations & analysis using python & SQL
3. **Azure Data Factory**: Used for orchestrating and automating data flow.
4. **Azure Key vault**: To store the secret and access token for security purposes.
5. **PowerBI**: Used for data visualization and presentation purposes.

## Set-up and working :

#####  Creating resources:
1.	Create a azure datalake gen 2 storage named as **‘formula1dldatastorage’** and create 3 containers raw, processed and presentation.
2.	Place all 8 csv files extracted from eargast API in the raw container for now processed and presentation containers will be empty.
3.	Create a azure data bricks service with standard tier named **‘formulaf1db’** and launch the workspace and create a cluster with min of 2 to max of 8 nodes and 14GB memory and 4 cores.
4.	We connect databricks compute with blob storage using service principal and Azure Key vault services. For that, Register the Service Principal also referred to as Azure AD Application
5.	Now, create a secret for the Service Principal and assign the Storage Blob Data Contributor role on the Data Lake for the Service Principal that gives full access to the storage account.
6.	Create a Azure key vault service named **‘formula1-kv-keys’** and create 3 secrets i.e clientid, tenanted, clientsecret taken from azure service principal.
7.	After that create a scope for the databricks to link it with the azure key vault.
8.	Create a new notebook in databricks in set-up folder **mount_adls_storage**. This contains the code to coonect to the datalake storage using service principal accessing secrets through azure key vault. Now mount all 3 containers onto databricks in the location **‘mnt/formula1dldatastorage’**

<img width="942" alt="image" src="https://github.com/psundari/spark_learning/assets/112127625/8c0779f1-6572-453d-bcc0-92b71368dedb">


#### Ingestion of raw files:

1. Created 8 notebooks for ingesting 8 CSV files into Databricks.
2. Performed common operations like defining schema, selecting columns, and adding ingestion date
3. Run all the 8 files using a single file **ingest_all_file** which contains run commands of all 8 based on the sucess of previous one which produce processed data stored in the processed container as parquet format.

#### Transformations of Processed Files:

Implemented 3 notebooks for transforming processed files:
1. **Race Results**: Joined multiple DataFrames and saved final DataFrame in the presentation container.
2. **Driver Standings**: Grouped race results based on driver name, assigned ranks to drivers, and saved the DataFrame in the presentation container.
3. **Constructor Standings**: Grouped race results based on teams, assigned ranks to constructors, and saved the DataFrame.
4. First run race_results notebook which produce tarnsmored data which is saved in presentation container in parquet format with name **race_results**. 
5. Driver_standings and constructor_standings use race_results data from presentation container.

#### Vizualization:
1.	Once data is available in the presentation container, we created f1_presentation database and top of that created 3 tables namely **race_results,driver_standings** and **constructor_standings**.
2.	Then we imported these tables into power BI connecting through databricks using url and access token which lists all the available databases and schemas, then we load race_results data and start creating reports

#### Integration & Automation:

1. Created ADF service named ‘formula1-adf-datafactory’ and linked it with Databricks workspace.
2. Created 3 pipelines and a trigger for automation:
- **Ingest_all_pipeline**: Automatically runs all 8 notebooks to produce parquet files.
- **Transformation Pipeline**: Runs after successful completion of ingestion, transforming parquet files.
- **Process Pipeline**: Master pipeline orchestrating ingestion and transformation pipelines.
- **Azure Trigger**: Scheduled to run on every Sunday at 10 PM, triggering the process pipeline to ingest and transform data.










