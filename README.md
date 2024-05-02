
# An end-to-end data pipeline using Azure on Formula F1 dataset

The project is an application project focused on building an end-to-end data pipeline using Azure platform. The objective is to design, implement and optimize a robust architecture that ingests, process analyze and visualize Formula f1 Ergast dataset to derive meaningful insights.

## Data Source

The project utilizes Formula 1 racing data obtained from the Ergest Developer API. The API provides various csv files including circuits, races, constructors, drivers, results, pitstops, lap times, and qualifying data.

https://ergast.com/mrd/db/#csv

Here is the ER diagram of dataset:

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
   <img width="950" alt="image" src="https://github.com/psundari/Formula-racing-project/assets/112127625/59710a06-e70e-498b-9a23-6c650d3c2175">

3.	Create a azure data bricks service with standard tier named **‘formulaf1db’** and launch the workspace and create a cluster with min of 2 to max of 8 nodes and 14GB memory and 4 cores.
4.	We connect databricks compute with blob storage using service principal and Azure Key vault services. For that, Register the Service Principal also referred to as Azure AD Application
   <img width="786" alt="image" src="https://github.com/psundari/Formula-racing-project/assets/112127625/1b6a47ba-7251-40fe-8931-ba38df3c8381">

5.	Now, create a secret for the Service Principal and assign the Storage Blob Data Contributor role on the Data Lake for the Service Principal that gives full access to the storage account.
   <img width="577" alt="image" src="https://github.com/psundari/Formula-racing-project/assets/112127625/a5eb007a-0faa-4dd0-b850-53aef6287a5b">

6.	Create a Azure key vault service named **‘formula1-kv-keys’** and create 3 secrets i.e clientid, tenanted, clientsecret taken from azure service principal.
7.	After that create a scope for the databricks to link it with the azure key vault.
8.	Create a new notebook in databricks in set-up folder **mount_adls_storage**. This contains the code to coonect to the datalake storage using service principal accessing secrets through azure key vault. Now mount all 3 containers onto databricks in the location **‘mnt/formula1dldatastorage’**

<img width="942" alt="image" src="https://github.com/psundari/spark_learning/assets/112127625/8c0779f1-6572-453d-bcc0-92b71368dedb">


#### Ingestion of raw files:

1. Created 8 notebooks for ingesting 8 CSV files into Databricks.
2. Performed common operations like defining schema, selecting columns, and adding ingestion date
3. Run all the 8 files using a single file **ingest_all_file** which contains run commands of all 8 based on the sucess of previous one which produce processed data stored in the processed container as parquet format.

<img width="398" alt="image" src="https://github.com/psundari/spark_learning/assets/112127625/e6eaa562-6072-4ea2-876d-7353f5be22cf">


#### Transformations of Processed Files:

Implemented 3 notebooks for transforming processed files:
1. **Race Results**: Joined multiple DataFrames and saved final DataFrame in the presentation container.
2. **Driver Standings**: Grouped race results based on driver name, assigned ranks to drivers, and saved the DataFrame in the presentation container.
3. **Constructor Standings**: Grouped race results based on teams, assigned ranks to constructors, and saved the DataFrame.
4. First run race_results notebook which produce tarnsmored data which is saved in presentation container in parquet format with name **race_results**. 
5. Driver_standings and constructor_standings use race_results data from presentation container.

   <img width="396" alt="image" src="https://github.com/psundari/spark_learning/assets/112127625/dacfdecb-4c7a-4a84-b6d3-dcf51ea98001">


#### Vizualization:
1.	Once data is available in the presentation container, we created f1_presentation database and top of that created 3 tables namely **race_results,driver_standings** and **constructor_standings**.
2.	Then we imported these tables into power BI connecting through databricks using url and access token which lists all the available databases and schemas, then we load race_results data and start creating reports

   <img width="635" alt="image" src="https://github.com/psundari/spark_learning/assets/112127625/b8ad25ea-cfed-4664-9308-09ccebd52ff7">


#### Integration & Automation:

1. Created ADF service named ‘formula1-adf-datafactory’ and linked it with Databricks workspace.
2. Created 3 pipelines and a trigger for automation:
- **Ingest_all_pipeline**: Automatically runs all 8 notebooks to produce parquet files.
- **Transformation Pipeline**: Runs after successful completion of ingestion, transforming parquet files.
- **Process Pipeline**: Master pipeline orchestrating ingestion and transformation pipelines.
- **Azure Trigger**: Scheduled to run on every Sunday at 10 PM, triggering the process pipeline to ingest and transform data.

  ![image](https://github.com/psundari/spark_learning/assets/112127625/09330215-cb3a-4309-8a4f-cdaf588e3197)

  ## Evaluation:
  we evaluate the project based on following criteria.
  1. Insights Generated
     - Driver Rankings
     - Constructor Rankings
  3. Data Processing Efficiency
     












