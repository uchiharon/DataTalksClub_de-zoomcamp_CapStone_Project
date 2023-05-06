# POWERING AMERICA: Analyzing the Latest Trends and Insights from the EIA-923 Report
---
# OUTLINE
I. Introduction \
II. Data Pipeline \
III. Exploratory Data Analysis \
VI. Pipeline automation
VIII. Acknowledgments

---
# INTRODUCTION

---
## Background and Motivation
The US electric power industry is undergoing a significant transformation as it strives to meet the increasing demand for electricity while simultaneously reducing greenhouse gas emissions. The US Energy Information Administration (EIA) collects and publishes data on energy production, consumption, and distribution in the United States. The [EIA-923 report](https://www.eia.gov/electricity/data/eia923/) is a critical source of data for the electric power industry, providing detailed information on electricity generation, fuel consumption, and other key metrics. However, the data contained in the report is complex and challenging to work with, making it difficult to extract insights and useful information.
> The motivation for this project is to create an ETL pipeline that will enable the efficient processing and analysis of the EIA-923 data. By automating the extraction, transformation, and loading of the data, we can streamline the data processing workflow and make it easier to generate insights and actionable information.

---
## Objective
Specifically, the project aims to address the following key objectives:

- Extract relevant data elements from the EIA-923 report and transform them into a structured format that is suitable for analysis and modeling.
- Standardize the data and ensure that it is consistent, accurate, and complete.
- Load the transformed data into a data warehouse or database that can be easily queried and analyzed.
- Develop a user-friendly interface for accessing and querying the data, enabling users to generate reports and visualizations that can support decision-making.
- Provide quick insight on the dataset \
> Overall, our project aims to make it easier for researchers, policymakers, and businesses to access and analyze the EIA-923 data, which is critical for understanding the US electric power industry and developing strategies to optimize energy production and distribution. By providing a standardized and streamlined data processing workflow, we hope to make it easier for stakeholders to generate insights and actionable information that can support their work.

---
## Methodology and Tools used

> This project involves the use of various tools and resources to build an ETL pipeline, containerize it, and model data for visualization.

## Tools and Resources Used
1. Terraform
2. GCP compute engine virtual machine
3. GCP cloud storage
4. GCP bigquery
5. GCP looker studio
6. Docker
7. Python
8. Prefect 
9. DBT

## Methodology

1. **Infrastructure Setup**: Use Terraform to build GCP VM, Cloud Storage and BigQuery resources.

2. **Environment Setup**: Install Docker, Python and the required libraries on the VM.

3. **ETL Pipeline**: Build an ETL pipeline using Python with the following steps:
    1. Download the data from EIA website.
    2. Store the data locally.
    3. Extract and read the necessary workbook into a Pandas DataFrame.
    4. Transform the data by converting each sheet to a Pandas DataFrame and formatting their data types.
    5. Store the transformed data as Parquet files and save locally.
    6. Upload the Parquet files to Google Cloud Storage.

4. **Orchestration**: Use Prefect to orchestrate the ETL pipeline.

5. **Containerization**: Containerize the ETL file using Docker.

6. **Deployment**: Use the Docker file to build a Prefect deployment.

7. **Data warehouse**: Create external tables in BigQuery using the Parquet files in cloud storage. No clustering but just partitioning. Partitioning took place after data modeling in DBT.

8. **Data Modeling**: Use DBT to model the tables. The model table where then partition by the years columns since the analysis is expect to be filtered annually.

9. **Data Visualization**: Visualization of the data was done using Looker. You can access it using this [link](https://lookerstudio.google.com/embed/reporting/c01535e2-f95f-4990-80f7-c55dc65f75fc/page/qgR)

10. **Start-up Script**: Create a start-up script for the VM to run the Prefect agent on start-up.

11. **Automation**: Automate the entire workflow using schedules and email notifications on Prefect.

---
## Overview of the data sources and formats 

> The EIA (Energy Information Administration) electric data is a public data source provided by the U.S. government. The data consists of electricity production, consumption, and other related metrics at various geographic levels such as state and region. The data is available in various formats including CSV, Excel, and JSON. For this project, the data was downloaded from the EIA website in Excel format.

> The Excel file contains multiple worksheets with each worksheet containing data for a specific metric such as net generation, consumption, and capacity. The data is arranged in tabular form with columns representing the different categories and rows representing time periods such as years or months.

> To extract the data from the Excel file, the pandas library was used to read the data into a pandas DataFrame. The data was then transformed to remove unnecessary columns and rows, and the data type of the columns was formatted to ensure consistency. Finally, the transformed data was saved as a Parquet file and uploaded to Google Cloud Storage.

---
# DATA PIPELINE
---
## Architecture
> ![png1](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/Project%20workflow.png)

---
# DATA EXPLORATION
---
## Analytics Dashboard
To get an insight of the electrics consumption and production in the united states.
> ![png2](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/looker.png)
---
# PIPELINE AUTOMATION
---
This starts with the VM automation
> ![png3](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/VM%20schedule.png)

And using a start up script to run prefect agent 
```
        #!/bin/bash
        pip --version
        python3 --version
        export PATH=/home/ubuntu/anaconda3/bin:$PATH
        pip --version
        python3 --version

        echo $USER



        # login
        prefect cloud login --key key --workspace workspacename

        # Start Prefect agent
        prefect agent start --work-queue "default"
```

Next is to schedule prefect to run 5 mins after the VM starts
> ![png4](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/Prefect%20Scheduling.png)

There after BiqQuery is set to refresh the external tables
> ![png5](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/External%20table%20refresh.png)

DBT is expected to run just after the external tables are run.
> ![png6](https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project/blob/main/Pictures/DBT%20Schedule.png)


*The VM is expected to shutdown 30mins after running*

---
Reproduce
---
Prequist: Ensure you have Google cloud, DBT, Prefect Cloud account
To run the project, use the following step:\
- On GCP create a service account with with GCE, GCS and BiqQuery admin previllage
- Create a VM with machine type `n1-standard-1` in `europe-west1` region
- Setup the VM [link](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13&pp=iAQB):
    - Install Anaconda using the following steps:
        - Download anaconda using `wget https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh` or the latest version from this [link](https://www.anaconda.com/download#downloads)
        - `bash Anaconda3-2023.03-1-Linux-x86_64.sh`
    - Install Docker and create a user by using the following steps:
        - `sudo apt-get update`
        - `sudo apt-get install docker.io`
        - `sudo groupadd docker`
        - `sudo gpasswd -a $USER docker` 
        - `sudo docker service restart`
- Restart VM       
- Clone this repo using: `git clone https://github.com/uchiharon/DataTalksClub_de-zoomcamp_CapStone_Project.git`
- Install terraform following the instruction in this [link](https://phoenixnap.com/kb/how-to-install-terraform)
- Navigate to the [2_terraform]() folder, then from your CLI run:
    - `terraform init`
    - `terraform plan`
    - `terraform apply`
- On your prefect cloud, create the following buckets:
    - Docker Container
        - name: `eia-etl-container`
        - image: `emmanuelikpesu/eia_etl:v01`
        - imagepullpolicy: `ALWAYS`
    - GCP Credentials
        - name: `zoom-gcp-creds`
        **INPUT GCP Credentials**
     - GCS Bucket
        -name: `zoom-gcs`
    - JSON
        -name: `excel-sheet-schema`
        - **NOTE**: Copy the information from the [excel file setting]() into it
    - String
        -name: `report-year`
        -input: 2014 (for start)
- Navigate to the [4_deployment]() folder, then run `python docker_deployment.py` to deploy the prefect workflow
- Run  `prefect agent start --work-queue "default"` on your VM to execute a prefect agent
- From prefect cloud, run the workflow
- To create the external table of the parquet files in Bigquery, copy the sql code the [6_bigquery]() folder, paste it on the console and run.
- **Finally**: To run the dbt model, copy all files from the [5_dbt]() folder and run the deployment.


---
# Acknowledgments
---

I would like to extend my heartfelt thanks and gratitude to [DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp) for their exceptional data engineering program. Their comprehensive and hands-on approach to teaching data engineering has been invaluable in helping me acquire the skills and knowledge needed to excel in my career.

The program was well-structured and provided a clear roadmap for learning, starting from the basics and progressing to advanced concepts. The instructors were knowledgeable, experienced, and supportive, and the course materials were well-designed and easy to follow.

I particularly appreciated the opportunity to work on real-world projects and gain practical experience in data engineering. The program has equipped me with the necessary skills to tackle complex data engineering challenges and has prepared me for success in the field.

Once again, I extend my sincere thanks and appreciation to [DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp) for their outstanding data engineering program.
