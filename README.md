# POWERING AMERICA: Analyzing the Latest Trends and Insights from the EIA-923 Report
---
# OUTLINE
I. Introduction \
II. Data Extraction \
III. Exploratory Data Analysis \
VI. Results and Discussion \
A. Key Findings and Insights from the Analysis \
B. Interpretation of the Predictive and Optimization Model Results \
C. Comparison of Results with Industry Trends and Benchmarks

VII. Conclusion and Future Work \
A. Summary of the Capstone Project and its Objectives \
B. Contributions and Implications of the Capstone Project \
C. Future Directions and Potential Extensions

VIII. References and Acknowledgments

---
# INTRODUCTION

---
## Background and Motivation
The US electric power industry is undergoing a significant transformation as it strives to meet the increasing demand for electricity while simultaneously reducing greenhouse gas emissions. The US Energy Information Administration (EIA) collects and publishes data on energy production, consumption, and distribution in the United States. The EIA-923 report is a critical source of data for the electric power industry, providing detailed information on electricity generation, fuel consumption, and other key metrics. However, the data contained in the report is complex and challenging to work with, making it difficult to extract insights and useful information.
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

7. **External Tables**: Create external tables in BigQuery using the Parquet files.

8. **Data Modeling**: Use DBT to model the tables.

9. **Data Visualization**: Visualize the modeled data using Looker.

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
