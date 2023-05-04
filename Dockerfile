FROM prefecthq/prefect:2.7.7-python3.9

RUN  apt-get update 
RUN apt-get install -y wget

COPY 4_deployment/dockerfile-requirement.txt ./

RUN pip install -r dockerfile-requirement.txt


COPY 1_etl_to_gcs/etl_web_to_gcs.py /opt/prefect/flows/etl_web_to_gcs.py
COPY 1_etl_to_gcs/data /opt/prefect/data
COPY 1_etl_to_gcs/downloaded_data /opt/prefect/downloaded_data