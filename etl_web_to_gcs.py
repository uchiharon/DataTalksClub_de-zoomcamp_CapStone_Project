import pandas as pd
import os
from pathlib import Path
from zipfile import ZipFile


def fetch(dataset_url: str) -> Path:
    """
        Read data from web as store to the local memory

        Args:
            dataset_url: str. This the url link of the data on EIA webpage
        Return:
            file_path: Path. The path of the downloaded file
    """
    # Extract file name form the url
    file_name = dataset_url.split('/')[-1]
    # Define the output file path
    file_path = Path(f'data/{file_name}').as_posix()
    # Fetch the data from EIA website
    os.system("wget {} -O {}".format(dataset_url, file_path))

    return file_path

def extract_zipFile(file_path: Path) -> Path:
    """
        The downloaded zip file is extract to get the excel file

        Args:
            file_path: Path. The path of the downloaded file
        Return:
            unzip_file_path: Path. The path to the extracted excel file
    """
    # Open and unzip the dataset file
    df_zip = ZipFile(file_path)
    # Extract excel file
    unzip_file_path = Path(df_zip.extract(df_zip.filelist[0].filename,'data')).as_posix()
    # Close the open file to free up ram memory
    df_zip.close()
    # Delete the zipped file to free up disk space

    return unzip_file_path

