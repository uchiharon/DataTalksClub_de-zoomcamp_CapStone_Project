import pandas as pd
import os
from pathlib import Path
from zipfile import ZipFile
from etl_web_to_gcs_setting import sheet_names


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

    try:
        # Open and unzip the dataset file
        df_zip = ZipFile(file_path)
        # Extract excel file
        unzip_file_path = Path(df_zip.extract(df_zip.filelist[0].filename,'data')).as_posix()
        # Close the open file to free up ram memory
        df_zip.close()
        # Delete the zipped file to free up disk space
    except:
        print('file does not exist')

    return unzip_file_path


def todo(input):
    df  = pd.read_excel("f923_2022/EIA923_Schedules_2_3_4_5_M_12_2022_21FEB2023.xlsx",sheet_name=input)
    return df.shape




def transform(unzip_file_path):

    try:
        file_sheets = pd.ExcelFile(unzip_file_path).sheet_names
        if file_sheets == sheet_names:
            check = {'Page 1 Generation and Fuel Data': todo('Page 1 Generation and Fuel Data'),
                        'Page 1 Puerto Rico': todo('Page 1 Puerto Rico'),
                        'Page 1 Energy Storage': todo('Page 1 Energy Storage'),
                        'Page 2 Stocks Data': todo('Page 2 Stocks Data'),
                        'Page 2 Oil Stocks Data': todo('Page 2 Oil Stocks Data'),
                        'Page 3 Boiler Fuel Data': todo('Page 3 Boiler Fuel Data'),
                        'Page 4 Generator Data': todo('Page 4 Generator Data'),
                        'Page 5 Fuel Receipts and Costs': todo('Page 5 Fuel Receipts and Costs'),
                        'Page 6 Plant Frame': todo('Page 6 Plant Frame'),
                        'Page 6 Plant Frame Puerto Rico': todo('Page 6 Plant Frame Puerto Rico'),
                        'Page 7 File Layout': todo('Page 7 File Layout')}
            for sheet in file_sheets:
                if sheet == 'Page 1 Generation and Fuel Data':
                    generation_and_fuel_data = todo(input)
                
    except:
        print('file does not exist')

    
    pass
