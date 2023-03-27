import pandas as pd
import os
from pathlib import Path
from zipfile import ZipFile
import excel_file_settings as efs
import re

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

import unittest

#############################################################################################################
"""
    Building workflow functions
"""
#############################################################################################################



def fetch(dataset_url: str) -> Path:
    """
        Read data from web as store to the local memory

        Args:
            dataset_url: str. This the url link of the data on EIA webpage
        Return:
            file_path: Path. The path of the downloaded file
    """
    print('Downloading data...\n')
    # Extract file name form the url
    file_name = 'output.zip'
    # Define the output file path
    file_path = Path(f'downloaded_data/{file_name}').as_posix()
    # Fetch the data from EIA website
    os.system("wget {} -O {}".format(dataset_url, file_path))
    print('Download completed.\n')

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
        print('Extracting zip file...\n')
        # Open and unzip the dataset file
        df_zip = ZipFile(file_path)
        # Get list of the name of files in the zip file
        fileNames_in_zipFile = df_zip.namelist()

        # Search for the desired file
        r = re.compile('.*EIA923_Schedules_2_3_4_5_*')
        desired_file_in_zipFile = list(filter(r.match, fileNames_in_zipFile))[0]
        

        # Extract the desired file from the zip file
        unzip_file_path = Path(df_zip.extract(desired_file_in_zipFile, 'downloaded_data')).as_posix()
        
        # Close the open file to free up ram memory
        df_zip.close()
        
        print('Zip file successfully extracted.\n')

    except:
        print('file does not exist\n')

    return unzip_file_path



def table_reader(unzip_file_path:Path, input:str) -> pd.DataFrame:
    """
        This help read the data, set the column, remove unwanted rows and format the columns name

        Args:
            unzip_file_path: Path. The path to the extracted excel file
            input: str. This is the name of the sheet to be read
        Return:
            df: pd.DataFrame. This is the excel table stored as a pandas dataFrame

    """

    print(f'Reading excel sheet {input}...\n')
    df  = pd.read_excel(unzip_file_path,sheet_name=input)
    print('Successfully read data.')

    print('Preprocessing data...')
    desired_columns = {'Page 1 Generation and Fuel Data': efs.sheet_0_columns ,
                        'Page 1 Puerto Rico': efs.sheet_1_columns ,
                        'Page 1 Energy Storage': efs.sheet_2_columns ,
                        'Page 2 Stocks Data': efs.sheet_3_columns,
                        'Page 2 Oil Stocks Data': efs.sheet_4_columns,
                        'Page 2 Coal Stocks Data': efs.sheet_5_columns,
                        'Page 2 Petcoke Stocks Data': efs.sheet_6_columns,
                        'Page 3 Boiler Fuel Data': efs.sheet_7_columns,
                        'Page 4 Generator Data': efs.sheet_8_columns,
                        'Page 5 Fuel Receipts and Costs': efs.sheet_9_columns,
                        'Page 6 Plant Frame': efs.sheet_10_columns,
                        'Page 6 Plant Frame Puerto Rico': efs.sheet_11_columns                       
                        }
    
    # Locate the row where the columns name is stored
    column_row_index = int(df[df.apply(lambda row: row.tolist()[0] == desired_columns[input][0], axis=1)].index[0])

    # Change the dataFrame columns names
    df.columns = list(df.iloc[column_row_index])
    
    
    # Drop the row that contains the columns name and the columns above it
    df.drop(df.index[:column_row_index + 1], inplace=True)
    

    # Select only the columns we desire
    df = df[desired_columns[input]]

    # Format columns name
    df.columns = [i.replace('\n','_').replace(' ','_').lower() for i in df.columns]
   
    # Set dataFrame data type to be str
    df = df.astype(str)


    print('Complete preprocessing.')
    print('table reader job completed successfully\n')

    return df



def transform(unzip_file_path) -> pd.DataFrame:
    """
        Transform all table using the table_reader function as a sub-task

        Args:
            unzip_file_path: Path. This location where the unzipped file is stored
        Return:
            list of dataFrames
    """

    try:
        print('Initiate data transformation...\n')
        file_sheets = pd.ExcelFile(unzip_file_path).sheet_names
        if len(file_sheets) == 13:
            print('Okay file name')
            df = {'Page 1 Generation and Fuel Data': table_reader(unzip_file_path,'Page 1 Generation and Fuel Data'),
                        'Page 1 Puerto Rico': table_reader(unzip_file_path, 'Page 1 Puerto Rico'),
                        'Page 1 Energy Storage': table_reader(unzip_file_path, 'Page 1 Energy Storage'),
                        'Page 2 Stocks Data': table_reader(unzip_file_path, 'Page 2 Stocks Data'),
                        'Page 2 Oil Stocks Data': table_reader(unzip_file_path, 'Page 2 Oil Stocks Data'),
                        'Page 2 Coal Stocks Data': table_reader(unzip_file_path, 'Page 2 Coal Stocks Data'),
                        'Page 2 Petcoke Stocks Data': table_reader(unzip_file_path, 'Page 2 Petcoke Stocks Data'),
                        'Page 3 Boiler Fuel Data': table_reader(unzip_file_path, 'Page 3 Boiler Fuel Data'),
                        'Page 4 Generator Data': table_reader(unzip_file_path, 'Page 4 Generator Data'),
                        'Page 5 Fuel Receipts and Costs': table_reader(unzip_file_path, 'Page 5 Fuel Receipts and Costs'),
                        'Page 6 Plant Frame': table_reader(unzip_file_path, 'Page 6 Plant Frame'),
                        'Page 6 Plant Frame Puerto Rico': table_reader(unzip_file_path, 'Page 6 Plant Frame Puerto Rico')
                        }
            df1_a = df['Page 1 Generation and Fuel Data']
            df1_b = df['Page 1 Puerto Rico']
            df8_a = df['Page 6 Plant Frame']
            df8_b = df['Page 6 Plant Frame Puerto Rico']
            df_a_temp = df1_a.append(df1_b, ignore_index=True)
            df_b_temp = df8_a.append(df8_b, ignore_index = True)


        elif len(file_sheets) == 11:
            print('Okay file name')
            df = {'Page 1 Generation and Fuel Data': table_reader(unzip_file_path,'Page 1 Generation and Fuel Data'),
                        'Page 1 Energy Storage': table_reader(unzip_file_path, 'Page 1 Energy Storage'),
                        'Page 2 Stocks Data': table_reader(unzip_file_path, 'Page 2 Stocks Data'),
                        'Page 2 Oil Stocks Data': table_reader(unzip_file_path, 'Page 2 Oil Stocks Data'),
                        'Page 2 Coal Stocks Data': table_reader(unzip_file_path, 'Page 2 Coal Stocks Data'),
                        'Page 2 Petcoke Stocks Data': table_reader(unzip_file_path, 'Page 2 Petcoke Stocks Data'),
                        'Page 3 Boiler Fuel Data': table_reader(unzip_file_path, 'Page 3 Boiler Fuel Data'),
                        'Page 4 Generator Data': table_reader(unzip_file_path, 'Page 4 Generator Data'),
                        'Page 5 Fuel Receipts and Costs': table_reader(unzip_file_path, 'Page 5 Fuel Receipts and Costs'),
                        'Page 6 Plant Frame': table_reader(unzip_file_path, 'Page 6 Plant Frame')
                        }
           
            df_a_temp = df['Page 1 Generation and Fuel Data']
            df_b_temp = df['Page 6 Plant Frame']




        df1,df2,df3,df4,df5,df6,df7,df8,df9,df10 = df_a_temp, df['Page 1 Energy Storage'], df['Page 2 Stocks Data'], df['Page 2 Oil Stocks Data'], df['Page 2 Coal Stocks Data'], df['Page 2 Petcoke Stocks Data'], df['Page 3 Boiler Fuel Data'], df['Page 4 Generator Data'], df['Page 5 Fuel Receipts and Costs'], df_b_temp
    
        print('Successfullu completed data transformation.\n')
            
                
    except:
        print('file does not exist')


    
    return df1,df2,df3,df4,df5,df6,df7,df8,df9,df10 


def write_local(year:int, df1:pd.DataFrame, df2:pd.DataFrame, df3:pd.DataFrame, df4:pd.DataFrame, df5:pd.DataFrame, df6:pd.DataFrame, df7:pd.DataFrame, df8:pd.DataFrame, df9:pd.DataFrame, df10:pd.DataFrame) -> list[Path]:
    """
        Store each dataFrame to a parquet file on the local disk

        Args:
            year: int. The year of the energy currently stored
            df1: pd.DataFrame. Generation and fuel data
            df2: pd.DataFrame. Energy Storage data
            df3: pd.DataFrame. Stocks Data
            df4: pd.DataFrame. Oil Stocks Data
            df5: pd.DataFrame. Coal Stocks Data
            df6: pd.DataFrame. Petcoke Stocks Data
            df7: pd.DataFrame. Boiler Fuel Data
            df8: pd.DataFrame. Generator Data
            df9: pd.DataFrame. Fuel Receipts and Costs
            df10: pd.DataFrame. Plant Frame

        Returns:
            path_list: Path[list]. A list of the path where each file is stored

    """

    print('Writing to local disk...\n')
    path_list = []
    tables_name = ['generation_and_fuel', 'energy_storage', 'stocks', 'oil_stocks', 'coal_stocks', 'petcoke_stocks', 'boiler_fuel', 'generator', 'fuel_recipts_and_cost', 'plant_frame']
    for i in tables_name:
        path = Path(f'data/{i}_{year}.parquet').as_posix()
        path_list.append(path)

    print('Writing data to local disk')
    df1.to_parquet(path_list[0],compression="gzip")
    df2.to_parquet(path_list[1],compression="gzip")
    df3.to_parquet(path_list[2],compression="gzip")
    df4.to_parquet(path_list[3],compression="gzip")
    df5.to_parquet(path_list[4],compression="gzip")
    df6.to_parquet(path_list[5],compression="gzip")
    df7.to_parquet(path_list[6],compression="gzip")
    df8.to_parquet(path_list[7],compression="gzip")
    df9.to_parquet(path_list[8],compression="gzip")
    df10.to_parquet(path_list[9],compression="gzip")
    print('Written data to local disk successfully.\n')

    return path_list



def write_gcs(path_list:list[Path]) ->None:
    """
        Uploading of the file to google cloud storage

        Args:
            path_list: list. A list containing the path of each table file stored locally
        Return:
            None
    """

    gcs_block = GcsBucket.load("zoom-gcs")

    print('Started Uploading...\n')
    for path in path_list:
        gcs_block.upload_from_path(
            from_path=path,
            to_path=path,
            timeout=600
        )
        filename = path.split('/')[-1]
        print(f'Uploaded {filename}')
    print('Uploading Completed\n')
    
    return



def clear_local_disk() -> None:
    """
        To delete file from the local disk in order to clear space and reduce disk cost on GCP

        args:
            None
        
        return:
            None
    """
    print("Clearing local disk...")
    # Remove the downloaded and extracted files
    for i in os.listdir('downloaded_data'):
        os.remove(os.path.join('downloaded_data', i))

    # Delete the dataFrame parquet file stored to the local disk
    for i in os.listdir('data'):
        os.remove(os.path.join('data', i))

    print('Local disk cleared.\n')
    




def main(year:int) -> None:
    print("Start ETL Pipeline")
    url = f'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip'
    zipped_file_path = fetch(url)
    unzipped_file_path = extract_zipFile(zipped_file_path)
    df1,df2,df3,df4,df5,df6,df7,df8,df9,df10 = transform(unzipped_file_path)
    local_path = write_local(year,df1,df2,df3,df4,df5,df6,df7,df8,df9,df10)
    # write_gcs(local_path)
    print('Completed ETL pipeline')


#############################################################################################################
"""
    UNIT TESTING
"""
#############################################################################################################


class DataQualityTest(unittest.TestCase):
    def setUp(self):
        file = "downloaded_data/output.zip"
        self.unzipped_file_path = extract_zipFile('downloaded_data/output.zip')

    def test_sheets_count(self):
        sheet_name = pd.ExcelFile(self.unzipped_file_path).sheet_names
        self.assertTrue(len(sheet_name) in [11,13])


    def test_df_dtype(self):
        df = table_reader(self.unzipped_file_path, 'Page 1 Generation and Fuel Data')
        self.assertTrue(all(df[col].dtype == 'O' for col in df.columns))

    def test_all_transformed_files_are_parquet(self):
        folder_path = 'data'
        for filename in os.listdir(folder_path):
            self.assertTrue(filename.endswith('.parquet'))

    def test_z(self):
        clear_local_disk()
        

    
################################################################################################################
"""
    Run workflow
"""
################################################################################################################
    


if __name__=='__main__':
    main(2021)
    unittest.main()
    
    

