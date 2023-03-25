import pandas as pd
import os
from pathlib import Path
from zipfile import ZipFile
import excel_file_settings as efs

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
    # Extract file name form the url
    file_name = 'output.zip'
    # Define the output file path
    file_path = Path(f'downloaded_data/{file_name}').as_posix()
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



def table_reader(unzip_file_path:Path, input:str) -> pd.DataFrame:
    """
        This help read the data, set the column, remove unwanted rows and format the columns name

        Args:
            unzip_file_path: Path. The path to the extracted excel file
            input: str. This is the name of the sheet to be read
        Return:
            df: pd.DataFrame. This is the excel table stored as a pandas dataFrame

    """


    df  = pd.read_excel(unzip_file_path,sheet_name=input)

    correct_column_name = {'Page 1 Generation and Fuel Data': efs.sheet_0_columns ,
                        'Page 1 Puerto Rico': efs.sheet_1_columns ,
                        'Page 1 Energy Storage': efs.sheet_2_columns ,
                        'Page 2 Stocks Data': efs.sheet_3_columns,
                        'Page 2 Oil Stocks Data': efs.sheet_4_columns,
                        'Page 3 Boiler Fuel Data': efs.sheet_5_columns,
                        'Page 4 Generator Data': efs.sheet_6_columns,
                        'Page 5 Fuel Receipts and Costs': efs.sheet_7_columns,
                        'Page 6 Plant Frame': efs.sheet_8_columns,
                        'Page 6 Plant Frame Puerto Rico': efs.sheet_9_columns
                        }
    
    desired_columns = {'Page 1 Generation and Fuel Data': efs.sheet_0_columns_rq ,
                        'Page 1 Puerto Rico': efs.sheet_1_columns_rq ,
                        'Page 1 Energy Storage': efs.sheet_2_columns_rq ,
                        'Page 2 Stocks Data': efs.sheet_3_columns_rq,
                        'Page 2 Oil Stocks Data': efs.sheet_4_columns_rq,
                        'Page 3 Boiler Fuel Data': efs.sheet_5_columns_rq,
                        'Page 4 Generator Data': efs.sheet_6_columns_rq,
                        'Page 5 Fuel Receipts and Costs': efs.sheet_7_columns_rq,
                        'Page 6 Plant Frame': efs.sheet_8_columns_rq,
                        'Page 6 Plant Frame Puerto Rico': efs.sheet_9_columns_rq
                        }
    

    df.columns = correct_column_name[input]

    column_row_index = int(df[df.apply(lambda row: row.tolist() == df.columns, axis=1)].index[0])

    df.drop(df.index[:column_row_index + 1], inplace=True)

    df.columns = desired_columns[input]

    df.columns = [i.replace('\n','_').replace(' ','_').lower() for i in df.columns]

    df = df.astype(str)

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
        file_sheets = pd.ExcelFile(unzip_file_path).sheet_names
        if file_sheets == efs.sheet_names:
            df = {'Page 1 Generation and Fuel Data': table_reader(unzip_file_path,'Page 1 Generation and Fuel Data'),
                        'Page 1 Puerto Rico': table_reader(unzip_file_path, 'Page 1 Puerto Rico'),
                        'Page 1 Energy Storage': table_reader(unzip_file_path, 'Page 1 Energy Storage'),
                        'Page 2 Stocks Data': table_reader(unzip_file_path, 'Page 2 Stocks Data'),
                        'Page 2 Oil Stocks Data': table_reader(unzip_file_path, 'Page 2 Oil Stocks Data'),
                        'Page 3 Boiler Fuel Data': table_reader(unzip_file_path, 'Page 3 Boiler Fuel Data'),
                        'Page 4 Generator Data': table_reader(unzip_file_path, 'Page 4 Generator Data'),
                        'Page 5 Fuel Receipts and Costs': table_reader(unzip_file_path, 'Page 5 Fuel Receipts and Costs'),
                        'Page 6 Plant Frame': table_reader(unzip_file_path, 'Page 6 Plant Frame'),
                        'Page 6 Plant Frame Puerto Rico': table_reader(unzip_file_path, 'Page 6 Plant Frame Puerto Rico'),
                        }
            
                
    except:
        print('file does not exist')

    
    return df['Page 1 Generation and Fuel Data'].append(df['Page 1 Puerto Rico'], ignore_index=True), df['Page 1 Energy Storage'], df['Page 2 Stocks Data'], df['Page 2 Oil Stocks Data'], df['Page 3 Boiler Fuel Data'], df['Page 4 Generator Data'], df['Page 5 Fuel Receipts and Costs'], df['Page 6 Plant Frame'].append(df['Page 6 Plant Frame Puerto Rico'], ignore_index = True)



def write_local(year:int, df1:pd.DataFrame, df2:pd.DataFrame, df3:pd.DataFrame, df4:pd.DataFrame, df5:pd.DataFrame, df6:pd.DataFrame, df7:pd.dataFrame, df8:pd.DataFrame) -> list[Path]:
    """
        Store each dataFrame to a parquet file on the local disk

        Args:
            year: int. The year of the energy currently stored
            df1: pd.DataFrame. Generation and fuel data
            df2: pd.DataFrame. Energy Storage data
            df3: pd.DataFrame. Stocks Data
            df4: pd.DataFrame. Oil Stocks Data
            df5: pd.DataFrame. Boiler Fuel Data
            df6: pd.DataFrame. Generator Data
            df6: pd.DataFrame. Fuel Receipts and Costs
            df7: pd.DataFrame. Plant Frame

        Returns:
            path_list: Path[list]. A list of the path where each file is stored

    """


    path_list = []
    tables_name = ['generation_and_fuel', 'energy_storage', 'stocks', 'oil_stocks', 'boiler_fuel', 'generator', 'fuel_recipts_and_cost', 'plant_frame']
    for i in tables_name:
        path = Path(f'data/{tables_name}_{year}.parquet').as_posix()

    print('Writing data to local disk')
    df1.to_parquet(path_list[0],compression="gzip")
    df2.to_parquet(path_list[1],compression="gzip")
    df3.to_parquet(path_list[2],compression="gzip")
    df4.to_parquet(path_list[3],compression="gzip")
    df5.to_parquet(path_list[4],compression="gzip")
    df6.to_parquet(path_list[5],compression="gzip")
    df7.to_parquet(path_list[6],compression="gzip")
    df8.to_parquet(path_list[7],compression="gzip")
    print('Written data to local disk')

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

    print('Started Uploading...')
    for path in path_list:
        gcs_block.upload_from_path(
            from_path=path,
            to_path=path,
            timeout=600
        )
        filename = path.split('/')[-1]
        print(f'Uploaded {filename}')
    print('Uploading Completed')
    
    return




def main(year:int) -> None:
    print("Start ETL Pipeline")
    url = f'https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip'
    zipped_file_path = fetch(url)
    unzipped_file_path = extract_zipFile(zipped_file_path)
    df1,df2,df3,df4,df5,df6,df7,df8 = transform(unzipped_file_path)
    local_path = write_local(year,df1,df2,df3,df4,df5,df6,df7,df8)
    write_gcs(local_path)
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
        self.assertEqual(len(sheet_name), len(efs.sheet_names))

    def test_df_1_and_2(self):
        df1_col = table_reader(self.unzipped_file_path, 'Page 1 Generation and Fuel Data').shape[1]
        df2_col = table_reader(self.unzipped_file_path, 'Page 1 Puerto Rico').shape[1]
        self.assertEqual(df1_col,df2_col)

    def test_df_9_and_10(self):
        df9_col = table_reader(self.unzipped_file_path, 'Page 1 Generation and Fuel Data').shape[1]
        df10_col = table_reader(self.unzipped_file_path, 'Page 1 Puerto Rico').shape[1]
        self.assertEqual(df9_col,df10_col)

    def test_df_dtype(self):
        df = table_reader(self.unzipped_file_path, 'Page 1 Generation and Fuel Data').shape[1]
        self.assertTrue(all(df[col].dtype == 'O' for col in df.columns))

    def test_all_transformed_files_are_parquet(self):
        folder_path = 'data'
        for filename in os.listdir(folder_path):
            self.assertTrue(filename.endswith('.parquet'))

    
################################################################################################################
"""
    Run workflow
"""
################################################################################################################
    


if __name__=='__main__':
    unittest.main()
    main(2008)

