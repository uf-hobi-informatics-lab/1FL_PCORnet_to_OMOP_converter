###################################################################################################################################
# This script will map a PCORNet provider table 
###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
from dictionaries import *
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
import glob
from settings import *
import os


###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()

parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_data_folder = args.data_folder

cf =CommonFuncitons()

# spin the pyspak cluster and
spark = cf.get_spark_session("provider_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        provider_input_path                                 = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PROVIDER/PROVIDER.csv*'
        provider_files = sorted(glob.glob(provider_input_path))


        provider_id_mapping_path                            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_providerid_provider_id.csv'
    
        mapped_data_folder_path                             = f'/app/data/{input_data_folder}/omop_tables/{folder}/provider_supplementary/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        provider_id_mapping                          = cf.spark_read(provider_id_mapping_path,spark)
        



        counter = 0
        for provider_file in provider_files:

            counter =  counter + 1
            suffix = provider_file.rsplit(".", 1)[-1]
            provider                 = cf.spark_read(provider_file, spark)

      
            joined_df = provider.join(provider_id_mapping, provider_id_mapping['pcornet_providerid']== provider['PROVIDERID'])




        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


            provider_supplementary = joined_df.select(              
                
                                        joined_df['omop_provider_id'].alias("provider_id"),
                                        joined_df['PROVIDER_NPI_FLAG'],
                                        joined_df['RAW_PROVIDER_SPECIALTY_PRIMARY'],
                                        cf.get_current_time_udf().alias("updated"),
                                        joined_df['SOURCE'].alias("source"),
                                        lit('PROVIDER').alias("mapped_from"),
                                    

                                                                )
        
    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
            files_count = len(provider_files)
            current_count= counter
            file_name = f"provider_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = provider_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = provider_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'provider_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')