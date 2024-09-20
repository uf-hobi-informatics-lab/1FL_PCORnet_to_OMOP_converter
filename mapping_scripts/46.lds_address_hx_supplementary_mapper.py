###################################################################################################################################
# This script will map a PCORNet lds_address_history table 
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
spark = cf.get_spark_session("lds_address_history_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        lds_address_history_input_path                                 = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/LDS_ADDRESS_HISTORY/LDS_ADDRESS_HISTORY.csv*'
        lds_address_history_files = sorted(glob.glob(lds_address_history_input_path))


        location_from_lds_address_hx_path                              = f'/app/data/{input_data_folder}/omop_tables/{folder}/location/location.csv.000{LOCATION_ID_FROM_LDS_ADDRESS_OFFSET}'
        patid_to_person_id_mapping_path                                = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'

        mapped_data_folder_path                                        = f'/app/data/{input_data_folder}/omop_tables/{folder}/lds_address_history_supplementary/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        location_from_lds_address_hx                            = cf.spark_read(location_from_lds_address_hx_path,spark)
        patid_to_person_id_mapping                              = cf.spark_read(patid_to_person_id_mapping_path,spark)




        counter = 0
        for lds_address_history_file in lds_address_history_files:

            counter =  counter + 1
            suffix = lds_address_history_file.rsplit(".", 1)[-1]
            lds_address_history                 = cf.spark_read(lds_address_history_file, spark)

            lds_address_history_id_mapping_path                            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_addressid_to_omop_id/mapping_addressid_to_omop_id.csv.{suffix}'
            lds_address_history_id_mapping                          = cf.spark_read(lds_address_history_id_mapping_path,spark)

      
            joined_df = lds_address_history.join(lds_address_history_id_mapping, lds_address_history_id_mapping['pcornet_addressid']== lds_address_history['ADDRESSID']).drop('updated').drop('source')\
                        .join(location_from_lds_address_hx, location_from_lds_address_hx["zip"] == lds_address_history["ADDRESS_ZIP5"], how='left' )\
                        .join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==lds_address_history['PATID'], how='inner').drop('pcornet_patid')




        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


            lds_address_history_supplementary = joined_df.select(              
                
                                        joined_df['omop_id'].alias("address_id"),
                                        joined_df['omop_person_id'].alias("person_id"),
                                        joined_df['location_id'],
                                        joined_df['ADDRESS_USE'],
                                        joined_df['ADDRESS_TYPE'],
                                        joined_df['ADDRESS_PREFERRED'],
                                        joined_df['ADDRESS_PERIOD_START'],
                                        joined_df['ADDRESS_PERIOD_END'],
                                        cf.get_current_time_udf().alias("updated"),
                                        joined_df['SOURCE'].alias("source"),
                                        lit('lds_address_history').alias("mapped_from"),
                                    

                                                                )
        
    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
            files_count = len(lds_address_history_files)
            current_count= counter
            file_name = f"lds_address_history_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = lds_address_history_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = lds_address_history_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'lds_address_history_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')