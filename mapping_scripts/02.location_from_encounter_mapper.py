###################################################################################################################################
# This script will map a PCORNet location table 
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
import pyspark.sql.functions as F

# from partners import partners_list
from itertools import chain
import argparse
from settings import *
import glob
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
spark = cf.get_spark_session("location_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :


    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        facility_location_to_location_id_path               = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_facility_location_to_location_id.csv'

        # address_id_mapping_path                             = f'/app/data/{input_data_folder}/mapping_tables/mapping_addressid_to_omop_id.csv'
        mapped_data_folder_path                             = f'/app/data/{input_data_folder}/omop_tables/{folder}/location/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        facility_location_to_location_id                           = cf.spark_read(facility_location_to_location_id_path,spark)




        # lds_address_history = lds_address_history.join(address_id_mapping, address_id_mapping['pcornet_addressid']== lds_address_history['ADDRESSID'])


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        location_from_encounter = facility_location_to_location_id.select(              
            
                                    facility_location_to_location_id['omop_location_id'].alias('location_id'),
                                    lit('').alias("address_1"),
                                    lit('').alias("address_2"),
                                    lit('').alias("city"),
                                    lit('').alias("state"),
                                    facility_location_to_location_id['pcornet_facility_location'].alias("zip"),
                                    lit('').alias("county"),
                                    lit('').alias("location_source_value"),
                                    cf.get_current_time_udf().alias("updated"),
                                    facility_location_to_location_id['source'].alias("source"),
                                    lit('ENCOUNTER').alias("mapped_from"),
                                

                                                            )

        file_name = f"location.csv.000{LOCATION_ID_FROM_ENCOUNTER_OFFSET}"

        # cf.print_mapping_file_status(

        #     total_count = '1',
        #     current_count = '1',
        #     output_file_name =  file_name,
        #     source_file = location_from_encounter

        #     )
    


            ###################################################################################################################################
            # Create the output file
            ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = location_from_encounter,
                        output_file_name = file_name,
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'location_from+_encounter_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')