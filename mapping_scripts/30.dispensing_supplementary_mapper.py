###################################################################################################################################
# This script will map a PCORNet drug_exposure table 
###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F

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
spark = cf.get_spark_session("dispensing_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



###################################################################################################################################
# This function will return the day value from a date input
###################################################################################################################################


 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                

        dispensing_input_path                = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DISPENSING/DISPENSING.csv*'
        dispensing_files = sorted(glob.glob(dispensing_input_path))


        
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/dispensing_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



    
        
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        ###################################################################################################################################
        # Mapping dispensing to drug_exposure
        ###################################################################################################################################


        counter = 0
        for dispensing_file in dispensing_files:

            counter =  counter + 1
            suffix = dispensing_file.rsplit(".", 1)[-1]
            dispensing                 = cf.spark_read(dispensing_file, spark)

            dispensingid_to_omop_id_mapping_path              = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_dispensingid_to_omop_id/mapping_dispensingid_to_omop_id.csv.{suffix}'
            dispensingid_to_omop_id_mapping                = cf.spark_read(dispensingid_to_omop_id_mapping_path,spark)


            joined_dispensing =     dispensing.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==dispensing['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(dispensingid_to_omop_id_mapping, dispensingid_to_omop_id_mapping['pcornet_dispensingid']==dispensing['DISPENSINGID'], how='inner')\








            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            dispensing_supplementary = joined_dispensing.select(              
                
                                                joined_dispensing['omop_id'].alias("dispensing_supplementary_id"),
                                                joined_dispensing['omop_person_id'].alias("person_id"),
                                                joined_dispensing['DISPENSE_SOURCE'],
                                                joined_dispensing['RAW_NDC'],
                                                joined_dispensing['RAW_DISPENSE_DOSE_DISP'],
                                                joined_dispensing['RAW_DISPENSE_DOSE_DISP_UNIT'],
                                                joined_dispensing['RAW_DISPENSE_ROUTE'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_dispensing['SOURCE'].alias("source"),
                                                lit('DISPENSING').alias("mapped_from"),
                                    
                                                                )



        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(dispensing_files)
            current_count= counter
            file_name = f"dispensing_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = dispensing_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = dispensing_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'dispensing_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')