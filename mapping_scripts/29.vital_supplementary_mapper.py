###################################################################################################################################
# This script will map a PCORNet measurement table 
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
spark = cf.get_spark_session("vital_supplementary_mapper")



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


                
        vital_input_path = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/VITAL/VITAL.csv*'
        vital_files = sorted(glob.glob(vital_input_path))


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/vital_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################






        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        ###################################################################################################################################
        # Mapping VITAL WT to COMMON table
        ###################################################################################################################################



        counter = 0
        for vital_file in vital_files:

            counter =  counter + 1
            suffix = vital_file.rsplit(".", 1)[-1]
            vital                 = cf.spark_read(vital_file, spark)

            vital_id_to_omop_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_vitalid_to_omop_id/mapping_vitalid_to_omop_id.csv.{suffix}'
            vital_id_to_omop_id_mapping               = cf.spark_read(vital_id_to_omop_id_mapping_path,spark)

            joined_vital = vital.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) , how='left')\

            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
            ###################################################################################################################################


            vital_supplementary = joined_vital.select(              
                
                                                joined_vital['omop_id'].alias("vital_supplementary_id"),
                                                joined_vital['omop_person_id'].alias("person_id"),
                                                joined_vital['RAW_DIASTOLIC'],
                                                joined_vital['RAW_SYSTOLIC'],
                                                joined_vital['RAW_BP_POSITION'],
                                                joined_vital['RAW_SMOKING'],
                                                joined_vital['RAW_TOBACCO'],
                                                joined_vital['RAW_TOBACCO_TYPE'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_vital['SOURCE'].alias("source"),
                                                lit('VITAL').alias("mapped_from"),
                                    
                                                                )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################




            files_count = len(vital_files)
            current_count= counter
            file_name = f"vital_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = vital_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = vital_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'vital_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')