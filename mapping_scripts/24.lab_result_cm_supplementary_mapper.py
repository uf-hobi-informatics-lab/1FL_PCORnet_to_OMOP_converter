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
spark = cf.get_spark_session("lab_result_cm_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]





 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        lab_result_cm_input_path = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/LAB_RESULT_CM/LAB_RESULT_CM.csv*'
        lab_result_cm_files = sorted(glob.glob(lab_result_cm_input_path))

        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/lab_result_cm_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################





        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################



        counter = 0
        for lab_result_cm_file in lab_result_cm_files:

            counter =  counter + 1
            suffix = lab_result_cm_file.rsplit(".", 1)[-1]
            lab_result_cm                 = cf.spark_read(lab_result_cm_file, spark)


            lab_result_cm_id_to_omop_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_lab_result_cm_id_to_omop_id/mapping_lab_result_cm_id_to_omop_id.csv.{suffix}'
            lab_result_cm_id_to_omop_id_mapping            = cf.spark_read(lab_result_cm_id_to_omop_id_mapping_path,spark)


            joined_lab_result = lab_result_cm.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==lab_result_cm['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(lab_result_cm_id_to_omop_id_mapping, lab_result_cm_id_to_omop_id_mapping['pcornet_lab_result_cm_id']==lab_result_cm['LAB_RESULT_CM_ID'], how='inner')\






            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            lab_result_cm_supplementary = joined_lab_result.select(              
                
                                                joined_lab_result['omop_id'].alias("Lab_result_cm_supplementary_id"),
                                                joined_lab_result['omop_person_id'].alias("person_id"),
                                                joined_lab_result['LAB_LOINC_SOURCE'],
                                                joined_lab_result['PRIORITY'],
                                                joined_lab_result['RESULT_LOC'],
                                                joined_lab_result['NORM_MODIFIER_LOW'] ,
                                                joined_lab_result['NORM_MODIFIER_HIGH'] , 
                                                joined_lab_result['ABN_IND'] ,
                                                joined_lab_result['RAW_LAB_NAME'] ,
                                                joined_lab_result['RAW_LAB_CODE'] ,
                                                joined_lab_result['RAW_PANEL'] ,
                                                joined_lab_result['RAW_RESULT'] ,
                                                joined_lab_result['RAW_UNIT'] ,
                                                joined_lab_result['RAW_ORDER_DEPT'] ,
                                                joined_lab_result['RAW_FACILITY_CODE'] ,
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_lab_result['SOURCE'].alias("source"),
                                                lit('LAB_RESULT_CM').alias("mapped_from"),
                                    
                                                                )



        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            files_count = len(lab_result_cm_files)
            current_count= counter
            file_name = f"lab_result_cm_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = lab_result_cm_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = lab_result_cm_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'lab_result_cm_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')