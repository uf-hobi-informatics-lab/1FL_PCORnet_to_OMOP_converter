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
spark = cf.get_spark_session("diagnosis_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]




try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        diagnosis_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DIAGNOSIS/DIAGNOSIS.csv*'
        diagnosis_files = sorted(glob.glob(diagnosis_input_path))



        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/diagnosis_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################




        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################


        counter = 0
        for diagnosis_file in diagnosis_files:

            counter =  counter + 1
            suffix = diagnosis_file.rsplit(".", 1)[-1]
            diagnosis                 = cf.spark_read(diagnosis_file, spark)

            diagnosisid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_diagnosisid_to_omop_id/mapping_diagnosisid_to_omop_id.csv.{suffix}'
            diagnosisid_to_omop_id_mapping                 = cf.spark_read(diagnosisid_to_omop_id_mapping_path,spark)


            joined_diagnosis = diagnosis.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==diagnosis['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(diagnosisid_to_omop_id_mapping, diagnosisid_to_omop_id_mapping['pcornet_diagnosisid']==diagnosis['DIAGNOSISID'], how='inner')\





            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            diagnosis_supplementary = joined_diagnosis.select(              
                
                                                joined_diagnosis['omop_id'].alias("diagnosis_supplentary_id"),
                                                joined_diagnosis['omop_person_id'].alias("person_id"),
                                                joined_diagnosis['PDX'],
                                                joined_diagnosis['DX_POA'],
                                                joined_diagnosis['RAW_DX'],
                                                joined_diagnosis['RAW_DX_TYPE'],
                                                joined_diagnosis['RAW_DX_SOURCE'],
                                                joined_diagnosis['RAW_PDX'],
                                                joined_diagnosis['RAW_DX_POA'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_diagnosis['SOURCE'].alias("source"),
                                                lit('DIAGNOSIS').alias("mapped_from"),
                                    
                                                                )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(diagnosis_files)
            current_count= counter
            file_name = f"diagnosis_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = diagnosis_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = diagnosis_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'diagnosis_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')