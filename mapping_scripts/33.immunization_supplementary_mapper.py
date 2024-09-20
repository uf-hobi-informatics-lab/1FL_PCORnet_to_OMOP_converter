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
spark = cf.get_spark_session("immunization_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]




 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


        immunization_input_path              = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/IMMUNIZATION/IMMUNIZATION.csv*'
        immunization_files = sorted(glob.glob(immunization_input_path))


        
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/immunization_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################





        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        ###################################################################################################################################
        # Mapping immunization to drug_exposure
        ###################################################################################################################################


        counter = 0
        for immunization_file in immunization_files:

            counter =  counter + 1
            suffix = immunization_file.rsplit(".", 1)[-1]
            immunization                 = cf.spark_read(immunization_file, spark)
            
            immunization_to_omop_id_mapping_path              = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_immunizationid_to_omop_id/mapping_immunizationid_to_omop_id.csv.{suffix}'
            immunizationid_to_omop_id_mapping                = cf.spark_read(immunization_to_omop_id_mapping_path,spark)

            joined_immunization =     immunization.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==immunization['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(immunizationid_to_omop_id_mapping, immunizationid_to_omop_id_mapping['pcornet_immunizationid']==immunization['IMMUNIZATIONID'], how='inner')\







            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            immunization_supplementary = joined_immunization.select(              
                
                                                joined_immunization['omop_id'].alias("immunization_supplementary_id"),
                                                joined_immunization['omop_person_id'].alias("person_id"),
                                                joined_immunization['VX_RECORD_DATE'],
                                                joined_immunization['VX_CODE_TYPE'],
                                                joined_immunization['VX_STATUS'],
                                                joined_immunization['VX_STATUS_REASON'],
                                                joined_immunization['VX_BODY_SITE'],
                                                joined_immunization['VX_MANUFACTURER'],
                                                joined_immunization['VX_EXP_DATE'],
                                                joined_immunization['RAW_VX_NAME'],
                                                joined_immunization['RAW_VX_CODE'],
                                                joined_immunization['RAW_VX_CODE_TYPE'],
                                                joined_immunization['RAW_VX_DOSE'],
                                                joined_immunization['RAW_VX_DOSE_UNIT'],
                                                joined_immunization['RAW_VX_ROUTE'],
                                                joined_immunization['RAW_VX_BODY_SITE'],
                                                joined_immunization['RAW_VX_STATUS'],
                                                joined_immunization['RAW_VX_STATUS_REASON'],
                                                joined_immunization['RAW_VX_MANUFACTURER'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_immunization['SOURCE'].alias("source"),
                                                lit('IMMUNIZATION').alias("mapped_from"),
                                    
                                                                )



        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

    

            files_count = len(immunization_files)
            current_count= counter
            file_name = f"immunization_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = immunization_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = immunization_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'immunization_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')