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
spark = cf.get_spark_session("obs_clin_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        obs_clin_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/OBS_CLIN/OBS_CLIN.csv*'
        obs_clin_files = sorted(glob.glob(obs_clin_input_path))
        


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        counter = 0

        for obs_clin_file in obs_clin_files:
                counter =  counter + 1
                obs_clin                                     = cf.spark_read(obs_clin_file, spark)
                suffix = obs_clin_file.rsplit(".", 1)[-1]



                obsclinid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_obsclinid_to_omop_id/mapping_obsclinid_to_omop_id.csv.{suffix}'
                obsclinid_to_omop_id_mapping                 = cf.spark_read(obsclinid_to_omop_id_mapping_path,spark)

                joined_obs_clin = obs_clin.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==obs_clin['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(obsclinid_to_omop_id_mapping, obsclinid_to_omop_id_mapping['pcornet_obsclinid']==obs_clin['OBSCLINID'], how='inner')\
                                        




                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                obs_clin_supplementary = joined_obs_clin.select(              
                    
                                                    joined_obs_clin['omop_id'].alias("obs_clin_supplementary_id"),
                                                    joined_obs_clin['omop_person_id'].alias("person_id"),
                                                    joined_obs_clin['OBSCLIN_RESULT_SNOMED'],
                                                    joined_obs_clin['OBSCLIN_ABN_IND'],
                                                    joined_obs_clin['RAW_OBSCLIN_NAME'],
                                                    joined_obs_clin['RAW_OBSCLIN_CODE'],
                                                    joined_obs_clin['RAW_OBSCLIN_TYPE'],
                                                    joined_obs_clin['RAW_OBSCLIN_RESULT'],
                                                    joined_obs_clin['RAW_OBSCLIN_MODIFIER'],
                                                    joined_obs_clin['RAW_OBSCLIN_UNIT'],
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_obs_clin['SOURCE'].alias("source"),
                                                    lit('OBS_CLIN').alias("mapped_from"),
                                        
                                                                    )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



                files_count = len(obs_clin_files)
                current_count= counter
                file_name = f"obs_clin_supplementary.csv.{OBSCLINID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = obs_clin_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = obs_clin_supplementary,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'obs_clin_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')