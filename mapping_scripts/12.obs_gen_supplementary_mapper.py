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
spark = cf.get_spark_session("obs_gen_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        obs_gen_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/OBS_GEN/OBS_GEN.csv*'
        obs_gen_files = sorted(glob.glob(obs_gen_input_path))




        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/obs_gen_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################




        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        counter = 0

        for obs_gen_file in obs_gen_files:
                counter =  counter + 1
                obs_gen                                     = cf.spark_read(obs_gen_file, spark)
                suffix = obs_gen_file.rsplit(".", 1)[-1]

                obsgenid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_obsgenid_to_omop_id/mapping_obsgenid_to_omop_id.csv.{suffix}'
                obsgenid_to_omop_id_mapping                    = cf.spark_read(obsgenid_to_omop_id_mapping_path,spark)


                joined_obs_gen = obs_gen.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==obs_gen['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(obsgenid_to_omop_id_mapping, obsgenid_to_omop_id_mapping['pcornet_obsgenid']==obs_gen['OBSGENID'], how='inner')\



                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                obs_gen_supplementary = joined_obs_gen.select(              
                    
                                                    joined_obs_gen['omop_id'].alias("obs_gen_supplementary_id"),
                                                    joined_obs_gen['omop_person_id'].alias("person_id"),
                                                    joined_obs_gen['OBSGEN_TABLE_MODIFIED'],
                                                    joined_obs_gen['OBSGEN_ID_MODIFIED'],
                                                    joined_obs_gen['OBSGEN_ABN_IND'],
                                                    joined_obs_gen['RAW_OBSGEN_NAME'],
                                                    joined_obs_gen['RAW_OBSGEN_CODE'],
                                                    joined_obs_gen['RAW_OBSGEN_TYPE'],
                                                    joined_obs_gen['RAW_OBSGEN_RESULT'],
                                                    joined_obs_gen['RAW_OBSGEN_UNIT'],
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_obs_gen['SOURCE'].alias("source"),
                                                    lit('OBS_GEN').alias("mapped_from"),
                                        
                                                                    )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



                files_count = len(obs_gen_files)
                current_count= counter
                file_name = f"obs_gen_supplementary.csv.{OBSGENID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = obs_gen_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = obs_gen_supplementary,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'obs_gen_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')