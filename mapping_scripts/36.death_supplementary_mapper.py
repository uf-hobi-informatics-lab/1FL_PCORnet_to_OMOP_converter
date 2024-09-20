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
spark = cf.get_spark_session("death_supplementary_mapper")



###################################################################################################################################
# This function will return the day value from a date input
###################################################################################################################################



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        pcornet_death_input_path             = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH/DEATH.csv*'
        pcornet_death_files = sorted(glob.glob(pcornet_death_input_path))


        patid_to_person_id_mapping_path      = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'

        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/death_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################




        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################
        counter = 0
        for pcornet_death_file in pcornet_death_files:

            counter =  counter + 1
            suffix = pcornet_death_file.rsplit(".", 1)[-1]
            pcornet_death                 = cf.spark_read(pcornet_death_file, spark)

            joined_pcornet_death = pcornet_death.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==pcornet_death['PATID'], how='inner').drop('pcornet_patid')\



            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            death_supplementary = joined_pcornet_death.select(              
                
                                                
                                                joined_pcornet_death['omop_person_id'].alias("person_id"),
                                                joined_pcornet_death['DEATH_DATE_IMPUTE'],
                                                joined_pcornet_death['DEATH_SOURCE'],
                                                joined_pcornet_death['DEATH_MATCH_CONFIDENCE'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_pcornet_death['SOURCE'].alias("source"),
                                                lit('DEATH').alias("mapped_from"),
                                    
                                                                )


        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(pcornet_death_files)
            current_count= counter
            file_name = f"death_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = pcornet_death_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = death_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


        
    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'death_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')