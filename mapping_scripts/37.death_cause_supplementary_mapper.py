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
spark = cf.get_spark_session("death_cause_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:


    for folder in folders :
        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        death_cause_input_path                            = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH_CAUSE/DEATH_CAUSE.csv*'
        death_cause_files = sorted(glob.glob(death_cause_input_path))


        death_input_path                                  = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH.csv'

        death_cause_to_omop_id_mapping_path               = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_death_cause_to_omop_id.csv'


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path                           = f'/app/data/{input_data_folder}/omop_tables/{folder}/death_cause_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        death_cause_to_omop_id_mapping                 = cf.spark_read(death_cause_to_omop_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        counter = 0
        for death_cause_file in death_cause_files:

            counter =  counter + 1
            suffix = death_cause_file.rsplit(".", 1)[-1]
            death_cause                 = cf.spark_read(death_cause_file, spark)

            joined_death_cause = death_cause.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==death_cause['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(death_cause_to_omop_id_mapping, death_cause_to_omop_id_mapping['pcornet_id']==concat( col("PATID"),col("DEATH_CAUSE"),col("DEATH_CAUSE_CODE"),col("DEATH_CAUSE_TYPE"),col("DEATH_CAUSE_SOURCE")), how='inner')\






            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            death_cause_supplementary = joined_death_cause.select(              
                
                                                joined_death_cause['omop_id'].alias("death_cause_supplementary_id"),
                                                joined_death_cause['omop_person_id'].alias("person_id"),
                                                joined_death_cause['DEATH_CAUSE_CONFIDENCE'],                          
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_death_cause['SOURCE'].alias("source"),
                                                lit('DEATH_CAUSE').alias("mapped_from"),
                                    
                                                                )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(death_cause_files)
            current_count= counter
            file_name = f"death_cause_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = death_cause_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = death_cause_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'death_cause_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')