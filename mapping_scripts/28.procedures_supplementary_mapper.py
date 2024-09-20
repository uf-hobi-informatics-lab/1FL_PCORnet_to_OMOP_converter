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
spark = cf.get_spark_session("procedures_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        procedures_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PROCEDURES/PROCEDURES.csv*'
        procedures_files = sorted(glob.glob(procedures_input_path))


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/procedures_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################







        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################




        counter = 0
        for procedures_file in procedures_files:

            counter =  counter + 1
            suffix = procedures_file.rsplit(".", 1)[-1]
            procedures                 = cf.spark_read(procedures_file, spark)

            proceduresid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_proceduresid_to_omop_id/mapping_proceduresid_to_omop_id.csv.{suffix}'
            proceduresid_to_omop_id_mapping            = cf.spark_read(proceduresid_to_omop_id_mapping_path,spark)

            
            joined_procedures = procedures.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==procedures['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(proceduresid_to_omop_id_mapping, proceduresid_to_omop_id_mapping['pcornet_proceduresid']==procedures['PROCEDURESID'], how='inner')\
                                            






            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            procedures_supplementary = joined_procedures.select(              
                
                                                joined_procedures['omop_id'].alias("procedures_supplementary_id"),
                                                joined_procedures['omop_person_id'].alias("person_id"),
                                                joined_procedures['RAW_PX'],
                                                joined_procedures['RAW_PX_TYPE'],
                                                joined_procedures['RAW_PPX'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_procedures['SOURCE'].alias("source"),
                                                lit('PROCEDURES').alias("mapped_from"),
                                    
                                                                )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(procedures_files)
            current_count= counter
            file_name = f"procedures_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = procedures_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = procedures_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'procedures_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')