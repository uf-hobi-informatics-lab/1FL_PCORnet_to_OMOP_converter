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
spark = cf.get_spark_session("demographic_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]





 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        demographic_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEMOGRAPHIC/DEMOGRAPHIC.csv*'
        demographic_files = sorted(glob.glob(demographic_input_path))


        patid_to_person_id_mapping_path                 = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/demographic_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################





        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################




        counter = 0
        for demographic_file in demographic_files:

            counter =  counter + 1
            suffix = demographic_file.rsplit(".", 1)[-1]
            demographic                 = cf.spark_read(demographic_file, spark)

            joined_demographic = demographic.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==demographic['PATID'], how='inner').drop('pcornet_patid')\






            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            demographic_supplementary = joined_demographic.select(              
                
                                                joined_demographic['omop_person_id'].alias("person_id"),
                                                joined_demographic['SEXUAL_ORIENTATION'],
                                                joined_demographic['GENDER_IDENTITY'],
                                                joined_demographic['BIOBANK_FLAG'],
                                                joined_demographic['PAT_PREF_LANGUAGE_SPOKEN'] ,
                                                joined_demographic['RAW_SEX'] , 
                                                joined_demographic['RAW_SEXUAL_ORIENTATION'] ,
                                                joined_demographic['RAW_GENDER_IDENTITY'] ,
                                                joined_demographic['RAW_HISPANIC'] ,
                                                joined_demographic['RAW_RACE'] ,
                                                joined_demographic['RAW_PAT_PREF_LANGUAGE_SPOKEN'] ,
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_demographic['SOURCE'].alias("source"),
                                                lit('DEMOGRAPHIC').alias("mapped_from"),
                                    
                                                                )



        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            files_count = len(demographic_files)
            current_count= counter
            file_name = f"demographic_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = demographic_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = demographic_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'demographic_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')