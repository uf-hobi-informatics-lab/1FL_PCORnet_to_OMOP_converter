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
spark = cf.get_spark_session("med_admin_supplementary_mapper")



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


                
        med_admin_input_path                 = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/MED_ADMIN/MED_ADMIN.csv*'
        med_admin_files = sorted(glob.glob(med_admin_input_path))


        
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/med_admin_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################






        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)


        ###################################################################################################################################
        # Mapping MED_ADMIN to drug_exposure
        ###################################################################################################################################


        counter = 0
        for med_admin_file in med_admin_files:

            counter =  counter + 1
            suffix = med_admin_file.rsplit(".", 1)[-1]
            med_admin                 = cf.spark_read(med_admin_file, spark)

            medadminid_to_omop_id_mapping_path                = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_medadminid_to_omop_id/mapping_medadminid_to_omop_id.csv.{suffix}'
            medadminid_to_omop_id_mapping                  = cf.spark_read(medadminid_to_omop_id_mapping_path,spark)


            joined_med_admin = med_admin.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==med_admin['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(medadminid_to_omop_id_mapping, medadminid_to_omop_id_mapping['pcornet_medadminid']==med_admin['MEDADMINID'], how='inner')\






            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            med_admin_supplementary = joined_med_admin.select(              
                
                                                joined_med_admin['omop_id'].alias("med_admin_supplementary_id"),
                                                joined_med_admin['omop_person_id'].alias("person_id"),
                                                joined_med_admin['MEDADMIN_TYPE'],
                                                joined_med_admin['MEDADMIN_SOURCE'],
                                                joined_med_admin['RAW_MEDADMIN_MED_NAME'],
                                                joined_med_admin['RAW_MEDADMIN_CODE'],
                                                joined_med_admin['RAW_MEDADMIN_DOSE_ADMIN'],
                                                joined_med_admin['RAW_MEDADMIN_DOSE_ADMIN_UNIT'],
                                                joined_med_admin['RAW_MEDADMIN_ROUTE'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_med_admin['SOURCE'].alias("source"),
                                                lit('MED_ADMIN').alias("mapped_from"),
                                    
                                                                )









        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

    
            files_count = len(med_admin_files)
            current_count= counter
            file_name = f"med_admin_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = med_admin_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = med_admin_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'med_admin_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')