###################################################################################################################################
# This script will map a PCORNet visit_occurrence table 
###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
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
spark = cf.get_spark_session("encouter_supplementary_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        encounter_input_path                              = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/ENCOUNTER/ENCOUNTER.csv*'
        encounter_files = sorted(glob.glob(encounter_input_path))


        visit_occurrence_id_mapping_path                  = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'



        mapped_data_folder_path                             = f'/app/data/{input_data_folder}/omop_tables/{folder}/encounter_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        visit_occurrence_id_mapping                          = cf.spark_read(visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                           = cf.spark_read(patid_to_person_id_mapping_path,spark)




        counter = 0
        for encounter_file in encounter_files:

            counter =  counter + 1
            suffix = encounter_file.rsplit(".", 1)[-1]
            encounter                 = cf.spark_read(encounter_file, spark)

            joined_encounter = encounter.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==encounter['PATID'], how='inner').drop('pcornet_patid')\
                                            .join(visit_occurrence_id_mapping, visit_occurrence_id_mapping['pcornet_encounterid']== encounter['ENCOUNTERID'], how = 'inner').drop('pcornet_encounterid')\



            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            encounter_supplementary = joined_encounter.select( 

                                        joined_encounter['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                        joined_encounter['omop_person_id'].alias("person_id"),
                                        joined_encounter['DRG'],
                                        joined_encounter['DRG_TYPE'],
                                        joined_encounter['PAYER_TYPE_PRIMARY'],
                                        joined_encounter['PAYER_TYPE_SECONDARY'],
                                        joined_encounter['FACILITY_TYPE'],
                                        joined_encounter['RAW_SITEID'],
                                        joined_encounter['RAW_ENC_TYPE'],
                                        joined_encounter['RAW_DISCHARGE_DISPOSITION'],
                                        joined_encounter['RAW_DISCHARGE_STATUS'],
                                        joined_encounter['RAW_DRG_TYPE'],
                                        joined_encounter['RAW_ADMITTING_SOURCE'],
                                        joined_encounter['RAW_FACILITY_TYPE'],
                                        joined_encounter['RAW_PAYER_TYPE_PRIMARY'],
                                        joined_encounter['RAW_PAYER_NAME_PRIMARY'],
                                        joined_encounter['RAW_PAYER_ID_PRIMARY'],
                                        joined_encounter['RAW_PAYER_TYPE_SECONDARY'],
                                        joined_encounter['RAW_PAYER_NAME_SECONDARY'],
                                        joined_encounter['RAW_PAYER_ID_SECONDARY'],
                                        cf.get_current_time_udf().alias("updated"),
                                        joined_encounter['SOURCE'].alias("source"),
                                        lit('ENCOUNTER').alias("mapped_from"),
                                    

                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################
            files_count = len(encounter_files)
            current_count= counter
            file_name = f"encounter_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = encounter_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = encounter_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'encounter_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')