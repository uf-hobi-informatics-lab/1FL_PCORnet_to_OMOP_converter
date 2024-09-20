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
spark = cf.get_spark_session("prescribing_supplementary_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



 
try:


    for folder in folders :
        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        prescribing_input_path               = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PRESCRIBING/PRESCRIBING.csv*'
        prescribing_files = sorted(glob.glob(prescribing_input_path))

        
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/prescribing_supplementary/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



    
        
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)


        ###################################################################################################################################
        # Mapping prescribing to drug_exposure
        ###################################################################################################################################


        counter = 0
        for prescribing_file in prescribing_files:

            counter =  counter + 1
            suffix = prescribing_file.rsplit(".", 1)[-1]
            prescribing                 = cf.spark_read(prescribing_file, spark)

            prescribingid_to_omop_id_mapping_path             = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_prescribingid_to_omop_id/mapping_prescribingid_to_omop_id.csv.{suffix}'
            prescribingid_to_omop_id_mapping               = cf.spark_read(prescribingid_to_omop_id_mapping_path,spark)


            joined_prescribing =     prescribing.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==prescribing['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(prescribingid_to_omop_id_mapping, prescribingid_to_omop_id_mapping['pcornet_prescribingid']==prescribing['PRESCRIBINGID'], how='inner')\
        





            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


            prescribing_supplementary = joined_prescribing.select(              
                
                                                joined_prescribing['omop_id'].alias("prescribing_supplementary_id"),
                                                joined_prescribing['omop_person_id'].alias("person_id"),
                                                joined_prescribing['RX_ORDER_DATE'],
                                                joined_prescribing['RX_ORDER_TIME'],
                                                joined_prescribing['RX_DOSE_FORM'],
                                                joined_prescribing['RX_PRN_FLAG'],
                                                joined_prescribing['RX_BASIS'],
                                                joined_prescribing['RX_SOURCE'],
                                                joined_prescribing['RX_DISPENSE_AS_WRITTEN'],
                                                joined_prescribing['RAW_RX_MED_NAME'],
                                                joined_prescribing['RAW_RX_FREQUENCY'],
                                                joined_prescribing['RAW_RXNORM_CUI'],
                                                joined_prescribing['RAW_RX_QUANTITY'],
                                                joined_prescribing['RAW_RX_NDC'],
                                                joined_prescribing['RAW_RX_DOSE_ORDERED'],
                                                joined_prescribing['RAW_RX_DOSE_ORDERED_UNIT'],
                                                joined_prescribing['RAW_RX_ROUTE'],
                                                joined_prescribing['RAW_RX_REFILLS'],
                                                cf.get_current_time_udf().alias("updated"),
                                                joined_prescribing['SOURCE'].alias("source"),
                                                lit('PRESCRIBING').alias("mapped_from"),
                                    
                                                                )






        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            files_count = len(prescribing_files)
            current_count= counter
            file_name = f"prescribing_supplementary.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = prescribing_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = prescribing_supplementary,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)

        
    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'prescribing_supplementary_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')