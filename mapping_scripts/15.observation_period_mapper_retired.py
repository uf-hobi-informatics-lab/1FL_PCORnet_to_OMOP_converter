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
spark = cf.get_spark_session("enrollment_to_combined_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        enrollment_input_path                            = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/ENROLLMENT.csv'

        enrollment_to_omop_id_mapping_path               = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_enrollment_to_omop_id.csv'


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path                           = f'/app/data/{input_data_folder}/omop_tables/{folder}/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        enrollment                                    = cf.spark_read(enrollment_input_path,spark)
        enrollment_to_omop_id_mapping                 = cf.spark_read(enrollment_to_omop_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)


        mapping_period_type_concept_id_dict = create_map([lit(x) for x in chain(*period_type_concept_id_dict.items())])


        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################


        # sys.exit('exiting ......')
        joined_enrollment = enrollment.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==enrollment['PATID'], how='inner').drop('pcornet_patid')\
                                            .join(enrollment_to_omop_id_mapping, enrollment_to_omop_id_mapping['pcornet_id']==concat( col("PATID"),col("ENR_START_DATE"),col("ENR_BASIS")), how='inner')\






        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


        observation_period = joined_enrollment.select(              
            
                                            joined_enrollment['omop_id'].alias("observation_period_id"),
                                            joined_enrollment['omop_person_id'].alias("person_id"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('ENR_START_DATE')).alias("observation_period_start_date"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('ENR_END_DATE')).alias("observation_period_end_date"),
                                            mapping_period_type_concept_id_dict[upper(col('ENR_BASIS'))].alias("period_type_concept_id"),
                                            cf.get_current_time_udf().alias("updated"),
                                            joined_enrollment['SOURCE'].alias("source"),
                                            lit('enrollment').alias("mapped_from"),
                                
                                                            )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = observation_period,
                        output_file_name = "observation_period.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'observation_period_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')