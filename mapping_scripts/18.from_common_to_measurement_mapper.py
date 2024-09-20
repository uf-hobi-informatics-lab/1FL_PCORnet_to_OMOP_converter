###################################################################################################################################
# This script will map a PCORNet demographic table 
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
spark = cf.get_spark_session("from_omop_to_measurement_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


counter = 0

###################################################################################################################################
# This function will return the year value from a date input
###################################################################################################################################

def map_common_to_measurement(common_df):

            measurement = common_df.select(  
                                common_df['common_id'].alias("measurement_id"),
                                common_df['person_id'].alias("person_id"),
                                common_df['measurement_concept_id'].alias("measurement_concept_id"),
                                common_df['event_start_date'].alias("measurement_date"),
                                common_df['event_start_datetime'].alias("measurement_datetime"),
                                common_df['event_start_time'].alias("measurement_time"),
                                common_df['common_data_type_concept_id'].alias("measurement_type_concept_id"),
                                common_df['operator_concept_id'].alias("operator_concept_id"),
                                common_df['value_as_number'].alias("value_as_number"),
                                common_df['value_as_concept_id'].alias("value_as_concept_id"),
                                common_df['unit_concept_id'].alias("unit_concept_id"),
                                common_df['range_low'].alias("range_low"),
                                common_df['range_high'].alias("range_high"),
                                common_df['provider_id'].alias("provider_id"),
                                common_df['visit_occurrence_id'].alias("visit_occurrence_id"),
                                common_df['visit_detail_id'].alias("visit_detail_id"),
                                common_df['common_data_source_value'].alias("measurement_source_value"),
                                common_df['measurement_source_concept_id'].alias("measurement_source_concept_id"),
                                common_df['unit_source_value'].alias("unit_source_value"),
                                common_df['value_source_value'].alias("value_source_value"),
                                common_df['updated'].alias("updated"),
                                common_df['source'].alias("source"),
                                common_df['mapped_from'].alias("mapped_from")

             )

            return measurement

 

def output_measurement(counter,files_count,measurement,suffix, mapped_data_folder_path,common_from_measurement_file):

        file_name = f"measurement.csv.{suffix}"


        cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = counter,
                    output_file_name =  file_name,
                    source_file = common_from_measurement_file

        )
        cf.write_pyspark_output_file(
                        payspark_df = measurement,
                        output_file_name = file_name ,
                        output_data_folder_path= mapped_data_folder_path)


 
try:

    for folder in folders :


        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        common_input_path        = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/common_from*'
        common_files = sorted(glob.glob(common_input_path))


                    

        measurement_concept_id_mapping_input_path                   = f'/app/data/{input_data_folder}/mapping_tables/mapping_measurement_concept_id.csv'
        measurement_source_concept_id_mapping_input_path            = f'/app/data/{input_data_folder}/mapping_tables/mapping_measurement_source_concept_id.csv'


        mapped_data_folder_path                 = f'/app/data/{input_data_folder}/omop_tables/{folder}/measurement/'

        files_count = len(common_files)

        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################




        measurement_concept_id_mapping         = cf.spark_read(measurement_concept_id_mapping_input_path, spark)
        measurement_source_concept_id_mapping  = cf.spark_read(measurement_source_concept_id_mapping_input_path, spark)



    ###################################################################################################################################
    ###################################################################################################################################



        for common_file in common_files:

            common          = cf.spark_read(common_file, spark)    
            filtered_common = cf.filter_common_table(
            
                                        common_table = common,
                                        concept_id_mapping = measurement_concept_id_mapping,
                                        source_concept_id_mapping = measurement_source_concept_id_mapping,
                                        concept_code_field_name = 'measurement_code',
                                        concept_code_type_filed_name = 'pcornet_measurement_code_type',
                                        common_code_field_name = 'common_data_source_value',
                                        common_code_type_field_name = 'pcornet_code_type')


            counter =  counter + 1
            suffix = common_file.rsplit(".", 1)[-1]

            measurement= map_common_to_measurement(filtered_common)
            
            output_measurement(
                counter,
                files_count,
                measurement,
                suffix,
                mapped_data_folder_path,
                common_file
            )




    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'measurement_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')