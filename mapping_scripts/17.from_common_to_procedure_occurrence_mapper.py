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
spark = cf.get_spark_session("from_omop_to_procedure_occurrence_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



counter = 0


def map_common_to_procedure_occurrence(common_df):

            procedure_occurrence = common_df.select(  
                                common_df['common_id'].alias("procedure_occurrence_id"),
                                common_df['person_id'].alias("person_id"),
                                common_df['procedure_concept_id'].alias("procedure_concept_id"),
                                common_df['event_start_date'].alias("procedure_date"),
                                common_df['event_start_datetime'].alias("procedure_datetime"),
                                common_df['common_data_type_concept_id'].alias("procedure_type_concept_id"),
                                common_df['qualifier_concept_id'].alias("modifier_concept_id"),
                                common_df['quantity'].alias("quantity"),
                                common_df['provider_id'].alias("provider_id"),
                                common_df['visit_occurrence_id'].alias("visit_occurrence_id"),
                                common_df['visit_detail_id'].alias("visit_detail_id"),
                                common_df['common_data_source_value'].alias("procedure_source_value"),
                                common_df['procedure_source_concept_id'].alias("procedure_source_concept_id"),
                                common_df['qualifier_source_value'].alias("modifier_source_value"),
                                common_df['updated'].alias("updated"),
                                common_df['source'].alias("source"),
                                common_df['mapped_from'].alias("mapped_from")

             )

            return procedure_occurrence

 

def output_procedure_occurrence(counter,files_count,procedure_occurrence,suffix, mapped_data_folder_path,common_from_procedure_file):

        file_name = f"procedure_occurrence.csv.{suffix}"


        cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = counter,
                    output_file_name =  file_name,
                    source_file = common_from_procedure_file

        )
        cf.write_pyspark_output_file(
                        payspark_df = procedure_occurrence,
                        output_file_name = file_name ,
                        output_data_folder_path= mapped_data_folder_path)






 
try:

    for folder in folders :


        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


        common_input_path        = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/common_from*'
        common_files = sorted(glob.glob(common_input_path))






        procedure_concept_id_mapping_input_path                   = f'/app/data/{input_data_folder}/mapping_tables/mapping_procedure_concept_id.csv'
        procedure_source_concept_id_mapping_input_path            = f'/app/data/{input_data_folder}/mapping_tables/mapping_procedure_source_concept_id.csv'


        mapped_data_folder_path                 = f'/app/data/{input_data_folder}/omop_tables/{folder}/procedure_occurrence/'
        files_count = len(common_files)



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################


        procedure_concept_id_mapping         = cf.spark_read(procedure_concept_id_mapping_input_path, spark)
        procedure_source_concept_id_mapping  = cf.spark_read(procedure_source_concept_id_mapping_input_path, spark)



        for common_file in common_files:

            common          = cf.spark_read(common_file, spark)    
            filtered_common = cf.filter_common_table(
            
                                        common_table = common,
                                        concept_id_mapping = procedure_concept_id_mapping,
                                        source_concept_id_mapping = procedure_source_concept_id_mapping,
                                        concept_code_field_name = 'procedure_code',
                                        concept_code_type_filed_name = 'pcornet_procedure_code_type',
                                        common_code_field_name = 'common_data_source_value',
                                        common_code_type_field_name = 'pcornet_code_type')


            counter =  counter + 1
            suffix = common_file.rsplit(".", 1)[-1]

            procedure_occurrence= map_common_to_procedure_occurrence(filtered_common)
            
            output_procedure_occurrence(
                counter,
                files_count,
                procedure_occurrence,
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
                            job     = 'procedure_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')