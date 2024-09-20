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
spark = cf.get_spark_session("visit_detail_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/visit_detail/'




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        schema = StructType([
                     StructField("visit_detail_id", StringType(), False),
                     StructField("person_id", StringType(), False),
                     StructField("visit_detail_concept_id", StringType(), False),
                     StructField("visit_detail_start_date", StringType(), False),
                     StructField("visit_detail_start_datetime", StringType(), False),
                     StructField("visit_detail_end_date", StringType(), False),
                     StructField("visit_detail_end_datetime", StringType(), False),
                     StructField("visit_detail_type_concept_id", StringType(), False),
                     StructField("provider_id", StringType(), False),
                     StructField("care_site_id", StringType(), False),
                     StructField("visit_detail_source_value", StringType(), False),
                     StructField("visit_detail_source_concept_id", StringType(), False),
                     StructField("admitting_source_value", StringType(), False),
                     StructField("admitting_source_concept_id", StringType(), False),
                     StructField("discharge_to_source_value", StringType(), False),
                     StructField("discharge_to_concept_id", StringType(), False),
                     StructField("preceding_visit_detail_id", StringType(), False),
                     StructField("visit_detail_parent_id", StringType(), False),
                     StructField("visit_occurrence_id", StringType(), False),
                     StructField("updated", StringType(), False),
                     StructField("source", StringType(), False),
                     StructField("mapped_from", StringType(), False),
                     
                     
                     ])

        visit_detail = spark.createDataFrame([], schema)
                                

                                                            

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        cf.write_pyspark_output_file(
                        payspark_df = visit_detail,
                        output_file_name = "visit_detail.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'visit_detail_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')