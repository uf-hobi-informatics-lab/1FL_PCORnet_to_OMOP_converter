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
spark = cf.get_spark_session("payer_plan_period_mapper")


 

path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


try:

    for folder in folders :

        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/payer_plan_period/'




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        schema = StructType([
                     StructField("payer_plan_period_id", StringType(), False),
                     StructField("person_id", StringType(), False),
                     StructField("payer_plan_period_start_date", StringType(), False),
                     StructField("payer_plan_period_end_date", StringType(), False),
                     StructField("payer_source_value", StringType(), False),
                     StructField("plan_source_value", StringType(), False),
                     StructField("family_source_value", StringType(), False),
                     StructField("payer_concept_id", StringType(), False),
                     StructField("payer_source_concept_id", StringType(), False),
                     StructField("plan_concept_id", StringType(), False),
                     StructField("plan_source_concept_id", StringType(), False),
                     StructField("sponsor_concept_id", StringType(), False),
                     StructField("sponsor_source_value", StringType(), False),
                     StructField("sponsor_source_concept_id", StringType(), False),
                     StructField("stop_reason_concept_id", StringType(), False),
                     StructField("stop_reason_source_concept_id", StringType(), False),

                     ])

        payer_plan_period = spark.createDataFrame([], schema)
                                

                                                            

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        cf.write_pyspark_output_file(
                        payspark_df = payer_plan_period,
                        output_file_name = "payer_plan_period.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'payer_plan_period_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')