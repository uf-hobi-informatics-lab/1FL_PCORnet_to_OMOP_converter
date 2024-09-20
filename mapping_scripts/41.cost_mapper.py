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
spark = cf.get_spark_session("cost_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/cost/'




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        schema = StructType([
                     StructField("cost_id", StringType(), False),
                     StructField("cost_event_id", StringType(), False),
                     StructField("cost_domain_id", StringType(), False),
                     StructField("cost_type_concept_id", StringType(), False),
                     StructField("currency_concept_id", StringType(), False),
                     StructField("total_charge", StringType(), False),
                     StructField("total_cost", StringType(), False),
                     StructField("total_paid", StringType(), False),
                     StructField("paid_by_payer", StringType(), False),
                     StructField("paid_by_patient", StringType(), False),
                     StructField("paid_patient_copay", StringType(), False),
                     StructField("paid_patient_coinsurance", StringType(), False),
                     StructField("paid_patient_deductible", StringType(), False),
                     StructField("paid_by_primary", StringType(), False),
                     StructField("paid_ingredient_cost", StringType(), False),
                     StructField("paid_dispensing_fee", StringType(), False),
                     StructField("payer_plan_period_id", StringType(), False),
                     StructField("amount_allowed", StringType(), False),
                     StructField("revenue_code_concept_id", StringType(), False),
                     StructField("drg_concept_id", StringType(), False),
                     StructField("revenue_code_source_value", StringType(), False),
                     StructField("drg_source_value", StringType(), False),
                     ])

        cost = spark.createDataFrame([], schema)
                                

                                                            

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        cf.write_pyspark_output_file(
                        payspark_df = cost,
                        output_file_name = "cost.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'cost_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')