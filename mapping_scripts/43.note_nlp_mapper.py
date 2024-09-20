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
spark = cf.get_spark_session("note_nlp_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/note_nlp/'




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        schema = StructType([
                     StructField("note_nlp_id", StringType(), False),
                     StructField("note_id", StringType(), False),
                     StructField("section_concept_id", StringType(), False),
                     StructField("snippet", StringType(), False),
                     StructField("offset", StringType(), False),
                     StructField("lexical_variant", StringType(), False),
                     StructField("note_nlp_concept_id", StringType(), False),
                     StructField("note_nlp_source_concept_id", StringType(), False),
                     StructField("nlp_system", StringType(), False),
                     StructField("nlp_date", StringType(), False),
                     StructField("nlp_date_time", StringType(), False),
                     StructField("nlp_datetime", StringType(), False),
                     StructField("term_exists", StringType(), False),
                     StructField("term_temporal", StringType(), False),
                     StructField("term_modifiers", StringType(), False),

                     ])

        note_nlp = spark.createDataFrame([], schema)
                                

                                                            

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        cf.write_pyspark_output_file(
                        payspark_df = note_nlp,
                        output_file_name = "note_nlp.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'note_nlp_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')