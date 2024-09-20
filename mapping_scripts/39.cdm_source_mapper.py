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
spark = cf.get_spark_session("cdm_source_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



 
try:

    for folder in folders :


    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        harvest_input_path            = f'/app/data/{input_data_folder}/pcornet_tables/{folder}//HARVEST/HARVEST.csv*'
        harvest_files = sorted(glob.glob(harvest_input_path))


        vocabulary_input_path         = f'/app/data/omop_vocabulary_tables/VOCABULARY.csv'

        
        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/cdm_source/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        harvest          = cf.spark_read(harvest_input_path,spark)
        vocabulary       = cf.spark_read(vocabulary_input_path,spark)


        filtered_vocabulary = vocabulary.filter(vocabulary["vocabulary_id"] == "None")
        vocabulary_version = filtered_vocabulary.select("vocabulary_version").collect()[0][0]
    


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        cdm_source = harvest.select(              
            
                                    harvest['datamart_name'].alias("cdm_source_name"),
                                    harvest['DATAMARTID'].alias("cdm_source_abbreviation"),
                                    lit('OneFlroida').alias("cdm_holder"),
                                    lit('OMOP implementation of OneFlorida').alias("source_description"),
                                    lit('').alias("source_documentation_reference"),
                                    lit('https://ohdsi.github.io/CommonDataModel/cdm54').alias("cdm_etl_reference"),
                                    harvest['REFRESH_ENCOUNTER_DATE'].alias("source_release_date"),
                                    cf.get_current_date_udf().alias("cdm_release_date"),
                                    lit('CDM v5.4.0').alias("cdm_version"),
                                    lit(vocabulary_version).alias("vocabulary_version"), 
                                    lit('756265').alias('cdm_version_concept_id'),                                 
                                    cf.get_current_time_udf().alias("updated"),
                                    lit('HARVEST').alias("mapped_from"),
                                

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################
        cf.write_pyspark_output_file(
                        payspark_df = cdm_source,
                        output_file_name = "cdm_source.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'cdm_source_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')