###################################################################################################################################
# This script will map a PCORNet condition table 
###################################################################################################################################

 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from commonFunctions import CommonFuncitons 
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
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
spark = cf.get_spark_session("enrollment_to_omop_id_mapping_updater")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


try:



    for folder in folders :


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        pcornet_enrollment_table_path              = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/ENROLLMENT/ENROLLMENT.csv*"
        enrollment_to_omop_id_mapping_file_path    = f"/app/data/{input_data_folder}/mapping_tables/{folder}/"



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        enrollment = spark.read.option("inferSchema", "false").load(pcornet_enrollment_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################


        enrollment_id_mapping = enrollment.select(              
            
                                    concat( col("PATID"),
                                            col("ENR_START_DATE"),
                                            col("ENR_BASIS")
                                            ).alias("pcornet_id"),
                                            ((F.monotonically_increasing_id()*100+ENROLLMENT_OFFSET)*100+partner_num).alias("omop_id")
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = enrollment_id_mapping,
                        output_file_name = "mapping_enrollment_to_omop_id.csv",
                        output_data_folder_path= enrollment_to_omop_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'person_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')