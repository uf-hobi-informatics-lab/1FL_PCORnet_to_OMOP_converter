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
spark = cf.get_spark_session("patid_location_id_mapping_updater")

path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:


    for folder in folders :


                partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)


            ###################################################################################################################################
            # Load the config file for the selected parnter
            ###################################################################################################################################
                    
                pcornet_lds_address_hx_table_path     = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/LDS_ADDRESS_HISTORY/LDS_ADDRESS_HISTORY.csv*"
                zip5_to_location_id_mapping_path      = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_facility_location_to_location_id.csv'
                patid_location_id_mapping_file_path   = f"/app/data/{input_data_folder}/mapping_tables/{folder}/"



            ###################################################################################################################################
            # Loading the unmapped enctounter table
            ###################################################################################################################################


                lds_address_hx         = spark.read.option("inferSchema", "false").load(pcornet_lds_address_hx_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
                
                zip5_to_location_id    = spark.read.option("inferSchema", "false").load(zip5_to_location_id_mapping_path,format="csv",   sep="\t", inferSchema="true", header="true",  quote= '"')
                lds_address_hx = lds_address_hx.select(lds_address_hx['PATID'],lds_address_hx['ADDRESS_ZIP5'],lds_address_hx['ADDRESS_PERIOD_START'])
                lds_address_hx_grouped = lds_address_hx.groupBy('PATID','ADDRESS_ZIP5').agg(F.max("ADDRESS_PERIOD_START").alias("MAX_ADDRESS_PERIOD_START"))

                join_df = lds_address_hx_grouped.join(zip5_to_location_id, lds_address_hx_grouped['ADDRESS_ZIP5']==zip5_to_location_id['pcornet_facility_location'])

            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


                patid_location_id_mapping = join_df.select(              
                    
                    
                                            join_df['PATID'].alias('pcornet_patid'),
                                            join_df['omop_location_id'],
                                                                    )

                deduplicated_patid_location_id_mapping= patid_location_id_mapping.groupBy('pcornet_patid').agg(F.max("omop_location_id").alias("omop_location_id"))
            ###################################################################################################################################
            # Create the output file
            ###################################################################################################################################

                cf.write_pyspark_output_file(
                                payspark_df = deduplicated_patid_location_id_mapping,
                                output_file_name = "mapping_patid_location_id.csv",
                                output_data_folder_path= patid_location_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'patid_location_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')