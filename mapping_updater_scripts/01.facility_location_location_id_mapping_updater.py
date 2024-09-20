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
spark = cf.get_spark_session("location_id_mapping_updater")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


try:


    for folder in folders :


                partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)

                        
            ###################################################################################################################################
            # Load the config file for the selected parnter
            ###################################################################################################################################



                pcornet_encounter_table_path             = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/ENCOUNTER/ENCOUNTER.csv*"
                pcornet_lds_address_history_table_path   = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/LDS_ADDRESS_HISTORY/LDS_ADDRESS_HISTORY.csv*"
                facility_location_location_id_mapping_file_path            = f"/app/data/{input_data_folder}/mapping_tables/{folder}/"



            ###################################################################################################################################
            # Loading the unmapped enctounter table
            ###################################################################################################################################


                encounter = spark.read.option("inferSchema", "false").load(pcornet_encounter_table_path,format="csv", sep="\t", inferSchema="false", header="true",  quote= '"')
                lds_address_history = spark.read.option("inferSchema", "false").load(pcornet_lds_address_history_table_path,format="csv", sep="\t", inferSchema="false", header="true",  quote= '"')
        
                distinct_facility_location = encounter.select("FACILITY_LOCATION", "SOURCE").distinct()
                distinct_address_zip5 = lds_address_history.select("ADDRESS_ZIP5", "SOURCE").distinct()

                appended_df = distinct_facility_location.union(distinct_address_zip5)




            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


                facility_location_location_id_mapping = appended_df.select(              
                    
                    
                                            distinct_facility_location['FACILITY_LOCATION'].alias("pcornet_facility_location"),
                                            ((F.monotonically_increasing_id()*100+LOCATION_ID_FROM_ENCOUNTER_OFFSET)*100+partner_num).alias("omop_location_id"),
                                            distinct_facility_location['SOURCE'].alias('source')

                                                                    )
                deduplicated_facility_location_location_id_mapping= facility_location_location_id_mapping.dropDuplicates(subset=["pcornet_facility_location"])

            ###################################################################################################################################
            # Create the output file
            ###################################################################################################################################

        

                cf.write_pyspark_output_file(
                                payspark_df = deduplicated_facility_location_location_id_mapping,
                                output_file_name = "mapping_facility_location_to_location_id.csv",
                                output_data_folder_path= facility_location_location_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'location_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')