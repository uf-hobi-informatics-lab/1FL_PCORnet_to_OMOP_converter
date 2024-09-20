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
spark = cf.get_spark_session("facility_id_care_site_id_mapping_updater")


path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


try:

# folders =['FLM']

    for folder in folders :


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################

                
        facilityid_table_path          = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/FACILITYID/FACILITYID.csv*"

        print(facilityid_table_path)


        facility_id_care_site_id_mapping_file_path         = f"/app/data/{input_data_folder}/mapping_tables/{folder}/"



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        distinct_facilityid = spark.read.option("inferSchema", "false").load(facilityid_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
    




    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

        window_spec = Window.orderBy(F.monotonically_increasing_id())

        facility_id_care_site_id_mapping = distinct_facilityid.select(              
            
            
                                    distinct_facilityid['FACILITYID'].alias("pcornet_facilityid"),
                                    (F.monotonically_increasing_id()*100+partner_num).alias("omop_care_site_id")
                                                            )

        deduplicated_facility_id_care_site_id_mapping= facility_id_care_site_id_mapping.dropDuplicates()

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = deduplicated_facility_id_care_site_id_mapping,
                        output_file_name = "mapping_facilityid_care_site_id.csv",
                        output_data_folder_path= facility_id_care_site_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'facility_id_care_site_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')