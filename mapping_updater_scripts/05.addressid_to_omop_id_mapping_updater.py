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

import glob



###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_data_folder = args.data_folder


cf =CommonFuncitons()

# spin the pyspak cluster and
spark = cf.get_spark_session("addressid_to_omop_id_mapping_updater")


path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]

try:


    for folder in folders :


                partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)


                ###################################################################################################################################
                # Load the config file for the selected parnter
                ###################################################################################################################################
                        
                pcornet_address_table_path                 = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/LDS_ADDRESS_HISTORY/LDS_ADDRESS_HISTORY.csv*"
                pcornet_address_files = sorted(glob.glob(pcornet_address_table_path))

                addressid_to_omop_id_mapping_file_path     = f"/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_addressid_to_omop_id/"



                ###################################################################################################################################
                # Loading the unmapped enctounter table
                ###################################################################################################################################

                counter = 0

                for pcornet_address_file in pcornet_address_files:

                    counter = counter +1 
                    pcornet_address                                     = cf.spark_read(pcornet_address_file, spark)
                    suffix = pcornet_address_file.rsplit(".", 1)[-1]




                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################

                    offset = 100*counter+ ADDRESSID_OFFSET

                    omop_id_mapping = pcornet_address.select(              
                        
                        
                                                pcornet_address['ADDRESSID'].alias("pcornet_addressid"),                                  
                                                ((F.monotonically_increasing_id()*100000*counter+offset)*100+partner_num).alias("omop_id") #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                        )

                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################
                    files_count = len(pcornet_address_files)
                    current_count= counter
                    file_name = f"mapping_addressid_to_omop_id.csv.{suffix}"

                    cf.print_mapping_file_status(

                                total_count = files_count,
                                current_count = current_count,
                                output_file_name =  file_name,
                                source_file = pcornet_address_file

                                )
                    cf.write_pyspark_output_file(
                                    payspark_df = omop_id_mapping,
                                    output_file_name = file_name,
                                    output_data_folder_path= addressid_to_omop_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'addressid_to_omop_id_mapping_updater.py' )

    cf.print_with_style(str(e), 'danger red','error')