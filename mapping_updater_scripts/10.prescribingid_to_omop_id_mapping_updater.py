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
spark = cf.get_spark_session("prescribingid_to_omop_id_mapping_updater")


path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)
    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        pcornet_prescribing_table_path                = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/PRESCRIBING/PRESCRIBING.csv*"
        prescribing_files = sorted(glob.glob(pcornet_prescribing_table_path))
        
        prescribingid_to_omop_id_mapping_file_path    = f"/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_prescribingid_to_omop_id/"



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        counter = 0

        for prescribing_file in prescribing_files:

            # print(pcornet_medadmin_file)

            counter = counter +1 
            prescribing                                     = cf.spark_read(prescribing_file, spark)
            suffix = prescribing_file.rsplit(".", 1)[-1]

            

        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################

            offset = 100*counter+ PRESCRIBING_OFFSET

            omop_id_mapping = prescribing.select(              
                
                
                                        prescribing['PRESCRIBINGID'].alias("pcornet_prescribingid"),                                  
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id") #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            files_count = len(prescribing_file)
            current_count= counter
            file_name = f"mapping_prescribingid_to_omop_id.csv.{suffix}"

            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = prescribing_file

                        )




            cf.write_pyspark_output_file(
                            payspark_df = omop_id_mapping,
                            output_file_name = file_name,
                            output_data_folder_path= prescribingid_to_omop_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'prescribingid_to_omop_id_mapping_updater.py' )

    cf.print_with_style(str(e), 'danger red','error')