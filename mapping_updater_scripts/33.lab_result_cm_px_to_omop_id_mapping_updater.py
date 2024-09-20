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
import glob
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
spark = cf.get_spark_session("lab_result_cm_px_to_omop_id_mapping_updater")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


try:



    for folder in folders :


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        pcornet_lab_result_cm_table_path                  = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/LAB_RESULT_CM/LAB_RESULT_CM.csv*"
        lab_result_cm_files = sorted(glob.glob(pcornet_lab_result_cm_table_path))


        lab_result_cm_px_to_omop_id_mapping_file_path     = f"/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_lab_result_cm_px_to_omop_id/"



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        counter = 0

        for lab_result_cm_file in lab_result_cm_files:

            # print(pcornet_medadmin_file)

            counter = counter +1 
            lab_result_cm                                     = cf.spark_read(lab_result_cm_file, spark)
            suffix = lab_result_cm_file.rsplit(".", 1)[-1]
            
            filtered_lab_result_cm = lab_result_cm.filter(col('LAB_PX').isNotNull())




        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################

            offset = 100*counter+ LAB_RESULT_CM_ID_PX_OFFSET
            omop_id_mapping = filtered_lab_result_cm.select(              
                
                
                                        filtered_lab_result_cm['LAB_RESULT_CM_ID'].alias("pcornet_lab_result_cm_id"),                                  
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id").alias("omop_id") #mulitplying by 10 and adding an offset to ensure this id is unique to this table
                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            files_count = len(lab_result_cm_files)
            current_count= counter
            file_name = f"mapping_lab_result_cm_px_to_omop_id.csv.{suffix}"

            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = lab_result_cm_file

                        )



            cf.write_pyspark_output_file(
                            payspark_df = omop_id_mapping,
                            output_file_name = file_name,
                            output_data_folder_path= lab_result_cm_px_to_omop_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'lab_result_cm_px_to_omop_id_mapping' )

    cf.print_with_style(str(e), 'danger red','error')