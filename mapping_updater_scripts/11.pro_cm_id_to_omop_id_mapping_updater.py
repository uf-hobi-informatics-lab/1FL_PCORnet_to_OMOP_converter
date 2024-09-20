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
spark = cf.get_spark_session("pro_cmid_to_omop_id_mapping_updater")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]
try:


    for folder in folders :

        print(folder)


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)


    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        pcornet_pro_cm_table_path                 = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/PRO_CM/PRO_CM.csv*"
        pro_cmid_to_omop_id_mapping_file_path     = f"/app/data/{input_data_folder}/mapping_tables/{folder}/"



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        try:

            pro_cm = spark.read.option("inferSchema", "false").load(pcornet_pro_cm_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
            
        




        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


            omop_id_mapping = pro_cm.select(              
                
                
                                        pro_cm['PRO_CM_ID'].alias("pcornet_pro_cm_id"),                                  
                                        ((F.monotonically_increasing_id()*100+PRO_CM_ID_OFFSET)*100+partner_num).alias("omop_id") #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            cf.write_pyspark_output_file(
                            payspark_df = omop_id_mapping,
                            output_file_name = "mapping_pro_cm_id_to_omop_id.csv",
                            output_data_folder_path= pro_cmid_to_omop_id_mapping_file_path)

        except Exception as e:
            print(e)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'pro_cm_id_to_omop_id_mapping_updater.py' )

    cf.print_with_style(str(e), 'danger red','error')