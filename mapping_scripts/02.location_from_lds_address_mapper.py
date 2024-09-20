###################################################################################################################################
# This script will map a PCORNet location table 
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
import pyspark.sql.functions as F

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
spark = cf.get_spark_session("location_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        lds_address_history_input_path                      = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/LDS_ADDRESS_HISTORY/LDS_ADDRESS_HISTORY.csv*'
        lds_address_history_files = sorted(glob.glob(lds_address_history_input_path))

        # address_id_mapping_path                             = f'/app/data/{input_data_folder}/mapping_tables/mapping_addressid_to_omop_id.csv'
        mapped_data_folder_path                             = f'/app/data/{input_data_folder}/omop_tables/{folder}/location/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################



        # address_id_mapping                           = cf.spark_read(address_id_mapping_path,spark)
        

        counter = 0

        

        for lds_address_history_file in lds_address_history_files:
                counter               =  counter + 1
                lds_address_history   = cf.spark_read(lds_address_history_file, spark)
                suffix                = lds_address_history_file.rsplit(".", 1)[-1]




                # lds_address_history = lds_address_history.join(address_id_mapping, address_id_mapping['pcornet_addressid']== lds_address_history['ADDRESSID'])


            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
            ###################################################################################################################################


                location = lds_address_history.select(              
                    
                                            
                                            lit('').alias("address_1"),
                                            lit('').alias("address_2"),
                                            lds_address_history['ADDRESS_CITY'].alias("city"),
                                            lds_address_history['ADDRESS_STATE'].alias("state"),
                                            lds_address_history['ADDRESS_ZIP5'].alias("zip"),
                                            lds_address_history['ADDRESS_COUNTY'].alias("county"),
                                            lit('').alias("location_source_value"),
                                            cf.get_current_time_udf().alias("updated"),
                                            lds_address_history['SOURCE'].alias("source"),
                                            lit('LDS_ADDRESS_HISTORY').alias("mapped_from"),
                                        

                                                                    )

                files_count = len(lds_address_history_files)
                current_count= counter
                file_name = f"location.csv.{suffix}"

                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = lds_address_history_file

                    )
           


                if counter  == 1:
                    
                    unioned_location = location
                else:
                    unioned_location = unioned_location.union(location)

                
        grouped_location =  unioned_location.groupBy('zip')\
            .agg(F.max("updated").alias("updated"),F.max("address_1").alias("address_1"),F.max("address_2").alias("address_2"),F.max("city").alias("city"),F.max("state").alias("state"),F.max("county").alias("county"),F.max("location_source_value").alias("location_source_value"),F.max("source").alias("source"),F.max("mapped_from").alias("mapped_from"))


        
        grouped_location_with_location_id = grouped_location.select(

                        (F.monotonically_increasing_id()*100+LOCATION_ID_FROM_LDS_ADDRESS_OFFSET).alias("location_id"),
                        grouped_location['address_1'].alias("address_1"),
                        grouped_location['address_2'].alias("address_2"),
                        grouped_location['city'].alias("city"),
                        grouped_location['state'].alias("state"),
                        grouped_location['zip'].alias("zip"),
                        grouped_location['county'].alias("county"),
                        grouped_location['location_source_value'].alias("location_source_value"),
                        grouped_location['updated'].alias("updated"),
                        grouped_location['SOURCE'].alias("source"),
                        grouped_location['mapped_from'].alias("mapped_from"),
                    

                                                )


            ###################################################################################################################################
            # Create the output file
            ###################################################################################################################################




        file_name = f"location.csv.000{LOCATION_ID_FROM_LDS_ADDRESS_OFFSET}"


        cf.write_pyspark_output_file(
                        payspark_df = grouped_location_with_location_id,
                        output_file_name = file_name,
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'location_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')