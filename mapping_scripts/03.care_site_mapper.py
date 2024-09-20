###################################################################################################################################
# This script will map a PCORNet care_site table 
###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F

from commonFunctions import CommonFuncitons
from dictionaries import *
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
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
spark = cf.get_spark_session("care_site_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]


 
try:

    for folder in folders :

    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################


                
        encounter_input_path                = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/ENCOUNTER/ENCOUNTER.csv*'
        encounter_files = sorted(glob.glob(encounter_input_path))

        care_site_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_facilityid_care_site_id.csv'
        location_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_facility_location_to_location_id.csv'
        mapped_data_folder_path             = f'/app/data/{input_data_folder}/omop_tables/{folder}/care_site/'



    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################




        care_site_id_mapping                = cf.spark_read(care_site_id_mapping_path,spark)
        location_id_mapping                 = cf.spark_read(location_id_mapping_path,spark)





        mapping_place_of_service_concept_id_dict = create_map([lit(x) for x in chain(*place_of_service_concept_id_dict.items())])



        counter = 0

        for encounter_file in encounter_files:
                counter =  counter + 1
                encounter                                     = cf.spark_read(encounter_file, spark)
                suffix = encounter_file.rsplit(".", 1)[-1]


                joined_df = encounter.join(care_site_id_mapping, care_site_id_mapping['pcornet_facilityid']== encounter['FACILITYID']).drop('SOURCE')\
                           .join(location_id_mapping, location_id_mapping['pcornet_facility_location']== encounter['FACILITY_LOCATION'], how = 'left')

            ###################################################################################################################################
            # Apply the mappings dictionaries and the common function on the fields of the unmmaped encounter table
            ###################################################################################################################################


                care_site = joined_df.select(              
                    
                                            joined_df['omop_care_site_id'].alias("care_site_id"),
                                            joined_df['FACILITYID'].alias("care_site_name"),
                                            mapping_place_of_service_concept_id_dict[upper(col('FACILITY_TYPE'))].alias("place_of_service_concept_id"),
                                            joined_df['omop_location_id'].alias("location_id"),
                                            joined_df['FACILITYID'].alias("care_site_source_value"),
                                            joined_df['FACILITY_TYPE'].alias("place_of_service_source_value"),
                                            cf.get_current_time_udf().alias("updated"),
                                            joined_df['SOURCE'].alias("source"),
                                            lit('ENCOUNTER').alias("mapped_from"),
                                        

                                                                    )

                care_site =  care_site.groupBy('care_site_id','care_site_name','location_id','care_site_source_value','source','mapped_from')\
                    .agg(F.max("place_of_service_concept_id").alias("place_of_service_concept_id"), F.max("place_of_service_source_value").alias("place_of_service_source_value") , F.max("updated").alias("updated"))

                if counter == 1:

                    care_site_combined = care_site
                else : 

                    care_site_combined = care_site_combined.union(care_site)


            ###################################################################################################################################
            # Create the output file
            ###################################################################################################################################
 
                files_count = len(encounter_files)
                current_count= counter

                file_name_tmp = f"care_site.csv.{suffix}"
                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name_tmp,
                    source_file = encounter_file

                    )

        care_site_combined =  care_site_combined.groupBy('care_site_id','care_site_name','location_id','care_site_source_value','source','mapped_from')\
            .agg(F.max("place_of_service_concept_id").alias("place_of_service_concept_id"), F.max("place_of_service_source_value").alias("place_of_service_source_value") , F.max("updated").alias("updated"))

        
        file_name = f"care_site.csv"

        cf.write_pyspark_output_file(
                        payspark_df = care_site_combined,
                        output_file_name = file_name,
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'care_site_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')