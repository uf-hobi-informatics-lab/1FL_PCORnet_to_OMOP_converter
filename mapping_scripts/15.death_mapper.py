###################################################################################################################################
# This script will map a PCORNet measurement table 
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
spark = cf.get_spark_session("death_mapper")





path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        pcornet_death_input_path             = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH/DEATH.csv*'
        pcornet_death_files = sorted(glob.glob(pcornet_death_input_path))


        death_cause_input_path               = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH_CAUSE/DEATH_CAUSE.csv*'

        patid_to_person_id_mapping_path      = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'
        condition_concept_id_mapping_path    = f'/app/data/{input_data_folder}/mapping_tables/mapping_condition_concept_id.csv' 
        condition_source_concept_id_mapping_path = f'/app/data/{input_data_folder}/mapping_tables/mapping_condition_source_concept_id.csv'
        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/death/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        pcornet_death                                  = cf.spark_read(pcornet_death_input_path,spark)
        pcornet_death_cause                            = cf.spark_read(death_cause_input_path,spark)

        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)
        condition_concept_id                           = cf.spark_read(condition_concept_id_mapping_path,spark)
        condition_source_concept_id                    = cf.spark_read(condition_source_concept_id_mapping_path,spark)



        mapping_death_cause_to_death_type_concept_id_dict = create_map([lit(x) for x in chain(*death_cause_to_death_type_concept_id_dict.items())])


        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        filter_death_cause = pcornet_death_cause.select(
                            pcornet_death_cause['PATID'].alias('death_cause_patid'),
                            pcornet_death_cause['DEATH_CAUSE_SOURCE'],
                            pcornet_death_cause['DEATH_CAUSE'],
        )

        joined_pcornet_death = pcornet_death.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==pcornet_death['PATID'], how='inner').drop('pcornet_patid')\
                                            .join(filter_death_cause, filter_death_cause['death_cause_patid']==pcornet_death['PATID'], how= 'left')\
                                            .join(condition_concept_id, (col('condition_code') == col('DEATH_CAUSE')) , how='left').drop('condition_code')\
                                            .join(condition_source_concept_id, (col('condition_code') == col('DEATH_CAUSE')) , how='left').drop('condition_code')\



        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


        omop_death = joined_pcornet_death.select(              
            
                                            
                                            joined_pcornet_death['omop_person_id'].alias("person_id"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('DEATH_DATE')).alias("death_date"),
                                            cf.get_datetime_from_date_and_time_with_default_value_udf(col('DEATH_DATE'), lit('00:00:00')).alias("death_datetime"),
                                            mapping_death_cause_to_death_type_concept_id_dict[upper(col('DEATH_CAUSE_SOURCE'))].alias("death_type_concept_id"),
                                            joined_pcornet_death['condition_concept_id'].alias("cause_concept_id"),
                                            joined_pcornet_death['DEATH_CAUSE'].alias("cause_source_value"),
                                            joined_pcornet_death['condition_source_concept_id'].alias("cause_source_concept_id"),
                                            cf.get_current_time_udf().alias("updated"),
                                            joined_pcornet_death['SOURCE'].alias("source"),
                                            lit('DEATH').alias("mapped_from"),
                                
                                                            )


        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


        cf.write_pyspark_output_file(
                        payspark_df = omop_death,
                        output_file_name = "death.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'death_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')