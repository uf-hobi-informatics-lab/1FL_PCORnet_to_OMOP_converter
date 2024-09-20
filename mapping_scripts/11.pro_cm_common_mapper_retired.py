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
spark = cf.get_spark_session("pro_cm_to_combined_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        pro_cm_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PRO_CM/PRO_CM.csv*'
        pro_cm_files = sorted(glob.glob(pro_cm_input_path))

        mapping_pro_cm_id_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_pro_cm_id_to_omop_id.csv'

        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'

        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        obsclinid_to_omop_id_mapping                   = cf.spark_read(obsclinid_to_omop_id_mapping_path,spark)
        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)




        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################


        counter = 0

        for pro_cm_file in pro_cm_files:
                counter =  counter + 1
                pro_cm                                     = cf.spark_read(pro_cm_file, spark)
                suffix = pro_cm_file.rsplit(".", 1)[-1]

                        
                values_to_replace = ["OT", "UN"]
                masked_pro_cm = pro_cm.withColumn("PRO_TYPE", when(col("PRO_TYPE").isin(values_to_replace), "NI").otherwise(col("PRO_TYPE")))

                joined_pro_cm = masked_pro_cm.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_pro_cm['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(obsclinid_to_omop_id_mapping, obsclinid_to_omop_id_mapping['pcornet_pro_cm_id']==masked_pro_cm['PRO_CM_ID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_pro_cm['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\






                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_pro_cm = joined_pro_cm.select(              
                    
                                                    joined_pro_cm['omop_id'].alias("common_id"),
                                                    joined_pro_cm['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('PRO_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('PRO_DATE'), col('PRO_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('PRO_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('PRO_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('PRO_DATE'), col('PRO_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('PRO_TIME')).alias("event_end_time"),
                                                    mapping_pro_cm_type_concept_id_dict[upper(col('OBSCLIN_SOURCE'))].alias("common_data_type_concept_id"),
                                                    joined_pro_cm['omop_provider_id'].alias("provider_id"),
                                                    joined_pro_cm['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_pro_cm['OBSCLIN_CODE'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_pro_cm['OBSCLIN_RESULT_NUM'].alias("value_as_number"),
                                                    mapping_pro_cm_result_qual_value_as_concept_id_dict[upper(col('OBSCLIN_RESULT_QUAL'))].alias("value_as_concept_id"),
                                                    mapping_result_unit_to_unit_concept_id_dict[upper(col('OBSCLIN_RESULT_UNIT'))].alias("unit_concept_id"),
                                                    joined_pro_cm['OBSCLIN_RESULT_UNIT'].alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    lit('').alias("value_source_value"),
                                                    joined_pro_cm['OBSCLIN_RESULT_MODIFIER'].alias("qualifier_source_value"),
                                                    mapping_pro_cm_result_modifier_qualifier_concept_id_dict[upper(col('OBSCLIN_RESULT_MODIFIER'))].alias("qualifier_concept_id"),
                                                    joined_pro_cm['OBSCLIN_RESULT_TEXT'].alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_pro_cm['OBSCLIN_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    joined_pro_cm['OBSCLIN_TYPE'].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_pro_cm['SOURCE'].alias("source"),
                                                    lit('pro_cm').alias("mapped_from"),
                                        
                                                                    )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = common_from_pro_cm,
                        output_file_name = "common_from_pro_cm.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'pro_cm_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')