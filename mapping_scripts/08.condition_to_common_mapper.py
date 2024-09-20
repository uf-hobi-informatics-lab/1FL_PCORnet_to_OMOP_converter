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
spark = cf.get_spark_session("condition_to_combined_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        condition_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/CONDITION/CONDITION.csv*'
        condition_files = sorted(glob.glob(condition_input_path))



        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'

        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_condition_type_concept_id_dict = create_map([lit(x) for x in chain(*condition_type_concept_id_dict.items())])
        mapping_condition_status_concept_id_dict = create_map([lit(x) for x in chain(*condition_status_concept_id_dict.items())])

        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################



        counter = 0

        for condition_file in condition_files:
                counter =  counter + 1
                condition                                     = cf.spark_read(condition_file, spark)
                suffix = condition_file.rsplit(".", 1)[-1]

                conditionid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_conditionid_to_omop_id/mapping_conditionid_to_omop_id.csv.{suffix}'
                conditionid_to_omop_id_mapping                 = cf.spark_read(conditionid_to_omop_id_mapping_path,spark)

                values_to_replace = ["OT", "UN"]
                masked_condition = condition.withColumn("CONDITION_TYPE", when(col("CONDITION_TYPE").isin(values_to_replace), "NI").otherwise(col("CONDITION_TYPE")))

                # sys.exit('exiting ......')
                joined_condition = masked_condition.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_condition['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(conditionid_to_omop_id_mapping, conditionid_to_omop_id_mapping['pcornet_conditionid']==masked_condition['CONDITIONID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_condition['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_condition = joined_condition.select(              
                    
                                                    joined_condition['omop_id'].alias("common_id"),
                                                    joined_condition['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('REPORT_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_str_with_default_value_udf(col('REPORT_DATE') ).alias("event_start_datetime"),
                                                    lit('').alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RESOLVE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_str_with_default_value_udf(col('RESOLVE_DATE')).alias("event_end_datetime"),
                                                    lit('').alias("event_end_time"),
                                                    mapping_condition_type_concept_id_dict[upper(col('CONDITION_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_condition['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_condition['CONDITION'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    joined_condition['CONDITION_STATUS'].alias("common_data_status_source_value"),
                                                    mapping_condition_status_concept_id_dict[upper(col('CONDITION_STATUS'))].alias("common_data_status_concept_id"),
                                                    lit('').alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('').alias("unit_concept_id"),
                                                    lit('').alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    lit('').alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_condition['CONDITION_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    joined_condition['CONDITION_TYPE'].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_condition['SOURCE'].alias("source"),
                                                    lit('CONDITION').alias("mapped_from"),
                                        
                                                                    )




                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################


                files_count = len(condition_files)
                current_count= counter
                file_name = f"common_from_condition.csv.{CONDITIONID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = condition_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = common_from_condition,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'condition_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')