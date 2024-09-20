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
spark = cf.get_spark_session("lab_result_cm_to_combined_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        lab_result_cm_input_path = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/LAB_RESULT_CM/LAB_RESULT_CM.csv*'
        lab_result_cm_files = sorted(glob.glob(lab_result_cm_input_path))


        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################




        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_lab_result_source_to_type_concept_id_dict = create_map([lit(x) for x in chain(*lab_result_source_to_type_concept_id_dict.items())])
        mapping_result_qual_to_value_as_concept_id_dict = create_map([lit(x) for x in chain(*result_qual_to_value_as_concept_id_dict.items())])
        mapping_result_unit_to_unit_concept_id_dict = create_map([lit(x) for x in chain(*result_unit_to_unit_concept_id_dict.items())])
        mapping_operator_concept_id_dict = create_map([lit(x) for x in chain(*operator_concept_id_dict.items())])

            ###################################################################################################################################
            # Mapping lab_result_cm to measurement
            ###################################################################################################################################


        counter = 0

        for lab_result_cm_file in lab_result_cm_files:
                counter =  counter + 1
                lab_result_cm                                     = cf.spark_read(lab_result_cm_file, spark)
                suffix = lab_result_cm_file.rsplit(".", 1)[-1]

                lab_result_cm_id_to_omop_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_lab_result_cm_id_to_omop_id/mapping_lab_result_cm_id_to_omop_id.csv.{suffix}'
                lab_result_cm_px_to_omop_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_lab_result_cm_px_to_omop_id/mapping_lab_result_cm_px_to_omop_id.csv.{suffix}'


                lab_result_cm_id_to_omop_id_mapping            = cf.spark_read(lab_result_cm_id_to_omop_id_mapping_path,spark)
                lab_result_cm_px_to_omop_id_mapping            = cf.spark_read(lab_result_cm_px_to_omop_id_mapping_path,spark)

                values_to_replace = ["OT", "UN"]
                masked_lab_result = lab_result_cm.withColumn("LAB_PX_TYPE", when(col("LAB_PX_TYPE").isin(values_to_replace), "NI").otherwise(col("LAB_PX_TYPE")))

                # sys.exit('exiting ......')
                joined_lab_result = masked_lab_result.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_lab_result['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(lab_result_cm_id_to_omop_id_mapping, lab_result_cm_id_to_omop_id_mapping['pcornet_lab_result_cm_id']==masked_lab_result['LAB_RESULT_CM_ID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_lab_result['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\






                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_lab_result_cm = joined_lab_result.select(              
                    
                                                    joined_lab_result['omop_id'].alias("common_id"),
                                                    joined_lab_result['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RESULT_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('RESULT_DATE'), col('RESULT_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('RESULT_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RESULT_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('RESULT_DATE'),col('RESULT_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('RESULT_TIME')).alias("event_end_time"),
                                                    mapping_lab_result_source_to_type_concept_id_dict[upper(col('LAB_RESULT_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_lab_result['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_lab_result['LAB_LOINC'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_lab_result['RESULT_NUM'].alias("value_as_number"),
                                                    mapping_result_qual_to_value_as_concept_id_dict[upper(col('RESULT_QUAL'))].alias("value_as_concept_id"),
                                                    mapping_result_unit_to_unit_concept_id_dict[upper(col('RESULT_UNIT'))].alias("unit_concept_id"),
                                                    joined_lab_result['RESULT_UNIT'].alias("unit_source_value"),
                                                    mapping_operator_concept_id_dict[upper(col('RESULT_MODIFIER'))].alias("operator_concept_id"),
                                                    joined_lab_result['NORM_RANGE_LOW'].alias("range_low"),
                                                    joined_lab_result['NORM_RANGE_HIGH'].alias("range_high"),
                                                    concat(col("RESULT_NUM"), lit(" - "), col("RESULT_QUAL")).alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_lab_result['LAB_RESULT_SOURCE'].alias("common_data_type_source_value"),
                                                    joined_lab_result['RESULT_MODIFIER'].alias("operator_source_value"),
                                                    lit('LC').alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_lab_result['SOURCE'].alias("source"),
                                                    lit('LAB_RESULT_CM').alias("mapped_from"),
                                        
                                                                    )

                                                                    


                ###################################################################################################################################
                # Mapping lab_result_cm to measurement
                ###################################################################################################################################


                lab_result_px = masked_lab_result.filter(col('LAB_PX').isNotNull())
                joined_lab_result_px = lab_result_px.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==lab_result_px['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(lab_result_cm_px_to_omop_id_mapping, lab_result_cm_px_to_omop_id_mapping['pcornet_lab_result_cm_id']==lab_result_px['LAB_RESULT_CM_ID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== lab_result_px['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_lab_result_cm_px = joined_lab_result_px.select(              
                    
                                                    joined_lab_result_px['omop_id'].alias("common_id"),
                                                    joined_lab_result_px['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('LAB_ORDER_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('LAB_ORDER_DATE'), lit('00:00:00')).alias("event_start_datetime"),
                                                    lit('').alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('LAB_ORDER_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('LAB_ORDER_DATE'),lit('00:00:00')).alias("event_end_datetime"),
                                                    lit('').alias("event_end_time"),
                                                    mapping_lab_result_source_to_type_concept_id_dict[upper(col('LAB_RESULT_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_lab_result_px['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_lab_result_px['LAB_PX'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    lit('').alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('').alias("unit_concept_id"),
                                                    lit('').alias("unit_source_value"),
                                                    mapping_operator_concept_id_dict[upper(col('RESULT_MODIFIER'))].alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    lit('').alias("value_source_value"),
                                                    lit('No Information').alias("qualifier_source_value"),
                                                    lit('44814650').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_lab_result_px['LAB_RESULT_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    lit('NI').alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_lab_result_px['SOURCE'].alias("source"),
                                                    lit('LAB_RESULT_CM_PX').alias("mapped_from"),
                                        
                                                                    )

                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################

                combined_common_table = common_from_lab_result_cm.union(common_from_lab_result_cm_px)

                files_count = len(lab_result_cm_files)
                current_count= counter
                file_name = f"common_from_lab_result_cm.csv.{LAB_RESULT_CM_ID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = lab_result_cm_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = combined_common_table,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)



    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'lab_result_cm_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')