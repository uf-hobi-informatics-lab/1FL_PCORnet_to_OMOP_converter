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
spark = cf.get_spark_session("vital_to_combined_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        vital_input_path = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/VITAL/VITAL.csv*'
        vital_files = sorted(glob.glob(vital_input_path))



        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'


        mapping_vital_source_to_type_concept_id_dict = create_map([lit(x) for x in chain(*vital_source_to_type_concept_id_dict.items())])
        mapping_bp_position_diastolic_to_pb_code_dict = create_map([lit(x) for x in chain(*bp_position_diastolic_to_pb_code_dict.items())])
        mapping_bp_position_systolic_to_pb_code_dict = create_map([lit(x) for x in chain(*bp_position_systolic_to_pb_code_dict.items())])
        mapping_bp_position_to_pb_code_type_dict = create_map([lit(x) for x in chain(*bp_position_to_pb_code_type_dict.items())])

        mapping_smoking_to_smoking_concept_id_dict=  create_map([lit(x) for x in chain(*smoking_to_smoking_concept_id_dict.items())])
        mapping_smoking_to_smoking_text_dict =  create_map([lit(x) for x in chain(*smoking_to_smoking_text_dict.items())])
        mapping_tobacco_to_tobacco_concept_id_dict =  create_map([lit(x) for x in chain(*tobacco_to_tobacco_concept_id_dict.items())])
        mapping_tobacco_to_tobacco_text_dict =   create_map([lit(x) for x in chain(*tobacco_to_tobacco_text_dict.items())])
        mapping_tobacco_type_to_tobacco_type_concept_id_dict =  create_map([lit(x) for x in chain(*tobacco_type_to_tobacco_type_concept_id_dict.items())])
        mapping_tobacco_type_to_tobacco_type_text_dict =  create_map([lit(x) for x in chain(*tobacco_type_to_tobacco_type_text_dict.items())])

        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################






        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)

    
        counter = 0

        for vital_file in vital_files:
                counter =  counter + 1
                vital                                     = cf.spark_read(vital_file, spark)
                suffix = vital_file.rsplit(".", 1)[-1]
            
                vital_id_to_omop_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_vitalid_to_omop_id/mapping_vitalid_to_omop_id.csv.{suffix}'
                vital_id_to_omop_id_mapping               = cf.spark_read(vital_id_to_omop_id_mapping_path,spark)



                vital_wt           = vital.filter((col('WT').isNotNull()) & ( col('WT') !=0) & ( col('WT') !='0' ))
                vital_ht           = vital.filter((col('HT').isNotNull()) & ( col('HT') !=0) & ( col('HT') !='0' ))
                vital_bmi          = vital.filter((col('ORIGINAL_BMI').isNotNull()) & ( col('ORIGINAL_BMI') !=0) & ( col('ORIGINAL_BMI') !='0' ))
                vital_diastolic    = vital.filter((col('DIASTOLIC').isNotNull()) & ( col('DIASTOLIC') !=0) & ( col('DIASTOLIC') !='0' ))
                vital_systolic     = vital.filter((col('SYSTOLIC').isNotNull()) & ( col('SYSTOLIC') !=0) & ( col('SYSTOLIC') !='0' ))
                vital_smoking      = vital.filter((col('SMOKING').isNotNull()) & ( col('SMOKING') !='OT') & ( col('SMOKING') !='NI' ) & ( col('SMOKING') !='UN' ))
                vital_tobacco      = vital.filter((col('TOBACCO').isNotNull()) & ( col('TOBACCO') !='OT') & ( col('TOBACCO') !='NI' ) & ( col('TOBACCO') !='UN' ))
                vital_tobacco_type = vital.filter((col('TOBACCO_TYPE').isNotNull()) & ( col('TOBACCO_TYPE') !='OT') & ( col('TOBACCO_TYPE') !='NI' ) & ( col('TOBACCO_TYPE') !='UN' ))



                ###################################################################################################################################
                # Mapping VITAL WT to COMMON table
                ###################################################################################################################################



                joined_vital_wt = vital_wt.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_wt['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'WT'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_wt['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_wt = joined_vital_wt.select(              
                    
                                                    joined_vital_wt['omop_id'].alias("common_id"),
                                                    joined_vital_wt['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_wt['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('29463-7').alias("common_data_source_value"), # body weight loinc code
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_vital_wt['WT'].alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('8739').alias("unit_concept_id"), # lb concept id
                                                    lit('[lb_us]').alias("unit_source_value"),
                                                    lit('4172703').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_wt['WT'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_wt['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('=').alias("operator_source_value"),
                                                    lit('LC').alias("pcornet_code_type"), #LOINC
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_wt['SOURCE'].alias("source"),
                                                    lit('VITAL_WT').alias("mapped_from"),
                                        
                                                                    )




                ###################################################################################################################################
                # Mapping VITAL HT to COMMON table
                ###################################################################################################################################



                joined_vital_ht = vital_ht.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_ht['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'HT'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_ht['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_ht = joined_vital_ht.select(              
                    
                                                    joined_vital_ht['omop_id'].alias("common_id"),
                                                    joined_vital_ht['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_ht['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('8302-2').alias("common_data_source_value"), # body weight loinc code
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_vital_ht['HT'].alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('9330').alias("unit_concept_id"), # lb concept id
                                                    lit('inches').alias("unit_source_value"),
                                                    lit('4172703').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_ht['HT'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_ht['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('=').alias("operator_source_value"),
                                                    lit('LC').alias("pcornet_code_type"), #LOINC
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_ht['SOURCE'].alias("source"),
                                                    lit('VITAL_HT').alias("mapped_from"),
                                        
                                                                    )





                ###################################################################################################################################
                # Mapping VITAL bmi to COMMON table
                ###################################################################################################################################



                joined_vital_bmi = vital_bmi.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_bmi['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'BMI'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_bmi['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_bmi = joined_vital_bmi.select(              
                    
                                                    joined_vital_bmi['omop_id'].alias("common_id"),
                                                    joined_vital_bmi['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_bmi['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('39156-5').alias("common_data_source_value"), # Body mass index (BMI) [Ratio]
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_vital_bmi['ORIGINAL_BMI'].alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('9531').alias("unit_concept_id"), # 
                                                    lit('kg/m2').alias("unit_source_value"),
                                                    lit('4172703').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_bmi['ORIGINAL_BMI'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_bmi['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('=').alias("operator_source_value"),
                                                    lit('LC').alias("pcornet_code_type"), #LOINC
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_bmi['SOURCE'].alias("source"),
                                                    lit('VITAL_BMI').alias("mapped_from"),
                                        
                                                                    )


                ###################################################################################################################################
                # Mapping VITAL diastolic to COMMON table
                ###################################################################################################################################



                joined_vital_diastolic = vital_diastolic.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_diastolic['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'DIASTOLIC'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_diastolic['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_diastolic = joined_vital_diastolic.select(              
                    
                                                    joined_vital_diastolic['omop_id'].alias("common_id"),
                                                    joined_vital_diastolic['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_diastolic['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    mapping_bp_position_diastolic_to_pb_code_dict[upper(col('BP_POSITION'))].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_vital_diastolic['DIASTOLIC'].alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('8876').alias("unit_concept_id"), # 
                                                    lit('mmHg').alias("unit_source_value"),
                                                    lit('4172703').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_diastolic['DIASTOLIC'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_diastolic['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('=').alias("operator_source_value"),
                                                    mapping_bp_position_to_pb_code_type_dict[upper(col('BP_POSITION'))].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_diastolic['SOURCE'].alias("source"),
                                                    lit('VITAL_DIASTOLIC').alias("mapped_from"),
                                        
                                                                    )


                ###################################################################################################################################
                # Mapping VITAL systolic to COMMON table
                ###################################################################################################################################



                joined_vital_systolic = vital_systolic.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_systolic['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'SYSTOLIC'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_systolic['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_systolic = joined_vital_systolic.select(              
                    
                                                    joined_vital_systolic['omop_id'].alias("common_id"),
                                                    joined_vital_systolic['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_systolic['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    mapping_bp_position_systolic_to_pb_code_dict[upper(col('BP_POSITION'))].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_vital_systolic['SYSTOLIC'].alias("value_as_number"),
                                                    lit('').alias("value_as_concept_id"),
                                                    lit('8876').alias("unit_concept_id"), # 
                                                    lit('mmHg').alias("unit_source_value"),
                                                    lit('4172703').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_systolic['SYSTOLIC'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    lit('').alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_systolic['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('=').alias("operator_source_value"),
                                                    mapping_bp_position_to_pb_code_type_dict[upper(col('BP_POSITION'))].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_systolic['SOURCE'].alias("source"),
                                                    lit('VITAL_SYSTOLIC').alias("mapped_from"),
                                        
                                                                    )


                ###################################################################################################################################
                # Mapping VITAL smoking to COMMON table
                ###################################################################################################################################



                joined_vital_smoking = vital_smoking.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_smoking['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'SMOKING'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_smoking['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_smoking = joined_vital_smoking.select(              
                    
                                                    joined_vital_smoking['omop_id'].alias("common_id"),
                                                    joined_vital_smoking['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_smoking['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('63638-1').alias("common_data_source_value"), # Smoking status [FTND]
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    lit('').alias("value_as_number"),
                                                    mapping_smoking_to_smoking_concept_id_dict[upper(col('SMOKING'))].alias("value_as_concept_id"),
                                                    lit('').alias("unit_concept_id"), # 
                                                    lit('').alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_smoking['smoking'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    mapping_smoking_to_smoking_text_dict[upper(col('SMOKING'))].alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_smoking['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    lit('LC').alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_smoking['SOURCE'].alias("source"),
                                                    lit('VITAL_SMOKING').alias("mapped_from"),
                                        
                                                                    )


                ###################################################################################################################################
                # Mapping VITAL tobacco to COMMON table
                ###################################################################################################################################



                joined_vital_tobacco = vital_tobacco.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_tobacco['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'TOBACCO'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_tobacco['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_tobacco = joined_vital_tobacco.select(              
                    
                                                    joined_vital_tobacco['omop_id'].alias("common_id"),
                                                    joined_vital_tobacco['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_tobacco['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('229819007').alias("common_data_source_value"), # tobacco status [FTND]
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    lit('').alias("value_as_number"),
                                                    mapping_tobacco_to_tobacco_concept_id_dict[upper(col('TOBACCO'))].alias("value_as_concept_id"),
                                                    lit('').alias("unit_concept_id"), # 
                                                    lit('').alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_tobacco['TOBACCO'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    mapping_tobacco_to_tobacco_text_dict[upper(col('TOBACCO'))].alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_tobacco['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    lit('SM').alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_tobacco['SOURCE'].alias("source"),
                                                    lit('VITAL_TOBACCO').alias("mapped_from"),
                                        
                                                                    )



                ###################################################################################################################################
                # Mapping VITAL tobacco_type to COMMON table
                ###################################################################################################################################



                joined_vital_tobacco_type = vital_tobacco_type.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==vital_tobacco_type['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(vital_id_to_omop_id_mapping, (col('pcornet_vitalid') == col('VITALID')) & (col('data_type') == 'TOBACCO_TYPE'), how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== vital_tobacco_type['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\

                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped  table
                ###################################################################################################################################


                common_from_vital_tobacco_type = joined_vital_tobacco_type.select(              
                    
                                                    joined_vital_tobacco_type['omop_id'].alias("common_id"),
                                                    joined_vital_tobacco_type['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'), col('MEASURE_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEASURE_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEASURE_DATE'),col('MEASURE_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('MEASURE_TIME')).alias("event_end_time"),
                                                    mapping_vital_source_to_type_concept_id_dict[upper(col('VITAL_SOURCE'))].alias("common_data_type_concept_id"),
                                                    lit('').alias("provider_id"),
                                                    joined_vital_tobacco_type['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    lit('229819007').alias("common_data_source_value"), # tobacco_type status [FTND]
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    lit('').alias("value_as_number"),
                                                    mapping_tobacco_type_to_tobacco_type_concept_id_dict[upper(col('TOBACCO_TYPE'))].alias("value_as_concept_id"),
                                                    lit('').alias("unit_concept_id"), # 
                                                    lit('').alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    joined_vital_tobacco_type['tobacco_type'].alias("value_source_value"),
                                                    lit('').alias("qualifier_source_value"),
                                                    lit('').alias("qualifier_concept_id"),
                                                    mapping_tobacco_type_to_tobacco_type_text_dict[upper(col('TOBACCO_TYPE'))].alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_vital_tobacco_type['VITAL_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    lit('SM').alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_vital_tobacco_type['SOURCE'].alias("source"),
                                                    lit('VITAL_TOBACCO_TYPE').alias("mapped_from"),
                                        
                                                                    )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

                common_from_vital_combined =       common_from_vital_wt\
                                            .union(common_from_vital_ht)\
                                            .union(common_from_vital_bmi)\
                                            .union(common_from_vital_diastolic)\
                                            .union(common_from_vital_systolic)\
                                            .union(common_from_vital_smoking)\
                                            .union(common_from_vital_tobacco)\
                                            .union(common_from_vital_tobacco_type)



                files_count = len(vital_files)
                current_count= counter
                file_name = f"common_from_vital.csv.{VITALID_WT_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = vital_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = common_from_vital_combined,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'vital_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')