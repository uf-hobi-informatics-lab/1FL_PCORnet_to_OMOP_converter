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
spark = cf.get_spark_session("diagnosis_to_combined_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]




try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        diagnosis_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DIAGNOSIS/DIAGNOSIS.csv*'
        diagnosis_files = sorted(glob.glob(diagnosis_input_path))



        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        providerid_to_provider_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_providerid_provider_id.csv'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/common/{folder}/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        providerid_to_provider_id_mapping              = cf.spark_read(providerid_to_provider_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_diagnosis_type_concept_id_dict = create_map([lit(x) for x in chain(*diagnosis_type_concept_id_dict.items())])
        mapping_diagnosis_status_concept_id_dict = create_map([lit(x) for x in chain(*diagnosis_status_concept_id_dict.items())])


        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################


        counter = 0

        for diagnosis_file in diagnosis_files:
                counter =  counter + 1
                diagnosis                                     = cf.spark_read(diagnosis_file, spark)
                suffix = diagnosis_file.rsplit(".", 1)[-1]
        
                diagnosisid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_diagnosisid_to_omop_id/mapping_diagnosisid_to_omop_id.csv.{suffix}'
                diagnosisid_to_omop_id_mapping                 = cf.spark_read(diagnosisid_to_omop_id_mapping_path,spark)


                values_to_replace = ["OT", "UN"]
                masked_diagnosis = diagnosis.withColumn("DX_TYPE", when(col("DX_TYPE").isin(values_to_replace), "NI").otherwise(col("DX_TYPE")))

                # sys.exit('exiting ......')
                joined_diagnosis = masked_diagnosis.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_diagnosis['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(diagnosisid_to_omop_id_mapping, diagnosisid_to_omop_id_mapping['pcornet_diagnosisid']==masked_diagnosis['DIAGNOSISID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_diagnosis['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== masked_diagnosis['PROVIDERID'], how = 'left').drop('pcornet_providerid')






                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_diagnosis = joined_diagnosis.select(              
                    
                                                    joined_diagnosis['omop_id'].alias("common_id"),
                                                    joined_diagnosis['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('ADMIT_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_str_with_default_value_udf(col('ADMIT_DATE') ).alias("event_start_datetime"),
                                                    lit('').alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('DX_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_str_with_default_value_udf(col('DX_DATE')).alias("event_end_datetime"),
                                                    lit('').alias("event_end_time"),
                                                    mapping_diagnosis_type_concept_id_dict[upper(col('DX_ORIGIN'))].alias("common_data_type_concept_id"),
                                                    joined_diagnosis['omop_provider_id'].alias("provider_id"),
                                                    joined_diagnosis['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_diagnosis['DX'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    joined_diagnosis['DX_SOURCE'].alias("common_data_status_source_value"),
                                                    mapping_diagnosis_status_concept_id_dict[upper(col('DX_SOURCE'))].alias("common_data_status_concept_id"),
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
                                                    joined_diagnosis['DX_ORIGIN'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    joined_diagnosis['DX_TYPE'].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_diagnosis['SOURCE'].alias("source"),
                                                    lit('DIAGNOSIS').alias("mapped_from"),
                                        
                                                                    )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


                files_count = len(diagnosis_files)
                current_count= counter
                file_name = f"common_from_diagnosis.csv.{DIAGNOSISID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = diagnosis_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = common_from_diagnosis,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'diagnosis_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')