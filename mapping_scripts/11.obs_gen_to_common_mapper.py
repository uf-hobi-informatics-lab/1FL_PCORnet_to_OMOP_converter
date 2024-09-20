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
spark = cf.get_spark_session("obs_gen_to_combined_mapper")




path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        obs_gen_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/OBS_GEN/OBS_GEN.csv*'
        obs_gen_files = sorted(glob.glob(obs_gen_input_path))



        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        providerid_to_provider_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_providerid_provider_id.csv'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        providerid_to_provider_id_mapping              = cf.spark_read(providerid_to_provider_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_obs_gen_type_concept_id_dict = create_map([lit(x) for x in chain(*obs_gen_type_concept_id_dict.items())])
        mapping_obs_gen_result_qual_value_as_concept_id_dict = create_map([lit(x) for x in chain(*obs_gen_result_qual_value_as_concept_id_dict.items())])
        mapping_obs_gen_result_modifier_qualifier_concept_id_dict = create_map([lit(x) for x in chain(*obs_gen_result_modifier_qualifier_concept_id_dict.items())])
        mapping_result_unit_to_unit_concept_id_dict = create_map([lit(x) for x in chain(*result_unit_to_unit_concept_id_dict.items())])

        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################


        counter = 0

        for obs_gen_file in obs_gen_files:
                counter =  counter + 1
                obs_gen                                     = cf.spark_read(obs_gen_file, spark)
                suffix = obs_gen_file.rsplit(".", 1)[-1]

                obsgenid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_obsgenid_to_omop_id/mapping_obsgenid_to_omop_id.csv.{suffix}'
                obsgenid_to_omop_id_mapping                    = cf.spark_read(obsgenid_to_omop_id_mapping_path,spark)

                values_to_replace = ["OT", "UN"]
                masked_obs_gen = obs_gen.withColumn("OBSGEN_TYPE", when(col("OBSGEN_TYPE").isin(values_to_replace), "NI").otherwise(col("OBSGEN_TYPE")))

                # sys.exit('exiting ......')
                joined_obs_gen = masked_obs_gen.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_obs_gen['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(obsgenid_to_omop_id_mapping, obsgenid_to_omop_id_mapping['pcornet_obsgenid']==masked_obs_gen['OBSGENID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_obs_gen['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== masked_obs_gen['OBSGEN_PROVIDERID'], how = 'left').drop('pcornet_providerid')






                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_obs_gen = joined_obs_gen.select(              
                    
                                                    joined_obs_gen['omop_id'].alias("common_id"),
                                                    joined_obs_gen['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('OBSGEN_START_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('OBSGEN_START_DATE'), col('OBSGEN_START_TIME')).alias("event_start_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('OBSGEN_START_TIME')).alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('OBSGEN_STOP_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('OBSGEN_STOP_DATE'), col('OBSGEN_STOP_TIME')).alias("event_end_datetime"),
                                                    cf.get_time_from_time_with_default_value_udf(col('OBSGEN_STOP_TIME')).alias("event_end_time"),
                                                    mapping_obs_gen_type_concept_id_dict[upper(col('OBSGEN_SOURCE'))].alias("common_data_type_concept_id"),
                                                    joined_obs_gen['omop_provider_id'].alias("provider_id"),
                                                    joined_obs_gen['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_obs_gen['OBSGEN_CODE'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
                                                    joined_obs_gen['OBSGEN_RESULT_NUM'].alias("value_as_number"),
                                                    mapping_obs_gen_result_qual_value_as_concept_id_dict[upper(col('OBSGEN_RESULT_QUAL'))].alias("value_as_concept_id"),
                                                    mapping_result_unit_to_unit_concept_id_dict[upper(col('OBSGEN_RESULT_UNIT'))].alias("unit_concept_id"),
                                                    joined_obs_gen['OBSGEN_RESULT_UNIT'].alias("unit_source_value"),
                                                    lit('').alias("operator_concept_id"),
                                                    lit('').alias("range_low"),
                                                    lit('').alias("range_high"),
                                                    lit('').alias("value_source_value"),
                                                    joined_obs_gen['OBSGEN_RESULT_MODIFIER'].alias("qualifier_source_value"),
                                                    mapping_obs_gen_result_modifier_qualifier_concept_id_dict[upper(col('OBSGEN_RESULT_MODIFIER'))].alias("qualifier_concept_id"),
                                                    joined_obs_gen['OBSGEN_RESULT_TEXT'].alias("value_as_string"),
                                                    lit('').alias("quantity"),
                                                    joined_obs_gen['OBSGEN_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    joined_obs_gen['OBSGEN_TYPE'].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_obs_gen['SOURCE'].alias("source"),
                                                    lit('OBS_GEN').alias("mapped_from"),
                                        
                                                                    )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



                files_count = len(obs_gen_files)
                current_count= counter
                file_name = f"common_from_obs_gen.csv.{OBSGENID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = obs_gen_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = common_from_obs_gen,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'obs_gen_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')