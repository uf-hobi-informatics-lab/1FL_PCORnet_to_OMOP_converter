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
spark = cf.get_spark_session("death_cause_to_combined_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]




try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        death_cause_input_path                            = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH_CAUSE/DEATH_CAUSE.csv*'
        death_cause_files = sorted(glob.glob(death_cause_input_path))


        death_input_path                                  = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEATH/DEATH.csv*'

        death_cause_to_omop_id_mapping_path               = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_death_cause_to_omop_id.csv'


        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'


        mapped_data_folder_path                           = f'/app/data/{input_data_folder}/omop_tables/{folder}/common/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        death_cause                                    = cf.spark_read(death_cause_input_path,spark)
        death                                          = cf.spark_read(death_input_path,spark)
        death_cause_to_omop_id_mapping                 = cf.spark_read(death_cause_to_omop_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_death_cause_type_concept_id_dict = create_map([lit(x) for x in chain(*death_cause_type_concept_id_dict.items())])


        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        values_to_replace = ["OT", "UN"]
        masked_death_cause = death_cause.withColumn("DEATH_CAUSE_CODE", when(col("DEATH_CAUSE_CODE").isin(values_to_replace), "NI").otherwise(col("DEATH_CAUSE_CODE")))

        death_date_data = death.select(
                                death['PATID'].alias('DEATH_DATE_PATID'),
                                death['DEATH_DATE']
        )

        # sys.exit('exiting ......')
        joined_death_cause = masked_death_cause.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_death_cause['PATID'], how='inner').drop('pcornet_patid')\
                                            .join(death_cause_to_omop_id_mapping, death_cause_to_omop_id_mapping['pcornet_id']==concat( col("PATID"),col("DEATH_CAUSE"),col("DEATH_CAUSE_CODE"),col("DEATH_CAUSE_TYPE"),col("DEATH_CAUSE_SOURCE")), how='inner')\
                                            .join(death_date_data, death_date_data['DEATH_DATE_PATID']==masked_death_cause['PATID'], how='left')\






        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


        common_from_death_cause = joined_death_cause.select(              
            
                                            joined_death_cause['omop_id'].alias("common_id"),
                                            joined_death_cause['omop_person_id'].alias("person_id"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('DEATH_DATE')).alias("event_start_date"),
                                            cf.get_datetime_from_date_and_time_with_default_value_udf(col('DEATH_DATE') , lit('00:00:00') ).alias("event_start_datetime"),
                                            lit('').alias("event_start_time"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('DEATH_DATE')).alias("event_end_date"),
                                            cf.get_datetime_from_date_and_time_with_default_value_udf(col('DEATH_DATE'), lit('00:00:00')).alias("event_end_datetime"),
                                            lit('').alias("event_end_time"),
                                            mapping_death_cause_type_concept_id_dict[upper(col('death_cause_SOURCE'))].alias("common_data_type_concept_id"),
                                            lit('').alias("provider_id"),
                                            lit('').alias("visit_occurrence_id"),
                                            lit('').alias("visit_detail_id"),
                                            joined_death_cause['DEATH_CAUSE'].alias("common_data_source_value"),
                                            lit('').alias("stop_reason"),
                                            lit('Final diagnosis (discharge)').alias("common_data_status_source_value"),
                                            lit('4230359').alias("common_data_status_concept_id"),
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
                                            joined_death_cause['DEATH_CAUSE_TYPE'].alias("common_data_type_source_value"),
                                            lit('').alias("operator_source_value"),
                                            joined_death_cause['DEATH_CAUSE_CODE'].alias("pcornet_code_type"),
                                            cf.get_current_time_udf().alias("updated"),
                                            joined_death_cause['SOURCE'].alias("source"),
                                            lit('DEATH_CAUSE').alias("mapped_from"),
                                
                                                            )




        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################



        cf.write_pyspark_output_file(
                        payspark_df = common_from_death_cause,
                        output_file_name = "common_from_death_cause.csv",
                        output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'death_cause_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')