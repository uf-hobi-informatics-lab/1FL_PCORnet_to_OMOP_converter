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
spark = cf.get_spark_session("procedures_to_combined_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



def map_procedure_type_concept_id(PX_SOURCE,PPX):

    type_concept_id = 0 

    if PX_SOURCE == 'OD' :  type_concept_id = 32817 
    if PX_SOURCE == 'BI' and PPX == 'P' :  type_concept_id = 44786630 
    if PX_SOURCE == 'BI' and PPX == 'S' :  type_concept_id = 44786631 
    if PX_SOURCE == 'BI' and PPX == 'NI':  type_concept_id = 32821 
    if PX_SOURCE == 'BI' and PPX == 'UN':  type_concept_id = 32821 
    if PX_SOURCE == 'BI' and PPX == 'OT':  type_concept_id = 32821 
    if PX_SOURCE == 'BI' and PPX == None:  type_concept_id = 32821
    if PX_SOURCE == 'CL' :  type_concept_id = 32810 
    if PX_SOURCE == 'DR' :  type_concept_id = 45754907
    if PX_SOURCE == 'NI' :  type_concept_id = 0 
    if PX_SOURCE == 'UN' :  type_concept_id = 0 
    if PX_SOURCE == 'OT' :  type_concept_id = 0 
    if PX_SOURCE == ''   :  type_concept_id = 0 
    if PX_SOURCE == None :  type_concept_id = 0  

    return type_concept_id

map_procedure_type_concept_id_udf = udf(map_procedure_type_concept_id, StringType())


 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        procedures_input_path                          = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PROCEDURES/PROCEDURES.csv*'
        procedures_files = sorted(glob.glob(procedures_input_path))


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



        procedures                                  = cf.spark_read(procedures_input_path,spark)

        counter = 0

        for procedures_file in procedures_files:
                counter =  counter + 1
                procedures                                     = cf.spark_read(procedures_file, spark)
                suffix = procedures_file.rsplit(".", 1)[-1]

                proceduresid_to_omop_id_mapping_path           = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_proceduresid_to_omop_id/mapping_proceduresid_to_omop_id.csv.{suffix}'
                proceduresid_to_omop_id_mapping            = cf.spark_read(proceduresid_to_omop_id_mapping_path,spark)


                ###################################################################################################################################
                # Mapping MED_ADMIN to measurement
                ###################################################################################################################################

                values_to_replace = ["OT", "UN"]
                masked_procedures = procedures.withColumn("PX_TYPE", when(col("PX_TYPE").isin(values_to_replace), "NI").otherwise(col("PX_TYPE")))

                # sys.exit('exiting ......')
                joined_procedures = masked_procedures.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_procedures['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(proceduresid_to_omop_id_mapping, proceduresid_to_omop_id_mapping['pcornet_proceduresid']==masked_procedures['PROCEDURESID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_procedures['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== masked_procedures['PROVIDERID'], how = 'left').drop('pcornet_providerid')







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                common_from_procedures = joined_procedures.select(              
                    
                                                    joined_procedures['omop_id'].alias("common_id"),
                                                    joined_procedures['omop_person_id'].alias("person_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('PX_DATE')).alias("event_start_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('PX_DATE'), lit('00:00:00') ).alias("event_start_datetime"),
                                                    lit('').alias("event_start_time"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('PX_DATE')).alias("event_end_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('PX_DATE'), lit('00:00:00')).alias("event_end_datetime"),
                                                    lit('').alias("event_end_time"),
                                                    map_procedure_type_concept_id_udf(joined_procedures['PX_SOURCE'],joined_procedures['PPX']).alias("common_data_type_concept_id"),
                                                    joined_procedures['omop_provider_id'].alias("provider_id"),
                                                    joined_procedures['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_procedures['PX'].alias("common_data_source_value"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("common_data_status_source_value"),
                                                    lit('').alias("common_data_status_concept_id"),
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
                                                    joined_procedures['PX_SOURCE'].alias("common_data_type_source_value"),
                                                    lit('').alias("operator_source_value"),
                                                    joined_procedures['PX_TYPE'].alias("pcornet_code_type"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_procedures['SOURCE'].alias("source"),
                                                    lit('PROCEDURES').alias("mapped_from"),
                                        
                                                                    )




                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################



                files_count = len(procedures_files)
                current_count= counter
                file_name = f"common_from_procedures.csv.{PROCEDURESID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = procedures_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = common_from_procedures,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)




    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'procedures_to_common_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')