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
spark = cf.get_spark_session("specimen_mapper")




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


        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/specimen/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################





        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)



        mapping_specimen_to_specimen_concept_id_dict = create_map([lit(x) for x in chain(*specimen_to_specimen_concept_id_dict.items())])



        ###################################################################################################################################
        # Mapping MED_ADMIN to measurement
        ###################################################################################################################################

        counter = 0

        for lab_result_cm_file in lab_result_cm_files:
                counter =  counter + 1
                lab_result_cm                                     = cf.spark_read(lab_result_cm_file, spark)
                suffix = lab_result_cm_file.rsplit(".", 1)[-1]

                lab_result_cm_specimen_to_omop_id_mapping_path    = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_lab_result_cm_specimen_to_omop_id/mapping_lab_result_cm_specimen_to_omop_id.csv.{suffix}'
                lab_result_cm_specimen_to_omop_id_mapping            = cf.spark_read(lab_result_cm_specimen_to_omop_id_mapping_path,spark)

    
                joined_lab_result = lab_result_cm.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==lab_result_cm['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(lab_result_cm_specimen_to_omop_id_mapping, lab_result_cm_specimen_to_omop_id_mapping['pcornet_lab_result_cm_id']==lab_result_cm['LAB_RESULT_CM_ID'], how='inner')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== lab_result_cm['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\






                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                specimen_from_lab_result_cm = joined_lab_result.select(              
                    
                                                    joined_lab_result['omop_id'].alias("specimen_id"),
                                                    joined_lab_result['omop_person_id'].alias("person_id"),
                                                    mapping_specimen_to_specimen_concept_id_dict[upper(col('SPECIMEN_SOURCE'))].alias("specimen_concept_id"),
                                                    lit(0).alias("specimen_type_concept_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('SPECIMEN_DATE')).alias("specimen_date"),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('SPECIMEN_DATE'), lit('00:00:00')).alias("specimen_datetime"),
                                                    lit('').alias("quantity"),
                                                    lit(0).alias("unit_concept_id"),
                                                    lit(0).alias("anatomic_site_concept_id"),
                                                    lit(0).alias("disease_status_concept_id"),
                                                    joined_lab_result['LAB_RESULT_CM_ID'].alias("specimen_source_id"),
                                                    joined_lab_result['SPECIMEN_SOURCE'].alias("specimen_source_value"),
                                                    lit('').alias("unit_source_value"),
                                                    lit('').alias("anatomic_site_source_value"),
                                                    lit('').alias("disease_status_source_value"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_lab_result['SOURCE'].alias("source"),
                                                    lit('LAB_RESULT_CM').alias("mapped_from"),
                                        
                                                                    )


        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


                files_count = len(lab_result_cm_files)
                current_count= counter
                file_name = f"specimen.csv.{suffix}"


                cf.print_mapping_file_status(

                            total_count = files_count,
                            current_count = current_count,
                            output_file_name =  file_name,
                            source_file = lab_result_cm_file

                )
                cf.write_pyspark_output_file(
                                payspark_df = specimen_from_lab_result_cm,
                                output_file_name = file_name ,
                                output_data_folder_path= mapped_data_folder_path)

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'specimen_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')