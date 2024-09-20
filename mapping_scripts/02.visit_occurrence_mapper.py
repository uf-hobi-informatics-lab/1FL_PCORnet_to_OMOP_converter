###################################################################################################################################
# This script will map a PCORNet visit_occurrence table 
###################################################################################################################################


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
from dictionaries import *
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
import glob
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
spark = cf.get_spark_session("visit_occurrence_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]

# folders = ['FLM']
 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        encounter_input_path                              = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/ENCOUNTER/ENCOUNTER.csv*'
        encounter_files = sorted(glob.glob(encounter_input_path))
        # visit_occurrence_id_mapping_path                  = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'
        facilityid_care_site_id_mapping_path              = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_facilityid_care_site_id.csv'
        providerid_to_provider_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_providerid_provider_id.csv'

        # visit_occurrence_specialty_concept_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/mapping_visit_occurrence_specialty_concept_id.csv'
        # visit_occurrence_specialty_source_concept_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/mapping_visit_occurrence_specialty_source_concept_id.csv'
        mapped_data_folder_path                             = f'/app/data/{input_data_folder}/omop_tables/{folder}/visit_occurrence/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################



        # visit_occurrence_id_mapping                          = cf.spark_read(visit_occurrence_id_mapping_path,spark)
        patid_to_person_id_mapping                           = cf.spark_read(patid_to_person_id_mapping_path,spark)
        providerid_to_provider_id_mapping                    = cf.spark_read(providerid_to_provider_id_mapping_path,spark)
        facilityid_care_site_id_mapping                      = cf.spark_read(facilityid_care_site_id_mapping_path,spark)


        # visit_occurrence_specialty_concept_id_mapping        = cf.spark_read(visit_occurrence_specialty_concept_id_mapping_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
        # visit_occurrence_specialty_source_concept_id_mapping = cf.spark_read(visit_occurrence_specialty_source_concept_id_mapping_path,format="csv",sep="\t", inferSchema="true", header="true",  quote= '"')


        mapping_enc_type_to_visit_concept_id_dict = create_map([lit(x) for x in chain(*enc_type_to_visit_concept_id_dict.items())])
        mapping_admitting_source_to_admitting_source_concept_id_dict = create_map([lit(x) for x in chain(*admitting_source_to_admitting_source_concept_id_dict.items())])
        mapping_discharge_status_to_discharge_concept_id_dict = create_map([lit(x) for x in chain(*discharge_status_to_discharge_concept_id_dict.items())])



        counter = 0

        for encounter_file in encounter_files:
                counter   =  counter + 1
                encounter = cf.spark_read(encounter_file, spark)
                suffix    = encounter_file.rsplit(".", 1)[-1]



                visit_occurrence_id_mapping_path                  = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv.{suffix}'
                visit_occurrence_id_mapping                       = cf.spark_read(visit_occurrence_id_mapping_path,spark)


                joined_encounter = encounter.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==encounter['PATID'], how='inner').drop('pcornet_patid')\
                                                .join(visit_occurrence_id_mapping, visit_occurrence_id_mapping['pcornet_encounterid']== encounter['ENCOUNTERID'], how = 'inner').drop('pcornet_encounterid')\
                                                .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== encounter['PROVIDERID'], how = 'left').drop('pcornet_providerid')\
                                                .join(facilityid_care_site_id_mapping, facilityid_care_site_id_mapping['pcornet_facilityid']== encounter['FACILITYID'], how = 'left').drop('pcornet_facilityid')


                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                visit_occurrence = joined_encounter.select( 

                                            joined_encounter['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                            joined_encounter['omop_person_id'].alias("person_id"),
                                            mapping_enc_type_to_visit_concept_id_dict[upper(col('ENC_TYPE'))].alias("visit_concept_id"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('ADMIT_DATE')).alias("visit_start_date"),
                                            cf.get_datetime_from_date_and_time_with_default_value_udf(col('ADMIT_DATE') , lit('00:00:00') ).alias("visit_start_datetime"),
                                            cf.get_date_from_date_str_with_default_value_udf(col('DISCHARGE_DATE')).alias("visit_end_date"),
                                            cf.get_datetime_from_date_and_time_with_default_value_udf(col('DISCHARGE_DATE'), lit('00:00:00')).alias("visit_end_datetime"),
                                            lit('32035').alias("visit_type_concept_id"), # 	Visit derived from EHR encounter record
                                            joined_encounter['omop_provider_id'].alias("provider_id"),
                                            joined_encounter['omop_care_site_id'].alias("care_site_id"),
                                            joined_encounter['ENC_TYPE'].alias("visit_source_value"),
                                            mapping_enc_type_to_visit_concept_id_dict[upper(col('ENC_TYPE'))].alias("visit_source_concept_id"),
                                            mapping_admitting_source_to_admitting_source_concept_id_dict[upper(col('ADMITTING_SOURCE'))].alias("admitting_source_concept_id"),
                                            joined_encounter['ADMITTING_SOURCE'].alias("admitting_source_value"),
                                            mapping_discharge_status_to_discharge_concept_id_dict[upper(col('DISCHARGE_STATUS'))].alias("discharge_to_concept_id"),
                                            joined_encounter['DISCHARGE_STATUS'].alias("discharge_to_source_value"),
                                            lit('').alias("preceding_visit_occurrence_id"), 
                                            cf.get_current_time_udf().alias("updated"),
                                            joined_encounter['SOURCE'].alias("source"),
                                            lit('ENCOUNTER').alias("mapped_from"),
                                        

                                                                    )

                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################
            
                files_count = len(encounter_files)
                current_count= counter
                file_name = f"visit_occurrence.csv.{suffix}"


                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = encounter_file

                    )           
                cf.write_pyspark_output_file(
                                payspark_df = visit_occurrence,
                                output_file_name =file_name,
                                output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'visit_occurrence_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')