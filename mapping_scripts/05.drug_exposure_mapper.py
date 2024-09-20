###################################################################################################################################
# This script will map a PCORNet drug_exposure table 
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
spark = cf.get_spark_session("drug_exposure_mapper")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



###################################################################################################################################
# This function will return the day value from a date input
###################################################################################################################################

def prescribing_sig(RX_FREQUENCY,RAW_RX_MED_NAME):

    try:
        sig = 'No information'

        if RX_FREQUENCY == '01' : sig = RAW_RX_MED_NAME+' Every day'
        if RX_FREQUENCY == '02' : sig = RAW_RX_MED_NAME+' Two times a day (BID)'
        if RX_FREQUENCY == '03' : sig = RAW_RX_MED_NAME+' Three times a day (TID)'
        if RX_FREQUENCY == '04' : sig = RAW_RX_MED_NAME+' Four times a day (QID)'
        if RX_FREQUENCY == '05' : sig = RAW_RX_MED_NAME+' Every morning'
        if RX_FREQUENCY == '06' : sig = RAW_RX_MED_NAME+' Every afternoon'
        if RX_FREQUENCY == '07' : sig = RAW_RX_MED_NAME+' Before meals'
        if RX_FREQUENCY == '08' : sig = RAW_RX_MED_NAME+' After meals'
        if RX_FREQUENCY == '10' : sig = RAW_RX_MED_NAME+' Every evening'
        if RX_FREQUENCY == '11' : sig = RAW_RX_MED_NAME+' Once'
        if RX_FREQUENCY == 'NI' : sig = 'No information'
        if RX_FREQUENCY == 'UN' : sig = 'Unknown'
        if RX_FREQUENCY == 'OT' : sig = 'Other' 
    except:
          sig = None
    return sig

prescribing_sig_udf = udf(prescribing_sig, StringType())


 
try:

    for folder in folders :

        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        med_admin_input_path                 = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/MED_ADMIN/MED_ADMIN.csv*'
        med_admin_files = sorted(glob.glob(med_admin_input_path))

        prescribing_input_path               = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/PRESCRIBING/PRESCRIBING.csv*'
        prescribing_files = sorted(glob.glob(prescribing_input_path))

        dispensing_input_path                = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DISPENSING/DISPENSING.csv*'
        dispensing_files = sorted(glob.glob(dispensing_input_path))


        immunization_input_path              = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/IMMUNIZATION/IMMUNIZATION.csv*'
        immunization_files = sorted(glob.glob(immunization_input_path))

        files_count = len(med_admin_files) +len(prescribing_files)  +len(dispensing_files)  +len(immunization_files) 


        encounterid_to_visit_occurrence_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_encounterid_visit_occurrence_id/mapping_encounterid_visit_occurrence_id.csv*'
        providerid_to_provider_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_providerid_provider_id.csv'
        patid_to_person_id_mapping_path                   = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'

        drug_concept_id_mapping_path                      = f'/app/data/{input_data_folder}/mapping_tables/mapping_drug_concept_id.csv'
        drug_source_concept_id_mapping_path               = f'/app/data/{input_data_folder}/mapping_tables/mapping_drug_source_concept_id.csv'



        mapped_data_folder_path              = f'/app/data/{input_data_folder}/omop_tables/{folder}/drug_exposure/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################






        encounterid_to_visit_occurrence_id_mapping     = cf.spark_read(encounterid_to_visit_occurrence_id_mapping_path,spark)
        providerid_to_provider_id_mapping              = cf.spark_read(providerid_to_provider_id_mapping_path,spark)
        patid_to_person_id_mapping                     = cf.spark_read(patid_to_person_id_mapping_path,spark)

        drug_concept_id_mapping                        = cf.spark_read(drug_concept_id_mapping_path,spark)
        drug_source_concept_id_mapping                 = cf.spark_read(drug_source_concept_id_mapping_path,spark)

        mapping_route_concept_id_dict = create_map([lit(x) for x in chain(*route_concept_id_dict.items())])
        mapping_immunization_type_concept_id_dict = create_map([lit(x) for x in chain(*immunization_type_concept_id_dict.items())])

        ###################################################################################################################################
        # Mapping MED_ADMIN to drug_exposure
        ###################################################################################################################################



        # prescribing                                    = cf.spark_read(prescribing_input_path,spark)
        # dispensing                                     = cf.spark_read(dispensing_input_path,spark)
        # immunization                                   = cf.spark_read(immunization_input_path,spark)



        counter = 0

        for med_admin_file in med_admin_files:
                counter =  counter + 1
                med_admin                                     = cf.spark_read(med_admin_file, spark)
                suffix = med_admin_file.rsplit(".", 1)[-1]

                medadminid_to_omop_id_mapping_path                = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_medadminid_to_omop_id/mapping_medadminid_to_omop_id.csv.{suffix}'

                medadminid_to_omop_id_mapping                  = cf.spark_read(medadminid_to_omop_id_mapping_path,spark)


                values_to_replace = ["OT", "UN"]
                masked_med_admin = med_admin.withColumn("MEDADMIN_TYPE", when(col("MEDADMIN_TYPE").isin(values_to_replace), "NI").otherwise(col("MEDADMIN_TYPE")))

                # sys.exit('exiting ......')
                joined_med_admin = masked_med_admin.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_med_admin['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(medadminid_to_omop_id_mapping, medadminid_to_omop_id_mapping['pcornet_medadminid']==masked_med_admin['MEDADMINID'], how='inner')\
                                                    .join(drug_concept_id_mapping, (col('drug_code') == col('MEDADMIN_CODE')) & (col('pcornet_drug_type') == col('MEDADMIN_TYPE')), how='left').drop('drug_code').drop('pcornet_drug_type')\
                                                    .join(drug_source_concept_id_mapping, (col('drug_code') == col('MEDADMIN_CODE')) & (col('pcornet_drug_type') == col('MEDADMIN_TYPE')), how='inner').drop('drug_code')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_med_admin['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== masked_med_admin['MEDADMIN_PROVIDERID'], how = 'left').drop('pcornet_providerid')







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                drug_exposure_from_med_admin = joined_med_admin.select(              
                    
                                                    joined_med_admin['omop_id'].alias("drug_exposure_id"),
                                                    joined_med_admin['omop_person_id'].alias("person_id"),
                                                    joined_med_admin['drug_concept_id'].alias("drug_concept_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEDADMIN_START_DATE')).alias('drug_exposure_start_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEDADMIN_START_DATE'), col("MEDADMIN_START_TIME")).alias('drug_exposure_start_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEDADMIN_STOP_DATE')).alias('drug_exposure_end_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('MEDADMIN_STOP_DATE'), col("MEDADMIN_STOP_TIME")).alias('drug_exposure_end_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('MEDADMIN_STOP_DATE')).alias('verbatim_end_date'),
                                                    lit('32830').alias("drug_type_concept_id"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("refills"),
                                                    lit('').alias("quantity"),
                                                    joined_med_admin['MEDADMIN_DOSE_ADMIN'].alias("days_supply"),
                                                    lit('').alias("sig"),
                                                    mapping_route_concept_id_dict[upper(col('MEDADMIN_ROUTE'))].alias("route_concept_id"),
                                                    lit('').alias("lot_number"),
                                                    joined_med_admin['omop_provider_id'].alias("provider_id"),
                                                    joined_med_admin['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_med_admin['MEDADMIN_CODE'].alias("drug_source_value"),
                                                    joined_med_admin['drug_source_concept_id'].alias("drug_source_concept_id"),
                                                    joined_med_admin['MEDADMIN_ROUTE'].alias("route_source_value"),
                                                    joined_med_admin['MEDADMIN_DOSE_ADMIN_UNIT'].alias("dose_unit_source_value"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_med_admin['SOURCE'].alias("source"),
                                                    lit('MED_ADMIN').alias("mapped_from"),
                                        
                                                                    )


                current_count= counter
                file_name = f"drug_exposure.csv.{MEDADMINID_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = med_admin_file

                    )
            

                cf.write_pyspark_output_file(
                                payspark_df = drug_exposure_from_med_admin,
                                output_file_name = file_name,
                                output_data_folder_path= mapped_data_folder_path)


                ###################################################################################################################################
                # Mapping prescribing to drug_exposure
                ###################################################################################################################################




        for prescribing_file in prescribing_files:
                counter =  counter + 1
                prescribing                                     = cf.spark_read(prescribing_file, spark)
                suffix = prescribing_file.rsplit(".", 1)[-1]

                prescribingid_to_omop_id_mapping_path             = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_prescribingid_to_omop_id/mapping_prescribingid_to_omop_id.csv.{suffix}'

                prescribingid_to_omop_id_mapping               = cf.spark_read(prescribingid_to_omop_id_mapping_path,spark)


                joined_prescribing =     prescribing.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==prescribing['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(prescribingid_to_omop_id_mapping, prescribingid_to_omop_id_mapping['pcornet_prescribingid']==prescribing['PRESCRIBINGID'], how='inner')\
                                                    .join(drug_concept_id_mapping, (col('drug_code') == col('RXNORM_CUI')) & (col('pcornet_drug_type') == 'RX'), how='left').drop('drug_code').drop('pcornet_drug_type')\
                                                    .join(drug_source_concept_id_mapping, (col('drug_code') == col('RXNORM_CUI')) & (col('pcornet_drug_type') == 'RX'), how='inner').drop('drug_code')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== prescribing['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== prescribing['RX_PROVIDERID'], how = 'left').drop('pcornet_providerid')







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                drug_exposure_from_prescribing = joined_prescribing.select(              
                    
                                                    joined_prescribing['omop_id'].alias("drug_exposure_id"),
                                                    joined_prescribing['omop_person_id'].alias("person_id"),
                                                    joined_prescribing['drug_concept_id'].alias("drug_concept_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RX_START_DATE')).alias('drug_exposure_start_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('RX_START_DATE'), lit('00:00:00')).alias('drug_exposure_start_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RX_END_DATE')).alias('drug_exposure_end_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('RX_END_DATE'), lit('00:00:00')).alias('drug_exposure_end_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('RX_END_DATE')).alias('verbatim_end_date'),
                                                    lit('32833').alias("drug_type_concept_id"),
                                                    lit('').alias("stop_reason"),
                                                    joined_prescribing['RX_REFILLS'].alias("refills"),
                                                    joined_prescribing['RX_QUANTITY'].alias("quantity"),
                                                    joined_prescribing['RX_DAYS_SUPPLY'].alias("days_supply"),
                                                    prescribing_sig_udf(joined_prescribing['RX_FREQUENCY'], joined_prescribing['RAW_RX_MED_NAME']).alias("sig"),
                                                    mapping_route_concept_id_dict[upper(col('RX_ROUTE'))].alias("route_concept_id"),
                                                    lit('').alias("lot_number"),
                                                    joined_prescribing['omop_provider_id'].alias("provider_id"),
                                                    joined_prescribing['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_prescribing['RXNORM_CUI'].alias("drug_source_value"),
                                                    joined_prescribing['drug_source_concept_id'].alias("drug_source_concept_id"),
                                                    joined_prescribing['RX_ROUTE'].alias("route_source_value"),
                                                    joined_prescribing['RX_DOSE_ORDERED_UNIT'].alias("dose_unit_source_value"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_prescribing['SOURCE'].alias("source"),
                                                    lit('PRESCRIBING').alias("mapped_from"),
                                        
                                                                    )

                current_count= counter
                file_name = f"drug_exposure.csv.{PRESCRIBING_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = prescribing_file

                    )
            

                cf.write_pyspark_output_file(
                                payspark_df = drug_exposure_from_prescribing,
                                output_file_name = file_name,
                                output_data_folder_path= mapped_data_folder_path)

                ###################################################################################################################################
                # Mapping dispensing to drug_exposure
                ###################################################################################################################################


        for dispensing_file in dispensing_files:
                counter =  counter + 1
                dispensing                                     = cf.spark_read(dispensing_file, spark)
                suffix = dispensing_file.rsplit(".", 1)[-1]

                dispensingid_to_omop_id_mapping_path              = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_dispensingid_to_omop_id/mapping_dispensingid_to_omop_id.csv.{suffix}'
                dispensingid_to_omop_id_mapping                = cf.spark_read(dispensingid_to_omop_id_mapping_path,spark)


                joined_dispensing =     dispensing.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==dispensing['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(dispensingid_to_omop_id_mapping, dispensingid_to_omop_id_mapping['pcornet_dispensingid']==dispensing['DISPENSINGID'], how='inner')\
                                                    .join(drug_concept_id_mapping, (col('drug_code') == col('NDC')) & (col('pcornet_drug_type') == 'ND'), how='left').drop('drug_code').drop('pcornet_drug_type')\
                                                    .join(drug_source_concept_id_mapping, (col('drug_code') == col('NDC')) & (col('pcornet_drug_type') == 'ND'), how='inner').drop('drug_code')
                                                    







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                drug_exposure_from_dispensing = joined_dispensing.select(              
                    
                                                    joined_dispensing['omop_id'].alias("drug_exposure_id"),
                                                    joined_dispensing['omop_person_id'].alias("person_id"),
                                                    joined_dispensing['drug_concept_id'].alias("drug_concept_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('DISPENSE_DATE')).alias('drug_exposure_start_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('DISPENSE_DATE'), lit('00:00:00')).alias('drug_exposure_start_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('DISPENSE_DATE')).alias('drug_exposure_end_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('DISPENSE_DATE'), lit('00:00:00')).alias('drug_exposure_end_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('DISPENSE_DATE')).alias('verbatim_end_date'),
                                                    lit('32825').alias("drug_type_concept_id"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("refills"),
                                                    joined_dispensing['DISPENSE_AMT'].alias("quantity"),
                                                    joined_dispensing['DISPENSE_SUP'].alias("days_supply"),
                                                    lit('').alias("sig"),
                                                    mapping_route_concept_id_dict[upper(col('DISPENSE_ROUTE'))].alias("route_concept_id"),
                                                    lit('').alias("lot_number"),
                                                    lit('').alias("provider_id"),
                                                    lit('').alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_dispensing['NDC'].alias("drug_source_value"),
                                                    joined_dispensing['drug_source_concept_id'].alias("drug_source_concept_id"),
                                                    joined_dispensing['DISPENSE_ROUTE'].alias("route_source_value"),
                                                    joined_dispensing['RAW_DISPENSE_DOSE_DISP_UNIT'].alias("dose_unit_source_value"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_dispensing['SOURCE'].alias("source"),
                                                    lit('DISPENSING').alias("mapped_from"),
                                        
                                                                    )

                current_count= counter
                file_name = f"drug_exposure.csv.{DISPENSING_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = dispensing_file

                    )
            

                cf.write_pyspark_output_file(
                                payspark_df = drug_exposure_from_dispensing,
                                output_file_name = file_name,
                                output_data_folder_path= mapped_data_folder_path)
                ###################################################################################################################################
                # Mapping immunization to drug_exposure
                ###################################################################################################################################


        for immunization_file in immunization_files:
                counter =  counter + 1
                immunization                                     = cf.spark_read(immunization_file, spark)
                suffix = immunization_file.rsplit(".", 1)[-1]

                immunization_to_omop_id_mapping_path              = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_immunizationid_to_omop_id/mapping_immunizationid_to_omop_id.csv.{suffix}'
                immunizationid_to_omop_id_mapping                = cf.spark_read(immunization_to_omop_id_mapping_path,spark)


                values_to_replace = ["OT", "UN"]
                masked_immunization = immunization.withColumn("VX_CODE_TYPE", when(col("VX_CODE_TYPE").isin(values_to_replace), "NI").otherwise(col("VX_CODE_TYPE")))

                joined_immunization =     masked_immunization.join(patid_to_person_id_mapping, patid_to_person_id_mapping['pcornet_patid']==masked_immunization['PATID'], how='inner').drop('pcornet_patid')\
                                                    .join(immunizationid_to_omop_id_mapping, immunizationid_to_omop_id_mapping['pcornet_immunizationid']==masked_immunization['IMMUNIZATIONID'], how='inner')\
                                                    .join(drug_concept_id_mapping, (col('drug_code') == col('VX_CODE')) & (col('pcornet_drug_type') == col('VX_CODE_TYPE')), how='left').drop('drug_code').drop('pcornet_drug_type')\
                                                    .join(drug_source_concept_id_mapping, (col('drug_code') == col('VX_CODE')) & (col('pcornet_drug_type') == col('VX_CODE_TYPE')), how='inner').drop('drug_code')\
                                                    .join(encounterid_to_visit_occurrence_id_mapping, encounterid_to_visit_occurrence_id_mapping['pcornet_encounterid']== masked_immunization['ENCOUNTERID'], how = 'left').drop('pcornet_encounterid')\
                                                    .join(providerid_to_provider_id_mapping, providerid_to_provider_id_mapping['pcornet_providerid']== masked_immunization['VX_PROVIDERID'], how = 'left').drop('pcornet_providerid')
                                                    







                ###################################################################################################################################
                # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
                ###################################################################################################################################


                drug_exposure_from_immunization = joined_immunization.select(              
                    
                                                    joined_immunization['omop_id'].alias("drug_exposure_id"),
                                                    joined_immunization['omop_person_id'].alias("person_id"),
                                                    joined_immunization['drug_concept_id'].alias("drug_concept_id"),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('VX_ADMIN_DATE')).alias('drug_exposure_start_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('VX_ADMIN_DATE'), lit('00:00:00')).alias('drug_exposure_start_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('VX_ADMIN_DATE')).alias('drug_exposure_end_date'),
                                                    cf.get_datetime_from_date_and_time_with_default_value_udf(col('VX_ADMIN_DATE'), lit('00:00:00')).alias('drug_exposure_end_datetime'),
                                                    cf.get_date_from_date_str_with_default_value_udf(col('VX_ADMIN_DATE')).alias('verbatim_end_date'),
                                                    mapping_immunization_type_concept_id_dict[upper(col('VX_SOURCE'))].alias("drug_type_concept_id"),
                                                    lit('').alias("stop_reason"),
                                                    lit('').alias("refills"),
                                                    joined_immunization['VX_DOSE'].alias("quantity"),
                                                    lit('').alias("days_supply"),
                                                    lit('').alias("sig"),
                                                    mapping_route_concept_id_dict[upper(col('VX_ROUTE'))].alias("route_concept_id"),
                                                    joined_immunization['VX_LOT_NUM'].alias("lot_number"),
                                                    joined_immunization['omop_provider_id'].alias("provider_id"),
                                                    joined_immunization['omop_visit_occurrence_id'].alias("visit_occurrence_id"),
                                                    lit('').alias("visit_detail_id"),
                                                    joined_immunization['VX_CODE'].alias("drug_source_value"),
                                                    joined_immunization['drug_source_concept_id'].alias("drug_source_concept_id"),
                                                    joined_immunization['VX_ROUTE'].alias("route_source_value"),
                                                    joined_immunization['VX_DOSE_UNIT'].alias("dose_unit_source_value"),
                                                    cf.get_current_time_udf().alias("updated"),
                                                    joined_immunization['SOURCE'].alias("source"),
                                                    lit('IMMUNIZATION').alias("mapped_from"),
                                        
                                                                    )

                current_count= counter
                file_name = f"drug_exposure.csv.{IMMUNIZATION_OFFSET}{suffix}"


                cf.print_mapping_file_status(

                    total_count = files_count,
                    current_count = current_count,
                    output_file_name =  file_name,
                    source_file = immunization_file

                    )
            

                cf.write_pyspark_output_file(
                                payspark_df = drug_exposure_from_immunization,
                                output_file_name = file_name,
                                output_data_folder_path= mapped_data_folder_path)

                ###################################################################################################################################
                # Create the output file
                ###################################################################################################################################

                # drug_exposure = drug_exposure_from_med_admin\
                #                 .union(drug_exposure_from_prescribing)\
                #                 .union(drug_exposure_from_dispensing)\
                #                 .union(drug_exposure_from_immunization)

                # cf.write_pyspark_output_file(
                #                 payspark_df = drug_exposure,
                #                 output_file_name = "drug_exposure.csv",
                #                 output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'drug_exposure_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')