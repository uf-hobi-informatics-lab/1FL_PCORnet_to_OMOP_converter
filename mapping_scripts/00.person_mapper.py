###################################################################################################################################
# This script will map a PCORNet demographic table 
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
spark = cf.get_spark_session("person_mapper")


path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]



###################################################################################################################################
# This function will return the year value from a date input
###################################################################################################################################

def get_year_from_date(date_str):

    try:

        date_format = "%Y-%m-%d"  # Specify the format of your date string

        date = datetime.strptime(date_str, date_format)

        return date.year
    except:

        return "1800"

get_year_from_date_udf = udf(get_year_from_date, StringType())


###################################################################################################################################
# This function will return the month value from a date input
###################################################################################################################################

def get_month_from_date(date_str):

    try:
        date_format = "%Y-%m-%d"  # Specify the format of your date string

        date = datetime.strptime(date_str, date_format)

        return date.month
    except:
        return '01'

get_month_from_date_udf = udf(get_month_from_date, StringType())


###################################################################################################################################
# This function will return the day value from a date input
###################################################################################################################################

def get_day_from_date(date_str):

    try:
        date_format = "%Y-%m-%d"  # Specify the format of your date string

        date = datetime.strptime(date_str, date_format)


        return date.day
    except:
        return '01'

get_day_from_date_udf = udf(get_day_from_date, StringType())

###################################################################################################################################
# This function will return the datetime value from a date input
###################################################################################################################################

def get_datetime_from_date(date_str):

    try:

        date_format = "%Y-%m-%d"  # Specify the format of your date string

        date = datetime.strptime(date_str, date_format)

        formatted_date_time = date.strftime("%Y-%m-%d %H:%M:%S")

        return formatted_date_time

    except:

        return '1800-01-01 00:00:00'

get_datetime_from_date_udf = udf(get_datetime_from_date, StringType())



 
try:


    for folder in folders :



        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################


                
        demographic_input_path            = f'/app/data/{input_data_folder}/pcornet_tables/{folder}/DEMOGRAPHIC/DEMOGRAPHIC.csv*'

        demographic_files = sorted(glob.glob(demographic_input_path))


        person_id_mapping_path            = f'/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_person_id.csv'
        # patid_location_id_mapping_path    = f'/app/data/{input_data_folder}/mapping_tables/mapping_patid_location_id.csv'
        # patid_care_site_id_mapping_path   = f'/app/data/{input_data_folder}/mapping_tables/mapping_patid_care_site_id.csv'
        # provider_id_mapping_path          = f'/app/data/{input_data_folder}/mapping_tables/provider_id_mapping.csv'
        mapped_data_folder_path           = f'/app/data/{input_data_folder}/omop_tables/{folder}/person/'



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################

        person_id_mapping    = cf.spark_read(person_id_mapping_path, spark)
        # patid_location_id_mapping  = cf.spark_read(patid_location_id_mapping_path, spark)
        # patid_care_site_id_mapping = cf.spark_read(patid_care_site_id_mapping_path, spark)

        mapping_gender_concept_id_dict = create_map([lit(x) for x in chain(*gender_concept_id_dict.items())])
        mapping_gender_source_concept_id_dict = create_map([lit(x) for x in chain(*gender_source_concept_id_dict.items())])
        mapping_race_concept_id_dict = create_map([lit(x) for x in chain(*race_concept_id_dict.items())])
        mapping_race_source_concept_id_dict = create_map([lit(x) for x in chain(*race_source_concept_id_dict.items())])
        mapping_ethnicity_concept_id_dict = create_map([lit(x) for x in chain(*ethnicity_concept_id_dict.items())])
        mapping_ethnicity_source_concept_id_dict= create_map([lit(x) for x in chain(*ethnicity_source_concept_id_dict.items())])


        counter = 0
        for demographic_file in demographic_files:

            counter =  counter + 1
            suffix = demographic_file.rsplit(".", 1)[-1]
            demographic                 = cf.spark_read(demographic_file, spark)



        # provider_id_mapping  = spark_read(provider_id_mapping_path, spark)

        
            joined_df = demographic.join(person_id_mapping, person_id_mapping['pcornet_patid']== demographic['PATID']).drop('pcornet_patid')
                # .join(patid_location_id_mapping,  patid_location_id_mapping['pcornet_patid']== demographic['PATID'], how="left").drop('pcornet_patid')\
                # .join(patid_care_site_id_mapping, patid_care_site_id_mapping['pcornet_patid']== demographic['PATID'], how="left").drop('pcornet_patid')
            # joined_df = joined_df.join(provider_id_mapping,  provider_id_mapping['pcornet_providerid']== demographic['PROVIDERID'], how="left")

            
            


        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################


            person = joined_df.select(              
                
                                        joined_df['omop_person_id'].alias("person_id"),
                                        mapping_gender_concept_id_dict[upper(col('SEX'))].alias("gender_concept_id"),
                                        get_year_from_date_udf(joined_df['BIRTH_DATE']).alias('year_of_birth'),
                                        get_month_from_date_udf(joined_df['BIRTH_DATE']).alias('month_of_birth'),
                                        get_day_from_date_udf(joined_df['BIRTH_DATE']).alias('day_of_birth'),
                                        get_datetime_from_date_udf(joined_df['BIRTH_DATE']).alias("birth_datetime"),
                                        mapping_race_concept_id_dict[upper(col("RACE"))].alias("race_concept_id"),
                                        mapping_ethnicity_concept_id_dict[upper(col("HISPANIC"))].alias("ethnicity_concept_id"),
                                        lit('').alias("location_id"),
                                        lit('').alias("provider_id"),
                                        lit('').alias("care_site_id"),
                                        joined_df['PATID'].alias("person_source_value"),
                                        joined_df['SEX'].alias("gender_source_value"),
                                        mapping_gender_source_concept_id_dict[upper(col('SEX'))].alias("gender_source_concept_id"),
                                        joined_df['RACE'].alias("race_source_value"),
                                        mapping_race_source_concept_id_dict[upper(col('RACE'))].alias("race_source_concept_id"),
                                        joined_df['HISPANIC'].alias("ethnicity_source_value"),
                                        mapping_ethnicity_source_concept_id_dict[upper(col('HISPANIC'))].alias("ethnicity_source_concept_id"),
                                        cf.get_current_time_udf().alias("updated"),
                                        joined_df['SOURCE'].alias("source"),
                                        lit('DEMOGRAPHIC').alias("mapped_from"),
                                    

                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################

            files_count = len(demographic_files)
            current_count= counter
            file_name = f"person.csv.{suffix}"


            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = demographic_file

            )
            cf.write_pyspark_output_file(
                            payspark_df = person,
                            output_file_name = file_name ,
                            output_data_folder_path= mapped_data_folder_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'person_mapper.py' )

    cf.print_with_style(str(e), 'danger red','error')