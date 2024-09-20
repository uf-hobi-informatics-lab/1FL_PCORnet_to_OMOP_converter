###################################################################################################################################
# This script will map a PCORNet condition table 
###################################################################################################################################

 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from dictionaries import *
from commonFunctions import CommonFuncitons 
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
from settings import *
import os

###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_data_folder = args.data_folder
# print(input_data_folder)


cf =CommonFuncitons()

# spin the pyspak cluster and
# spark = cf.get_spark_session("onefl_pcornet_to_omop_converter")
# spark = SparkSession.builder.master("spark://master:7077").getOrCreate()
# spark.sparkContext.setLogLevel('OFF')

spark = cf.get_spark_session("onefl_pcornet_to_omop_converter")

# test = os.listdir('/app/data/CHAR_BND/')
# print (test)
try:




    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
            
    condition_source_concept_id_table_path = f"/app/data/{input_data_folder}/mapping_tables/mapping_condition_source_concept_id.csv"
    concept_relationship_table_path          = f"/app/data/omop_vocabulary_tables/CONCEPT_RELATIONSHIP.csv"

    condition_concept_id_mapping_file_path = f"/app/data/{input_data_folder}/mapping_tables/"




    ###################################################################################################################################
    # Loading the unmapped concept tables table and create the mapping table
    ###################################################################################################################################

    condition_source_concept_id = cf.spark_read(condition_source_concept_id_table_path,spark)

    # print("###################################################################################################################################  21.condition_concept_id_mapping_updater")


    condition_source_concept_id_left = condition_source_concept_id.select(
        condition_source_concept_id['condition_code'],
        condition_source_concept_id['condition_source_concept_id'].alias('condition_source_concept_id_left'),
        condition_source_concept_id['pcornet_condition_code_type'].alias('pcornet_condition_code_type'),
    )
    condition_source_concept_id_right = condition_source_concept_id.select(
        
        condition_source_concept_id['condition_source_concept_id'].alias('condition_source_concept_id_right'),
        condition_source_concept_id['invalid_reason'],
    )





    concept_relationship = spark.read.option("inferSchema", "false").load(concept_relationship_table_path,format="csv", sep="\t", inferSchema="false", header="true",  quote= '"')
    filterd_concept_relationship  = concept_relationship.filter(concept_relationship.relationship_id == 'Maps to')


    grouped_concept_relationship = filterd_concept_relationship.groupBy("concept_id_1").agg(max("valid_start_date").alias("max_date"))

    grouped_concept_relationship = grouped_concept_relationship.select(
        grouped_concept_relationship['concept_id_1'].alias('concept_id_1_temp'),
        grouped_concept_relationship['max_date'],
    )

    concept_relationship = filterd_concept_relationship.join(grouped_concept_relationship, (col("concept_id_1") == col("concept_id_1_temp")) & (col("valid_start_date") == col("max_date")), "inner").drop('invalid_reason')


    joined_table = condition_source_concept_id_left.join(concept_relationship, condition_source_concept_id_left["condition_source_concept_id_left"]==concept_relationship["concept_id_1"], how = 'left')\
                            .join(condition_source_concept_id_right, condition_source_concept_id_right["condition_source_concept_id_right"]==concept_relationship["concept_id_2"], how='left')


    grouped_joined_table = joined_table.groupBy("condition_code",'pcornet_condition_code_type','invalid_reason').agg(max("condition_source_concept_id_right").alias("condition_source_concept_id_right"))



    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

    condition_concept_id_mapping = grouped_joined_table.select(              
        
        
                                grouped_joined_table['condition_code'].alias("condition_code"),
                                grouped_joined_table['condition_source_concept_id_right'].alias("condition_concept_id"),
                                grouped_joined_table['pcornet_condition_code_type'].alias("pcornet_condition_code_type"),
                                grouped_joined_table['invalid_reason']

                                                        )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

    cf.write_pyspark_output_file(
                    payspark_df = condition_concept_id_mapping,
                    output_file_name = "mapping_condition_concept_id.csv",
                    output_data_folder_path= condition_concept_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'condition_concept_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')