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



###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
args = parser.parse_args()
input_data_folder = args.data_folder


cf =CommonFuncitons()

# spin the pyspak cluster and
spark = cf.get_spark_session("measurement_concept_id_mapping_updater")


try:



    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        measurement_source_concept_id_table_path = f"/app/data/{input_data_folder}/mapping_tables/mapping_measurement_source_concept_id.csv"
        concept_relationship_table_path          = f"/app/data/omop_vocabulary_tables/CONCEPT_RELATIONSHIP.csv"

        measurement_concept_id_mapping_file_path = f"/app/data/{input_data_folder}/mapping_tables/"




    ###################################################################################################################################
    # Loading the unmapped concept tables table and create the mapping table
    ###################################################################################################################################

        measurement_source_concept_id = cf.spark_read(measurement_source_concept_id_table_path,spark)
        measurement_source_concept_id_left = measurement_source_concept_id.select(
            measurement_source_concept_id['measurement_code'],
            measurement_source_concept_id['measurement_source_concept_id'].alias('measurement_source_concept_id_left'),
            
            measurement_source_concept_id['pcornet_measurement_code_type'].alias('pcornet_measurement_code_type'),
        )
        measurement_source_concept_id_right = measurement_source_concept_id.select(
          
            measurement_source_concept_id['measurement_source_concept_id'].alias('measurement_source_concept_id_right'),
        )





        concept_relationship = spark.read.option("inferSchema", "false").load(concept_relationship_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
        filterd_concept_relationship  = concept_relationship.filter(concept_relationship.relationship_id == 'Maps to')


        joined_table = measurement_source_concept_id_left.join(filterd_concept_relationship, measurement_source_concept_id_left["measurement_source_concept_id_left"]==filterd_concept_relationship["concept_id_1"], how = 'inner')\
                              .join(measurement_source_concept_id_right, measurement_source_concept_id_right["measurement_source_concept_id_right"]==filterd_concept_relationship["concept_id_2"], how='inner')





    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

        measurement_concept_id_mapping = joined_table.select(              
            
            
                                    joined_table['measurement_code'].alias("measurement_code"),
                                    joined_table['measurement_source_concept_id_right'].alias("measurement_concept_id"),
                                    joined_table['pcornet_measurement_code_type'].alias("pcornet_measurement_code_type"),
                                    joined_table['invalid_reason']

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = measurement_concept_id_mapping,
                        output_file_name = "mapping_measurement_concept_id.csv",
                        output_data_folder_path= measurement_concept_id_mapping_file_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'measurement_concept_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')