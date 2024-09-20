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
spark = cf.get_spark_session("drug_concept_id_mapping_updater")


try:




    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        drug_source_concept_id_table_path           = f"/app/data/{input_data_folder}/mapping_tables/mapping_drug_source_concept_id.csv"
        concept_relationship_table_path          = f"/app/data/omop_vocabulary_tables/CONCEPT_RELATIONSHIP.csv"

        drug_concept_id_mapping_file_path = f"/app/data/{input_data_folder}/mapping_tables/"




    ###################################################################################################################################
    # Loading the unmapped concept tables table and create the mapping table
    ###################################################################################################################################


        drug_source_concept_id = cf.spark_read(drug_source_concept_id_table_path,spark)
        drug_source_concept_id_left = drug_source_concept_id.select(
            drug_source_concept_id['drug_code'],
            drug_source_concept_id['drug_source_concept_id'].alias('drug_source_concept_id_left'),
            
            drug_source_concept_id['pcornet_drug_type'].alias('pcornet_drug_type'),
        )
        drug_source_concept_id_right = drug_source_concept_id.select(
          
            drug_source_concept_id['drug_source_concept_id'].alias('drug_source_concept_id_right'),
        )

        concept_relationship = cf.spark_read(concept_relationship_table_path,spark)
        filterd_concept_relationship  = concept_relationship.filter(concept_relationship.relationship_id == 'Maps to')


        joined_table = drug_source_concept_id_left.join(filterd_concept_relationship, drug_source_concept_id_left["drug_source_concept_id_left"]==filterd_concept_relationship["concept_id_1"], how ='inner')\
                              .join(drug_source_concept_id_right, drug_source_concept_id_right["drug_source_concept_id_right"]==filterd_concept_relationship["concept_id_2"], how ='inner')





    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

        drug_concept_id_mapping = joined_table.select(              
            
            
                                    joined_table['drug_code'].alias("drug_code"),
                                    joined_table['drug_source_concept_id_right'].alias("drug_concept_id"),
                                    joined_table['pcornet_drug_type'].alias("pcornet_drug_type"),
                                    joined_table['invalid_reason']

                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = drug_concept_id_mapping,
                        output_file_name = "mapping_drug_concept_id.csv",
                        output_data_folder_path= drug_concept_id_mapping_file_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'drug_concept_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')