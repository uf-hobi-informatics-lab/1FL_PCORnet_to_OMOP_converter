###################################################################################################################################
# This script will map a PCORNet provider table 
###################################################################################################################################

 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

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
spark = cf.get_spark_session("provider_specialty_concept_id_mapping_updater")


try:


    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        concept_table_path                       = f"/app/data/omop_vocabulary_tables/CONCEPT.csv"
        concept_relationship_table_path          = f"/app/data/omop_vocabulary_tables/CONCEPT_RELATIONSHIP.csv"

        provider_specialty_concept_id_mapping_file_path = f"/app/data/{input_data_folder}/mapping_tables/"




    ###################################################################################################################################
    # Loading the unmapped concept tables table and create the mapping table
    ###################################################################################################################################


        concept = spark.read.option("inferSchema", "false").load(concept_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
        provider_specialty_concept = concept.filter(concept.concept_class_id == 'Physician Specialty').select('concept_id')

        concept_relationship = spark.read.option("inferSchema", "false").load(concept_relationship_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
        filterd_concept_relationship  = concept_relationship.filter(concept_relationship.relationship_id == 'Maps to')


        joined_table_1 = concept.join(filterd_concept_relationship, concept["concept_id"]==filterd_concept_relationship["concept_id_1"])
        joined_table_2 = joined_table_1.join(provider_specialty_concept, provider_specialty_concept["concept_id"]==joined_table_1["concept_id_2"])





    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

        provider_specialty_concept_id_mapping = joined_table_2.select(              
            
            
                                    joined_table_2['concept_code'].alias("provider_specialty_source_value_1"),
                                    joined_table_2['concept_id_2'].alias("specialty_concept_id"),
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = provider_specialty_concept_id_mapping,
                        output_file_name = "mapping_provider_specialty_concept_id.csv",
                        output_data_folder_path= provider_specialty_concept_id_mapping_file_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'provider_spcialty_concept_id_mapping_updater.py' )

    cf.print_with_style(str(e), 'danger red','error')