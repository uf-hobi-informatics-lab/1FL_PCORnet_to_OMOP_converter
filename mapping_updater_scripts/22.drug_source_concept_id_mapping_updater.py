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
spark = cf.get_spark_session("drug_source_concept_id_mapping_updater")


try:



    ###################################################################################################################################
    # Load the config file for the selected parnter
    ###################################################################################################################################
              
        concept_table_path                       = f"/app/data/omop_vocabulary_tables/CONCEPT.csv"
        drug_source_concept_id_mapping_file_path = f"/app/data/{input_data_folder}/mapping_tables/"




    ###################################################################################################################################
    # Loading the unmapped enctounter table
    ###################################################################################################################################


        concept = spark.read.option("inferSchema", "false").load(concept_table_path,format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
        drug_concept = concept.filter(concept.domain_id == 'Drug')
    

        default_value = lit('NI')
        mapping_pcornet_code_type_dict = create_map([lit(x) for x in chain(*pcornet_code_type_dict.items())])


    ###################################################################################################################################
    # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
    ###################################################################################################################################

        drug_source_concept_id_mapping = drug_concept.select(              
            
            
                                    drug_concept['concept_code'].alias("drug_code"),
                                    drug_concept['concept_id'].alias("drug_source_concept_id"),
                                    coalesce(mapping_pcornet_code_type_dict[upper(col('vocabulary_id'))], default_value).alias("pcornet_drug_type"),
                                    drug_concept['invalid_reason']
                                                            )

    ###################################################################################################################################
    # Create the output file
    ###################################################################################################################################

        cf.write_pyspark_output_file(
                        payspark_df = drug_source_concept_id_mapping,
                        output_file_name = "mapping_drug_source_concept_id.csv",
                        output_data_folder_path= drug_source_concept_id_mapping_file_path)


        spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'drug_source_concept_id_mapping.py' )

    cf.print_with_style(str(e), 'danger red','error')