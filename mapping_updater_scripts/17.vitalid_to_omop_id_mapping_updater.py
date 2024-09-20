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

from commonFunctions import CommonFuncitons 
import importlib
import sys
# from partners import partners_list
from itertools import chain
import argparse
from settings import *
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
spark = cf.get_spark_session("vitalid_to_omop_id_mapping_updater")



path = f"/app/data/{input_data_folder}/pcornet_tables/"
files_and_folders = os.listdir(f"/app/data/{input_data_folder}/pcornet_tables/")
folders = [folder for folder in files_and_folders if os.path.isdir(os.path.join(path, folder))]

try:



    for folder in folders :


        partner_num = PARTNER_SOURCE_NAMES_LIST.get(folder, 0)
        ###################################################################################################################################
        # Load the config file for the selected parnter
        ###################################################################################################################################
                
        pcornet_vital_table_path                = f"/app/data/{input_data_folder}/pcornet_tables/{folder}/VITAL/VITAL.csv*"
        vital_files = sorted(glob.glob(pcornet_vital_table_path))


        vitalid_to_omop_id_mapping_file_path    = f"/app/data/{input_data_folder}/mapping_tables/{folder}/mapping_vitalid_to_omop_id/"



        ###################################################################################################################################
        # Loading the unmapped enctounter table
        ###################################################################################################################################


        counter = 0

        for vital_file in vital_files:

            # print(pcornet_medadmin_file)

            counter = counter +1 
            vital                                     = cf.spark_read(vital_file, spark)
            suffix = vital_file.rsplit(".", 1)[-1]

            
            vital_wt           = vital.filter((col('WT').isNotNull()) & ( col('WT') !=0) & ( col('WT') !='0' ))
            vital_ht           = vital.filter((col('HT').isNotNull()) & ( col('HT') !=0) & ( col('HT') !='0' ))
            vital_bmi          = vital.filter((col('ORIGINAL_BMI').isNotNull()) & ( col('ORIGINAL_BMI') !=0) & ( col('ORIGINAL_BMI') !='0' ))
            vital_diastolic    = vital.filter((col('DIASTOLIC').isNotNull()) & ( col('DIASTOLIC') !=0) & ( col('DIASTOLIC') !='0' ))
            vital_systolic     = vital.filter((col('SYSTOLIC').isNotNull()) & ( col('SYSTOLIC') !=0) & ( col('SYSTOLIC') !='0' ))
            vital_smoking      = vital.filter((col('SMOKING').isNotNull()) & ( col('SMOKING') !='OT') & ( col('SMOKING') !='NI' ) & ( col('SMOKING') !='UN' ))
            vital_tobacco      = vital.filter((col('TOBACCO').isNotNull()) & ( col('TOBACCO') !='OT') & ( col('TOBACCO') !='NI' ) & ( col('TOBACCO') !='UN' ))
            vital_tobacco_type = vital.filter((col('TOBACCO_TYPE').isNotNull()) & ( col('TOBACCO_TYPE') !='OT') & ( col('TOBACCO_TYPE') !='NI' ) & ( col('TOBACCO_TYPE') !='UN' ))






        ###################################################################################################################################
        # Apply the mappings dictionaries and the common function on the fields of the unmmaped encoutner table
        ###################################################################################################################################

            offset = 100*counter+ VITALID_WT_OFFSET
            omop_id_mapping_wt = vital_wt.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('WT').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )
            offset = 100*counter+ VITALID_HT_OFFSET

            omop_id_mapping_ht = vital_ht.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('HT').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )
            offset = 100*counter+ VITALID_BMI_OFFSET
            omop_id_mapping_bmi = vital_bmi.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('BMI').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

            offset = 100*counter+ VITALID_DIASTOLIC_OFFSET
            omop_id_mapping_diastolic = vital_diastolic.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('DIASTOLIC').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )


            offset = 100*counter+ VITALID_SYSTOLIC_OFFSET
                            
            omop_id_mapping_systolic = vital_systolic.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('SYSTOLIC').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )
            offset = 100*counter+ VITALID_SMOKING_OFFSET


            omop_id_mapping_smoking = vital_smoking.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('SMOKING').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

            offset = 100*counter+ VITALID_TOBACCO_OFFSET
                                                
            omop_id_mapping_tobacco = vital_tobacco.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('TOBACCO').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

            offset = 100*counter+ VITALID_TOBACCO_TYPE_OFFSET

            omop_id_mapping_tobacco_type = vital_tobacco_type.select(              
                
                
                                        vital['VITALID'].alias("pcornet_vitalid"), 
                                        lit('TOBACCO_TYPE').alias("data_type"),                                 
                                        ((F.monotonically_increasing_id()*100000+offset)*100+partner_num).alias("omop_id"), #mulitplying by 100 and adding an offset to ensure this id is unique to this table
                                                                )

        ###################################################################################################################################
        # Create the output file
        ###################################################################################################################################


            common_from_vital_combined =       omop_id_mapping_wt\
                                    .union(omop_id_mapping_ht)\
                                    .union(omop_id_mapping_bmi)\
                                    .union(omop_id_mapping_diastolic)\
                                    .union(omop_id_mapping_systolic)\
                                    .union(omop_id_mapping_smoking)\
                                    .union(omop_id_mapping_tobacco)\
                                    .union(omop_id_mapping_tobacco_type)



            files_count = len(vital_files)
            current_count= counter
            file_name = f"mapping_vitalid_to_omop_id.csv.{suffix}"

            cf.print_mapping_file_status(

                        total_count = files_count,
                        current_count = current_count,
                        output_file_name =  file_name,
                        source_file = vital_file

                        )



            cf.write_pyspark_output_file(
                            payspark_df = common_from_vital_combined,
                            output_file_name = file_name,
                            output_data_folder_path= vitalid_to_omop_id_mapping_file_path)


    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_data_folder,
                            job     = 'mapping_vitalid_to_omop_id.py' )

    cf.print_with_style(str(e), 'danger red','error')