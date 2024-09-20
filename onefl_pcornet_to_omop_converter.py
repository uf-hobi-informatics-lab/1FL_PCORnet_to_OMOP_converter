
from commonFunctions import CommonFuncitons
import importlib
import sys
import os
import subprocess
import pyspark
from pyspark.sql import SparkSession
import argparse
from datetime import datetime
import pytz
from settings import PCORNET_TABLES, OMOP_TABLES,OMOP_ID_MAPPING_TABLE_1,OMOP_ID_MAPPING_TABLE_2, OMOP_VOCABULRY_TABLES, schemas, datetime_fields, SUPPLEMENTARY_TABLES


spark = SparkSession.builder.master("spark://master:7077").appName("Initial Run").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
spark.stop()

print("""
                                                                                                                                         


                            ▄█  ░█▀▀▀ ░█    　 ░█▀▀▀█ ░█▀▄▀█ ░█▀▀▀█ ░█▀▀█ 　 ░█▀▀█ ░█▀▀▀█ ░█▄ ░█ ░█  ░█ ░█▀▀▀ ▀▀█▀▀ ░█▀▀▀ ░█▀▀█ 
                             █  ░█▀▀▀ ░█    　 ░█  ░█ ░█░█░█ ░█  ░█ ░█▄▄█ 　 ░█    ░█  ░█ ░█░█░█  ░█░█  ░█▀▀▀  ░█   ░█▀▀▀ ░█▄▄▀ 
                            ▄█▄ ░█    ░█▄▄█ 　 ░█▄▄▄█ ░█  ░█ ░█▄▄▄█ ░█    　 ░█▄▄█ ░█▄▄▄█ ░█  ▀█   ▀▄▀  ░█▄▄▄  ░█   ░█▄▄▄ ░█ ░█


                                                                              """
                                                   )






job_num = 0

# spark = SparkSession.builder.master("spark://master:7077").appName("onefl_converter").getOrCreate()

###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()

parser.add_argument("-j", "--job", nargs="+")
parser.add_argument("-ot", "--omop_table_name", nargs="+")
parser.add_argument("-pt", "--pcornet_table_name", nargs="+")
parser.add_argument("-sdb", "--source_db_name")
parser.add_argument("-ss", "--source_db_server")
parser.add_argument("-ddb", "--destination_db_name")
parser.add_argument("-ds", "--destination_db_server")
parser.add_argument("-vdb", "--vocabulary_db_name")
parser.add_argument("-vs", "--vocabulary_db_server")
parser.add_argument("-mu", "--mapping_updaters", nargs="+")
parser.add_argument("-mt", "--mapping_tables", nargs="+")
parser.add_argument("-ut", "--upload_tables", nargs="+")




args = parser.parse_args()

jobs = args.job
if args.omop_table_name != None:  omop_tables = [item.lower() for item in args.omop_table_name]
if args.pcornet_table_name  != None:  input_pcornet_tables = [item.upper() for item in args.pcornet_table_name]
source_db_name = args.source_db_name
source_db_server = args.source_db_server
destination_db_name = args.destination_db_name
destination_db_server = args.destination_db_server
vocab_db_name = args.vocabulary_db_name
vocab_db_server = args.vocabulary_db_server
input_mapping_updaters = args.mapping_updaters
input_mapping_tables = args.mapping_tables
input_upload_tables = args.upload_tables


cf =CommonFuncitons()


# ###################################################################################################################################
# # This function will get all the created mappers python script and return a list of thier names
# ###################################################################################################################################

def get_mappers_list():


        folder_path = '/app/mapping_scripts/'
        suffix = 'mapper.py' 
        file_names = os.listdir(folder_path)

        partner_mappers_list = [file for file in file_names if file.endswith(suffix)]

        return partner_mappers_list

# ###################################################################################################################################
# # This function will get all the list names of all the mapped files
# ###################################################################################################################################

def get_partner_uploads_list(folder_path, table_list):

       
        # prefix = 'mapped_' 
        file_names = os.listdir(folder_path)

        mapped_tables_list = [file for file in file_names if file.replace('.csv','') in table_list]

        return mapped_tables_list









###################################################################################################################################
# This function will run the mapper script for a single table
###################################################################################################################################
def run_mapper( mapper, folder):

    global job_num

    job_num = job_num +1
    cf.print_run_status(job_num,total_jobs_count, f"{mapper}", f'Local {source_db_name}/omop_tables', f'Local {source_db_name}/pcornet_tables')

    command = ["python", "/app/mapping_scripts/"+mapper, '-f', folder ]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)


###################################################################################################################################
# This function will run the mapper script for a single table
###################################################################################################################################
def run_update_mapping(mapping_updater, folder):

    global job_num

    job_num = job_num +1

    cf.print_run_status(job_num,total_jobs_count, f"{mapping_updater}", f'Local {source_db_name}/mapping_tables', f'Local {source_db_name}')

    command = ["python", "/app/mapping_updater_scripts/"+mapping_updater, '-f', folder ]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)

    


###################################################################################################################################
# This function will run the mapper scripts specified by the user
###################################################################################################################################

def run_mappers_jobs(folder):

    # global total_mappers_count

    if 'all' in input_mapping_tables:

        partner_mappers_list = sorted(get_mappers_list())
        # total_mappers_count = len(partner_mappers_list)


        for mapper  in partner_mappers_list:
            run_mapper(mapper, folder)
    else:

        folder_path = '/app/mapping_scripts/'
  
        file_names = os.listdir(folder_path)

        for table in input_mapping_tables:

            # mapper = table.lower()+"_mapper.py"

            mappers_list = sorted([file for file in file_names if file.endswith('_mapper.py') and table in file])

            for mapper in mappers_list:
                           
                run_mapper(mapper, folder)


###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_upload_data_jobs(folder):

    
    
    mapped_files_path = f"/app/data/{folder}/omop_tables/"

    

    # global total_uploads_count
    global job_num

    if 'all' in input_upload_tables:

        omop_list = get_partner_uploads_list(mapped_files_path, OMOP_TABLES)
        sup_list = get_partner_uploads_list(mapped_files_path, SUPPLEMENTARY_TABLES)
        combined_list = list(set(omop_list + sup_list))
        uploads_list = sorted(list(combined_list))
        # total_uploads_count = len(partner_uploads_list)
        
        

        for file_name  in uploads_list:

            table_name = file_name.replace('.csv',"")


            if table_name in OMOP_TABLES or table_name in SUPPLEMENTARY_TABLES:

                job_num = job_num +1
                cf.print_run_status(job_num,total_jobs_count, f"Uploading {table_name}", f'Remote {destination_db_name}', f'Local {source_db_name}/omop_tables')


                schema = schemas.get(table_name, None)

                temp_mapped_files_path = f"/app/data/{folder}/omop_tables/{file_name}/"

                cf.db_upload(db= destination_db_name,
                            db_server=destination_db_server,
                            schema=schema,
                            file_name=file_name, 
                            table_name = table_name.lower(),
                            file_path= temp_mapped_files_path,
                            )
    else:

        for table in input_upload_tables:
            mapped_table_name = table.lower()+".csv"
            
            temp_mapped_files_path = f"/app/data/{folder}/omop_tables/{table}/"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"Uploading {mapped_table_name}", f'Remote {destination_db_name}', f'Remote {source_db_name}/omop_tables')
            schema = schemas.get(table, None)

            cf.db_upload(db= destination_db_name,
                         db_server=destination_db_server,
                         schema=schema,
                         file_name=mapped_table_name, 
                         table_name = table.lower(),
                         file_path= temp_mapped_files_path,
                          )

# ###################################################################################################################################
# # This function will run pull all the specified by  user pcornet tables to the destination folder 
# ###################################################################################################################################

def run_pulling_data():

    global job_num

    if 'ALL' in input_pcornet_tables:

        

        for pcornet_table in PCORNET_TABLES:
            
            job_num = job_num +1

            cf.print_run_status(job_num,total_jobs_count, f"Pulling {pcornet_table}", f'Local {source_db_name}/pcornet_tables', f'Remote {source_db_name}')
            cf.pull_table_to_csv(
                table_name = pcornet_table,
                db_name = source_db_name,
                db_server = source_db_server,
                output_file_path = f"/app/data/{source_db_name}/pcornet_tables/"
            )

            cf.split_csv_with_chunks_with_a_max_size(

                table_name = pcornet_table,
                input_file_path = f"/app/data/{source_db_name}/pcornet_tables/"
            )
    else:
        for  pcornet_table in input_pcornet_tables:
               job_num = job_num +1

               cf.print_run_status(job_num,total_jobs_count, f"Pulling {pcornet_table}", f'Local {source_db_name}/pcornet_tables', f'Remote {source_db_name}')
               cf.pull_table_to_csv(
                table_name = pcornet_table,
                db_name = source_db_name,
                db_server = source_db_server,
                output_file_path = f"/app/data/{source_db_name}/pcornet_tables/"
                     )
               cf.split_csv_with_chunks_with_a_max_size(

                table_name = pcornet_table,
                input_file_path = f"/app/data/{source_db_name}/pcornet_tables/"
            )


# ###################################################################################################################################
# # This function will run pull omop vocabulary tables into the dataset vocabulary data folder
# ###################################################################################################################################

def run_pull_vocabulary():

    global job_num


    for vocabulry_table in OMOP_VOCABULRY_TABLES:
               
        job_num = job_num +1
               
        cf.print_run_status(job_num,total_jobs_count, f"Pulling {vocabulry_table}", f'Local vocabulary_tables', f'Remote {vocab_db_name}')
        cf.pull_table_to_csv(
            table_name = vocabulry_table,
            db_name = vocab_db_name,
            db_server = vocab_db_server,
            output_file_path = f"/app/data/omop_vocabulary_tables/"
        )



# ###################################################################################################################################
# # This function will run pull omop vocabulary tables into the dataset vocabulary data folder
# ###################################################################################################################################

def run_upload_mapping_jobs():

    mapped_files_path = f"/app/data/{source_db_name}/mapping_tables/"
    global job_num


    for mapping_table in OMOP_ID_MAPPING_TABLE_1:
               
        mapped_table_name = mapping_table.lower()+".csv"
        job_num = job_num +1
               
        cf.print_run_status(job_num,total_jobs_count, f"Uploading {mapping_table}", f'Remote {destination_db_name}', f'Local {source_db_name}/mapping_tables')
        schema = schemas.get(mapping_table, None)

        cf.db_upload(db= destination_db_name,
                         db_server=destination_db_server,
                         schema=schema,
                         file_name=mapped_table_name, 
                         table_name = mapping_table.lower(),
                         file_path= mapped_files_path,
                          )


    for mapping_table in OMOP_ID_MAPPING_TABLE_2:
               
        mapped_table_name = mapping_table.lower()+'/'+ mapping_table.lower()+".csv*"
        job_num = job_num +1
               
        cf.print_run_status(job_num,total_jobs_count, f"Uploading {mapping_table}", f'Remote {destination_db_name}', f'Local {source_db_name}/mapping_tables')
        schema = schemas.get(mapping_table, None)

        cf.db_upload(db= destination_db_name,
                         db_server=destination_db_server,
                         schema=schema,
                         file_name=mapped_table_name, 
                         table_name = mapping_table.lower(),
                         file_path= mapped_files_path,
                          )

# ###################################################################################################################################
# # This function will run pull omop vocabulary tables into the dataset vocabulary data folder
# ###################################################################################################################################

def run_upload_vocab_jobs():

    vocabulary_files_path = f"/app/data/omop_vocabulary_tables/"
    global job_num


    for vocabulary_table in OMOP_VOCABULRY_TABLES:
               
        vocabulary_table_name = vocabulary_table.upper()+".csv"
        job_num = job_num +1
               
        cf.print_run_status(job_num,total_jobs_count, f"Uploading {vocabulary_table}", f'Remote {destination_db_name}', f'Local omop_vocabulary_tables')
        schema = schemas.get(vocabulary_table, None)

        cf.db_upload(db= destination_db_name,
                         db_server=destination_db_server,
                         schema=schema,
                         file_name=vocabulary_table_name, 
                         table_name = vocabulary_table.lower(),
                         file_path= vocabulary_files_path,
                          )




# ###################################################################################################################################
# # This function will get all the mapping updater python scripts and return a list of thier names
# ###################################################################################################################################


def get_mapping_updaters_list():


        folder_path = '/app/mapping_updater_scripts/'
        suffix = 'mapping_updater.py' 
        file_names = os.listdir(folder_path)

        mapping_updaters_list = [file for file in file_names if file.endswith(suffix)]

        return mapping_updaters_list


###################################################################################################################################
# This function will run the mapper scripts specified by the user
###################################################################################################################################

def run_update_mappings(folder):

    # # global total_mappers_count
    # try:
    #     input_mapping_updaters = sorted(input_mapping_updaters())

    # except:
    #     None


    if 'all' in input_mapping_updaters:

        mapping_updaters_list = sorted(get_mapping_updaters_list())
        # total_mappers_count = len(partner_mappers_list)


        for mapping_updater  in mapping_updaters_list:
            run_update_mapping(mapping_updater, folder)
    else:

        # for mapping_updater in input_mapping_updaters:
        #     mapping_updater = mapping_updater.lower()+"_mapping_updater.py" 
        #     run_update_mapping(mapping_updater, folder)




    # else:

        folder_path = '/app/mapping_updater_scripts/'
  
        file_names = os.listdir(folder_path)

        for table in input_mapping_updaters:

        
            updaters_list = sorted([file for file in file_names if file.endswith('_updater.py') and table in file])

            for updater in updaters_list:
                           
                run_update_mapping(updater, folder)



###################################################################################################################################
# checking for correct input
###################################################################################################################################




valid_jobs = ['all','pull_data','pull_vocab', 'map','upload_data','upload_mapping','upload_vocab','update_mapping']

for job in jobs:
   
    if job not in valid_jobs:

        cf.print_with_style(job+ " is not a valid job!!!!!! Please enter a valid job name eg. -j pull_data or -j map, or -j all", 'danger red')
        cf.print_with_style("The valid jobs are: 'all','pull_data','pull_vocab', 'map','upload_data','upload_mapping','upload_vocab','update_mapping'", 'danger red')

        sys.exit()

if 'all' in jobs or "pull_data" in jobs:

    if source_db_name == None:

        cf.print_with_style("For job= pull_data, -sdb(source database) is required!!!", 'danger red')
        sys.exit()
    if source_db_server == None:

        cf.print_with_style("For job= pull_data, -ss(source server name) is required!!! ", 'danger red')
        sys.exit()

    if input_pcornet_tables == None:

        cf.print_with_style("For job= pull_data, -pt(pcornet table(s)) is required!!!", 'danger red')
        sys.exit()
    elif 'ALL' not in input_pcornet_tables:

        for table in input_pcornet_tables:
            if table not in PCORNET_TABLES:
                cf.print_with_style(table+ " is not a valid PCORnet table", 'danger red')
                sys.exit()

if 'all' in jobs or "pull_vocab" in jobs:

    if vocab_db_name == None:

        cf.print_with_style("For job= pull_vocab, -vbd(vocabulary database) is required!!!", 'danger red')
        sys.exit()

    if vocab_db_server == None:

        cf.print_with_style("For job= pull_vocab, -vs(vocabulary server name) is required!!! ", 'danger red')
        sys.exit()
    

if 'all' in jobs or "update_mapping" in jobs:

    if source_db_name == None:

        cf.print_with_style("For job= update_mapping, -sdb(source data) is required!!!", 'danger red')
        sys.exit()

    if input_mapping_updaters == None:

        cf.print_with_style("For job= update_mapping, -mu(mapping updater(s)) is required!!!", 'danger red')
        sys.exit()

if 'all' in jobs or "map" in jobs:

    if source_db_name == None:

        cf.print_with_style("For job= map, -sdb(source data) is required!!!", 'danger red')
        sys.exit()

    if input_mapping_tables == None:

        cf.print_with_style("For job= map, -mt(map table(s)) is required!!!", 'danger red')
        sys.exit()

if 'all' in jobs or "upload_data" in jobs:

    if source_db_name == None:

        cf.print_with_style("For job= upload_data, -sdb(source data) is required!!!", 'danger red')
        sys.exit()

    if destination_db_name == None:

        cf.print_with_style("For job= upload_data, -ddb(destination database) is required!!!", 'danger red')
        sys.exit()
        
    if destination_db_server == None:

        cf.print_with_style("For job= upload_data, -ds(destination server) is required!!!", 'danger red')
        sys.exit()
    
    if input_upload_tables == None:

        cf.print_with_style("For job= upload_data, -ut(table to be uploaded) is required!!!", 'danger red')
        sys.exit()
        

if 'all' in jobs or "upload_mapping" in jobs:

    if source_db_name == None:

        cf.print_with_style("For job= upload_mapping, -sdb(source data) is required!!!", 'danger red')
        sys.exit()

    if destination_db_name == None:

        cf.print_with_style("For job= upload_mapping, -ddb(destination database) is required!!!", 'danger red')
        sys.exit()
        
    if destination_db_server == None:

        cf.print_with_style("For job= upload_mapping, -ds(destination server) is required!!!", 'danger red')
        sys.exit()
    


if 'all' in jobs or "upload_vocab" in jobs:



    if destination_db_name == None:

        cf.print_with_style("For job= upload_vocab, -ddb(destination database) is required!!!", 'danger red')
        sys.exit()
        
    if destination_db_server == None:

        cf.print_with_style("For job= upload_vocab, -ds(destination server) is required!!!", 'danger red')
        sys.exit()
    

        

##################################################################################################################################
# Getting Jobs count
########################################

def get_names_with_pattern(pattern, file_names):

        list = sorted([file for file in file_names if  pattern in file])

        return list




##################################################################################################################################
# Getting Jobs count
##################################################################################################################################

global total_jobs_count

data_tables_to_be_pullled_count = 0
vocab_tables_to_be_pullled_count = 0
mapping_updaters_to_be_run_count = 0
mapping_to_be_run_count = 0
mapped_tables_to_be_uploaded_count = 0
mapping_tables_to_be_uploaded_count = 0
vocab_tables_to_be_uploaded_count = 0



if 'all' in jobs or 'pull_data' in jobs:

    if 'ALL' in  input_pcornet_tables:
        data_tables_to_be_pullled_count = len(PCORNET_TABLES)
    else: 
        data_tables_to_be_pullled_count = len(input_pcornet_tables)


if 'all' in jobs or 'pull_vocab' in jobs:

    vocab_tables_to_be_pullled_count = len(OMOP_VOCABULRY_TABLES)
    

if 'all' in jobs or 'update_mapping' in jobs:

    if 'all' in  input_mapping_updaters:
        mapping_updaters_to_be_run_count = len(get_mapping_updaters_list())
    else: 

        list_of_unique_updaters = []
        all_mapping_updaters_list = get_mapping_updaters_list()
        for updater in input_mapping_updaters :

            pattern_updaters = get_names_with_pattern(updater, all_mapping_updaters_list)
            combined_list = list(set(list_of_unique_updaters + pattern_updaters))
            list_of_unique_updaters = list(combined_list)
       

        mapping_updaters_to_be_run_count = len(list_of_unique_updaters)



if 'all' in jobs or 'map' in jobs:

    if 'all' in  input_mapping_tables:
        mapping_to_be_run_count = len(get_mappers_list())
    else: 

        list_of_unique_mappers = []
        all_mapping_mappers_list = get_mappers_list()
        for mapper in input_mapping_tables :

            pattern_mappers = get_names_with_pattern(mapper, all_mapping_mappers_list)
            combined_list = list(set(list_of_unique_mappers + pattern_mappers))
            list_of_unique_mappers = list(combined_list)



        mapping_to_be_run_count = len(list_of_unique_mappers)


if 'all' in jobs or 'upload_data' in jobs:

    if 'all' in  input_upload_tables:

        mapped_tables_to_be_uploaded_count = len(OMOP_TABLES)+len(SUPPLEMENTARY_TABLES)
    else: 
        mapped_tables_to_be_uploaded_count = len(input_upload_tables)






if 'all' in jobs or 'upload_mapping' in jobs:

   mapping_tables_to_be_uploaded_count= len(get_mapping_updaters_list())
   

if 'all' in jobs or 'upload_vocab' in jobs:

   vocab_tables_to_be_uploaded_count= len(OMOP_VOCABULRY_TABLES)



total_jobs_count = data_tables_to_be_pullled_count +vocab_tables_to_be_pullled_count+mapping_updaters_to_be_run_count+mapping_to_be_run_count+mapped_tables_to_be_uploaded_count +mapping_tables_to_be_uploaded_count +vocab_tables_to_be_uploaded_count 


###################################################################################################################################
# submitting the jobs to be run
###################################################################################################################################
 

# for folder in input_data_folders:


if  'all' in jobs:
    run_pulling_data()
    run_pull_vocabulary()
    run_update_mappings(source_db_name)
    run_mappers_jobs(source_db_name)
    run_upload_data_jobs(source_db_name)
    run_upload_mapping_jobs()
    run_upload_vocab_jobs()


else :


    if  'pull_data' in jobs:
         run_pulling_data()

    if  'pull_vocab' in jobs:
         run_pull_vocabulary()

    if  'update_mapping' in jobs:
         run_update_mappings(source_db_name)

    if  'map' in jobs:
        run_mappers_jobs(source_db_name)

    if  'upload_data' in jobs:
        run_upload_data_jobs(source_db_name)
    
    if  'upload_mapping' in jobs:
        run_upload_mapping_jobs()

    if  'upload_vocab' in jobs:
        run_upload_vocab_jobs()

        

    

print("""

                                                    ▀▀█▀▀ ░█ ░█ ░█▀▀▀ 　 ░█▀▀▀ ░█▄ ░█ ░█▀▀▄ 　 █ 
                                                     ░█   ░█▀▀█ ░█▀▀▀ 　 ░█▀▀▀ ░█░█░█ ░█ ░█ 　 ▀ 
                                                     ░█   ░█ ░█ ░█▄▄▄ 　 ░█▄▄▄ ░█  ▀█ ░█▄▄▀ 　 ▄
            
    """)
             