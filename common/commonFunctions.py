
###################################################################################################################################
# This class will host all the common functions used in the mappings by diffrenet mapping scripts
###################################################################################################################################



from itertools import chain
from Crypto.Hash import SHA
from Crypto.Cipher import XOR
from base64 import b64decode, b64encode
from pyspark.sql.functions import udf, col
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark
import time
import subprocess
from pyspark.sql.types import StructType, StructField, StringType, NullType
import sys
from secrets_1 import * 
from pyspark.conf import SparkConf
import pandas as pd
from urllib import parse
from datetime import datetime
import pytz
import os
import glob
import logging


from dateutil import parser as dp
import pyspark.sql.functions as F
from settings import POOL_SIZE, MAX_OVERFLOW, POOL_RECYCLE, BCP_DRIVER, PCORNET_TABLES, OMOP_TABLES, schemas, PULL_PARTITION_CHUNK_SIZE

class CommonFuncitons:


    def __init__(self):



        # self.partner = partner # specifying the partner / used for the hashing
        # # print(self.partner)
        # # sys.exit()
        self.get_time_from_datetime_udf = udf(self.get_time_from_datetime, StringType())
        self.get_date_from_datetime_udf = udf(self.get_date_from_datetime, StringType())
        self.get_date_from_date_str_with_default_value_udf = udf(self.get_date_from_date_str_with_default_value, StringType())
        self.get_datetime_from_date_str_with_default_value_udf = udf(self.get_datetime_from_date_str_with_default_value, StringType())
        self.format_date_udf = udf(self.format_date, StringType())
        self.format_time_udf = udf(self.format_time, StringType())
        self.encrypt_id_udf = udf(self.encrypt_id, StringType())
        self.get_current_time_udf = udf(self.get_current_time, StringType())
        self.get_current_date_udf = udf(self.get_current_date, StringType())
        self.get_date_from_date_with_default_value_udf = udf(self.get_date_from_date_with_default_value, StringType())
        self.get_datetime_from_date_with_default_value_udf = udf(self.get_datetime_from_date_with_default_value, StringType())
        self.get_datetime_from_date_and_time_with_default_value_udf = udf(self.get_datetime_from_date_and_time_with_default_value,StringType())
        self.get_time_from_time_with_default_value_udf = udf(self.get_time_from_time_with_default_value, StringType())





    


###################################################################################################################################
# This function will return the xor hashed value based on the partner name and input value/id
###################################################################################################################################

   
   

    # @classmethod
    def encrypt_id(cls, id):

            if id is None:

                return None
            else: 


                partner_encryption_value_dict = {

                        'partner_1':'partner_1',
                 


                }

                partner_encryption_value = partner_encryption_value_dict.get(cls.partner.upper(), None)

                HASHING_KEY = SHA.new(SEED.encode('utf-8')).digest()

                cipher = XOR.new(HASHING_KEY)

                return b64encode(cipher.encrypt('{}{}'.format(partner_encryption_value, id))).decode()


###################################################################################################################################
# This function will return the current datetime
###################################################################################################################################

   

    @classmethod
    def get_current_time(cls):

       # Set the timezone to Eastern Time (New York)
        eastern_timezone = pytz.timezone("US/Eastern")

        # Get the current time in the Eastern Time Zone
        current_time = datetime.now(eastern_timezone)

        # Define the desired time format
        time_format = "%Y-%m-%d %H:%M:%S"

        # Format and print the current time
        formatted_time = current_time.strftime(time_format)

        return formatted_time


###################################################################################################################################
# This function will return the current datetime
###################################################################################################################################

   

    @classmethod
    def get_current_date(cls):

       # Set the timezone to Eastern Time (New York)
        eastern_timezone = pytz.timezone("US/Eastern")

        # Get the current time in the Eastern Time Zone
        current_time = datetime.now(eastern_timezone)

        # Define the desired time format
        time_format = "%Y-%m-%d"

        # Format and print the current time
        formatted_date = current_time.strftime(time_format)

        return formatted_date
        
    
###################################################################################################################################
# This function will output a pyspark dataframe and combine all the sub files into one file and delete the temprary files
###################################################################################################################################

   

    @classmethod
    def write_pyspark_output_file(cls,payspark_df, output_file_name, output_data_folder_path ):


        work_directory = output_data_folder_path+"tmp_"+output_file_name

        payspark_df = payspark_df.select([
            F.lit(None).cast('string').alias(i.name)
            if isinstance(i.dataType, NullType)
            else i.name
            for i in payspark_df.schema
            ])
        
        payspark_df.coalesce(1).write.format("csv").option("header", True).option("nullValue",None).option("delimiter", "\t").option("quote", "").mode("overwrite").save(work_directory)

        

        command = ["cp", work_directory+"/part-*.csv", output_data_folder_path+output_file_name]
        # Execute the command
        subprocess.run(" ".join(command), shell=True)

        command = ["rm","-r", work_directory]
        # Execute the command
        subprocess.run(" ".join(command), shell=True)








###################################################################################################################################
# This function will upload a pyspark df to mssql table in a specified server/dabase
###################################################################################################################################

   

    @classmethod
    def db_upload(cls,db, db_server,schema,file_name,table_name, file_path):





        date_format_pattern = "yyyy-MM-dd"

        spark = SparkSession.builder.master("spark://master:7077").appName("onefl_pcornet_to_omop_converter").getOrCreate()

        try:
            conf = SparkConf()
            conf.set("spark.driver.extraClassPath", "mssql-jdbc-driver.jar")
            file_df = spark.read.option("inferSchema", "true").load(file_path+file_name+'*',format="csv", sep="\t", inferSchema="true", header="true",  quote= '"')
            
            # string_columns = [f'CAST({col} AS STRING)' for file_df in file_df.columns]
            string_columns = [f'CAST(`{col}` AS STRING) AS `{col}`' for col in file_df.columns]
            result_df = file_df.selectExpr(*string_columns)

            
            jdbc_url = f"jdbc:sqlserver://{db_server};databaseName={db}"
            
            if schema != None:
                properties = {
                    "user": DB_USER,
                    "password": DB_PASS,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true",
                    "createTableColumnTypes": schema}
            else:
                properties = {
                    "user": DB_USER,
                    "password": DB_PASS,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "trustServerCertificate": "true"}


            table_name = f"{table_name}"
            
            result_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

            spark.stop()
        except Exception as e:

            spark.stop()
            

            cls.print_with_style(str(e), 'danger red', 'error')





###################################################################################################################################
###################################################################################################################################

   

    @classmethod
    def get_partner_source_names_list(cls, db_name,db_server ):



        spark = cls.get_spark_session(f'pulling {db_name} partner_source_names_list')


        conf = SparkConf()
        conf.set("spark.driver.extraClassPath", "mssql-jdbc-driver.jar")        
        jdbc_url = f"jdbc:sqlserver://{db_server};databaseName={db_name}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "trustServerCertificate": "true"
            }
            
        query = f"(SELECT DISTINCT(SOURCE) FROM DEMOGRAPHIC ) AS SOURCES_LIST"

        partner_source_names_list_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

        partner_source_names_list_df = partner_source_names_list_df.filter(partner_source_names_list_df['SOURCE'] != 'LNK')


        column_values = partner_source_names_list_df.select('SOURCE').rdd.flatMap(lambda x: x).collect()

        # print(list(column_values))
        return list(column_values)

    



###################################################################################################################################
# This function will a sql table to a csv file using pyspark
###################################################################################################################################

   

    @classmethod
    def pull_data_to_csv(cls,partner_source_name,table_name, db_name,db_server, output_file_path ):


        spark = cls.get_spark_session(f'pulling {db_name}.{table_name}')


        conf = SparkConf()
        conf.set("spark.driver.extraClassPath", "mssql-jdbc-driver.jar")        
        jdbc_url = f"jdbc:sqlserver://{db_server};databaseName={db_name}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "trustServerCertificate": "true"
            }

        try: 

            if table_name.upper() == 'DEMOGRAPHIC'  and 'PROD' in db_name:

                query = f"(SELECT * FROM {table_name} WHERE PATID in (select CAST(PATID AS VARCHAR) from linkage_view_table where found_in_{partner_source_name} = '1')) AS {table_name}"
                           
            elif table_name.upper() == 'HARVEST' :
                query = f"(SELECT * FROM {table_name}) AS {table_name}"


            elif table_name.upper() == 'FACILITYID' :
                query = f"(SELECT distinct(FACILITYID) FROM  ENCOUNTER WHERE source = '{partner_source_name}' group by PATID, FACILITYID ) AS {table_name}"

            else:

                query = f"(SELECT * FROM {table_name} WHERE source = '{partner_source_name}') AS {table_name}"

            # print(query)

            table_df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)


            # table_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

            # table_df.write.csv(output_file_path, header=True, mode="overwrite")

            cls.write_pyspark_output_file(
                            payspark_df = table_df,
                            output_file_name = f"{table_name}.csv",
                            output_data_folder_path= output_file_path)
            spark.stop()
        except Exception as e:

            spark.stop()
            cls.print_failure_message(
                                    folder  = output_file_path,
                                    partner = db_name,
                                    job     = f"pulling {db_name}" )

            cls.print_with_style(str(e), 'danger red', 'error')



###################################################################################################################################
# This function will a sql table to a csv file using pyspark
###################################################################################################################################

   

    @classmethod
    def pull_table_to_csv(cls,table_name, db_name,db_server, output_file_path ):


        spark = cls.get_spark_session(f'pulling {db_name}.{table_name}')


        conf = SparkConf()
        conf.set("spark.driver.extraClassPath", "mssql-jdbc-driver.jar")        
        jdbc_url = f"jdbc:sqlserver://{db_server};databaseName={db_name}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "trustServerCertificate": "true"
            }

        try: 




            table_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

            # table_df.write.csv(output_file_path, header=True, mode="overwrite")

            cls.write_pyspark_output_file(
                            payspark_df = table_df,
                            output_file_name = f"{table_name}.csv",
                            output_data_folder_path= output_file_path)
            spark.stop()
        except Exception as e:

            spark.stop()
            cls.print_failure_message(
                                    folder  = output_file_path,
                                    partner = db_name,
                                    job     = f"pulling {db_name}" )

            cls.print_with_style(str(e), 'danger red','error')



###################################################################################################################################
# This function will a sql table to a csv file using pyspark
###################################################################################################################################

   

    @classmethod
    def split_csv_with_chunks_with_a_max_size(cls,table_name, input_file_path ):




        # print('1')
        input_file = f"{input_file_path}{table_name}.csv"
        # print(input_file)
        matching_files = glob.glob(f"{input_file}.0*")
        # print(matching_files) 
        output_file_path = f"{input_file_path}{table_name}"

        
        if len(matching_files)> 0:
        
                    command = ["rm", f'{input_file}.0*', output_file_path]
                    subprocess.run(" ".join(command), shell=True)
                    # print('2')


        # print('3')

        command = ["mkdir",'-p', f"{output_file_path}"]
        subprocess.run(" ".join(command), shell=True)
        # print('4')
        if  os.listdir(output_file_path):
             command = ["rm","-f" ,f"{output_file_path}/*"]
             subprocess.run(" ".join(command), shell=True)
            #  print('5')




               
        # command = ["mkdir",'-p', f"{output_file_path}"]
        # subprocess.run(" ".join(command), shell=True)

        # try:

        #     command = ["rm","-f" ,f"{output_file_path}/*"]
        #     subprocess.run(" ".join(command), shell=True)
        # except:
        #     None

# output_dir="/path/to/output/directory"
# awk -v m=50000000 -v output_dir="$output_dir" '(NR==1){h=$0;next} (NR%m==2) { close(f); f=sprintf("%s.%0.5d", FILENAME, ++c); print h > f; system("mv " f " " output_dir) } { print > (output_dir "/" f) }' filename.csv

        
        command = ["awk",
                   '-v',f'm={PULL_PARTITION_CHUNK_SIZE}',
                   '-v', f'output_dir="{output_file_path}"' ,
                   "' (NR==1){h=$0;next} (NR%m==2) { close(f); f=sprintf(\"%s.%0.5d\",FILENAME,++c); print h > f } {print > f}'",

                   input_file,

                   ]
        subprocess.run(" ".join(command), shell=True)
        # print('6')

        matching_files = glob.glob(f"{input_file}.0*")

        if len(matching_files)> 0:
        
                    command = ["mv","-f", f'{input_file}.0*', output_file_path]
                    subprocess.run(" ".join(command), shell=True)
                    # print('7')

        if not os.listdir(output_file_path):

                    command = ["cp", f"{input_file}", f"{output_file_path}/{table_name}.csv.00001"]
                    subprocess.run(" ".join(command), shell=True)

                    # print('8')


        command = ["rm", f"{input_file}"]
        subprocess.run(" ".join(command), shell=True)

        # print('9')
        # command = ["cp", work_directory+"/part-*.csv", output_data_folder_path+output_file_name]
        # # Execute the command
        # subprocess.run(" ".join(command), shell=True)

        # command = ["rm","-r", work_directory]
        # # Execute the command
        # subprocess.run(" ".join(command), shell=True)






###################################################################################################################################
# This function will append any additional fields that are not in the Pcornet CDM to the mapper output
###################################################################################################################################

   

    @classmethod
    def append_additional_fields(cls,mapped_df, file_name, formatted_data_folder_path,join_field, spark ):


        # read the formatted file
        formatted_file = spark.read.load(formatted_data_folder_path+file_name,format="csv", sep="\t", inferSchema="true", header="true", quote= '"')
        
        #get the headers from the formatted and the mapped files for comparison
        formatted_file_columns = formatted_file.columns
        mapped_df_columns = mapped_df.columns


        # Identify common headers
        common_headers = set(mapped_df_columns).intersection(formatted_file_columns)
        common_headers_without_the_join_field = [item for item in common_headers if item != join_field]

        # Drop common headers from the second DataFrame
        additional_fields_data = formatted_file.drop(*common_headers_without_the_join_field)
        
        #Renaming the join field to avoid conflect during the join
        join_field_temp_name = join_field+"_new"

        additional_fields_data = additional_fields_data.withColumnRenamed(join_field, join_field_temp_name)
        
        # Join DataFrames based on the "Name" column
        appended_payspark_data_frame = mapped_df.join(additional_fields_data, mapped_df["JOIN_FIELD"]==additional_fields_data[join_field_temp_name])
       
       #Dropping the temporary join fields

        columns_to_drop = ["JOIN_FIELD",join_field_temp_name ]
        appended_payspark_data_frame = appended_payspark_data_frame.drop(*columns_to_drop)

        return appended_payspark_data_frame
        


###################################################################################################################################
# This function will return a pysark dataframe for a givin file path
###################################################################################################################################


    @classmethod
    def spark_read(cls,file_path, spark):

        return spark.read.option("inferSchema", "false").load(file_path,format="csv",   sep="\t", inferSchema="false", header="true",  quote= '"')



###################################################################################################################################
# This function will convert all known time formats to the '%H:%M'format
###################################################################################################################################

    @staticmethod
    def format_time( val):
        """ Convenience method to try all known formats """
        if not val:
            return None
        try:
            parsed_time = dp.parse(val)
        except Exception as exc:
           
            return ""
        return parsed_time.strftime('%H:%M')


###################################################################################################################################
# This function will convert all known date formats to the '%Y-%m-%d format
###################################################################################################################################

   
    @staticmethod
    def format_date( val):
        """ Convenience method to try all known formats """
        if not val or val == 'NULL':
            return None
        try:
            parsed_date = dp.parse(val)
        except:
           
            return None

        return parsed_date.strftime('%Y-%m-%d')



###################################################################################################################################
# 
###################################################################################################################################

   
    @staticmethod
    def get_time_from_time_with_default_value(time):
        
            
            try:
                if time  == None or time == '':
                    return '00:00:00'
            except:
                return time
           



###################################################################################################################################
# 
###################################################################################################################################

   
    @staticmethod
    def get_datetime_from_date_and_time_with_default_value(  date_str, time_str):
        
            
            try:
                time = datetime.strptime(time_str,"%H:%M:%S").time()
            except:
                try:
                    time = datetime.strptime(time_str,"%H:%M").time()
                except:
                    time = datetime.strptime("00:00:00","%H:%M:%S").time()


            try:
                date =datetime.strptime(date_str,"%Y-%m-%d").date()
                datetime_value = str(datetime.combine(date, time))
            except:

                try:

                    datetime_value =str(date.strftime("%Y-%m-%d %H:%M:%S"))
                except:

                    datetime_value = '1900-01-01 00:00:00'


            
  
            return datetime_value
           


###################################################################################################################################
# This function will return the time portion from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_time_from_datetime(  dateTime):
        
            try:
                
                return str(dateTime.time().strftime('%H:%M:%S'))
            except:
                return None


###################################################################################################################################
# This function will  list of all the valid pcornet table names
###################################################################################################################################

   
    @staticmethod
    def get_pcornet_table_names():

        return pcornet_table_names

###################################################################################################################################
# This function will return the date portion from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_date_from_datetime(  dateTime):
        
        try:
            
            return str(dateTime.date().strftime("%Y-%m-%d"))
        except:
            return None


###################################################################################################################################
# This function will return the date portion from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_date_from_date_str_with_default_value(  input_date):
        
        

        try:
            
            return  str(datetime.strptime(input_date, "%Y-%m-%d").date())
        except:
            return '1900-01-01'        



###################################################################################################################################
# This function will return the datetime  from a datetime value
###################################################################################################################################

   
    @staticmethod
    def get_datetime_from_date_str_with_default_value(  input_date):
        
        try:
            
            return str(datetime.strptime(input_date+ " 00:00:00","%Y-%m-%d %H:%M:%S"))
        except:
            return '1900-01-01 00:00:00'        


###################################################################################################################################
# This function will a list of the invalid pcornet table names
###################################################################################################################################

   

    @classmethod
    def get_invalid_omop_table_names(cls, table_names ):

        

        invalid_table_names = []
        if table_names!= None : 
            for table_name in table_names:
                if table_name not in omop_tables_list:
                    invalid_table_names.append(table_name)

        
        return invalid_table_names





###################################################################################################################################
# This function will validate if a giving partenr name is valid or not
###################################################################################################################################

   

    @classmethod
    def valid_partner_name(cls, input_partner_name ):

        partners_list = ["partner_1"]

        if input_partner_name in  partners_list:

            return True
        else:
            return None


###################################################################################################################################
# This function will verify if a date is valie
###################################################################################################################################
    @classmethod
    def is_valid_date(cls, date_string, date_format):
        try:
            # Attempt to parse the string as a date using the specified format
            datetime.strptime(date_string, date_format)
            return True
        except ValueError:
            return False

###################################################################################################################################
# This function will create and return a spark session
###################################################################################################################################

    @classmethod
    def get_spark_session(cls, appName):
        # conf = cls.load_spark_conf('/app/spark_conf.txt')
        # print("##################################################")

        # print(conf)
        # spark_builder = SparkSession.builder
        # for key, value in conf.items():
        #     spark_builder = spark_builder.config(key, value)
        # spark = spark_builder.getOrCreate()


        try:
            spark.stop()
        except:
            None

        spark = SparkSession.builder.master("spark://master:7077").appName(appName).getOrCreate()
        spark.sparkContext.setLogLevel('OFF')

        return spark




###################################################################################################################################
# This function will return the datetime value from a date input
###################################################################################################################################
    @classmethod
    def get_date_from_date_with_default_value(cls, date):

        date_format = "%Y-%m-%d"

        if cls.is_valid_date(date, date_format):

            return date

        else:

            return '1900-01-01'



###################################################################################################################################
# This function will return the datetime value from a date input
###################################################################################################################################
    @classmethod
    def get_datetime_from_date_with_default_value(cls, date):

        if date != None :

            formatted_date_time = date.strftime("%Y-%m-%d %H:%M:%S")

            return formatted_date_time

        else:

            return '1900-01-01 00:00:00'


###################################################################################################################################
    @classmethod
    def print_failure_message(cls, partner, folder, job):


        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime


        while len(message) < 39:
            message = message + ' '


        message = message + "--Source: " + partner

    
        while len(message) < 65:
            message = message + ' '


        message = message + "-- destination: " + folder

    
        while len(message) < 80:
            message = message + ' '
        

        message = message+' --Running '+ job+ ' Failed!!!' 

        while len(message) < 125:
            message = message + ' '

        while len(message) < 145:
            message = message + '.'


        cls.print_with_style(message, 'danger red', 'error')


###################################################################################################################################
# This function will get all the created mappers python script and return a list of thier names
###################################################################################################################################
    @classmethod
    def print_run_status(cls,job_num,total_jobs_count, job,  destination, source):
        


        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime

        while len(message) < 25:
            message = message + ' '

        message = message + 'Job: '+str(job_num)+'/'+str(total_jobs_count)
        while len(message) < 39:
            message = message + ' '

        message = message + "--Source: " + source+ '      '

    
        while len(message) < 100:
            message = message + ' '


        message = message + "-- destination: " +  destination+ '      '

    
        while len(message) < 130:
            message = message + ' '
        

        message = message+' --Running: '+job + '      '

        while len(message) < 115:
            message = message + ' '

        while len(message) < 260:
            message = message + '.'


        cls.print_with_style(message, 'matrix green', "info")


###################################################################################################################################
# This function will get all the created mappers python script and return a list of thier names
###################################################################################################################################
    @classmethod
    def print_mapping_file_status(cls,total_count,current_count, output_file_name,  source_file):
        


        current_utc_datetime = datetime.utcnow()
        new_york_tz = pytz.timezone('America/New_York')

        current_ny_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(new_york_tz)

        formatted_ny_datetime = current_ny_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        message = formatted_ny_datetime

        while len(message) < 25:
            message = message + ' '

        message = message + 'file: '+str(current_count)+'/'+str(total_count)
        while len(message) < 39:
            message = message + ' '

        message = message + "--Mapping: " + output_file_name+ '      '

    
        while len(message) < 100:
            message = message + ' '


        message = message + "-- Source: " +  source_file+ '      '

    
 

        while len(message) < 185:
            message = message + ' '

        while len(message) < 260:
            message = message + '.'


        cls.print_with_style(message, 'dark pink', 'info')





###################################################################################################################################
# This function will retrive the network_id 
###################################################################################################################################
    @classmethod
    def get_network_id(cls):



        # # Create a SparkSession
        # spark = SparkSession.builder \
        #     .appName("getting netword id") \
        #     .getOrCreate()

        # # Get the SparkContext
        # sc = spark.sparkContext

        # # Get the application properties
        # app_properties = sc.getConf().getAll()

        # # Find the network-related properties
        # network_properties = {key: value for key, value in app_properties if key.startswith("spark.driver.host") or key.startswith("spark.driver.port")}


        today_date = datetime.now().strftime('%Y-%m-%d')


        # network_id = network_properties.get("spark.driver.host")

        network_id = today_date #+"_"+network_id

        # spark.stop()

        return network_id



###################################################################################################################################
# This function will store a text in the session log file
###################################################################################################################################
    @classmethod
    def add_to_log(cls, text, log_type):



        # Get the network ID
        network_id = cls.get_network_id()

        # Create the logs directory if it doesn't exist
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        # Create a logger if it doesn't exist
        logger_name = f'logger-{network_id}'
        logger = logging.getLogger(logger_name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)

            # Create a file handler and set its level to INFO
            file_handler = logging.FileHandler(f'logs/{network_id}.log')
            file_handler.setLevel(logging.INFO)

            # Create a formatter and set it to the file handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)

            # Add the file handler to the logger
            logger.addHandler(file_handler)

        # Log the message
        if log_type == 'info':
            logger.info(text)
        elif log_type == 'error':
            logger.error(text)









###################################################################################################################################
# This function will print a text with matrix green color
###################################################################################################################################

    @classmethod
    def print_with_style(cls, text, color, log_type):

        if color == 'matrix green':

            bold_code = '\033[1m'
            color_code = '\033[92m'
            reset_code = '\033[0m'  # Reset to default style
       
        if color == 'danger red':

            bold_code = '\033[1m'
            color_code = RED='\033[0;31m'
            reset_code = '\033[0m'  # Reset to default style

        if color == 'blue':
            bold_code = '\033[1m'
            color_code = BLUE='\033[0;34m'
            reset_code = '\033[0m'  # Reset to default style

        if color == 'pomegranate':
            bold_code = '\033[1m'
            color_code =  POMEGRANATE='\033[0;91m'
            reset_code = '\033[0m'  # Reset to default style

        if color == 'dark pink':
            bold_code = '\033[1m'
            color_code =   DARK_PINK='\033[0;35m'
            reset_code = '\033[0m'  # Reset to default style
   
        # Print bold green text
        print(bold_code + color_code + text + reset_code)
        cls.add_to_log(text, log_type)


###################################################################################################################################
# This function filter the common table accoding to the passed concept id tables
###################################################################################################################################

    @classmethod
    def filter_common_table(cls, common_table, concept_id_mapping, source_concept_id_mapping, concept_code_field_name,concept_code_type_filed_name, common_code_field_name, common_code_type_field_name ):

                valid_source_concept_id_mapping = source_concept_id_mapping.filter(col("invalid_reason").isNull())

                filtered_common_table = common_table.join(concept_id_mapping, (col(concept_code_field_name) == col(common_code_field_name)) & (col(common_code_type_field_name) == col(concept_code_type_filed_name)), how='left').drop(concept_code_field_name).drop(concept_code_type_filed_name)\
                                        .join(valid_source_concept_id_mapping, (col(concept_code_field_name) == col(common_code_field_name)) & (col(common_code_type_field_name) == col(concept_code_type_filed_name)), how='inner')\
                                            

                return filtered_common_table
    

###################################################################################################################################
# This function filter the common table accoding to the passed concept id tables
###################################################################################################################################

    @classmethod

    def save_spark_conf(csl, spark, file_path):
        conf = spark.sparkContext.getConf().getAll()
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        print(conf)
        with open(file_path, 'w') as f:
            for key, value in conf:
                f.write(f"{key}={value}\n") 
            

###################################################################################################################################
# This function filter the common table accoding to the passed concept id tables
###################################################################################################################################

    @classmethod
    def load_spark_conf(cls,file_path):
        with open(file_path, 'r') as f:
            conf = {}
            for line in f:
                key, value = line.strip().split('=', 1)
                conf[key] = value
        return conf  