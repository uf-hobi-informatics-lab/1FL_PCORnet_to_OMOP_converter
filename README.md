# Working with the Cluster
Developmental implementation of the PySpark Cluster for the OneFL Converter. This cluster should be used for testing scripts. Please do not push changes to the cluster files (`Dockerfile`, `requirements.txt`, `run_cluster.sh`, `docker-compose.yml`).

Author: Jason Glover <br>
Contact: jasonglover@ufl.edu <br>
Last Updated: 8/3/2023 <br>

## Running the Cluster
1. Clone the repo onto your machine.
2. In the parent directory of the repo, update the directory permissions to allow for scripts to write output by running the following command:

        sudo chmod 777 docker_container
3. Ensure that you are on the `dev` branch.

        git status
        
4. In the repo directory, paste the following command into your command line to build the image:

        docker build -t cluster-image .
> **Note**: If planning to run this cluster on the IQ Server, then you may skip step 4 as the image is already installed there.
5. (For running on the IQ Server) User must rename the repo directory and modify two files to avoid conflicts with other devs. Update the repo directory name by going up a level from the repo's root and running `mv docker_container/ [NEW_NAME]/`. Next, use a text editor to open the `docker-compose.yml` file. In the last line of the file, update `pyNet` to a unique name, such as your gatorlink ID. Save the changes, then open `run_cluster.sh`. In the `docker run` command, modify the `--network` flag to reflect the updated network name that you set in `docker-compose.yml`.
6. Once the image has been built, you can paste the following command into your command line to run the cluster, convert the sample file, and then shut down the cluster:

        sh run_cluster.sh verify_cluster.py
7. Alternatively, you can run the individual commands from the `run_cluster.sh` script in the terminal yourself to boot the cluster. There are comments in the file explaining what each command does. First, call the `docker compose up` command to boot the cluster. Once finished booting, call the `docker run` command. When passing the `docker run` command through the command line, replace the `$1` at the end of the command with the name of the script you want to test (e.g. `verify_cluster.py`). If your file takes command line arguments, replace `$args` with the neccesary arguments. If your file doesn't take command line arguments, remove `$args`. Finally, shut down the cluster by calling the `docker compose down`.

> **Note**: When passing the commands yourself, ensure that you shut down the cluster before trying to boot another instance of the cluster. In its current state, the cluster can only be instantiated once at a time on a machine.

> **Note**: As of 5/16/2023, the `run_cluster.sh` script has been updated to accept command line arguments for the python script being passed in. Simply add the arguments to the end of the `sh run_cluster.sh` command. Ex: `sh run_cluster.sh foo.py arg1 arg2 arg3`

8. The test script, `verify_cluster.py` has run correctly if the ouput contains the message `Cluster is running as expected.`

## Expected Output

The `demo.py` script has run correctly if a directory called `output_demographic` has been created and contains a variable number of .csv files which are named like `part-00000-95819a38-1814-4600-b64c-defddd66352f-c000.csv`. The reason a variable number is created is that it is dependent upon the cluster and partition size. Each file name will be incremental.

For example:
- The first file will have `part-0000-*.csv`
- The second will be `part-0001-*.csv`

## Configuring the Cluster

### Number of Nodes

The number of worker nodes to create is set when booting the cluster with the `docker-compose up -d --scale worker=5` command. Replace the `5` with your desired number of worker nodes.

> **Note**: The number of worker nodes you can have on the cluster is bound by your machine's resources. If you have an 8-core CPU on your machine and each worker is given 1 core, then your cluster will crash when trying to build more than 7 worker nodes (since the master node also takes a core).

### Worker Hardware Allocation

You can configure the amount of RAM and number of CPU cores each worker node has in the `docker-compose.yml` file. In the `worker` section, the flags `SPARK_WORKER_MEMORY` and `SPARK_WORKER_CORES` allocate the RAM and number of CPU cores, respectively, for a single node.

In order to take advantage of the full amount of RAM each node has been allocated, the `docker run` command in the `run_cluster.sh` file must be modified. Modify the following flag in the `docker run` command to have the same value as given to the `SPARK_WORKER_MEMORY` parameter in the `docker-compose.yml`:

        --conf spark.executor.memory=1g

## Developing for the Cluster

For any operations involving the Spark environment, the script must include a call to the `SparkSession.builder`, to create the Spark session. Here is where the master node location will be set. In order to submit the script to the cluster, you must pass `spark://master:7077` to the master parameter. Examples of this can be found in `demo.py` and `ad_merger_test.py`.

## Extra Notes

1. To monitor the status of your cluster, simply paste the following URL into your browser and refresh the page as necessary:

        http://localhost:8080/ 
> **Note**: This does not work for the IQ Server.


2. If running the `demo.py` file multiple times, run the following command in between runs:

        rm -r output_demographic/

    Otherwise, the script will throw an error saying the directory already exists.

3. If attempting to run `ad_merge_test.py`, then the user will need to pull the necessary dependencies of the ad-merger (the directories in the ad_merger repo) into their directory. 

4. In its current state, this cluster can only view the files within the repo directory. Attempting to read files that are not in the repo or a subdirectory will throw a File not Found error.

5. When developing programs to run against the cluster, keep the following in mind. Since the python script being submitted to the cluster is run in a Docker container, hardcoded directories on your host machine will throw an error. The `/app` directory in the container is mounted to the repo on the host machine, this is where output or input should be written to. For example: I want to write the output `result` from the cluster. My repo is located at `/Users/jason/code/cluster` on my host machine. Thus, `/app` is mounted to `/Users/jason/code/cluster`. In my python script, I would write the output to `/app/result`, not `/Users/jason/code/cluster/result`.



# Running the formatter and the mapper scripts


## Before running your scripts:

1. Rename your /data_example subfolder to /data

        mv  data_example  data


2. Rename secrets_example.py to secrets.py

        cp common/secrets_example.py  common/secrets.py


3. update secrets.py by populating the following:

        DB_USER          = 
        DB_PASS          = 


4. Change to permission all the folders and the files in the repository to 777 by simply go to the upper folder and run the following command:

        chmod -R 777 .


## Parameter:

                -j (job)  : all, pull_data, pull_vocab, update_mapping, map, upload_data, upload_mapping, and upload_vocab

                -sdb (source_db_name): eg. CHAR_UAB, CHAR_USF, etc

                -ss (source_db_server): eg. AHC-ONEFL-DB01.ahc.ufl.edu or AHC-ONEFL-DB03.ahc.ufl.edu

                -vdb (vocabulary_db_name): eg. AOU_VOCAB_20230504

                -vs (vocabulary_db_server): eg. AHC-ONEFL-DB01.ahc.ufl.edu or AHC-ONEFL-DB03.ahc.ufl.edu

                -ddb (destination_db_name): eg. ONEFL_OMOP_STAGING

                -ds (destination_db_server): eg. AHC-ONEFL-DB01.ahc.ufl.edu or AHC-ONEFL-DB03.ahc.ufl.edu

                -pt (pcornet_table_name): all, DEMOGRPAHIC, ENROLLMENT, LAB_RESULT_CM, ENCOUNTER, DIAGNOSIS, PROCEDURES, VITAL, DISPENSING, CONDITION, PRO_CM, PRESCRIBING, PCORNET_TRIAL, DEATH, DEATH_CAUSE, MED_ADMIN, PROVIDER, OBS_CLIN, OBS_GEN, HASH_TOKEN, LDS_ADDRESS_HISTORY, HARVEST, LAB_HISTORY, IMMUNIZATION

                -mu (mapping_updaters):  all, 00.person_id, 01.facility_location_location_id, 02.facilityid_care_site_id, 03.patid_location_id,
                    04.patid_care_site_id, 05.addressid_to_omop_id, 06.medadminid_to_omop_id, 07.diagnosisid_to_omop_id,  08.dispensingid_to_omop_id,
                    09.immunizationid_to_omop_id,  10.prescribingid_to_omop_id, 11.pro_cm_id_to_omop_id, 12.lab_result_cm_id_to_omop_id,
                    13.obsgenid_to_omop_id, 14.obsclinid_to_omop_id, 15.procesuresid_to_omop_id, 16.providerid_to_provider_id, 17.vitalid_to_omop_id,
                    18.conditionid_to_omop_id, 19.encounterid_to_visit_occurrence_id, 20.condition_source_concept_id, 21.condition_concept_id, 
                    22.drug_source_concept_id, 23.drug_concept_id, 24.measurement_source_concept_id, 25.measurement_concept_id,
                    6.observation_source_concept_id, 27.observation_concept_id, 28.procedure_source_concept_id,  29.procedure_concept_id,
                    30.provider_specialty_concept_id, 31.provider_specialty_source_concept_id

                -mt (mapping_tables): all, 00.person, 01.provider, 02.location, 02.visit_occurrence,  03.care_site, 05.drug_exposure, 
                    06.lab_result_cm_to_common, 07.procedures_to_common, 08.condition_to_common, 09.diagnosis_to_common, 10.obs_clin_to_common,
                    11.obs_clin_supplementary, 11.obs_gen_to_common, 11.pro_cm_common_mapper_retired.py 12.obs_gen_supplementary,
                    12.vital_to_common, 13.death_cause_to_common, 13.vital_to_common_mapper_retired.py 14.observation_period, 14.specimen, 15.death, 
                    16.from_common_to_condition_occurrence, 17.from_common_to_procedure_occurrence, 18.from_common_to_measurement, 
                    19.from_common_to_observation, 20.device_exposure, 21.visit_detail, 22.note, 23.fact_relationship, 24.lab_result_cm_supplementary,
                    25.demographic_supplementary, 26.encounter_supplementary, 27.diagnosis_supplementary, 28.procedures_supplementary,
                    29.vital_supplementary, 30.dispensing_supplementary, 31.med_admin_supplementary, 32.prescribing_supplementary, 
                    33.immunization_supplementary, 34.condition_supplementary, 36.death_supplementary, 37.death_cause_supplementary, 
                    38.provider_supplementary, 39.cdm_source

                -ut (upload_tables): all, person, visit_occurrence, visit_detail, condition_occurrence, drug_exposure, procedure_occurrence,
                    device_exposure, measurement, observation, death,note, specimen, location, fact_relationship, provider, care_site,
                    cdm_source, condition_supplementary, death_cause_supplementary, death_supplementary, demographic_supplementary, 
                    diagnosis_supplementary, dispensing_supplementary, encounter_supplementary, immunization_supplementary,
                    lab_result_cm_supplementary,med_admin_supplementary, obs_clin_supplementary, obs_gen_supplementary,
                    prescribing_supplementary, procedures_supplementary, provider_supplementary, vital_supplementary,




## To run the pull_data job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j pull_data -sdb [db_name]   -ss [server_name]    -pt all or [table_name_1, table_name_2, .....] 

                eg. Example 1:    Load all the pcornet tables from CHAR_UFH
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j pull_data        -sdb  CHAR_UFH     -ss  AHC-ONEFL-DB01.ahc.ufl.edu  -pt  all  

                    Example 2:    Load the demographic and the enrollment tables from CHAR_UFH
                        cluster   run  -a    --  onefl_pcornet_to_omop_converter.py     -j pull_data     -sdb  CHAR_UFH   -pt  demographic    enrollment   -ss  AHC-ONEFL-DB01.ahc.ufl.edu


## To run the pull_vocab job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j pull_vocab -vdb [db_name]   -vs [server_name] 

                eg. Example 1:    Load  the OMOP vocabulary tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j pull_vocab        -vdb  AOU_VOCAB_20230504     -vs   AHC-ONEFL-DB01.ahc.ufl.edu    



## To run the update_mapping job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j update_mapping -sdb [db_name]      -mu all or [mapping_updater_1, mapping_updater_2, .....] 

                eg. Example 1:    Update condition_concept_id and person_id mappings
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j   update_mapping       -sdb  CHAR_UAB             -mu   condition_concept_id            person_id  

                    Example 2:    Update all mappings
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py             update_mapping       -sdb  CHAR_UAB             -mu   all



## To run the map job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j map -sdb [db_name]    -mt all or [table_name_1, table_name_2, .....] 

                eg. Example 1:    Map measurement and  person tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j  map       -sdb CHAR_UAB             -mt measurement   person

                    Example 2:     Map all tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j  map       -sdb CHAR_UAB             -mt all


## To run the upload_data job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j upload_data   -sdb [db_name]      -ddb [db_name]  -ds [db_server]  -ut [table_name_1, table_name_2, .....] 

                eg. Example 1:    Upload measurement and  person tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j     upload_data     -ut     measurement    person  -sdb   CHAR_UAB      -ddb    ONFL_OMOP_STAGING     -ds    AHC-ONEFL-DB01.ahc.ufl.edu  

                    Example 2:    Upload all tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j     upload_data       -ut     all   -sdb   CHAR_UAB  -ddb    ONFL_OMOP_STAGING     -ds    AHC-ONEFL-DB01.ahc.ufl.edu 



## To run the upload_mapping job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j upload_mapping   -sdb [db_name]      -ddb [db_name]  -ds [db_server]  
 

                eg. Example 1:    Upload all the mapping tables
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j upload_mapping        -sdb  CHAR_UFH  -ddb    ONFL_OMOP_STAGING      -ds  AHC-ONEFL-DB01.ahc.ufl.edu   

                  


## To run the upload_vocab job:

        cluster run -a -- onefl_pcornet_to_omop_converter.py -j upload_vocab        -ddb [db_name]  -ds [db_server]  

                eg. Example 1:    Upload all the vocabulary data
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j upload_vocab          -ddb    ONFL_OMOP_STAGING      -ds  AHC-ONEFL-DB01.ahc.ufl.edu 



## To run the all the jobs at once:

        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py   -j  all    -mt   all       -pt  all   -mu   all       -pt   all     -sdb [db_name]    -ss [db_server]      -vdb [db_name]   -vs [db_server]   -ddb [db_name]   -ds [db_server] 

                eg. Example 1:    Convert CHAR_UFH to OMOP and load the results to ONEFL_OMOP_STAGING
                        cluster   run   -a     --  onefl_pcornet_to_omop_converter.py        -j  all    -mt   all       -pt  all        -mu   all       -pt   all     -sdb CHAR_UAB   -ss AHC-ONEFL-DB01.ahc.ufl.edu       -vdb AOU_VOCAB_20230504   -vs AHC-ONEFL-DB01.ahc.ufl.edu   -ddb ONEFL_OMOP_STAGING   -ds AHC-ONEFL-DB01.ahc.ufl.edu            

        