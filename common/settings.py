POOL_SIZE      = 10
MAX_OVERFLOW   = 5
POOL_RECYCLE   = 3600
BCP_DRIVER     = 'mssql'

# PULL_PARTITION_CHUNK_SIZE =  5000000
PULL_PARTITION_CHUNK_SIZE =  10000000

max_workers = 10

PCORNET_TABLES = ['DEMOGRAPHIC',
                  'ENROLLMENT',
                  'ENCOUNTER',
                  'DIAGNOSIS',
                  'PROCEDURES',
                  'VITAL',
                  'DISPENSING',
                  'LAB_RESULT_CM',
                  'CONDITION',
                  'PRO_CM',
                  'PRESCRIBING',
                  'PCORNET_TRIAL',
                  'DEATH',
                  'DEATH_CAUSE',
                  'MED_ADMIN',
                  'PROVIDER',
                  'OBS_CLIN',
                  'OBS_GEN',
                  'HASH_TOKEN',
                  'LDS_ADDRESS_HISTORY',
                  'HARVEST',
                  'LAB_HISTORY',
                  'IMMUNIZATION',
                  'FACILITYID']

OMOP_TABLES = ["person",
               "visit_occurrence",
               "visit_detail",
               "condition_occurrence",
               "drug_exposure",
               "procedure_occurrence",
               "device_exposure",
               "measurement",
               "observation",
               "death",
               "note",
               "specimen",
               "location",
               "fact_relationship",
               "provider",
               "care_site",
               "cdm_source",
               "cost",
               "payer_plan_period",
               "dose_era",
               "condition_era",
               "note_nlp",
               "drug_era",

               ]


OMOP_VOCABULRY_TABLES = ["CONCEPT",
                        "VOCABULARY",
                        "DOMAIN",
                        "CONCEPT_CLASS",
                        "CONCEPT_RELATIONSHIP",
                        "RELATIONSHIP",
                        "CONCEPT_SYNONYM",
                        "CONCEPT_ANCESTOR",
                        "SOURCE_TO_CONCEPT_MAP"]

SUPPLEMENTARY_TABLES = [
                        "condition_supplementary",
                        "death_cause_supplementary",
                        "death_supplementary",
                        "demographic_supplementary",
                        "diagnosis_supplementary",
                        "dispensing_supplementary",
                        "encounter_supplementary",
                        "immunization_supplementary",
                        "lab_result_cm_supplementary",
                        "med_admin_supplementary",
                        "obs_clin_supplementary",
                        "obs_gen_supplementary",
                        "prescribing_supplementary",
                        "procedures_supplementary",
                        "provider_supplementary",
                        "vital_supplementary",
                    ]


OMOP_ID_MAPPING_TABLE_1 = [

                        "mapping_death_cause_to_omop_id",
                        "mapping_enrollment_to_omop_id",
                        "mapping_facilityid_care_site_id",
                        "mapping_facility_location_to_location_id",
                        "mapping_patid_care_site_id",
                        "mapping_patid_location_id",
                        "mapping_person_id",
                        "mapping_pro_cm_id_to_omop_id",
                        "mapping_providerid_provider_id",
                    ]

OMOP_ID_MAPPING_TABLE_2 = [
                        "mapping_addressid_to_omop_id",
                        "mapping_conditionid_to_omop_id",
                        "mapping_diagnosisid_to_omop_id",
                        "mapping_encounterid_visit_occurrence_id",
                        "mapping_immunizationid_to_omop_id",
                        "mapping_lab_result_cm_id_to_omop_id",
                        "mapping_lab_result_cm_px_to_omop_id",
                        "mapping_lab_result_cm_specimen_to_omop_id",
                        "mapping_medadminid_to_omop_id",
                        "mapping_obsclinid_to_omop_id",
                        "mapping_obsgenid_to_omop_id",
                        "mapping_prescribingid_to_omop_id",
                        "mapping_proceduresid_to_omop_id",
                        "mapping_vitalid_to_omop_id",
                    ]


PARTNER_SOURCE_NAMES_LIST = {

                    "AVH" : 11,
                    "BND" : 12,
                    "CHP" : 13,
                    "CHT" : 14,
                    "EMY" : 14,
                    "FLM" : 16,
                    "JHS" : 17,
                    "LRH" : 18,
                    "NCH" : 19,
                    "ORH" : 20,
                    "TMA" : 21,
                    "TMC" : 22,
                    "UAB" : 23,
                    "AMS" : 24,
                    "UCI" : 25,
                    "UFH" : 25,
                    "UMI" : 27,
                    "USF" : 28,
}


LAB_RESULT_CM_ID_OFFSET              = 1
CONDITIONID_OFFSET                   = 2
DIAGNOSISID_OFFSET                   = 3
MEDADMINID_OFFSET                    = 4
PRESCRIBING_OFFSET                   = 5
DISPENSING_OFFSET                    = 6
OBSCLINID_OFFSET                     = 7
OBSGENID_OFFSET                      = 8
ADDRESSID_OFFSET                     = 9
IMMUNIZATION_OFFSET                  = 10
PRO_CM_ID_OFFSET                     = 11
PROCEDURESID_OFFSET                  = 12
DEATH_CAUSE_OFFSET                   = 13
LAB_RESULT_CM_ID_PX_OFFSET           = 14
LAB_RESULT_CM_ID_SPECIMEN_ID_OFFSET  = 15
ENROLLMENT_OFFSET                    = 16
VITALID_WT_OFFSET                    = 20
VITALID_HT_OFFSET                    = 21
VITALID_BMI_OFFSET                   = 22
VITALID_DIASTOLIC_OFFSET             = 23
VITALID_SYSTOLIC_OFFSET              = 24
VITALID_SMOKING_OFFSET               = 25
VITALID_TOBACCO_OFFSET               = 26
VITALID_TOBACCO_TYPE_OFFSET          = 27
LOCATION_ID_FROM_LDS_ADDRESS_OFFSET  = 28
LOCATION_ID_FROM_ENCOUNTER_OFFSET    = 29


datetime_fields = {

    'person' : ['updated','birth_datetime']
}


schemas = {

"person":"""
                        person_id BIGINT,
                        gender_concept_id INT, 
                        year_of_birth     INT ,
                        month_of_birth INT,
                        day_of_birth INT,
                        birth_datetime VARCHAR(50),
                        race_concept_id INT,
                        ethnicity_concept_id INT,
                        location_id BIGINT,
                        provider_id BIGINT,
                        care_site_id BIGINT,
                        person_source_value VARCHAR(50),
                        gender_source_value VARCHAR(50),
                        gender_source_concept_id INT,
                        race_source_value VARCHAR(50),
                        race_source_concept_id INT,
                        ethnicity_source_value VARCHAR(50),
                        ethnicity_source_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)                       
                        """,
"visit_occurrence":"""
                        visit_occurrence_id BIGINT,
                        person_id BIGINT,
                        visit_concept_id INT,
                        visit_start_date DATE,
                        visit_start_datetime VARCHAR(50),
                        visit_end_date DATE,
                        visit_end_datetime VARCHAR(50),
                        visit_type_concept_id INT,
                        provider_id BIGINT,
                        care_site_id BIGINT,
                        visit_source_value VARCHAR(50),
                        visit_source_concept_id INT,
                        admitting_source_concept_id INT,
                        admitting_source_value VARCHAR(50),
                        discharge_to_concept_id INT,
                        discharge_to_source_value VARCHAR(50),
                        preceding_visit_occurrence_id BIGINT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)

                        """,
"observation_period":"""
                        observation_period_id BIGINT,
                        person_id BIGINT,
                        observation_period_start_date DATE,
                        observation_period_end_date DATE,
                        period_type_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)

                        """,                      
"care_site":"""
                        care_site_id BIGINT,
                        care_site_name VARCHAR(50),
                        place_of_service_concept_id INT,
                        location_id BIGINT,
                        care_site_source_value VARCHAR(50),
                        place_of_service_source_value VARCHAR(250),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)

                        """,
"location":"""
                        location_id BIGINT,
                        address_1 VARCHAR(50),
                        address_2 VARCHAR(50),
                        city VARCHAR(50),
                        state VARCHAR(2),
                        zip VARCHAR(9),
                        county VARCHAR(100),
                        location_source_value VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,

"condition_occurrence":"""
                        condition_occurrence_id BIGINT,
                        person_id BIGINT,
                        condition_concept_id INT,
                        condition_start_date DATE,
                        condition_start_datetime VARCHAR(50),
                        condition_end_date DATE,
                        condition_end_datetime VARCHAR(50),
                        condition_type_concept_id INT,
                        condition_status_concept_id INT,
                        stop_reason VARCHAR(50),
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        condition_source_value VARCHAR(50),
                        condition_source_concept_id INT,
                        condition_status_source_value VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,
"death":"""
                        person_id BIGINT,
                        death_date DATE,
                        death_datetime VARCHAR(50),
                        death_type_concept_id INT,
                        cause_concept_id INT,
                        cause_source_value VARCHAR(50),
                        cause_source_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,                        
"measurement":"""
                        measurement_id BIGINT,
                        person_id BIGINT,
                        measurement_concept_id INT,
                        measurement_date DATE,
                        measurement_datetime VARCHAR(50),
                        measurement_time VARCHAR(50),
                        measurement_type_concept_id INT,
                        operator_concept_id INT,
                        value_as_number VARCHAR(50),
                        value_as_concept_id INT,
                        unit_concept_id INT,
                        range_low VARCHAR(50),
                        range_high VARCHAR(50),
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        measurement_source_value VARCHAR(50),
                        measurement_source_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,

"observation":"""
                        observation_id BIGINT,
                        person_id BIGINT,
                        observation_concept_id INT,
                        observation_date DATE,
                        observation_datetime VARCHAR(50),
                        observation_type_concept_id INT,
                        value_as_number FLOAT,
                        value_as_string VARCHAR(250),
                        value_as_concept_id INT,
                        qualifier_concept_id INT,
                        unit_concept_id INT,
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        observation_source_value VARCHAR(50),
                        observation_source_concept_id INT,
                        unit_source_value VARCHAR(50),
                        qualifier_source_value VARCHAR(50),
                        value_source_concept_id INT,
                        value_source_value VARCHAR(50),
                        questionnaire_response_id BIGINT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,

"procedure_occurrence":"""

                        procedure_occurrence_id BIGINT,
                        person_id BIGINT,
                        procedure_concept_id INT,
                        procedure_date DATE,
                        procedure_datetime VARCHAR(50),
                        procedure_type_concept_id INT,
                        modifier_concept_id INT,
                        quantity FLOAT,
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        procedure_source_value VARCHAR(50),
                        procedure_source_concept_id INT,
                        modifier_source_value VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,
"provider":"""
                        provider_id BIGINT,
                        npi VARCHAR(20),
                        specialty_concept_id INT,
                        gender_concept_id INT,
                        provider_source_value  VARCHAR(50),
                        specialty_source_value  VARCHAR(50),
                        specialty_source_concept_id INT,
                        gender_source_value  VARCHAR(50),
                        gender_source_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,

"specimen":"""
                        specimen_id BIGINT,
                        person_id BIGINT,
                        specimen_concept_id INT,
                        specimen_type_concept_id INT,
                        specimen_date DATE,
                        specimen_datetime VARCHAR(50),
                        quantity FLOAT,
                        unit_concept_id INT,
                        anatomic_site_concept_id INT,
                        disease_status_concept_id INT,
                        specimen_source_id VARCHAR(50),
                        specimen_source_value VARCHAR(50),
                        unit_source_value INT,
                        anatomic_site_source_value VARCHAR(50),
                        disease_status_source_value VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,
"drug_exposure":"""
                        drug_exposure_id BIGINT,
                        person_id BIGINT,
                        drug_concept_id INT,
                        drug_exposure_start_date DATE,
                        drug_exposure_start_datetime VARCHAR(50),
                        drug_exposure_end_date DATE,
                        drug_exposure_end_datetime VARCHAR(50),
                        verbatim_end_date DATE,
                        drug_type_concept_id INT,
                        stop_reason VARCHAR(50),
                        refills FLOAT,
                        quantity FLOAT,
                        days_supply FLOAT,
                        sig VARCHAR(350),
                        route_concept_id INT,
                        lot_number  VARCHAR(50),
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        drug_source_value VARCHAR(50),
                        drug_source_concept_id INT,
                        route_source_value VARCHAR(50),
                        dose_unit_source_value VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,                        
"visit_detail":"""
                        visit_detail_id BIGINT,
                        person_id BIGINT,
                        visit_detail_concept_id INT,
                        visit_detail_start_date DATE,
                        visit_detail_start_datetime VARCHAR(50),
                        visit_detail_end_date DATE,
                        visit_detail_end_datetime VARCHAR(50),
                        visit_detail_type_concept_id INT,
                        provider_id BIGINT,
                        care_site_id BIGINT,
                        visit_detail_source_value VARCHAR(50),
                        visit_detail_source_concept_id INT,
                        admitting_source_concept_id INT,
                        admitting_source_value VARCHAR(50),
                        discharge_to_concept_id INT,
                        discharge_to_source_value VARCHAR(50),
                        preceding_visit_detail_id BIGINT,
                        visit_occurrence_id BIGINT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)

                        """,

"device_exposure":"""
                        device_exposure_id BIGINT,
                        person_id BIGINT,
                        device_concept_id INT,
                        device_exposure_start_date DATE,
                        device_exposure_start_datetime VARCHAR(50),
                        device_exposure_end_date DATE,
                        device_exposure_end_datetime VARCHAR(50),
                        device_type_concept_id INT,
                        unique_device_id INT,
                        quantity FLOAT,
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        device_source_value VARCHAR(50),
                        device_source_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,

"note":"""
                        note_id BIGINT,
                        person_id BIGINT,
                        note_date DATE,
                        note_datetime VARCHAR(50),
                        note_type_concept_id INT,
                        note_class_concept_id INT,
                        note_title VARCHAR(250),
                        encoding_concept_id INT,
                        language_concept_id INT,
                        provider_id BIGINT,
                        visit_occurrence_id BIGINT,
                        visit_detail_id BIGINT,
                        note_source_value  VARCHAR(50),
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                      """,
"fact_relationship":"""

                        domain_concept_id_1 INT,
                        fact_id_1 INT,
                        domain_concept_id_2 INT,
                        fact_id_2 INT,
                        relationship_concept_id INT,
                        updated VARCHAR(50),
                        source VARCHAR(10),
                        mapped_from VARCHAR(50)
                        """,


"lab_result_cm_supplementary":"""

                        Lab_result_cm_supplementary_id BIGINT,
                        person_id BIGINT
  
                        """,


"demographic_supplementary":"""

                        person_id BIGINT

                        """,

"encounter_supplementary":"""

                        visit_occurrence_id BIGINT,
                        person_id BIGINT

                        """,


"diagnosis_supplementary":"""

                        diagnosis_supplentary_id BIGINT,
                        person_id BIGINT

                        """,

"cdm_source":"""

                         cdm_source_name VARCHAR(255),
                         cdm_source_abbreviation VARCHAR(25),
                         cdm_holder VARCHAR(255),
                         source_documentation_reference VARCHAR(255),
                         cdm_etl_reference VARCHAR(255),
                         source_release_date DATE,
                         cdm_release_date DATE,
                         cdm_version VARCHAR(10),
                         vocabulary_version VARCHAR(255)
                        """,

"VOCABULARY":"""

     vocabulary_id varchar(20),
     vocabulary_name varchar(255),
     vocabulary_reference varchar(255),
     vocabulary_version varchar(255),
     vocabulary_concept_id INT


                        """,
"CONCEPT":"""
        concept_id int,
        concept_name varchar(255),
        domain_id varchar(20),
        vocabulary_id varchar(20),
        concept_class_id varchar(20),
        standard_concept varchar(1),
        concept_code varchar(50),
        valid_start_date date,
        valid_end_date date,
        invalid_reason varchar(1)
                         

                        """,
 "DOMAIN":"""
        domain_id varchar(20),
        domain_name varchar(255),
        domain_concept_id int

                        """,
 "CONCEPT_CLASS":"""

     concept_class_id   varchar(20),
     concept_class_name  varchar(255),                 
     concept_class_concept_id int
                        """,
"CONCEPT_RELATIONSHIP":"""

       concept_id_1 int,
       concept_id_2  int ,
       relationship_id varchar(20),
        valid_start_date date,
        valid_end_date date, 
        invalid_reason  varchar(1)    

                        """,
 "RELATIONSHIP":"""
        relationship_id varchar(20),
        relationship_name varchar(255),
        is_hierarchical varchar(1),
        defines_ancestry varchar(1),
        reverse_relationship_id varchar(20),
        relationship_concept_id int

                         

                        """,
"CONCEPT_SYNONYM":"""

          concept_id int,
          concept_synonym_name   varchar(1000),
          language_concept_id int           

                        """,
 "CONCEPT_ANCESTOR":"""

       ancestor_concept_id int,
       descendant_concept_id int,
       min_levels_of_separation int,
       max_levels_of_separation int

                        """,
 "SOURCE_TO_CONCEPT_MAP":"""

                source_code   varchar(50),
                source_concept_id int,
                source_vocabulary_id varchar(20),       
                source_code_description varchar(255),
                target_concept_id int,
                target_vocabulary_id varchar(20),
                valid_start_date date,
                valid_end_date date,
                invalid_reason varchar(1)
                        """,


"template":"""

                         BIGINT,
                         person_id BIGINT,
                         VARCHAR(200),
                         VARCHAR(200),
                         VARCHAR(200),
                         VARCHAR(200),
                         VARCHAR(200),
                         VARCHAR(200),
                         updated VARCHAR(200),
                         source VARCHAR(10),
                         mapped_from VARCHAR(200)
                        """,

"condition_supplementary":"""

                         condition_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"immunization_supplementary":"""

                         immunization_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"prescribing_supplementary":"""

                        prescribing_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"med_admin_supplementary":"""

                         med_admin_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"dispensing_supplementary":"""

                         dispensing_supplementary_id BIGINT,
                         person_id BIGINT

                        """,


"vital_supplementary":"""

                         vital_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"procedures_supplementary":"""

                         procedures_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"death_supplementary":"""

                   
                         person_id BIGINT

                        """,


"death_cause_supplementary":"""

                         death_cause_supplementary_id BIGINT,
                         person_id BIGINT

                        """,

"provider_supplementary":"""

                         provider_id BIGINT

                        """,
"obs_clin_supplementary":"""

                         obs_clin_supplementary_id BIGINT,
                         person_id BIGINT


                        """,

"obs_gen_supplementary":"""

                         obs_gen_supplementary_id BIGINT,
                         person_id BIGINT


                        """,
}








