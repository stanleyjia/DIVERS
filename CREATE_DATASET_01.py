#!/usr/bin/env python
# coding: utf-8

# # Setup

# ## Define save_df function

# In[1]:


get_ipython().run_line_magic('pip', 'install tables')


# In[125]:


# SETUP
import os
import subprocess
import numpy as np
import pandas as pd


# In[126]:


# define save_df that saves into csv
def save_df(df, name):
    save_df_hdf(df, name)
    # This code saves your dataframe into a csv file in a "data" folder in Google Bucket

#    # Replace df with THE NAME OF YOUR DATAFRAME
#     my_dataframe = df   

#     # Replace 'test.csv' with THE NAME of the file you're going to store in the bucket (don't delete the quotation marks)
#     destination_filename = f'{name}.csv'

#     ########################################################################
#     ##
#     ################# DON'T CHANGE FROM HERE ###############################
#     ##
#     ########################################################################

#     # save dataframe in a csv file in the same workspace as the notebook
#     my_dataframe.to_csv(destination_filename, index=False)

#     # get the bucket name
#     my_bucket = os.getenv('WORKSPACE_BUCKET')

#     # copy csv file to the bucket
#     os.system(f"gsutil cp './{destination_filename}' '{my_bucket}/data/'")
#     print(f'[INFO] {destination_filename} is successfully uploaded in your bucket.')


# In[127]:


# define save_df that saves into hdf
def save_df_hdf(df, name):

    # Replace df with THE NAME OF YOUR DATAFRAME
    my_dataframe = df   

    # Replace 'test.csv' with THE NAME of the file you're going to store in the bucket (don't delete the quotation marks)
    destination_filename = f'{name}.h5'

    ########################################################################
    ##
    ################# DON'T CHANGE FROM HERE ###############################
    ##
    ########################################################################

    # save dataframe in a csv file in the same workspace as the notebook
#     my_dataframe.to_csv(destination_filename, index=False)
    my_dataframe.to_hdf(destination_filename, key='stage', mode='w')

    # get the bucket name
    my_bucket = os.getenv('WORKSPACE_BUCKET')

    # copy csv file to the bucket
    os.system(f"gsutil cp './{destination_filename}' '{my_bucket}/data/'")
    print(f'[INFO] {destination_filename} is successfully uploaded in your bucket.')


# In[128]:


# Define import_df function
def import_df(filename):
    # Replace 'test.csv' with THE NAME of the file you're going to download from the bucket (don't delete the quotation marks)
    name_of_file_in_bucket = f'{filename}.h5'
    
    # get the bucket name
    my_bucket = os.getenv('WORKSPACE_BUCKET')

    # copy csv file from the bucket to the current working space
    os.system(f"gsutil cp '{my_bucket}/data/{name_of_file_in_bucket}' .")

    print(f'[INFO] {name_of_file_in_bucket} is successfully downloaded into your working space')
    # save dataframe in a csv file in the same workspace as the notebook
    temp_df = pd.read_hdf(name_of_file_in_bucket, dtype=object)
    globals()[filename] = temp_df


# In[129]:


# create download links
import base64
import pandas as pd
from IPython.display import HTML

def create_download_link( df, title = "Download CSV file", filename = "data.csv"):
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload,title=title,filename=filename)
    return HTML(html)


# ## Define functions

# In[130]:


# Define change_df_types_for_id_suffix function
def change_df_types_for_id_suffix(dfs):
    date_suffix = 'date'
    id_suffix = 'id'
    for df in dfs:
        id_columns = [col for col in df.columns if col.endswith(id_suffix)]
        df[id_columns] =  df[id_columns].astype(str)
        


# ## Define Concept Sets and Cohorts

# In[131]:


# Define drug concept sets
# CVX Concept Set
hepB_CVX_drug_concepts_sql = """
    drug_concept_id IN (
                40213317, 40213303, 706105, 40213286, 40213293, 40213308, 40213305, 40213304, 40213307, 40213306
            ) 
            /*OR  drug_source_concept_id IN (
                40213317, 40213303, 706105, 40213286, 40213293, 40213308, 40213305, 40213304, 40213307, 40213306
            )*/
"""
influenza_CVX_drug_concepts_sql = """
    drug_concept_id IN (
                40213157, 40213189, 40213156, 40213188, 40213158, 40213153, 40213152, 40213155, 40213187, 40213154, 40213186, 40213327, 40213141, 40213140, 40213143, 40213142, 40213149, 40213148, 40213151, 40213150, 40213145, 40213144, 40213147, 40213146
            ) 
            /*OR  drug_source_concept_id IN (
                40213157, 40213189, 40213156, 40213188, 40213158, 40213153, 40213152, 40213155, 40213187, 40213154, 40213186, 40213327, 40213141, 40213140, 40213143, 40213142, 40213149, 40213148, 40213151, 40213150, 40213145, 40213144, 40213147, 40213146
            )*/
"""
hpv_CVX_drug_concepts_sql ="""
    drug_concept_id IN (
                40213319, 40213321, 40213320, 40213322
            ) 
            /*OR  drug_source_concept_id IN (
                40213319, 40213321, 40213320, 40213322
            )*/
"""
pneumococcal_CVX_drug_concepts_sql ="""
    drug_concept_id IN (
                40213201, 40213202, 40213199, 40213198
            ) 
            /*OR  drug_source_concept_id IN (
                40213201, 40213202, 40213199, 40213198
            )*/
"""
# RxNorm Concept Sets
hepB_RxNorm_ingredient_concepts_sql = """
    (528323)
"""

influenza_RxNorm_ingredient_concepts_sql = """
    (46275993,46275996,46275999,42873918,42873956,43531944,43531942,40164828,42873961,
    40225028,40225012,40224997,40166605,40240922,45776076,40220901,40225031,40225038,40164833)
"""

hpv_RxNorm_ingredient_concepts_sql = """
    (529076,   529112,   529114, 45892474, 45892475, 45892476,
       45892477, 45892478,   529116)
"""
pneumococcal_RxNorm_ingredient_concepts_sql = """
    (528986,   528988,   528990,   529042,   529044,   529046,
         529072, 40163661, 40163668, 40163670, 40163672, 40163674,
       40163676, 40163678, 40163680, 40163682, 40163684, 40163686,
       40163688, 40163690, 40163692, 40163694, 40163696, 40163698,
       40163700, 40163702, 40163704, 40163706, 40163708, 40163710,
       40174004, 40174011, 40174015, 40174018, 40174020, 40174023)
"""

pneumococcal_RxNorm_PCV13_concepts_sql = """
    (528986,528988,528990,529042,529044,529046,529072,40174004,40174011,40174015,40174018, 40174020, 40174023)
"""
pneumococcal_RxNorm_PPSV23_concepts_sql = """
    (40163661,40163668,40163670,40163672,40163674,40163676,40163678,40163680,
    40163682,40163684,40163686,40163688,40163690,40163692,40163694,40163696,
    40163698,40163700,40163702,40163704, 40163706,40163708,40163710)
"""


# In[132]:


# Define get_drugs_from_ingredients function
def get_drugs_from_ingredients(ingredient_concepts):
    query_sql = f"""
        with descendants as (
            select descendant_concept_id as drug_concept_id from {os.environ["WORKSPACE_CDR"]}.concept_ancestor
            where max_levels_of_separation!=0 and ancestor_concept_id in {ingredient_concepts}
        ),
        drugs_from_ingredients as (
            select a.*, b.concept_name as drug_name, b.concept_class_id
            from descendants a 
            left join (
                select * from {os.environ["WORKSPACE_CDR"]}.concept
            ) as b
            on a.drug_concept_id = b.concept_id
            where concept_class_id!='Ingredient'
        )
        select distinct drug_concept_id from drugs_from_ingredients order by drug_concept_id asc
    """
    query_df = pd.read_gbq(query_sql, dialect="standard")
    return list(query_df['drug_concept_id'].astype(str).values)


# In[133]:


# Define list_to_drug_concept_sql_str function
def drug_concept_list_to_sql_str(x):
    return f" drug_concept_id IN ({','.join(x)})"


# In[134]:


# Define cohort_from_df function
def cohort_from_df(df):
    return list(influenza_df['person_id'].astype(str).values)


# In[135]:


# Define list_to_sql_str function
def list_to_sql_str(x):
    return f"({','.join(x)})"


# In[136]:


get_drugs_from_ingredients(hepB_RxNorm_ingredient_concepts_sql)


# In[137]:


# Get drug concept ids from RxNorm ingredients
hepB_RxNorm_drugs_sql = drug_concept_list_to_sql_str(get_drugs_from_ingredients(hepB_RxNorm_ingredient_concepts_sql))
influenza_RxNorm_drugs_sql = drug_concept_list_to_sql_str(get_drugs_from_ingredients(influenza_RxNorm_ingredient_concepts_sql))
hpv_RxNorm_drugs_sql = drug_concept_list_to_sql_str(get_drugs_from_ingredients(hpv_RxNorm_ingredient_concepts_sql))
pneumococcal_RxNorm_drugs_sql = drug_concept_list_to_sql_str(get_drugs_from_ingredients(pneumococcal_RxNorm_ingredient_concepts_sql))
19020053


# In[14]:


'19020053' in hepB_RxNorm_drugs_sql


# In[15]:


query_sql = f"""
        with descendants as (
            select descendant_concept_id as drug_concept_id from {os.environ["WORKSPACE_CDR"]}.concept_ancestor
            where max_levels_of_separation!=0 and ancestor_concept_id in {hepB_RxNorm_ingredient_concepts_sql}
        ),
        drugs_from_ingredients as (
            select a.*, b.concept_name as drug_name, b.concept_class_id
            from descendants a 
            left join (
                select * from {os.environ["WORKSPACE_CDR"]}.concept
            ) as b
            on a.drug_concept_id = b.concept_id
            where concept_class_id!='Ingredient'
        )
        --select distinct drug_concept_id from drugs_from_ingredients order by drug_concept_id asc
        select * from drugs_from_ingredients --where drug_name like '%acetaminophen%'
       -- where drug_concept_id = 19020053
    """
query_df = pd.read_gbq(query_sql, dialect="standard")
query_df


# In[138]:


# download codes for each comorbidity : don't want to run comorbidities section again
comorbidity_names = [
    "hypertension", "heart_failure", "ischemic_heart", "atrial_fibrillation", "hyperlipidemia",
    "stroke", "arthritis", "asthma", "autism", 'cancer', "chronic_kidney_disease", "chronic_pulmonary_disease",
    "alzheimers", "depression", "diabetes", "hepatitis", "HIV", "osteoporosis", "schizophrenia", "substance_abuse"
]
condition_codes ={}
for name in comorbidity_names:
    exec(f"""
import_df('{name}_condition_codes')
condition_codes[name] = {name}_condition_codes
    """)


# In[140]:


hypertension_condition_codes


# In[116]:


name='hypertension'
# create_download_link(condition_codes[name], f'{name}', f'{name}.csv')
for name in comorbidity_names:
#     print(name, condition_codes[name])
    display(create_download_link(condition_codes[name], f'{name}', f'{name}.csv'))


# In[17]:


# Find patients who are in v3 but not in v4
test_sql = f"""
    with drug_exposures as (
        SELECT
            d_exposure.*
        from
            `{os.environ["WORKSPACE_CDR"] }`.drug_exposure d_exposure 
        WHERE
            (
                PERSON_ID = 1001591
            ) 
    ), 
    with_drug_name as (
        select c.concept_name as drug_name, de.* from drug_exposures de
         LEFT JOIN(
              select * from `{os.environ["WORKSPACE_CDR"]}`.concept
        ) c
         on de.drug_concept_id = c.CONCEPT_ID
    ),
    in_time_period as (
        select * from with_drug_name
        where drug_exposure_start_date <= '2018-12-31'
    )
    
    select * from with_drug_name 
    --where drug_name like '%hepatitis%' or drug_exposure_id = 11000000066164102
    order by drug_exposure_start_date asc
    
"""

test_df = pd.read_gbq(test_sql, dialect="standard")
create_download_link(test_df)


# In[16]:


def check_drug_concept_ids():
    query_sql = f"""
    select * from {os.environ["WORKSPACE_CDR"]}.concept 
    --where concept_id in (40213317, 40213303, 706105, 40213286, 40213293, 40213308, 40213305, 40213304, 40213307, 40213306)
    where concept_id = 40213293 
        """
    query_df = pd.read_gbq(query_sql, dialect="standard")
    display(query_df)
    
check_drug_concept_ids()


# In[19]:


# Get pneumococcal PCV13 vaccine drugs
pneumococcal_PCV13_drugs = []
pneumococcal_PCV13_drugs = get_drugs_from_ingredients(pneumococcal_RxNorm_PCV13_concepts_sql)
#CVX for pneumococcal conjugate vaccine, 13 valent
pneumococcal_PCV13_drugs.append("40213198")
pneumococcal_PCV13_drugs = list_to_sql_str(pneumococcal_PCV13_drugs)
# pneumococcal_PCV13_drugs


# In[20]:


# Get pneumococcal PPSV23 vaccine drugs
pneumococcal_PPSV23_drugs = []
pneumococcal_PPSV23_drugs = get_drugs_from_ingredients(pneumococcal_RxNorm_PPSV23_concepts_sql)
# #CVX for pneumococcal vaccine, 23 valent
pneumococcal_PPSV23_drugs.append("40213201")
pneumococcal_PPSV23_drugs = list_to_sql_str(pneumococcal_PPSV23_drugs)
# pneumococcal_PPSV23_drugs


# In[21]:


# Create Influenza Cohort
# Influenza patients from September 1st, 2017 to May 31st, 2018
influenza_cohort_sql = f"""
    all_influenza_patients as (
        SELECT
            d_exposure.PERSON_ID,
            d_exposure.DRUG_EXPOSURE_START_DATE
        from
            `{os.environ["WORKSPACE_CDR"] }`.drug_exposure d_exposure 
        WHERE
            (
                {influenza_CVX_drug_concepts_sql} OR {influenza_RxNorm_drugs_sql}
            ) 
    ), 
    in_time_period as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from all_influenza_patients
        where drug_exposure_start_date between '2017-9-1' and '2018-5-31'
        group by person_id
    ),
    cohort as (
        select * from in_time_period
    )
"""


# In[22]:


# Create Hep B Cohort
# Hep B patients by 2018
hepB_cohort_sql = f"""
    hepb_patients as (
        SELECT
            d_exposure.PERSON_ID,
            d_exposure.DRUG_EXPOSURE_START_DATE
        from
            `{os.environ["WORKSPACE_CDR"] }`.drug_exposure d_exposure 
        WHERE
            (
                {hepB_CVX_drug_concepts_sql} OR {hepB_RxNorm_drugs_sql}
            ) 
    ), 
    in_time_period as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from hepb_patients
        where drug_exposure_start_date <= '2018-12-31'
        group by person_id
    ),
    cohort as (
        select * from in_time_period
    )
"""


# In[23]:


# Create HPV Cohort
# HPV patients by 2018
hpv_cohort_sql = f"""
    hpv_patients as (
        SELECT
            d_exposure.PERSON_ID,
            d_exposure.DRUG_EXPOSURE_START_DATE
        from
            `{os.environ["WORKSPACE_CDR"] }`.drug_exposure d_exposure 
        WHERE
            (
                {hpv_CVX_drug_concepts_sql} OR {hpv_RxNorm_drugs_sql}
            ) 
    ), 
    in_time_period as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from hpv_patients
        where drug_exposure_start_date <= '2018-12-31'
        group by person_id
    ),
    cohort as (
        select * from in_time_period
        )
"""


# In[24]:


# Create Pneumococcal under 65 Cohort
# Pneumococcal patients 
pneumococcal_under_65_cohort_sql = f"""
    pneumococcal_patients as (
        SELECT
            d_exposure.PERSON_ID,
           drug_exposure_start_date,
           drug_concept_id
        from
            `{os.environ["WORKSPACE_CDR"]}`.drug_exposure d_exposure 
        WHERE
            (
                {pneumococcal_CVX_drug_concepts_sql} OR {pneumococcal_RxNorm_drugs_sql}
            )
    ),
   in_time_period as (
        select person_id, drug_exposure_start_date, drug_concept_id from pneumococcal_patients
        where drug_exposure_start_date <= '2018-12-31'
    ),
    with_person_info_concept_ids as (
        select a.person_id, a.drug_exposure_start_date, drug_concept_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date(date_of_birth) as date_of_birth, race_concept_id 
        from in_time_period a
        left join (
            select GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID, ETHNICITY_CONCEPT_ID, BIRTH_DATETIME as DATE_OF_BIRTH,PERSON_ID,RACE_CONCEPT_ID
            from `{os.environ["WORKSPACE_CDR"] }`.person
        ) as b
        on a.person_id = b.PERSON_ID
    ),
    with_vaccination_age as (
        select a.*, date_diff(drug_exposure_start_date, date_of_birth, YEAR) as age_at_vaccination 
        from with_person_info_concept_ids a
        where date_diff(drug_exposure_start_date, date_of_birth, YEAR) < 65
    ),
    group_by_patient as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date, drug_concept_id, gender_concept_id, sex_at_birth_concept_id,
        ethnicity_concept_id, date_of_birth, race_concept_id, min(age_at_vaccination) as age_at_vaccination
        from with_vaccination_age
        group by person_id, drug_concept_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date_of_birth, race_concept_id
        
    ), cohort as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from group_by_patient group by person_id
    )
"""


# In[25]:


# Create Pneumococcal 65 and over Cohort
# Pneumococcal patients 
pneumococcal_65_and_over_cohort_sql = f"""
    pneumococcal_patients as (
        SELECT
            d_exposure.PERSON_ID,
           drug_exposure_start_date,
           drug_concept_id
        from
            `{os.environ["WORKSPACE_CDR"]}`.drug_exposure d_exposure 
        WHERE
            (
                {pneumococcal_CVX_drug_concepts_sql} OR {pneumococcal_RxNorm_drugs_sql}
            )
    ),
   in_time_period as (
        select person_id, drug_exposure_start_date, drug_concept_id from pneumococcal_patients
        where drug_exposure_start_date <= '2018-12-31'
    ),
    with_person_info_concept_ids as (
        select a.person_id, a.drug_exposure_start_date, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date(date_of_birth) as date_of_birth, race_concept_id 
        from in_time_period a
        left join (
            select GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID, ETHNICITY_CONCEPT_ID, BIRTH_DATETIME as DATE_OF_BIRTH,PERSON_ID,RACE_CONCEPT_ID
            from `{os.environ["WORKSPACE_CDR"] }`.person
        ) as b
        on a.person_id = b.PERSON_ID
    ),
    with_vaccination_age as (
        select a.*, date_diff(drug_exposure_start_date, date_of_birth, YEAR) as age_at_vaccination 
        from with_person_info_concept_ids a
        where date_diff(drug_exposure_start_date, date_of_birth, YEAR) >= 65
    ),
    group_by_patient as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date , gender_concept_id, sex_at_birth_concept_id,
        ethnicity_concept_id, date_of_birth, race_concept_id, min(age_at_vaccination) as age_at_vaccination
        from with_vaccination_age
        group by person_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date_of_birth, race_concept_id
        
    ), cohort as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date
        from group_by_patient group by person_id
    )
"""


# In[26]:


# # %%script false --no-raise-error


# # Demographics

# In[27]:


# Demographics SQL
demographics_sql = f"""
    with_person_info_concept_ids as (
        select a.person_id, a.drug_exposure_start_date, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date(date_of_birth) as date_of_birth, race_concept_id
        from cohort a
        left join (
            select GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID, ETHNICITY_CONCEPT_ID, BIRTH_DATETIME as DATE_OF_BIRTH,PERSON_ID,RACE_CONCEPT_ID
            from {os.environ["WORKSPACE_CDR"] }.person
        ) as b
        on a.person_id = b.PERSON_ID
    ),
    with_concept_names as (
        select person.*, 
        p_race_concept.concept_name as race,
        p_gender_concept.concept_name as gender,
        p_ethnicity_concept.concept_name as ethnicity,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
        from with_person_info_concept_ids person
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_race_concept 
                on person.race_concept_id = p_race_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_gender_concept 
                on person.gender_concept_id = p_gender_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_ethnicity_concept 
                on person.ethnicity_concept_id = p_ethnicity_concept.CONCEPT_ID 
        LEFT JOIN
             {os.environ["WORKSPACE_CDR"]}.concept p_sex_at_birth_concept 
                on person.sex_at_birth_concept_id = p_sex_at_birth_concept.CONCEPT_ID
    ),
    with_ages as (
        select a.*,
        date_diff(drug_exposure_start_date, date_of_birth, YEAR) as age_at_vaccination,
        date_diff(date(2018, 12, 31), date_of_birth, YEAR) as age_in_2018
        from with_concept_names a
    ),
    with_state as (
        select a.*, b.state from with_ages a left join (
            select person_id, concept_name as state from {os.environ["WORKSPACE_CDR"]}.observation 
            join {os.environ["WORKSPACE_CDR"]}.concept on value_as_concept_id=concept_id
            WHERE observation_source_concept_id=1585249
        )as b
        on a.person_id = b.person_id

    )
    select * from with_state order by drug_exposure_start_date asc
"""


# ## Define Datasets

# In[28]:


# Influenza - Demographics
influenza_demographics_sql = f"""
    with {influenza_cohort_sql},
    {demographics_sql}
"""
influenza_demographics = pd.read_gbq(influenza_demographics_sql, dialect="standard")
#influenza_cohort = cohort_from_df(influenza_demographics)
#print(influenza_demographics_sql)


# In[29]:


# HepB - Demographics
hepB_demographics_sql = f"""
    with {hepB_cohort_sql},
    {demographics_sql}
"""
hepB_demographics = pd.read_gbq(hepB_demographics_sql, dialect="standard")


# In[30]:


len(hepB_demographics[hepB_demographics['age_at_vaccination'] < 18].person_id.unique())


# In[31]:


create_download_link(hepB_demographics)


# In[32]:


under_18_for_v3 = ['2363184',
 '2890395',
 '1025145',
 '1619517',
 '1167213',
 '1086068',
 '3026659',
 '1345184',
 '2425037',
 '1336144',
 '1496457',
 '1922441',
 '2836085',
 '1817914',
 '2341120',
 '2920418',
 '3079380',
 '1305986',
 '1717352',
 '2295535',
 '1281474',
 '3376569',
 '2214011',
 '1712436',
 '2170927',
 '2969047',
 '2418599',
 '1007290',
 '2237969',
 '1956961',
 '2904800',
 '3389185',
 '1411311',
 '1800274',
 '2870780',
 '2150400',
 '1015672',
 '3489121',
 '3372732',
 '2468800',
 '1374302',
 '1302315',
 '2072150',
 '2897919',
 '2421735',
 '2775048',
 '3481598',
 '1274400',
 '3037882',
 '1828223',
 '3210356',
 '3008436',
 '2741601',
 '1302966',
 '1124686',
 '1292753',
 '2762388',
 '1689069',
 '1120986',
 '2093649',
 '1854274',
 '1148862',
 '2228805',
 '2450163',
 '1223585',
 '2286061',
 '1858172',
 '2040288',
 '2065746',
 '3383900',
 '3365649',
 '1249073',
 '1848599',
 '2610332',
 '3247223',
 '2604728',
 '2896898',
 '3115713',
 '2723145',
 '3248349',
 '1834436',
 '3153384',
 '2803510',
 '2007243',
 '1321842',
 '2608253',
 '3055979',
 '1630936',
 '2503124',
 '2974522',
 '1974743',
 '2782219',
 '1714350',
 '2335434',
 '2198500',
 '1119508',
 '2556742',
 '1376322',
 '2005678',
 '1842039',
 '2284742',
 '3244703',
 '1213039',
 '1491017',
 '1862135',
 '1219811',
 '1458925',
 '1639328',
 '1450923',
 '2699435',
 '2657619',
 '2393916',
 '2278893',
 '1281378',
 '1845074',
 '1964322',
 '1146655',
 '2819319',
 '3250876',
 '2171472',
 '3374847',
 '2818719',
 '2646638',
 '1019556',
 '2142717',
 '1845682',
 '3403646',
 '2773071',
 '2195685',
 '3374869',
 '2552706',
 '2718462',
 '3295773',
 '1025697',
 '1429473',
 '2818069',
 '1429561',
 '1909473',
 '2541504',
 '1835932',
 '1508122',
 '2306134',
 '2426102',
 '2108242',
 '1580526',
 '2856596',
 '3484908',
 '3459063',
 '1313271',
 '1683908',
 '3094091',
 '1049928',
 '1219034',
 '2876870',
 '1178431',
 '3026924',
 '1318459',
 '3269355',
 '1295010',
 '1933824',
 '1926622',
 '3385733',
 '2465616',
 '2679604',
 '3330269',
 '3462344',
 '2597710',
 '3470657',
 '1826860',
 '2487722',
 '2617967',
 '3007191',
 '3026366',
 '2986488',
 '1820325',
 '1365899',
 '1904600',
 '3212948',
 '2019832',
 '1203156',
 '1347061',
 '3294840',
 '2780543',
 '3113427',
 '3243055',
 '1345944',
 '1188735',
 '2747526',
 '1823616',
 '3043377',
 '2501757',
 '3333089',
 '3226773',
 '1302443',
 '1767175',
 '2555943',
 '2033095',
 '2846974',
 '2057491',
 '1759073',
 '2295748',
 '1642446',
 '3381411',
 '1224760',
 '1056644',
 '1829879',
 '1493023',
 '1705045',
 '2509870',
 '3461391',
 '2865654',
 '2528915',
 '1491831',
 '1268349',
 '1763092',
 '2669088',
 '1764793',
 '3068735',
 '3389563',
 '3087091',
 '3291418',
 '3101207',
 '2090967',
 '2102266',
 '1999451',
 '2654349',
 '3155273',
 '2734893',
 '2821798',
 '2632703',
 '2229467',
 '2240990',
 '1624238',
 '3422887',
 '1173327',
 '1195708',
 '1267647',
 '1387439',
 '3337947',
 '2248780',
 '2070994',
 '1679211',
 '1981592',
 '2433712',
 '3248061',
 '3417514',
 '1274439',
 '1916711',
 '3093287',
 '2797304',
 '2871761',
 '1297088',
 '2700096',
 '1644711',
 '2268929',
 '3015211',
 '2617080',
 '1950385',
 '2443491',
 '2674772',
 '2782955',
 '2578934',
 '1215251',
 '3485911',
 '3248242',
 '2368821',
 '1405407',
 '2575524',
 '3205279',
 '1979861',
 '3225956',
 '2217947',
 '3367288',
 '2635781',
 '3432917',
 '2815778',
 '2109274',
 '1692383',
 '3186471',
 '1545747',
 '1152158',
 '3147882',
 '2655040',
 '1344799',
 '2791709',
 '2615333',
 '2634740',
 '1557310',
 '2073847',
 '2187708',
 '2802169',
 '1382357',
 '1137011',
 '1414171',
 '2144447',
 '2426160',
 '1857217',
 '1805534',
 '2597667',
 '3069832',
 '1421448',
 '1945775',
 '3273367',
 '3509252',
 '1978213',
 '2045685',
 '2382189',
 '2161023',
 '2667784',
 '1403627',
 '1107748',
 '3015782',
 '1978282',
 '1792826',
 '2488445',
 '2198787',
 '2848301',
 '3431414',
 '1320230',
 '2968666',
 '2620099',
 '2742396',
 '1112136',
 '2038692',
 '3346573',
 '1722213',
 '2399988',
 '1029371',
 '2132858',
 '2998863',
 '3096852',
 '3488668',
 '1485828',
 '2497975',
 '2162346',
 '1199984',
 '2514847',
 '1805920',
 '1192492',
 '3303526',
 '2674545',
 '3257068',
 '1740200',
 '2036536',
 '2698760',
 '3333172',
 '3014024',
 '1796635',
 '1260096',
 '2369988',
 '1261380',
 '1857651',
 '2308724',
 '1085064',
 '2922182',
 '1355152',
 '2755394',
 '3376353',
 '2404850',
 '1183192',
 '1438950',
 '2809839',
 '2007931',
 '3244742',
 '2307841',
 '2440373',
 '2462548',
 '2712199',
 '1434200',
 '3153236',
 '3290973',
 '1949512',
 '3067725',
 '1450166',
 '2920566',
 '1048449',
 '1748160',
 '1999803',
 '2301871',
 '2390531',
 '2295492',
 '2932228',
 '2301296',
 '1120517',
 '3119382',
 '3403715',
 '1670710',
 '1587702',
 '1966620',
 '2284485',
 '2748235',
 '1289008',
 '2318692',
 '3277320',
 '1097033',
 '1560210',
 '2211751',
 '3323969',
 '3229330',
 '2056908',
 '1742576',
 '2339087',
 '1990686',
 '1784000',
 '2717011',
 '2344149',
 '2943411',
 '1557883',
 '2730283',
 '2798551',
 '1486706',
 '2607615',
 '3441872',
 '2290528',
 '3191550',
 '2247010',
 '3067110',
 '1878763',
 '3160237',
 '1589076',
 '1190428',
 '1335334',
 '1511151',
 '3326876',
 '2720324',
 '1855115',
 '2727508',
 '1895629',
 '1608566',
 '3297319',
 '1722695',
 '3007628',
 '2296852',
 '2937360',
 '2048213',
 '1316505',
 '3004551',
 '3472062',
 '2424698',
 '2500829',
 '1082811',
 '2753689',
 '1287055',
 '1550579',
 '1278322',
 '1430552',
 '2381237',
 '3397654',
 '1263393',
 '1240042',
 '2476624',
 '3054024',
 '2802549',
 '2639081',
 '2293792',
 '2751408',
 '1996803',
 '2387074',
 '2547934',
 '1157638',
 '2299577',
 '2091572',
 '3225050',
 '2410675',
 '3104233',
 '3280619',
 '2700636',
 '1995123',
 '2307403',
 '2295582',
 '2980670',
 '3005298',
 '1109037',
 '3242972',
 '2773926',
 '3446443',
 '1802194',
 '3194493',
 '2120031',
 '2537309',
 '1212675',
 '1887619',
 '3411747',
 '1157592',
 '3439700',
 '3168917',
 '1407590',
 '1369394',
 '1313637',
 '1486831',
 '3190886',
 '3423370',
 '2218441',
 '2246405',
 '2727474',
 '1671721',
 '1040879',
 '1220527',
 '2873016',
 '2739600',
 '1767702',
 '2295837',
 '1417842',
 '2590185',
 '1371434',
 '2508163',
 '1292512',
 '1080898',
 '1758756',
 '1130187',
 '1063777',
 '3121813',
 '1456304',
 '1304247',
 '1402789',
 '3370259',
 '1219020',
 '1389425',
 '1572246',
 '1148535',
 '2244864',
 '2378935',
 '2191144',
 '2477320',
 '1659116',
 '2597960',
 '1828371',
 '1331911',
 '1671173',
 '2200856',
 '2809011',
 '2794784',
 '3367241',
 '3181003',
 '3432521',
 '1991853',
 '2760757',
 '2879862',
 '2416197',
 '3035700',
 '1362781',
 '1873162',
 '2882604',
 '3060951',
 '1758717',
 '3034147',
 '2390825',
 '1781243',
 '2176214',
 '1137210',
 '1085813',
 '2642594',
 '1975605',
 '1206791',
 '2466940',
 '1590114',
 '1403193',
 '1142960',
 '1970035',
 '1604288',
 '1197231',
 '2702778',
 '2685736',
 '1378689',
 '1618618',
 '1670265',
 '1937844',
 '2177277',
 '2065761',
 '1380927',
 '1823435',
 '2590370',
 '1385745',
 '2473965',
 '1553242',
 '2873964',
 '1498997',
 '2515008',
 '1124176',
 '1464768',
 '1948324',
 '1308366',
 '2917506',
 '2750225',
 '3221030',
 '2880707',
 '3071115',
 '1322644',
 '2921576',
 '3330150',
 '1456346',
 '3492248',
 '3156647',
 '3180115',
 '1242407',
 '3507810',
 '3322148',
 '1384992',
 '2336449',
 '1625663',
 '2979325',
 '1667873',
 '1405375',
 '1143176',
 '2115443',
 '2810888',
 '3180557',
 '1622463',
 '1610268',
 '1079101',
 '2409934',
 '2255591',
 '2185759',
 '2751015',
 '1781205',
 '3467925',
 '2104049',
 '3018880',
 '2386904',
 '3500660',
 '3223135',
 '2655657',
 '1418804',
 '1482911',
 '2315742',
 '3102598',
 '1867859',
 '2551170',
 '1697757',
 '2493639',
 '1563560',
 '2952974',
 '1674852',
 '3295762',
 '1130817',
 '2992646',
 '1847055',
 '2561145',
 '1549332',
 '3280326',
 '3012970',
 '3479683',
 '3133774',
 '1378314',
 '1384091',
 '1554970',
 '2839361',
 '1725775',
 '2899256',
 '2871106',
 '1768874',
 '1257045',
 '1325586',
 '3361096',
 '3249347',
 '1665640',
 '1354918',
 '2877365',
 '1505960',
 '1325880',
 '2961051',
 '1433051',
 '3086685',
 '2418603',
 '1720584',
 '2174781',
 '1846142',
 '3441875',
 '2448178',
 '3112256',
 '2557819',
 '2093408',
 '2672189',
 '1890581',
 '1834534',
 '2450600',
 '2769091',
 '2648728',
 '2445161',
 '2608985',
 '2350076',
 '1869318',
 '2155561',
 '1369934',
 '1114390',
 '3114664',
 '3269820',
 '3251030',
 '1646680',
 '1984295',
 '2634765',
 '1756551',
 '2577669',
 '3051150',
 '3467065',
 '1891576',
 '2267655',
 '3320548',
 '1793674',
 '1651305',
 '2698957',
 '1112820',
 '1136011',
 '2692439',
 '1122489',
 '1954740',
 '2031050',
 '2513404',
 '1388551',
 '2574856',
 '2628736',
 '1741742',
 '1932368',
 '2140846',
 '1317928',
 '3128717',
 '2174488',
 '1884927',
 '1323189',
 '2889847',
 '1126011',
 '2838384',
 '1370320',
 '2455520',
 '2216132',
 '1089757',
 '1556731',
 '1199569',
 '2655086',
 '2336842',
 '1375491',
 '1684350',
 '1481490',
 '2547750',
 '3458670',
 '3310570',
 '2687991',
 '1370620',
 '2951117',
 '1574787',
 '1939150',
 '1222184',
 '2228956',
 '2893672',
 '3312945',
 '2688722',
 '3106356',
 '3108390',
 '1860928',
 '1851567',
 '2700077',
 '2740971',
 '2465046',
 '3506862',
 '1979983',
 '3257577',
 '1510071',
 '2135122',
 '3192208',
 '1665624',
 '2642543',
 '3291144',
 '1687186',
 '2637851',
 '1672155',
 '2727518',
 '3222880',
 '1751860',
 '2712098',
 '1120135',
 '2169570',
 '2874295',
 '3153304',
 '3413544',
 '3321570',
 '1444845',
 '2808666',
 '2419405',
 '1251524',
 '1972368',
 '3409225',
 '1931392',
 '1871373',
 '1117263',
 '2631959',
 '3395647',
 '3287508',
 '1782871',
 '2040224',
 '1128093',
 '2194988',
 '2940178',
 '1676293',
 '2373992',
 '3284259',
 '2065147',
 '2410707',
 '3273175',
 '2109835',
 '3379774',
 '1827758',
 '2998935',
 '1054789',
 '2234314',
 '1595028',
 '1620034',
 '1006652',
 '2456079',
 '3425378',
 '1273731',
 '3340403',
 '3019801',
 '1893326',
 '2622123',
 '2871552',
 '3160252',
 '3073362',
 '3510423',
 '1719430',
 '2238001',
 '1675903',
 '3040891',
 '2803085',
 '2784056',
 '1683565',
 '2689488',
 '2786665',
 '3445144',
 '1220651',
 '1000093',
 '3325249',
 '2744101',
 '1819454',
 '3411971',
 '2323409',
 '1880031',
 '1892140',
 '3099420',
 '2915532',
 '3373384',
 '3472263',
 '2950342',
 '3073030',
 '1875427',
 '2157336',
 '1647499',
 '2140336',
 '2068249',
 '2808761',
 '2570471',
 '1419900',
 '1258030',
 '3134320',
 '3064186',
 '1419656',
 '2408673',
 '2853821',
 '2593670',
 '2077642',
 '3109820',
 '1707493',
 '1689388',
 '3074699',
 '3199629',
 '2018078',
 '2161095',
 '1262358',
 '1967849',
 '1271290',
 '3114724',
 '1519180',
 '1602629',
 '2420202',
 '1295965',
 '1832125',
 '2506039',
 '1842324',
 '1124515',
 '2256568',
 '2167828',
 '2773603',
 '2285322',
 '1438161',
 '2771490',
 '2391163',
 '3312101',
 '1956042',
 '3099835',
 '3496693',
 '3107831',
 '2321661',
 '2182027',
 '1837867',
 '3003899',
 '1309808',
 '3247569',
 '1347596',
 '1918866',
 '3374501',
 '3126840',
 '1257075',
 '1846047',
 '1423019',
 '1279493',
 '2074315',
 '1159223',
 '1021827',
 '2135956',
 '2172342',
 '2438572',
 '1381557',
 '3220273',
 '3291419',
 '3502907',
 '3019276',
 '1182346',
 '2301018',
 '1920613',
 '1788124',
 '2688315',
 '2915225',
 '2079591',
 '2891263',
 '2137536',
 '3215757',
 '1661447',
 '3289481',
 '3399445',
 '1632952',
 '2574500',
 '1839814',
 '1136216',
 '1417378',
 '2386928',
 '1491899',
 '2422021',
 '3478653',
 '2148668',
 '1341670',
 '1803483',
 '1900616',
 '2987014',
 '1344352',
 '3211977',
 '3439103',
 '2490874',
 '3203031',
 '2970001',
 '1448938',
 '1380714',
 '2126754',
 '3038739',
 '1736775',
 '2757759',
 '1004559',
 '3309604',
 '1931701',
 '2945642',
 '1756034',
 '3328827',
 '2374244',
 '2083487',
 '1567324',
 '2387457',
 '2122964',
 '2601299',
 '2301307',
 '1550538',
 '1712347',
 '3366017',
 '3385140',
 '2464460',
 '3359315',
 '1763238',
 '3324087',
 '2709258',
 '2230464',
 '3393982',
 '2166465',
 '2403013',
 '1062915',
 '2099089',
 '1159368',
 '3373895',
 '2944363',
 '1412670',
 '2211199',
 '1372606',
 '3365587',
 '2635053',
 '1446561',
 '2302576',
 '1152270',
 '1532475',
 '1920641',
 '2562715',
 '1901698',
 '3197439',
 '1223952',
 '1028041']


# In[33]:


len(under_18_for_v3)


# In[34]:


hepB_demographics[(hepB_demographics['person_id'].isin(under_18_for_v3) &(hepB_demographics.age_at_vaccination >= 18) )]


# In[35]:


hepB_demographics[(hepB_demographics['person_id']


# In[ ]:


1759843 in hepB_demographics.person_id.values


# In[27]:


# HPV - Demographics
hpv_demographics_sql = f"""
    with {hpv_cohort_sql},
    {demographics_sql}
"""
hpv_demographics = pd.read_gbq(hpv_demographics_sql, dialect="standard")


# In[28]:


# Define pneumococcal_demographics_under_65
pneumococcal_demographics_under_65_sql = f"""
with pneumococcal_patients as (
        SELECT
            d_exposure.PERSON_ID,
           drug_exposure_start_date,
           drug_concept_id
        from
            {os.environ["WORKSPACE_CDR"]}.drug_exposure d_exposure 
        WHERE
            (
                {pneumococcal_CVX_drug_concepts_sql} OR {pneumococcal_RxNorm_drugs_sql}
            )
    ),
   in_time_period as (
        select person_id, drug_exposure_start_date, drug_concept_id from pneumococcal_patients
        where drug_exposure_start_date <= '2018-12-31'
    ),
    with_person_info_concept_ids as (
        select a.person_id, a.drug_exposure_start_date, drug_concept_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date(date_of_birth) as date_of_birth, race_concept_id 
        from in_time_period a
        left join (
            select GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID, ETHNICITY_CONCEPT_ID, BIRTH_DATETIME as DATE_OF_BIRTH,PERSON_ID,RACE_CONCEPT_ID
            from {os.environ["WORKSPACE_CDR"] }.person
        ) as b
        on a.person_id = b.PERSON_ID
    ),
    with_vaccination_age as (
        select a.*, date_diff(drug_exposure_start_date, date_of_birth, YEAR) as age_at_vaccination 
        from with_person_info_concept_ids a
        where date_diff(drug_exposure_start_date, date_of_birth, YEAR) < 65
    ),
    with_vaccine_indicator as (
        select a.*, 
        (case when a.drug_concept_id in {pneumococcal_PCV13_drugs} then 1
        else 0 end) PCV13,
        
        (case when a.drug_concept_id in {pneumococcal_PPSV23_drugs} then 1
        else 0 end) PPSV23
        from with_vaccination_age a
    ),
    group_by_patient as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date ,
        gender_concept_id, sex_at_birth_concept_id,
        ethnicity_concept_id, date_of_birth, race_concept_id, 
        min(age_at_vaccination) as age_at_vaccination, max(PCV13) as PCV13, max(PPSV23) as PPSV23
        from with_vaccine_indicator
        group by person_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date_of_birth, race_concept_id
        
    ), 
    with_more_indicators as (
        select a.*, 
        
        (case when PCV13 = 1 and PPSV23 = 0 then 1
        else 0 end) only_PCV13,
        
        (case when PCV13 = 0 and PPSV23 = 1 then 1
        else 0 end) only_PPSV23,
        
        (case when PCV13 = 1 and PPSV23 = 1 then 1
        else 0 end) PCV13_and_PPSV23,
        
        (case when PCV13 = 0 and PPSV23 = 0 then 1
        else 0 end) neither_PCV13_nor_PPSV23

        from group_by_patient a
    ),
    cohort as ( -- output here for only cohort
       select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from with_more_indicators 
        group by person_id
    ),
    with_2018_age as (
        select a.*, date_diff(date(2018, 12, 31), date_of_birth, YEAR) as age_in_2018
        from with_more_indicators a
    ),
    with_concept_names as (
        select person.*, 
        p_race_concept.concept_name as race,
        p_gender_concept.concept_name as gender,
        p_ethnicity_concept.concept_name as ethnicity,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
        from with_2018_age person
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_race_concept 
                on person.race_concept_id = p_race_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_gender_concept 
                on person.gender_concept_id = p_gender_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_ethnicity_concept 
                on person.ethnicity_concept_id = p_ethnicity_concept.CONCEPT_ID 
        LEFT JOIN
             {os.environ["WORKSPACE_CDR"]}.concept p_sex_at_birth_concept 
                on person.sex_at_birth_concept_id = p_sex_at_birth_concept.CONCEPT_ID
    ),
    with_state as (
        select a.*, b.state from with_concept_names a 
        left join (
            select person_id, concept_name as state from  {os.environ["WORKSPACE_CDR"]}.observation 
            join {os.environ["WORKSPACE_CDR"]}.concept on value_as_concept_id=concept_id
            WHERE observation_source_concept_id=1585249
        )as b
        on a.person_id = b.person_id

    )
    select * from with_state
"""
pneumococcal_demographics_under_65 = pd.read_gbq(pneumococcal_demographics_under_65_sql, dialect="standard")
assert pneumococcal_demographics_under_65["age_at_vaccination"].max() == 64
assert pneumococcal_demographics_under_65['person_id'].duplicated().any()== False
assert pneumococcal_demographics_under_65['only_PPSV23'].value_counts()[1] <= pneumococcal_demographics_under_65['PPSV23'].value_counts()[1]
# pneumococcal_demographics_under_65.head(5)


# In[29]:


# Define pneumococcal_demographics_65_and_over_sql
pneumococcal_demographics_65_and_over_sql = f"""
  with pneumococcal_patients as (
        SELECT
            d_exposure.PERSON_ID,
           drug_exposure_start_date,
           drug_concept_id
        from
            {os.environ["WORKSPACE_CDR"]}.drug_exposure d_exposure 
        WHERE
            (
                {pneumococcal_CVX_drug_concepts_sql} OR {pneumococcal_RxNorm_drugs_sql}
            )
    ),
   in_time_period as (
        select person_id, drug_exposure_start_date, drug_concept_id from pneumococcal_patients
        where drug_exposure_start_date <= '2018-12-31'
    ),
    with_person_info_concept_ids as (
        select a.person_id, a.drug_exposure_start_date, drug_concept_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date(date_of_birth) as date_of_birth, race_concept_id 
        from in_time_period a
        left join (
            select GENDER_CONCEPT_ID, SEX_AT_BIRTH_CONCEPT_ID, ETHNICITY_CONCEPT_ID, BIRTH_DATETIME as DATE_OF_BIRTH,PERSON_ID,RACE_CONCEPT_ID
            from {os.environ["WORKSPACE_CDR"] }.person
        ) as b
        on a.person_id = b.PERSON_ID
    ),
    with_vaccination_age as (
        select a.*, date_diff(drug_exposure_start_date, date_of_birth, YEAR) as age_at_vaccination 
        from with_person_info_concept_ids a
        where date_diff(drug_exposure_start_date, date_of_birth, YEAR) >= 65
    ),
    with_vaccine_indicator as (
        select a.*, 
        (case when a.drug_concept_id in {pneumococcal_PCV13_drugs} then 1
        else 0 end) PCV13,
        
        (case when a.drug_concept_id in {pneumococcal_PPSV23_drugs} then 1
        else 0 end) PPSV23
        from with_vaccination_age a
    ),
    group_by_patient as (
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date ,
        gender_concept_id, sex_at_birth_concept_id,
        ethnicity_concept_id, date_of_birth, race_concept_id, 
        min(age_at_vaccination) as age_at_vaccination, max(PCV13) as PCV13, max(PPSV23) as PPSV23
        from with_vaccine_indicator
        group by person_id, gender_concept_id, sex_at_birth_concept_id, ethnicity_concept_id, date_of_birth, race_concept_id
        
    ), 
    with_more_indicators as (
        select a.*, 
        
        (case when PCV13 = 1 and PPSV23 = 0 then 1
        else 0 end) only_PCV13,
        
        (case when PCV13 = 0 and PPSV23 = 1 then 1
        else 0 end) only_PPSV23,
        
        (case when PCV13 = 1 and PPSV23 = 1 then 1
        else 0 end) PCV13_and_PPSV23,
        
        (case when PCV13 = 0 and PPSV23 = 0 then 1
        else 0 end) neither_PCV13_nor_PPSV23

        from group_by_patient a
    ),
    cohort as ( -- output here for only cohort
        select person_id, min(drug_exposure_start_date) as drug_exposure_start_date from with_more_indicators 
        group by person_id
    ),
    with_2018_age as (
        select a.*, date_diff(date(2018, 12, 31), date_of_birth, YEAR) as age_in_2018
        from with_more_indicators a
    ),
    with_concept_names as (
        select person.*, 
        p_race_concept.concept_name as race,
        p_gender_concept.concept_name as gender,
        p_ethnicity_concept.concept_name as ethnicity,
        p_sex_at_birth_concept.concept_name as sex_at_birth 
        from with_2018_age person
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_race_concept 
                on person.race_concept_id = p_race_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_gender_concept 
                on person.gender_concept_id = p_gender_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_ethnicity_concept 
                on person.ethnicity_concept_id = p_ethnicity_concept.CONCEPT_ID 
        LEFT JOIN
             {os.environ["WORKSPACE_CDR"]}.concept p_sex_at_birth_concept 
                on person.sex_at_birth_concept_id = p_sex_at_birth_concept.CONCEPT_ID
    ),
    with_state as (
        select a.*, b.state from with_concept_names a 
        left join (
            select person_id, concept_name as state from  {os.environ["WORKSPACE_CDR"]}.observation 
            join {os.environ["WORKSPACE_CDR"]}.concept on value_as_concept_id=concept_id
            WHERE observation_source_concept_id=1585249
        )as b
        on a.person_id = b.person_id

    )
    select * from with_state
"""
pneumococcal_demographics_65_and_over = pd.read_gbq(pneumococcal_demographics_65_and_over_sql, dialect="standard")
assert pneumococcal_demographics_65_and_over["age_at_vaccination"].min() == 65
assert pneumococcal_demographics_65_and_over['person_id'].duplicated().any()== False
assert pneumococcal_demographics_65_and_over['only_PPSV23'].value_counts()[1] <= pneumococcal_demographics_65_and_over['PPSV23'].value_counts()[1]


# In[30]:


# Define all_patients_demographics_sql
all_patients_demographics_sql = f"""
    with person_info as (
        SELECT
            person.gender_concept_id,
            person.sex_at_birth_concept_id,
            person.ethnicity_concept_id,
            date(person.BIRTH_DATETIME) as date_of_birth,
            person.person_id,
            person.race_concept_id,
            p_race_concept.concept_name as race,
            p_gender_concept.concept_name as gender,
            p_ethnicity_concept.concept_name as ethnicity,
            p_sex_at_birth_concept.concept_name as sex_at_birth 
        FROM
            {os.environ["WORKSPACE_CDR"]}.person person 
        LEFT JOIN
           {os.environ["WORKSPACE_CDR"]}.concept p_race_concept 
                on person.race_concept_id = p_race_concept.CONCEPT_ID 
        LEFT JOIN
            {os.environ["WORKSPACE_CDR"]}.concept p_gender_concept 
                on person.gender_concept_id = p_gender_concept.CONCEPT_ID 
        LEFT JOIN
           {os.environ["WORKSPACE_CDR"]}.concept p_ethnicity_concept 
                on person.ethnicity_concept_id = p_ethnicity_concept.CONCEPT_ID 
        LEFT JOIN
           {os.environ["WORKSPACE_CDR"]}.concept p_sex_at_birth_concept 
                on person.sex_at_birth_concept_id = p_sex_at_birth_concept.CONCEPT_ID
    ),
    with_age as (
        select a.*, date_diff(date(2018, 12, 31), date_of_birth, YEAR) as age_in_2018 from person_info a
    ),
    with_state as (
        select a.*, b.state from with_age a left join (
            select person_id, concept_name as state from  {os.environ["WORKSPACE_CDR"]}.observation 
            join {os.environ["WORKSPACE_CDR"]}.concept on value_as_concept_id=concept_id
            WHERE observation_source_concept_id=1585249
        )as b
        on a.person_id = b.person_id

    )
    select * from with_state
    """

all_patients_demographics = pd.read_gbq(all_patients_demographics_sql, dialect="standard")


# In[31]:


# Prepare dataframes for visualization
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]


# ## Save Datasets

# In[32]:


# Change column types
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
change_df_types_for_id_suffix(demographic_dfs)


# In[33]:


# Save demographics datasets
save_df(influenza_demographics, "influenza_demographics")
save_df(hepB_demographics, "hepB_demographics")
save_df(hpv_demographics, "hpv_demographics")
save_df(pneumococcal_demographics_under_65, "pneumococcal_demographics_under_65")
save_df(pneumococcal_demographics_65_and_over, "pneumococcal_demographics_65_and_over")
save_df(all_patients_demographics, "all_patients_demographics")


# # BASICS Survey

# ## Create Datasets

# In[84]:


# Define BASICS survey question concept IDs
basics_concept_set_sql = f"""
 question_concept_id IN (
                        1585386, 43528428, 1585886, 1585375, 1585952, 1585892, 1585389, 1585940
                    )
"""


# In[85]:


# check if all surveys take place on same day per person
test_sql = f"""
    with surveys as (
        SELECT answer.person_id, answer.answer, answer.question_concept_id, date(answer.survey_datetime) as survey_date,
                    answer.answer_concept_id, answer.question    
                FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
                WHERE
                    (
                        {basics_concept_set_sql}
                    )
    ), 
    group_by_date as (
        select person_id, survey_date from surveys group by person_id, survey_date
    ),
    group_by_patient as (
     select person_id, count(*) as number_of_survey_dates, string_agg(Cast(survey_date as string), ', ') as dates from group_by_date group by person_id
    )
    select person_id, number_of_survey_dates, dates from group_by_patient where number_of_survey_dates > 1 order by number_of_survey_dates desc
    --select * from surveys where person_id = 3162254
    """
test_df = pd.read_gbq(test_sql, dialect="standard")
# test_df.head(10)


# Check insurance question types
# test_sql = f"""
#     with surveys as (
#         SELECT answer.person_id, answer.answer, answer.question_concept_id, 
#         date(answer.survey_datetime) as survey_date,
#             answer.answer_concept_id, answer.question    
#                 FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
#     )
#     select distinct question from surveys where question like '%_nsurance%'
#     """
# test_df = pd.read_gbq(test_sql, dialect="standard")
# test_df.head(20)


# In[86]:


# Define SQL query for add BASICS survey
add_basics_sql = f"""
    with_basics as (
        select a.drug_exposure_start_date, b.* from cohort a
        inner join (
            SELECT
                answer.person_id, answer.answer,
                answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
                answer.answer_concept_id, answer.question    
            FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
            WHERE
                (
                    {basics_concept_set_sql}
                )
            )  b
            on a.person_id = b.person_id
    )
"""


# In[77]:


# # find question concept
# education_concept = f"""
#  SELECT
#  distinct
#     answer.question_concept_id, answer.question    
# FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
# limit 10000
    
#     """
# education = pd.read_gbq(education_concept, dialect="standard")


# In[83]:


# education[education['question'].str.contains('Education')]


# In[94]:


# Define influenza_basics
influenza_basics_sql = f"""
    with {influenza_cohort_sql},
    {add_basics_sql}
    select * from with_basics
    
    """
influenza_basics = pd.read_gbq(influenza_basics_sql, dialect="standard")


# In[97]:


# Define hepB_basics
hepB_basics_sql = f"""
    with {hepB_cohort_sql},
    {add_basics_sql}
    --select person_id, survey_date from with_basics group by person_id, survey_date
    select * from with_basics
    """

hepB_basics = pd.read_gbq(hepB_basics_sql, dialect="standard")
# hepB_basics_df.head(10)


# In[87]:


# Define hpv_basics
hpv_basics_sql = f"""
    with {hpv_cohort_sql},
    {add_basics_sql}
    select * from with_basics
    
    """

hpv_basics = pd.read_gbq(hpv_basics_sql, dialect="standard")


# In[89]:


# hpv_basics.question.unique()


# In[90]:


# Define pneumococcal_65_and_over_basics
pneumococcal_65_and_over_basics_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_basics_sql}
    select * from with_basics
    
    """

pneumococcal_65_and_over_basics = pd.read_gbq(pneumococcal_65_and_over_basics_sql, dialect="standard")

# pneumococcal_65_and_over_basics_df.head(5)


# In[91]:


# Define pneumococcal_under_65_basics
pneumococcal_under_65_basics_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_basics_sql}
    select * from with_basics
    
    """
pneumococcal_under_65_basics = pd.read_gbq(pneumococcal_under_65_basics_sql, dialect="standard")


# In[92]:


# Define all_patients_basics
all_basics_sql = f"""
    with with_basics as (

        SELECT
            answer.person_id, answer.answer,
            answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
            answer.answer_concept_id, answer.question    
        FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
        WHERE
            (
                {basics_concept_set_sql}
            )
    )
    select * from with_basics
"""

all_patients_basics = pd.read_gbq(all_basics_sql, dialect="standard")


# In[93]:


# Assertions for BASICS Dataframes
# assert len(pneumococcal_under_65_basics['person_id'].unique()) <= len(pneumococcal_under_65_basics['person_id'].unique())
# assert len(pneumococcal_65_and_over_basics['person_id'].unique()) <= len(pneumococcal_65_and_over_basics['person_id'].unique())
# assert len(hpv_basics['person_id'].unique())<=len(hpv_demographics['person_id'].unique())
# assert len(hepB_basics['person_id'].unique())<=len(hepB_demographics['person_id'].unique())
# assert len(influenza_basics['person_id'].unique())<=len(influenza_demographics['person_id'].unique())


# In[ ]:


# Aggregate BASICS dataframes
with_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, pneumococcal_under_65_basics, pneumococcal_65_and_over_basics, all_patients_basics]
basics_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]

# with_basics_dfs


# In[69]:


hpv_basics['question'].unique()


# ## Save Datasets

# In[95]:


# Change column types
with_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, 
                   pneumococcal_under_65_basics, pneumococcal_65_and_over_basics, all_patients_basics]
change_df_types_for_id_suffix(with_basics_dfs)


# In[98]:


save_df(hepB_basics, "hepB_basics")


# In[96]:


# Save basics survey datasets
save_df(influenza_basics, "influenza_basics")
save_df(hepB_basics, "hepB_basics")
save_df(hpv_basics, "hpv_basics")
save_df(pneumococcal_under_65_basics, "pneumococcal_under_65_basics")
save_df(pneumococcal_65_and_over_basics, "pneumococcal_65_and_over_basics")
save_df(all_patients_basics, "all_patients_basics")


# # Overall Health Survey

# ## Create Datasets

# In[48]:


# Define overall_health_concept_set_sql
overall_health_concept_set_sql = f"""
 question_concept_id IN (
                         1585803, 1585806
                    )
"""


# In[49]:


# Define SQL query for add Overall Health survey
add_overall_health_sql = f"""
    with_overall_health as (
        select a.person_id, a.drug_exposure_start_date, answer, 
        cast(cast(question_concept_id as int64) as string) as question_concept_id,  
        survey_date,
        cast(cast(answer_concept_id as int64) as string) as answer_concept_id, 
        question from cohort a
        inner join (
            SELECT
                answer.person_id, answer.answer,
                answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
                answer.answer_concept_id, answer.question    
            FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
            WHERE
                (
                    {overall_health_concept_set_sql}
                )
            )  b
            on a.person_id = b.person_id
    )
"""


# In[50]:


# Define influenza_overall_health_sql
influenza_overall_health_sql = f"""
    with {influenza_cohort_sql},
    {add_overall_health_sql}
    select * from with_overall_health
    """
influenza_overall_health = pd.read_gbq(influenza_overall_health_sql, dialect="standard")


# In[51]:


# Define hepB_overall_health
hepB_overall_health_sql = f"""
    with {hepB_cohort_sql},
    {add_overall_health_sql}
    select * from with_overall_health
    """

hepB_overall_health = pd.read_gbq(hepB_overall_health_sql, dialect="standard")
# hepB_overall_health.head(10)


# In[52]:


# hepB_overall_health.head(10)


# In[53]:


# Define hpv_overall_health
hpv_overall_health_sql = f"""
    with {hpv_cohort_sql},
    {add_overall_health_sql}
    select * from with_overall_health
    
    """

hpv_overall_health = pd.read_gbq(hpv_overall_health_sql, dialect="standard")


# In[54]:


# Define pneumococcal_65_and_over_overall_health
pneumococcal_65_and_over_overall_health_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_overall_health_sql}
    select * from with_overall_health
    
    """

pneumococcal_65_and_over_overall_health = pd.read_gbq(pneumococcal_65_and_over_overall_health_sql, dialect="standard")

# pneumococcal_65_and_over_overall_health_df.head(5)


# In[55]:


# Define pneumococcal_under_65_overall_health
pneumococcal_under_65_overall_health_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_overall_health_sql}
    select * from with_overall_health
    
    """
pneumococcal_under_65_overall_health = pd.read_gbq(pneumococcal_under_65_overall_health_sql, dialect="standard")


# In[56]:


pneumococcal_under_65_overall_health


# In[57]:


# Define all_patients_overall_health
all_overall_health_sql = f"""
    with with_overall_health as (

        SELECT
            answer.person_id, answer.answer,
            answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
            answer.answer_concept_id, answer.question    
        FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
        WHERE
            (
                {overall_health_concept_set_sql}
            )
    )
    select * from with_overall_health
"""

all_patients_overall_health = pd.read_gbq(all_overall_health_sql, dialect="standard")


# ## Save Datasets

# In[58]:


# Change column types
with_overall_health_dfs = [influenza_overall_health, hepB_overall_health, hpv_overall_health, 
                   pneumococcal_under_65_overall_health, pneumococcal_65_and_over_overall_health, all_patients_overall_health]
change_df_types_for_id_suffix(with_overall_health_dfs)


# In[59]:


# Save overall_health survey datasets
save_df(influenza_overall_health, "influenza_overall_health")
save_df(hepB_overall_health, "hepB_overall_health")
save_df(hpv_overall_health, "hpv_overall_health")
save_df(pneumococcal_under_65_overall_health, "pneumococcal_under_65_overall_health")
save_df(pneumococcal_65_and_over_overall_health, "pneumococcal_65_and_over_overall_health")
save_df(all_patients_overall_health, "all_patients_overall_health")


# In[60]:


len(pneumococcal_under_65_overall_health['person_id'].unique())


# In[61]:


#organ_transplant = pneumococcal_under_65_overall_health[pneumococcal_under_65_overall_health['question_concept_id']=='1585803']
# organ_transplant[organ_transplant.duplicated(['person_id', 'answer_concept_id'])].sort_values('person_id')


# # Lifestyle Surveys

# ## Create Datasets

# In[62]:


# Define lifestyle_concept_set_sql
lifestyle_concept_set_sql = f"""
    question_concept_id IN (
                1586169, 1586201, 1586174, 1586207, 1586193, 1586166, 1586198, 
                1586185, 1586190, 1585857, 1586177, 1585860, 1586182
            )
"""


# In[63]:


# Define SQL query for add Overall Health survey
add_lifestyle_sql = f"""
    with_lifestyle as (
        select a.person_id, a.drug_exposure_start_date, answer, 
        cast(cast(question_concept_id as int64) as string) as question_concept_id,  
        survey_date,
        cast(cast(answer_concept_id as int64) as string) as answer_concept_id, 
        question from cohort a
        inner join (
            SELECT
                answer.person_id, answer.answer,
                answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
                answer.answer_concept_id, answer.question    
            FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
            WHERE
                (
                    {lifestyle_concept_set_sql}
                )
            )  b
            on a.person_id = b.person_id
    )
"""


# In[64]:


# Define influenza_lifestyle_sql
influenza_lifestyle_sql = f"""
    with {influenza_cohort_sql},
    {add_lifestyle_sql}
    select * from with_lifestyle
    """
influenza_lifestyle = pd.read_gbq(influenza_lifestyle_sql, dialect="standard")


# In[65]:


# Define hepB_lifestyle
hepB_lifestyle_sql = f"""
    with {hepB_cohort_sql},
    {add_lifestyle_sql}
    select * from with_lifestyle
    """
hepB_lifestyle = pd.read_gbq(hepB_lifestyle_sql, dialect="standard")
# hepB_lifestyle_df.head(10)


# In[66]:


# Define hpv_lifestyle
hpv_lifestyle_sql = f"""
    with {hpv_cohort_sql},
    {add_lifestyle_sql}
    select * from with_lifestyle
    """
hpv_lifestyle = pd.read_gbq(hpv_lifestyle_sql, dialect="standard")


# In[67]:


# Define pneumococcal_65_and_over_lifestyle
pneumococcal_65_and_over_lifestyle_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_lifestyle_sql}
    select * from with_lifestyle
    """
pneumococcal_65_and_over_lifestyle = pd.read_gbq(pneumococcal_65_and_over_lifestyle_sql, dialect="standard")


# In[68]:


# Define pneumococcal_under_65_lifestyle
pneumococcal_under_65_lifestyle_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_lifestyle_sql}
    select * from with_lifestyle
    """
pneumococcal_under_65_lifestyle = pd.read_gbq(pneumococcal_under_65_lifestyle_sql, dialect="standard")


# In[69]:


# Define all_patients_lifestyle
all_lifestyle_sql = f"""
    with with_lifestyle as (
        SELECT
            answer.person_id, answer.answer,
            answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
            answer.answer_concept_id, answer.question    
        FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
        WHERE
            (
                {lifestyle_concept_set_sql}
            )
    )
    select * from with_lifestyle
"""
all_patients_lifestyle = pd.read_gbq(all_lifestyle_sql, dialect="standard")


# ## Save Datasets

# In[70]:


# Change column types
with_lifestyle_dfs = [influenza_lifestyle, hepB_lifestyle, hpv_lifestyle, 
                   pneumococcal_under_65_lifestyle, pneumococcal_65_and_over_lifestyle, all_patients_lifestyle]
change_df_types_for_id_suffix(with_lifestyle_dfs)
# influenza_lifestyle


# In[71]:


# Save lifestyle survey datasets
save_df(influenza_lifestyle, "influenza_lifestyle")
save_df(hepB_lifestyle, "hepB_lifestyle")
save_df(hpv_lifestyle, "hpv_lifestyle")
save_df(pneumococcal_under_65_lifestyle, "pneumococcal_under_65_lifestyle")
save_df(pneumococcal_65_and_over_lifestyle, "pneumococcal_65_and_over_lifestyle")
save_df(all_patients_lifestyle, "all_patients_lifestyle")


# In[72]:


pneumococcal_65_and_over_lifestyle.dtypes


# In[73]:


pneumococcal_65_and_over_lifestyle['question_concept_id'].astype(str)


# In[74]:


pneumococcal_65_and_over_lifestyle.groupby(['person_id', 'question_concept_id'])['answer_concept_id'].min()


# In[75]:


pneumococcal_65_and_over_lifestyle[pneumococcal_65_and_over_lifestyle['person_id']=='1000222']


# In[76]:


with_lifestyle_dfs = [influenza_lifestyle, hepB_lifestyle, hpv_lifestyle, 
                   pneumococcal_under_65_lifestyle, pneumococcal_65_and_over_lifestyle, all_patients_lifestyle]
lifestyle_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]


# In[77]:


def get_smoking_anomalies(df):
    hundred_cigs = df[(df['question_concept_id']=='1585857') & 
                                       (df['answer_concept_id']=='1585858') ]['person_id'].unique()
#     print(len(hundred_cigs))
    # people who answered frequency without smoking 100 cigs
    anomlies = df[(df['question_concept_id']=='1585860') 
                            & (~df['person_id'].isin(hundred_cigs)) ]['person_id'].unique()
    print(len(anomlies))
    # people who were asked frequency
    people_asked_freq = df[df['question_concept_id']=='1585860']['person_id'].unique()
    
    print(len(people_asked_freq) - len(hundred_cigs))

#     not_asked_100_cigs = df[(df['question_concept_id']=='1585857')]
    
#     c = df[(~df['question_concept_id']=='1585857') & 
#                      (df['question_concept_id']=='1585860') ]['person_id'].unique()
    
#     print(c)
    #
    b = df[(df['question_concept_id'].isin([ '1585860'])) 
                            & (df['person_id'].isin(anomlies))]['answer'].value_counts()
    display(b)
    


# In[78]:


for df, name in zip(with_lifestyle_dfs, lifestyle_df_names):
    print(name)
    get_smoking_anomalies(df)


# In[79]:


# c = influenza_lifestyle[(influenza_lifestyle['person_id'].isin(no_hundred_cigs)) &
#                         (influenza_lifestyle['question_concept_id']=='1585860')]
# c


# In[ ]:


# influenza_lifestyle[influenza_lifestyle['person_id']=='3513694']


# # Utilization Survey

# ## Create Datasets

# In[80]:


# Define utilization_concept_set_sql
utilization_concept_set_sql = f"""
    question_concept_id IN (
             43528664, 43530584, 43528665, 43530585, 43528666, 43530268, 
             43528662, 43528663, 43530583, 43530438, 43530557, 43529904, 
             43530416, 43529905, 43530417, 43529906, 43530408, 43530409, 
             43530410, 43530411, 43530412, 43530413, 43529903, 43530415, 43530594 
            )
"""


# In[81]:


# Define SQL query for add Overall Health survey
add_utilization_sql = f"""
    with_utilization as (
        select a.person_id, a.drug_exposure_start_date, answer, 
        cast(cast(question_concept_id as int64) as string) as question_concept_id,  
        survey_date,
        cast(cast(answer_concept_id as int64) as string) as answer_concept_id, 
        question from cohort a
        inner join (
            SELECT
                answer.person_id, answer.answer,
                answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
                answer.answer_concept_id, answer.question    
            FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
            WHERE
                (
                    {utilization_concept_set_sql}
                )
            )  b
            on a.person_id = b.person_id
    )
"""


# In[82]:


# Define influenza_utilization_sql
influenza_utilization_sql = f"""
    with {influenza_cohort_sql},
    {add_utilization_sql}
    select * from with_utilization
    """
influenza_utilization = pd.read_gbq(influenza_utilization_sql, dialect="standard")


# In[83]:


# Define hepB_utilization
hepB_utilization_sql = f"""
    with {hepB_cohort_sql},
    {add_utilization_sql}
    select * from with_utilization
    """
hepB_utilization = pd.read_gbq(hepB_utilization_sql, dialect="standard")
# hepB_utilization_df.head(10)


# In[84]:


# Define hpv_utilization
hpv_utilization_sql = f"""
    with {hpv_cohort_sql},
    {add_utilization_sql}
    select * from with_utilization
    """
hpv_utilization = pd.read_gbq(hpv_utilization_sql, dialect="standard")


# In[85]:


# Define pneumococcal_65_and_over_utilization
pneumococcal_65_and_over_utilization_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_utilization_sql}
    select * from with_utilization
    """
pneumococcal_65_and_over_utilization = pd.read_gbq(pneumococcal_65_and_over_utilization_sql, dialect="standard")


# In[86]:


# Define pneumococcal_under_65_utilization
pneumococcal_under_65_utilization_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_utilization_sql}
    select * from with_utilization
    """
pneumococcal_under_65_utilization = pd.read_gbq(pneumococcal_under_65_utilization_sql, dialect="standard")


# In[87]:


# Define all_patients_utilization
all_utilization_sql = f"""
    with with_utilization as (
        SELECT
            answer.person_id, answer.answer,
            answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
            answer.answer_concept_id, answer.question    
        FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
        WHERE
            (
                {utilization_concept_set_sql}
            )
    )
    select * from with_utilization
"""
all_patients_utilization = pd.read_gbq(all_utilization_sql, dialect="standard")


# ## Save Datasets

# In[88]:


# Change column types
with_utilization_dfs = [influenza_utilization, hepB_utilization, hpv_utilization, 
                   pneumococcal_under_65_utilization, pneumococcal_65_and_over_utilization, all_patients_utilization]
change_df_types_for_id_suffix(with_utilization_dfs)
# influenza_utilization


# In[89]:


# Save utilization survey datasets
save_df(influenza_utilization, "influenza_utilization")
save_df(hepB_utilization, "hepB_utilization")
save_df(hpv_utilization, "hpv_utilization")
save_df(pneumococcal_under_65_utilization, "pneumococcal_under_65_utilization")
save_df(pneumococcal_65_and_over_utilization, "pneumococcal_65_and_over_utilization")
save_df(all_patients_utilization, "all_patients_utilization")


# # PROMIS_health Survey

# ## Create Datasets

# In[90]:


# Define PROMIS_health_concept_set_sql
PROMIS_health_concept_set_sql = f"""
    question_concept_id IN (
               1585723, 1585741, 1585747, 1585748 -- Physical Health
               ,1585760, 1585729, 1585717, 1585735 -- Mental Health
            )
"""


# In[91]:


# Define SQL query for add Overall Health survey
add_PROMIS_health_sql = f"""
    with_PROMIS_health as (
        select a.person_id, a.drug_exposure_start_date, answer, 
        cast(cast(question_concept_id as int64) as string) as question_concept_id,  
        survey_date,
        cast(cast(answer_concept_id as int64) as string) as answer_concept_id, 
        question from cohort a
        inner join (
            SELECT
                answer.person_id, answer.answer,
                answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
                answer.answer_concept_id, answer.question    
            FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
            WHERE
                (
                    {PROMIS_health_concept_set_sql}
                )
            )  b
            on a.person_id = b.person_id
    )
"""


# In[92]:


# Define influenza_PROMIS_health_sql
influenza_PROMIS_health_sql = f"""
    with {influenza_cohort_sql},
    {add_PROMIS_health_sql}
    select * from with_PROMIS_health
    """
influenza_PROMIS_health = pd.read_gbq(influenza_PROMIS_health_sql, dialect="standard")


# In[93]:


# Define hepB_PROMIS_health
hepB_PROMIS_health_sql = f"""
    with {hepB_cohort_sql},
    {add_PROMIS_health_sql}
    select * from with_PROMIS_health
    """
hepB_PROMIS_health = pd.read_gbq(hepB_PROMIS_health_sql, dialect="standard")
# hepB_PROMIS_health_df.head(10)


# In[94]:


# Define hpv_PROMIS_health
hpv_PROMIS_health_sql = f"""
    with {hpv_cohort_sql},
    {add_PROMIS_health_sql}
    select * from with_PROMIS_health
    """
hpv_PROMIS_health = pd.read_gbq(hpv_PROMIS_health_sql, dialect="standard")


# In[95]:


# Define pneumococcal_65_and_over_PROMIS_health
pneumococcal_65_and_over_PROMIS_health_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_PROMIS_health_sql}
    select * from with_PROMIS_health
    """
pneumococcal_65_and_over_PROMIS_health = pd.read_gbq(pneumococcal_65_and_over_PROMIS_health_sql, dialect="standard")


# In[96]:


# Define pneumococcal_under_65_PROMIS_health
pneumococcal_under_65_PROMIS_health_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_PROMIS_health_sql}
    select * from with_PROMIS_health
    """
pneumococcal_under_65_PROMIS_health = pd.read_gbq(pneumococcal_under_65_PROMIS_health_sql, dialect="standard")


# In[97]:


# Define all_patients_PROMIS_health
all_PROMIS_health_sql = f"""
    with with_PROMIS_health as (
        SELECT
            answer.person_id, answer.answer,
            answer.question_concept_id, extract(date from answer.survey_datetime) as survey_date,
            cast(cast(answer_concept_id as int64) as string) as answer_concept_id, answer.question    
        FROM {os.environ["WORKSPACE_CDR"]}.ds_survey answer 
        WHERE
            (
                {PROMIS_health_concept_set_sql}
            )
    )
    select * from with_PROMIS_health
"""
all_patients_PROMIS_health = pd.read_gbq(all_PROMIS_health_sql, dialect="standard")


# ## Save Datasets

# In[98]:


# Change column types
with_PROMIS_health_dfs = [influenza_PROMIS_health, hepB_PROMIS_health, hpv_PROMIS_health, 
                   pneumococcal_under_65_PROMIS_health, pneumococcal_65_and_over_PROMIS_health, all_patients_PROMIS_health]
change_df_types_for_id_suffix(with_PROMIS_health_dfs)
# influenza_PROMIS_health


# In[99]:


# Save PROMIS_health survey datasets
save_df(influenza_PROMIS_health, "influenza_PROMIS_health")
save_df(hepB_PROMIS_health, "hepB_PROMIS_health")
save_df(hpv_PROMIS_health, "hpv_PROMIS_health")
save_df(pneumococcal_under_65_PROMIS_health, "pneumococcal_under_65_PROMIS_health")
save_df(pneumococcal_65_and_over_PROMIS_health, "pneumococcal_65_and_over_PROMIS_health")
save_df(all_patients_PROMIS_health, "all_patients_PROMIS_health")


# In[100]:


all_patients_PROMIS_health


# In[101]:


# cast(cast(answer_concept_id as int64) as string) as answer_concept_id


# # Insurance

# ## Create Datasets

# In[102]:


# Define SQL query for insurance 
add_insurance_sql = f"""
observations as (
    select a.drug_exposure_start_date, b.* from cohort a
        inner join (
            SELECT
                observation_source_concept_id,
                PERSON_ID,
                OBSERVATION_CONCEPT_ID,
                OBSERVATION_DATETIME,
                value_source_concept_id,
                value_as_concept_id,
                value_source_value
                from {os.environ["WORKSPACE_CDR"]}.observation
                WHERE
                (
                    observation_concept_id IN (
                        43528428, 1585389, 40766240
                    ) 
                    OR  value_source_concept_id IN (
                        43528428, 1585389, 40766240
                    )
                )
        )  b
            on a.person_id = b.person_id
), 
with_insurance as (
    select a.*,
        o_value.concept_name as value_as_concept_name,
        o_standard_concept.concept_name as standard_concept_name,
        o_source_concept.concept_code as source_concept_code,
        o_source_concept.concept_name as source_concept_name,
        o_source_concept.vocabulary_id as source_vocabulary
    from observations a
    LEFT JOIN
        {os.environ["WORKSPACE_CDR"]}.concept o_value 
            on value_as_concept_id = o_value.CONCEPT_ID 
    LEFT JOIN
         {os.environ["WORKSPACE_CDR"]}.concept o_standard_concept 
            on OBSERVATION_CONCEPT_ID = o_standard_concept.CONCEPT_ID 
    LEFT JOIN
         {os.environ["WORKSPACE_CDR"]}.concept o_source_concept 
            on OBSERVATION_SOURCE_CONCEPT_ID = o_source_concept.CONCEPT_ID 
)
        
        """


# In[103]:


# Define influenza_insurance_sql
influenza_insurance_sql = f"""
    with {influenza_cohort_sql},
    {add_insurance_sql}
    select * from with_insurance
    """
influenza_insurance = pd.read_gbq(influenza_insurance_sql, dialect="standard")


# In[104]:


len(influenza_insurance['PERSON_ID'].unique())


# In[105]:


# Define hepB_insurance
hepB_insurance_sql = f"""
    with {hepB_cohort_sql},
    {add_insurance_sql}
    select * from with_insurance
    """
hepB_insurance = pd.read_gbq(hepB_insurance_sql, dialect="standard")
# hepB_insurance_df.head(10)


# In[106]:


# Define hpv_insurance
hpv_insurance_sql = f"""
    with {hpv_cohort_sql},
    {add_insurance_sql}
    select * from with_insurance
    """
hpv_insurance = pd.read_gbq(hpv_insurance_sql, dialect="standard")


# In[107]:


# Define pneumococcal_65_and_over_insurance
pneumococcal_65_and_over_insurance_sql = f"""
    with {pneumococcal_65_and_over_cohort_sql},
    {add_insurance_sql}
    select * from with_insurance
    """
pneumococcal_65_and_over_insurance = pd.read_gbq(pneumococcal_65_and_over_insurance_sql, dialect="standard")


# In[ ]:





# In[108]:


# Define pneumococcal_under_65_insurance
pneumococcal_under_65_insurance_sql = f"""
    with {pneumococcal_under_65_cohort_sql},
    {add_insurance_sql}
    select * from with_insurance
    """
pneumococcal_under_65_insurance = pd.read_gbq(pneumococcal_under_65_insurance_sql, dialect="standard")


# In[109]:


# Define all_patients_insurance
all_insurance_sql = f"""
 SELECT
        observation.PERSON_ID,
        observation.OBSERVATION_CONCEPT_ID,
        observation.OBSERVATION_DATETIME,
        observation.VALUE_AS_CONCEPT_ID,
        value_source_concept_id,
        observation.value_source_value,
        observation.OBSERVATION_TYPE_CONCEPT_ID,
        observation.OBSERVATION_SOURCE_VALUE,
        o_value.concept_name as value_as_concept_name,
        o_standard_concept.concept_name as standard_concept_name,
        o_source_concept.concept_code as SOURCE_CONCEPT_CODE,
        o_source_concept.concept_name as SOURCE_CONCEPT_NAME,
        o_source_concept.vocabulary_id as SOURCE_VOCABULARY,
    from
        `""" + os.environ["WORKSPACE_CDR"] + """.ds_observation` observation 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_value 
            on observation.value_as_concept_id = o_value.CONCEPT_ID 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_standard_concept 
            on observation.OBSERVATION_CONCEPT_ID = o_standard_concept.CONCEPT_ID 
    LEFT JOIN
        `""" + os.environ["WORKSPACE_CDR"] + """.concept` o_source_concept 
            on observation.OBSERVATION_SOURCE_CONCEPT_ID = o_source_concept.CONCEPT_ID 
    WHERE
        (
            observation_concept_id IN (
                43528428, 1585389, 40766240
            ) 
            OR  value_source_concept_id IN (
                43528428, 1585389, 40766240
            )
        )
"""
all_patients_insurance = pd.read_gbq(all_insurance_sql, dialect="standard")


# In[110]:


all_patients_insurance


# ## Save Datasets

# In[111]:


# Change column types
with_insurance_dfs = [influenza_insurance, hepB_insurance, hpv_insurance, 
                   pneumococcal_under_65_insurance, pneumococcal_65_and_over_insurance, all_patients_insurance]
change_df_types_for_id_suffix(with_insurance_dfs)
for df in with_insurance_dfs:
    df.columns = map(str.lower, df.columns)
# influenza_insurance


# In[112]:


# Save insurance survey datasets
save_df(influenza_insurance, "influenza_insurance")
save_df(hepB_insurance, "hepB_insurance")
save_df(hpv_insurance, "hpv_insurance")
save_df(pneumococcal_under_65_insurance, "pneumococcal_under_65_insurance")
save_df(pneumococcal_65_and_over_insurance, "pneumococcal_65_and_over_insurance")
save_df(all_patients_insurance, "all_patients_insurance")


# # Condition Codes (SNOMED)

# In[17]:


# create sql string for codes
def split_codes(string):
    output_codes = []
    for s in string.split(",\n"):
        output_codes += (s.split(', '))
    return([x.replace('\n', '') for x in output_codes])

# add quotes around each string
def add_quotes(input_list):
    return [ f"'{i}'" for i in input_list]

# create string from list
def list_to_sql_str(x):
    return f"({','.join(x)})"

# combined function to turn input str of codes to sql str
def create_sql_for_code(input_str):
    codes = split_codes(input_str)
    codes_q = add_quotes(codes)
    #print(codes_q)
    codes_s = list_to_sql_str(codes_q)
    return codes_s


# In[38]:


# use sql strs to get concept ids in ICD9CM and ICD10CM
def get_icd9_concept_ids(codes_str):
    query_sql = f"""
        select * from {os.environ["WORKSPACE_CDR"]}.concept
        where vocabulary_id in ("ICD9CM", "ICD9Proc") and 
        concept_code in {codes_str}
        --limit 10
    """
    query_df = pd.read_gbq(query_sql, dialect="standard")
#     display(query_df)
    return query_df
#     return list(query_df['concept_id'].astype(str).values)

# get condition source concept codes for ICD10CM
def get_icd10_concept_ids(codes_str):
    query_sql = f"""
        select * from {os.environ["WORKSPACE_CDR"]}.concept
        where vocabulary_id in ("ICD10CM", "ICD10PCS") and 
        concept_code in {codes_str}
        --limit 10
    """
    query_df = pd.read_gbq(query_sql, dialect="standard")
#     return list(query_df['concept_id'].astype(str).values)
    return query_df
    
    
# get condition source concept codes for DRG and MS-DRG
def get_drg_concept_ids(codes_str):
    query_sql = f"""
        select * from {os.environ["WORKSPACE_CDR"]}.concept
        where vocabulary_id in ("DRG", "MS-DRG") and 
        concept_code in {codes_str}
        --limit 10
    """
    query_df = pd.read_gbq(query_sql, dialect="standard")
#     return list(query_df['concept_id'].astype(str).values)
    return query_df


# In[39]:


# map from ICD9CM and ICD10CM to SNOMED
def map_to_standard(concept_ids_str):
    query_sql = f"""
    with with_relationships as (
        select * from {os.environ["WORKSPACE_CDR"]}.concept_relationship cr
        join {os.environ["WORKSPACE_CDR"]}.concept c
        on cr.concept_id_2 = c.concept_id
        where relationship_id = 'Maps to' and
        concept_id_1 in {concept_ids_str} and concept_id_1 != concept_id_2 and vocabulary_id="SNOMED" 
        and domain_id="Condition"
    )/*,
    with_source_vocab as (
    select distinct concept_id_1 as source_id, concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code 
    from with_relationships wr
    join {os.environ["WORKSPACE_CDR"]}.concept c
    ) on wr.concept_id*/
    
    select distinct concept_id_1 as source_id, concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code 
    from with_relationships wr
    """
    query_df = pd.read_gbq(query_sql, dialect="standard")
    #return list(query_df['concept_id'].astype(str).values)
    return query_df


# In[68]:


# get standard codes
def get_standard_codes(icd9_str, icd10_str, drg_str=None):

    # ICD9CM
    icd9_sql = create_sql_for_code(icd9_str)
    icd9_codes = get_icd9_concept_ids(icd9_sql)
    icd9_concept_ids =  list(icd9_codes['concept_id'].astype(str).values)

    icd10_sql = create_sql_for_code(icd10_str)
    icd10_codes = get_icd10_concept_ids(icd10_sql) 
    icd10_concept_ids =  list(icd10_codes['concept_id'].astype(str).values)
    
    icd9_codes = icd9_codes[['vocabulary_id', 'concept_code', 'concept_id']]
    icd9_codes.columns = ['vocabulary_id', 'concept_code', 'source_id']
    
    icd10_codes = icd10_codes[['vocabulary_id', 'concept_code', 'concept_id']]
    icd10_codes.columns = ['vocabulary_id', 'concept_code', 'source_id']
    
   
    
     # DRG (optional)
    if (drg_str != None):
        drg_sql = create_sql_for_code(drg_str)
        drg_codes = get_drg_concept_ids(drg_sql)
        drg_concept_ids =  list(drg_codes['concept_id'].astype(str).values)
        print(f"DRG with {len(drg_codes)} codes")
        drg_codes = drg_codes[['vocabulary_id', 'concept_code', 'concept_id']]
        drg_codes.columns = ['vocabulary_id', 'concept_code', 'source_id']
        # join ICD codes with drg_codes
        combined = pd.concat([icd9_codes, icd10_codes])
        ICD_codes = drg_concept_ids + icd9_concept_ids + icd10_concept_ids
    else:
         # join ICD9CM codes and ICD10CM codes
        combined = pd.concat([icd9_codes, icd10_codes])
        ICD_codes = icd9_concept_ids + icd10_concept_ids
        
    #with_quotes = add_quotes(ICD_codes)
    ICD_codes_sql = list_to_sql_str(ICD_codes)
#     print(ICD_codes_sql)
#     display(combined)
    standard_concepts = map_to_standard(ICD_codes_sql)#.drop("source_id", axis=1).drop_duplicates()
    standard_concepts = standard_concepts.drop(['concept_code', 'vocabulary_id'], axis=1)
#     display(standard_concepts)
    with_concept_code = standard_concepts.merge(combined, how="left", on="source_id").drop('source_id', axis=1)
#     display(with_concept_code)
    return with_concept_code

#     return standard_concepts


# In[42]:


def remove_codes(codes_df, list_of_codes):
    df = codes_df.copy()
#     list_of_codes_str = [str(x) for x in list_of_codes]
    df = df[~df.concept_id.isin(list_of_codes)]
    return df


# ## Hypertension

# In[71]:


# Hypertension Standard Concepts
condition_name = "hypertension"
exec_str = f"""
{condition_name}_icd9_str = '''
362.11, 401.0,
401.1, 401.9,
402.00, 402.01,
402.10, 402.11,
402.90, 402.91,
403.00, 403.01,
403.10, 403.11,
403.90, 403.91,
404.00, 404.01,
404.02, 404.03,
404.10, 404.11,
404.12, 404.13,
404.90, 404.91,
404.92, 404.93,
405.01, 405.09,
405.11, 405.19,
405.91, 405.99,
437.2
'''
{condition_name}_icd10_str ='''
H35.031, H35.032, H35.033, H35.039, I10, I11.0, I11.9, I12.0, I12.9, I13.0, I13.10,
I13.11, I13.2, I15.0, I15.1, I15.2, I15.8, I15.9, I67.4, N26.2
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Congestive Heart Failure

# In[72]:


# Heart Failure Standard Concepts
condition_name = "heart_failure"
exec_str = f"""
{condition_name}_icd9_str = '''
398.91,
402.01, 402.11,
402.91, 404.01,
404.03, 404.11,
404.13, 404.91,
404.93, 428.0,
428.1, 428.20,
428.21, 428.22,
428.23, 428.30,
428.31, 428.32,
428.33, 428.40,
428.41, 428.42,
428.43, 428.9
'''
{condition_name}_icd10_str ='''
I09.81, I11.0, I13.0, I13.2, I50.1, I50.20, I50.21, I50.22, I50.23, I50.30, I50.31,
I50.32, I50.33, I50.40, I50.41, I50.42, I50.43, I50.810, I50.811, I50.812, I50.813,
I50.814, I50.82, I50.83, I50.84, I50.89, I50.9
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Coronary Artery Disease

# In[73]:


# Ischemic Heart Disease Standard Concepts
condition_name = "ischemic_heart"
exec_str = f"""
{condition_name}_icd9_str = '''
410.00,
410.01, 410.02,
410.10, 410.11,
410.12, 410.20,
410.21, 410.22,
410.30, 410.31,
410.32, 410.40,
410.41, 410.42,
410.50, 410.51,
410.52, 410.60,
410.61, 410.62,
410.70, 410.71,
410.72, 410.80,
410.81, 410.82,
410.90, 410.91,
410.92, 411.0,
411.1, 411.81,
411.89, 412,
413.0, 413.1,
413.9, 414.00,
414.01, 414.02,
414.03, 414.04,
414.05, 414.06,
414.07, 414.12,
414.2, 414.3,
414.4, 414.8,
414.9
'''
{condition_name}_icd10_str ='''
I20.0, I20.1, I20.8, I20.9, I21.01, I21.02, I21.09, I21.11, I21.19, I21.21, I21.29, I21.3,
I21.4, I21.A1, I21.A9, I22.0, I22.1, I22.2, I22.8, I22.9, I23.0, I23.1, I23.2, I23.3, I23.4,
I23.5, I23.6, I23.7, I23.8, I24.0, I24.1, I24.8, I24.9, I25.10, I25.110, I25.111, I25.118,
I25.119, I25.2, I25.3, I25.41, I25.42, I25.5, I25.6, I25.700, I25.701, I25.708, I25.709,
I25.710, I25.711, I25.718, I25.719, I25.720, I25.721, I25.728, I25.729, I25.730, I25.731,
I25.738, I25.739, I25.750, I25.751, I25.758, I25.759, I25.760, I25.761, I25.768, I25.769,
I25.790, I25.791, I25.798, I25.799, I25.810, I25.811, I25.812, I25.82, I25.83, I25.84,
I25.89, I25.9
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Cardiac Arrhythmias

# In[74]:


# Atrial Fibrillation Standard Concepts
condition_name = "atrial_fibrillation"
exec_str = f"""
{condition_name}_icd9_str = '''
427.31
'''
{condition_name}_icd10_str ='''
I48.0, I48.1, I48.11, I48.19, I48.2, I48.20, I48.21, I48.91
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Hyperlipidemia

# In[75]:


# Hyperlipidemia Standard Concepts
condition_name = "hyperlipidemia"
exec_str = f"""
{condition_name}_icd9_str = '''
272.0, 272.1,
272.2, 272.3,
272.4
'''
{condition_name}_icd10_str ='''
E78.0, E78.00, E78.01, E78.1, E78.2, E78.3, E78.4, E78.41, E78.49, E78.5
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Stroke

# In[76]:


# Stroke Standard Concepts
condition_name = "stroke"
exec_str = f"""
{condition_name}_icd9_str = '''
430, 431,
433.01, 433.11,
433.21, 433.31,
433.81, 433.91,
434.00, 434.01,
434.10, 434.11,
434.90, 434.91,
435.0, 435.1,
435.3, 435.8,
435.9, 436, 997.02 
'''
{condition_name}_icd10_str ='''
G45.0, G45.1, G45.2, G45.8, G45.9, G46.0, G46.1, G46.2, G46.3, G46.4, G46.5,
G46.6, G46.7, G46.8, G97.31, G97.32, I60.00, I60.01, I60.02, I60.10, I60.11, I60.12,
I60.20, I60.21, I60.22, I60.30, I60.31, I60.32, I60.4, I60.50, I60.51, I60.52, I60.6, I60.7,
I60.8, I60.9, I61.0, I61.1, I61.2, I61.3, I61.4, I61.5, I61.6, I61.8, I61.9, I63.00, I63.02,
I63.011, I63.012, I63.013, I63.019, I63.02, I63.031, I63.032, I63.039, I63.09, I63.10,
I63.111, I63.112, I63.119, I63.12, I63.131, I63.132, I63.139, I63.19, I63.20, I63.211,
I63.212, I63.213, I63.219, I63.22, I63.231, I63.232, I63.233, I63.239, I63.29, I63.30,
I63.311, I63.312, I63.313, I63.319, I63.321, I63.322, I63.323, I63.329, I63.331, I63.332,
I63.333, I63.339, I63.341, I63.342, I63.343, I63.349, I63.39, I63.40, I63.411, I63.412,
I63.413, I63.419, I63.421, I63.422, I63.423, I63.429, I63.431, I63.432, I63.433, I63.439,
I63.441, I63.442, I63.443, I63.449, I63.49, I63.50, I63.511, I63.512, I63.513, I63.519,
I63.521, I63.522, I63.523, I63.529, I63.531, I63.532, I63.533, I63.539, I63.541, I63.542,
I63.543, I63.549, I63.59, I63.6, I63.8, I63.9, I66.01, I66.02, I66.03, I66.09, I66.11,
I66.12, I66.13, I66.19, I66.21, I66.22, I66.23, I66.29, I66.3, I66.8, I66.9, I67.841,
I67.848, I67.89, I97.810, I97.811, I97.820, I97.821


'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[77]:


condition_name = "stroke"
exec_str = f"""{condition_name}_remove_codes = [
435141,376106
]
before = len({condition_name}_standard_concepts)
{condition_name}_standard_concepts =  remove_codes({condition_name}_standard_concepts, {condition_name}_remove_codes)
display({condition_name}_standard_concepts.head(5))
print("Removal: ", before, " --> ", len({condition_name}_standard_concepts))
"""

exec(exec_str)


# ## Arthritis

# In[78]:


# Arthritis Standard Concepts
condition_name = "arthritis"
exec_str = f"""
{condition_name}_icd9_str = '''
714.0, 714.1,
714.2, 714.30,
714.31, 714.32,
714.33, 715.00,
715.04, 715.09,
715.10, 715.11,
715.12, 715.13,
715.14, 715.15,
715.16, 715.17,
715.18, 715.20,
715.21, 715.22,
715.23, 715.24,
715.25, 715.26,
715.27, 715.28,
715.30, 715.31,
715.32, 715.33,
715.34, 715.35,
715.36, 715.37,
715.38, 715.80,
715.89, 715.90,
715.91, 715.92,
715.93, 715.94,
715.95, 715.96,
715.97, 715.98,
720.0, 721.0,
721.1, 721.2,
721.3, 721.90,
721.91
'''
{condition_name}_icd10_str ='''
M05.00, M05.011, M05.012, M05.019, M05.021, M05.022, M05.029, M05.031,
M05.032, M05.039, M05.041, M05.042, M05.049, M05.051, M05.052, M05.059,
M05.061, M05.062, M05.069, M05.071, M05.072, M05.079, M05.09, M05.20,
M05.211, M05.212, M05.219, M05.221, M05.222, M05.229, M05.231, M05.232,
M05.239, M05.241, M05.242, M05.249, M05.251, M05.252, M05.259, M05.261,
M05.262, M05.269, M05.271, M05.272, M05.279, M05.29, M05.30, M05.311,
M05.312, M05.319, M05.321, M05.322, M05.329, M05.331, M05.332, M05.339,
M05.341, M05.342, M05.349, M05.351, M05.352, M05.359, M05.361, M05.362,
M05.369, M05.371, M05.372, M05.379, M05.39, M05.40, M05.411, M05.412,
M05.419, M05.421, M05.422, M05.429, M05.431, M05.432, M05.439, M05.441,
M05.442, M05.449, M05.451, M05.452, M05.459, M05.461, M05.462, M05.469,
M05.471, M05.472, M05.479, M05.49, M05.50, M05.511, M05.512, M05.519,
M05.521, M05.522, M05.529, M05.531, M05.532, M05.539, M05.541, M05.542,
M05.549, M05.551, M05.552, M05.559, M05.561, M05.562, M05.569, M05.571,
M05.572, M05.579, M05.59, M05.60, M05.611, M05.612, M05.619, M05.621,
M05.622, M05.629, M05.631, M05.632, M05.639, M05.641, M05.642, M05.649,
M05.651, M05.652, M05.659, M05.661, M05.662, M05.669, M05.671, M05.672,
M05.679, M05.69, M05.70, M05.711, M05.712, M05.719, M05.721, M05.722,
M05.729, M05.731, M05.732, M05.739, M05.741, M05.742, M05.749, M05.751,
M05.752, M05.759, M05.761, M05.762, M05.769, M05.771, M05.772, M05.779,
M05.79, M05.80, M05.811, M05.812, M05.819, M05.821, M05.822, M05.829,
M05.831, M05.832, M05.839, M05.841, M05.842, M05.849, M05.851, M05.852,
M05.859, M05.861, M05.862, M05.869, M05.871, M05.872, M05.879, M05.89,
M05.9, M06.00, M06.011, M06.012, M06.019, M06.021, M06.022, M06.029,
M06.031, M06.032, M06.039, M06.041, M06.042, M06.049, M06.051, M06.052,
M06.059, M06.061, M06.062, M06.069, M06.071, M06.072, M06.079, M06.08,
M06.09, M06.1, M06.20, M06.211, M06.212, M06.219, M06.221, M06.222, M06.229,
M06.231, M06.232, M06.239, M06.241, M06.242, M06.249, M06.251, M06.252,
M06.259, M06.261, M06.262, M06.269, M06.271, M06.272, M06.279, M06.28,
M06.29, M06.30, M06.311, M06.312, M06.319, M06.321, M06.322, M06.329, 

M06.331, M06.332, M06.339, M06.341, M06.342, M06.349, M06.351, M06.352,
M06.359, M06.361, M06.362, M06.369, M06.371, M06.372, M06.379, M06.38,
M06.39, M06.80, M06.811, M06.812, M06.819, M06.821, M06.822, M06.829,
M06.831, M06.832, M06.839, M06.841, M06.842, M06.849, M06.851, M06.852,
M06.859, M06.861, M06.862, M06.869, M06.871, M06.872, M06.879, M06.88,
M06.89, M06.9, M08.00, M08.011, M08.012, M08.019, M08.021, M08.022, M08.029,
M08.031, M08.032, M08.039, M08.041, M08.042, M08.049, M08.051, M08.052,
M08.059, M08.061, M08.062, M08.069, M08.071, M08.072, M08.079, M08.08,
M08.09, M08.1, M08.20, M08.211, M08.212, M08.219, M08.221, M08.222, M08.229,
M08.231, M08.232, M08.239, M08.241, M08.242, M08.249, M08.251, M08.252,
M08.259, M08.261, M08.262, M08.269, M08.271, M08.272, M08.279, M08.28,
M08.29, M08.3, M08.40, M08.411, M08.412, M08.419, M08.421, M08.422, M08.429,
M08.431, M08.432, M08.439, M08.441, M08.442, M08.449, M08.451, M08.452,
M08.459, M08.461, M08.462, M08.469, M08.471, M08.472, M08.479, M08.48,
M08.80, M08.811, M08.812, M08.819, M08.821, M08.822, M08.829, M08.831,
M08.832, M08.839, M08.841, M08.842, M08.849, M08.851, M08.852, M08.859,
M08.861, M08.862, M08.869, M08.871, M08.872, M08.879, M08.88, M08.89,
M08.90, M08.911, M08.912, M08.919, M08.921, M08.922, M08.929, M08.931,
M08.932, M08.939, M08.941, M08.942, M08.949, M08.951, M08.952, M08.959,
M08.961, M08.962, M08.969, M08.971, M08.972, M08.979, M08.98, M08.99, M15.0,
M15.1, M15.2, M15.3, M15.4, M15.8, M15.9, M16.0, M16.10, M16.11, M16.12,
M16.2, M16.30, M16.31, M16.32, M16.4, M16.50, M16.51, M16.52, M16.6, M16.7,
M16.9, M17.0, M17.10, M17.11, M17.12, M17.2, M17.30, M17.31, M17.32, M17.4,
M17.5, M17.9, M18.0, M18.10, M18.11, M18.12, M18.2, M18.30, M18.31, M18.32,
M18.4, M18.50, M18.51, M18.52, M18.9, M19.011, M19.012, M19.019, M19.021,
M19.022, M19.029, M19.031, M19.032, M19.039, M19.041, M19.042, M19.049,
M19.071, M19.072, M19.079, M19.111, M19.112, M19.119, M19.121, M19.122,
M19.129, M19.131, M19.132, M19.139, M19.141, M19.142, M19.149, M19.171,
M19.172, M19.179, M19.211, M19.212, M19.219, M19.221, M19.222, M19.229,
M19.231, M19.232, M19.239, M19.241, M19.242, M19.249, M19.271, M19.272,
M19.279, M19.90, M19.91, M19.92, M19.93, M45.0, M45.1,

M45.2, M45.3, M45.4, M45.5, M45.6, M45.7, M45.8, M45.9, M47.011, M47.012,
M47.013, M47.014, M47.015, M47.016, M47.019, M47.021, M47.022, M47.029,
M47.10, M47.11, M47.12, M47.13, M47.20, M47.21, M47.22, M47.23, M47.24,
M47.25, M47.26, M47.27, M47.28, M47.811, M47.812, M47.813, M47.814, M47.815,
M47.816, M47.817, M47.818, M47.819, M47.891, M47.892, M47.893, M47.894,
M47.895, M47.896, M47.897, M47.898, M47.899, M47.9, M48.8X1, M48.8X2,
M48.8X3, M48.8X4, M48.8X5, M48.8X6, M48.8X7, M48.8X8, M48.8X9
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Asthma

# In[79]:


# Asthma Standard Concepts
condition_name = "asthma"
exec_str = f"""
{condition_name}_icd9_str = '''
493.00,
493.01, 493.02,
493.10, 493.11,
493.12, 493.20,
493.21, 493.22,
493.81, 493.82,
493.90, 493.91,
493.92,
'''
{condition_name}_icd10_str ='''
J45.20, J45.21, J45.22, J45.30, J45.31, J45.32, J45.40, J45.41, J45.42, J45.50, J45.51,
J45.52, J45.901, J45.902, J45.909, J45.990, J45.991, J45.998
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Autism Spectrum Disorder

# In[80]:


#  Autism Standard Concepts
autism_icd9_str = """
299.0, 299.00, 299.01, 299.1,
299.11, 299.8, 299.80, 299.81, 299.9,
299.90, 299.91
"""

autism_icd10_str = """
 F84.0, F84.3, F84.5, F84.8, F84.9
"""

autism_standard_concepts = get_standard_codes(autism_icd9_str, autism_icd10_str )
display(autism_standard_concepts.head(5))


# ## Cancer

# In[81]:


# Leukemia Standard Concepts: leukemia
condition_name = "leukemia"
exec_str = f"""
{condition_name}_icd9_str = '''
200.0, 200.00, 200.01, 200.02,
200.03, 200.04, 200.05, 200.06, 200.07,
200.08, 200.1, 200.10, 200.11, 200.12,
200.13, 200.14, 200.15, 200.16, 200.17,
200.18, 200.2, 200.20, 200.21, 200.22,
200.23, 200.24, 200.25, 200.26, 200.27,
200.28, 200.3, 200.30, 200.31, 200.32,
200.33, 200.34, 200.35, 200.36, 200.37,
200.38, 200.4, 200.40, 200.41, 200.42,
200.43, 200.44, 200.45, 200.46, 200.47,
200.48, 200.5, 200.50, 200.51, 200.52,
200.53, 200.54, 200.55, 200.56, 200.57,
200.58, 200.6, 200.60, 200.61, 200.62,

200.63, 200.64, 200.65, 200.66, 200.67,
200.68, 200.7, 200.70, 200.71, 200.72,
200.73, 200.74, 200.75, 200.76, 200.77,
200.78, 200.8, 200.80, 200.81, 200.82,
200.83, 200.84, 200.85, 200.86, 200.87,
200.88, 201.0, 201.00, 201.01, 201.02,
201.03, 201.04, 201.05, 201.06, 201.07,
201.08, 201.1, 201.10, 201.11, 201.12,
201.13, 201.14, 201.15, 201.16, 201.17,
201.18, 201.2, 201.20, 201.21, 201.22,
201.23, 201.24, 201.25, 201.26, 201.27,
201.28, 201.4, 201.40, 201.41, 201.42,
201.43, 201.44, 201.45, 201.46, 201.47,
201.48, 201.5, 201.50, 201.51, 201.52,
201.53, 201.54, 201.55, 201.56, 201.57,
201.58, 201.6, 201.60, 201.61, 201.62,
201.63, 201.64, 201.65, 201.66, 201.67,
201.68, 201.7, 201.70, 201.71, 201.72,
201.73, 201.74, 201.75, 201.76, 201.77,
201.78, 201.9, 201.90, 201.91, 201.92,
201.93, 201.94, 201.95, 201.96, 201.97,
201.98, 202.0, 202.00, 202.01, 202.02,
202.03, 202.04, 202.05, 202.06, 202.07,
202.08, 202.1, 202.10, 202.11, 202.12,
202.13, 202.14, 202.15, 202.16, 202.17,
202.18, 202.2, 202.20, 202.21, 202.22,
202.23, 202.24, 202.25, 202.26, 202.27,
202.28, 202.4, 202.40, 202.41, 202.42,
202.43, 202.44, 202.45, 202.46, 202.47,

202.48, 202.7, 202.70, 202.71, 202.72,
202.73, 202.74, 202.75, 202.76, 202.77,
202.78, 202.8, 202.80, 202.81, 202.82,
202.83, 202.84, 202.85, 202.86, 202.87,
202.88, 202.9, 202.90, 202.91, 202.92,
202.93, 202.94, 202.95, 202.96, 202.97,
202.98, 203.1, 203.10, 203.11, 203.12,
204.0, 204.00, 204.01, 204.02, 204.1,
204.10, 204.11, 204.12, 204.2, 204.20,
204.21, 204.22, 204.8, 204.80, 204.81,
204.82, 204.9, 204.90, 204.91, 204.92,
205.0, 205.00, 205.01, 205.02, 205.1,
205.10, 205.11, 205.12, 205.2, 205.20,
205.21, 205.22, 205.3, 205.30, 205.31,
205.32, 205.8, 205.80, 205.81, 205.82,
205.9, 205.90, 205.91, 205.92, 206.0,
206.00, 206.01, 206.02, 206.1, 206.10,
206.11, 206.12, 206.2, 206.20, 206.21,
206.22, 206.2, 206.20, 206.21, 206.22,
206.8, 206.80, 206.81, 206.82, 206.9,
206.90, 206.91, 206.92, 207.0, 207.00,
207.01, 207.02, 207.1, 207.10, 207.11,
207.12, 207.2, 207.20, 207.21, 207.22,
207.8, 207.80, 207.81, 207.82, 208.0,
208.00, 208.01, 208.02, 208.1, 208.10,
208.11, 208.12, 208.2, 208.20, 208.21,
208.22, 208.8, 208.80, 208.81, 208.82,
208.9, 208.90, 208.91, 208.92, V10.6,
V10.60, V10.61, V10.62, V10.63,
V10.69, V10.7, V10.71, V10.72, V10.79
'''
{condition_name}_icd10_str ='''
C81.00, C81.01, C81.02, C81.03, C81.04, C81.05, C81.06,
C81.07, C81.08, C81.09, C81.10, C81.11, C81.12, C81.13, C81.14,
C81.15, C81.16, C81.17, C81.18, C81.19, C81.20, C81.21, C81.22,
C81.23, C81.24, C81.25, C81.26, C81.27, C81.28, C81.29, C81.30,
C81.31, C81.32, C81.33, C81.34, C81.35, C81.36, C81.37, C81.38,
C81.39, C81.40, C81.41, C81.42, C81.43, C81.44, C81.45, C81.46,
C81.47, C81.48, C81.49, C81.70, C81.71, C81.72, C81.73, C81.74,
C81.75, C81.76, C81.77, C81.78, C81.79, C81.90, C81.91, C81.92,
C81.93, C81.94, C81.95, C81.96, C81.97, C81.98, C81.99, C82.00,
C82.01, C82.02, C82.03, C82.04, C82.05, C82.06, C82.07, C82.08,
C82.09, C82.10, C82.11, C82.12, C82.13, C82.14, C82.15, C82.16,
C82.17, C82.18, C82.19, C82.20, C82.21, C82.22, C82.23, C82.24,
C82.25, C82.26, C82.27, C82.28, C82.29, C82.30, C82.31, C82.32,

C82.33, C82.34, C82.35, C82.36, C82.37, C82.38, C82.39, C82.40,
C82.41, C82.42, C82.43, C82.44, C82.45, C82.46, C82.47, C82.48,
C82.49, C82.50, C82.51, C82.52, C82.53, C82.54, C82.55, C82.56,
C82.57, C82.58, C82.59, C82.60, C82.61, C82.62, C82.63, C82.64,
C82.65, C82.66, C82.67, C82.68, C82.69, C82.80, C82.81, C82.82,
C82.83, C82.84, C82.85, C82.86, C82.87, C82.88, C82.89, C82.90,
C82.91, C82.92, C82.93, C82.94, C82.95, C82.96, C82.97, C82.98,
C82.99, C83.00, C83.01, C83.02, C83.03, C83.04, C83.05, C83.06,
C83.07, C83.08, C83.09, C83.10, C83.11, C83.12, C83.13, C83.14,
C83.15, C83.16, C83.17, C83.18, C83.19, C83.30, C83.31, C83.32,
C83.33, C83.34, C83.35, C83.36, C83.37, C83.38, C83.39, C83.50,
C83.51, C83.52, C83.53, C83.54, C83.55, C83.56, C83.57, C83.58,
C83.59, C83.70, C83.71, C83.72, C83.73, C83.74, C83.75, C83.76,
C83.77, C83.78, C83.79, C83.80, C83.81, C83.82, C83.83, C83.84,
C83.85, C83.86, C83.87, C83.88, C83.89, C83.90, C83.91, C83.92,
C83.93, C83.94, C83.95, C83.96, C83.97, C83.98, C83.99, C84.00,
C84.01, C84.02, C84.03, C84.04, C84.05, C84.06, C84.07, C84.08,
C84.09, C84.10, C84.11, C84.12, C84.13, C84.14, C84.15, C84.16,
C84.17, C84.18, C84.19, C84.40, C84.41, C84.42, C84.43, C84.44,
C84.45, C84.46, C84.47, C84.48, C84.49, C84.60, C84.61, C84.62,
C84.63, C84.64, C84.65, C84.66, C84.67, C84.68, C84.69, C84.70,
C84.71, C84.72, C84.73, C84.74, C84.75, C84.76, C84.77, C84.78,
C84.79, C84.90, C84.91, C84.92, C84.93, C84.94, C84.95, C84.96,
C84.97, C84.98, C84.99, C84.A0, C84.A1, C84.A2, C84.A3,
C84.A4, C84.A5, C84.A6, C84.A7, C84.A8, C84.A9, C84.Z0,
C84.Z1, C84.Z2, C84.Z3, C84.Z4, C84.Z5, C84.Z6, C84.Z7, C84.Z8,
C84.Z9, C85.10, C85.11, C85.12, C85.13, C85.14, C85.15, C85.16,
C85.17, C85.18, C85.19, C85.20, C85.21, C85.22, C85.23, C85.24,
C85.25, C85.26, C85.27, C85.28, C85.29, C85.80, C85.81, C85.82, 

C85.83, C85.84, C85.85, C85.86, C85.87, C85.88, C85.89, C85.90,
C85.91, C85.92, C85.93, C85.94, C85.95, C85.96, C85.97, C85.98,
C85.99, C86.0, C86.1, C86.2, C86.3, C86.4, C86.5, C86.6, C88.4,
C90.10, C90.11, C90.12, C91.00, C91.01, C91.02, C91.10, C91.11,
C91.12, C91.30, C91.31, C91.32, C91.40, C91.41, C91.42, C91.50,
C91.51, C91.52, C91.60, C91.61, C91.62, C91.A0, C91.A1,
C91.A2, C91.Z0, C91.Z1, C91.Z2, C91.90, C91.91, C91.92, C92.00,
C92.01, C92.02, C92.10, C92.11, C92.12, C92.20, C92.21, C92.22,
C92.30, C92.31, C92.32, C92.40, C92.41, C92.42, C92.50, C92.51,
C92.52, C92.60, C92.61, C92.62, C92.90, C92.91, C92.92, C92.A0,
C92.A1, C92.A2, C92.Z0, C92.Z1, C92.Z2, C93.00, C93.01, C93.02,
C93.10, C93.11, C93.12, C93.30, C93.31, C93.32, C93.Z0, C93.Z1,
C93.Z2, C93.90, C93.91, C93.92, C94.00, C94.01, C94.02, C94.20,
C94.21, C94.22, C94.30, C94.31, C94.32, C94.80, C94.81, C94.82,
C95.00, C95.01, C95.02, C95.10, C95.11, C95.12, C95.90, C95.91,
C95.92, C96.4, C96.9, C96.Z, D45, Z85.6, Z85.71, Z85.79 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[82]:


# Breast Cancer Standard Concepts: breast_cancer
condition_name = "breast_cancer"
exec_str = f"""
{condition_name}_icd9_str = '''
174.0, 174.1,
174.2, 174.3,
174.4, 174.5,
174.6, 174.8,
174.9, 175.0,
175.9, 233.0,
V10.3
'''
{condition_name}_icd10_str ='''
C50.011, C50.012, C50.019, C50.021, C50.022, C50.029, C50.111, C50.112,
C50.119, C50.121, C50.122, C50.129, C50.211, C50.212, C50.219, C50.221, C50.222,
C50.229, C50.311, C50.312, C50.319, C50.321, C50.322, C50.329, C50.411, C50.412,
C50.419, C50.421, C50.422, C50.429, C50.511, C50.512, C50.519, C50.521, C50.522,
C50.529, C50.611, C50.612, C50.619, C50.621, C50.622, C50.629, C50.811, C50.812,
C50.819, C50.821, C50.822, C50.829, C50.911, C50.912, C50.919, C50.921, C50.922,
C50.929, D05.00, D05.01, D05.02, D05.10, D05.11, D05.12, D05.80, D05.81, D05.82,
D05.90, D05.91, D05.92, Z85.3
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[83]:


# Colorectal Cancer Standard Concepts: colorectal_cancer
condition_name = "colorectal_cancer"
exec_str = f"""
{condition_name}_icd9_str = '''
153.0, 153.1,
153.2, 153.3,
153.4, 153.5,
153.6, 153.7,
153.8,
153.9,154.0,154.1,
230.3, 230.4,
V10.05, V10.06
'''
{condition_name}_icd10_str ='''
C18.0, C18.1, C18.2, C18.3, C18.4, C18.5, C18.6, C18.7, C18.8, C18.9, C19, C20,
D01.0, D01.1, D01.2, Z85.038, Z85.040, Z85.048
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[84]:


# Prostate Cancer Standard Concepts: prostate_cancer
condition_name = "prostate_cancer"
exec_str = f"""
{condition_name}_icd9_str = '''
185, 233.4,
V10.46
'''
{condition_name}_icd10_str ='''
C61, D07.5, Z85.46
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[85]:


# Lung Cancer Standard Concepts: lung_cancer
condition_name = "lung_cancer"
exec_str = f"""
{condition_name}_icd9_str = '''
162.2, 162.3,
162.4, 162.5,
162.8, 162.9,
231.2, V10.11
'''
{condition_name}_icd10_str ='''
C34.00, C34.01, C34.02, C34.10, C34.11, C34.12, C34.2, C34.30, C34.31, C34.32,
C34.80, C34.81, C34.82, C34.90, C34.91, C34.92, D02.20, D02.21, D02.22, Z85.110,
Z85.118 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[86]:


# Endometrial Cancer Standard Concepts: endometrial
condition_name = "endometrial"
exec_str = f"""
{condition_name}_icd9_str = '''
182.0, 233.2,
V10.42
'''
{condition_name}_icd10_str ='''
C54.1, C54.2, C54.3, C54.8, C54.9, D07.0, Z85.42
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[87]:


# Group cancers
cancer_names = [
    "leukemia", "breast_cancer", "colorectal_cancer", "prostate_cancer", "lung_cancer", "endometrial"
]
#cancer_standard_concepts = leukemia_standard_concepts;
list_of_cancer_dfs = []
for cancer in cancer_names:
    #exec(f"display({cancer}_standard_concepts)")
    #exec(f"print(len({cancer}_standard_concepts))")
    exec(f"list_of_cancer_dfs.append({cancer}_standard_concepts)")
    
cancer_standard_concepts = pd.concat(list_of_cancer_dfs)
display(cancer_standard_concepts)


# ## Chronic Kidney Disease

# In[88]:


# Chronic Kidney Disease Standard Concepts
condition_name = "chronic_kidney_disease"
exec_str = f"""
{condition_name}_icd9_str = '''
016.00,
016.01, 016.02,
016.03, 016.04,
016.05, 016.06,
095.4, 189.0,
189.9, 223.0,
236.91, 249.40,
249.41, 250.40,
250.41, 250.42,
250.43, 271.4,
274.10, 283.11,
403.01, 403.11,
403.91, 404.02,

404.03, 404.12,
404.13, 404.92,
404.93, 440.1,
442.1, 572.4,
580.0, 580.4,
580.81, 580.89,
580.9, 581.0,
581.1, 581.2,
581.3, 581.81,
581.89, 581.9,
582.0, 582.1,
582.2, 582.4,
582.81, 582.89,
582.9, 583.0,
583.1, 583.2,
583.4, 583.6,
583.7, 583.81,
583.89, 583.9,
584.5, 584.6,
584.7, 584.8,
584.9, 585.1,
585.2, 585.3,
585.4, 585.5,
585.6, 585.9, 586,
587, 588.0, 588.1,
588.81, 588.89,
588.9, 591,
753.12, 753.13,
753.14, 753.15,
753.16, 753.17,
753.19, 753.20,
753.21, 753.22,
753.23, 753.29,
794.4
'''
{condition_name}_icd10_str ='''
DX A18.11, A52.75, B52.0, C64.1, C64.2, C64.9, C68.9, D30.00, D30.01, D30.02, D41.00,
D41.01, D41.02, D41.10, D41.11, D41.12, D41.20, D41.21, D41.22, D59.3, E08.21,
E08.22, E08.29, E08.65, E09.21, E09.22, E09.29, E10.21, E10.22, E10.29, E10.65,
E11.21, E11.22, E11.29, E11.65, E13.21, E13.22, E13.29, E74.8, I12.0, I12.9, I13.0,
I13.10, I13.11, I13.2, I70.1, I72.2, K76.7, M10.30, M10.311, M10.312, M10.319,
M10.321, M10.322, M10.329, M10.331, M10.332, M10.339, M10.341, M10.342,
M10.349, M10.351, M10.352, M10.359, M10.361, M10.362, M10.369, M10.371,
M10.372, M10.379, M10.38, M10.39, M32.14, M32.15, M35.04, N00.0, N00.1, N00.2,
N00.3, N00.4, N00.5, N00.6, N00.7, N00.8, N00.9, N01.0, N01.1, N01.2, N01.3, N01.4,
N01.5, N01.6, N01.7, N01.8, N01.9, N02.0, N02.1, N02.2, N02.3, N02.4, N02.5, N02.6,
N02.7, N02.8, N02.9, N03.0, N03.1, N03.2, N03.3, N03.4, N03.5, N03.6, N03.7, N03.8,
N03.9, N04.0, N04.1, N04.2, N04.3, N04.4, N04.5, N04.6, N04.7, N04.8, N04.9, N05.0,
N05.1, N05.2, N05.3, N05.4, N05.5, N05.6, N05.7, N05.8, N05.9, N06.0, N06.1, N06.2,
N06.3, N06.4, N06.5,


N06.6, N06.7, N06.8, N06.9, N07.0, N07.1, N07.2, N07.3, N07.4, N07.5, N07.6, N07.7,
N07.8, N07.9, N08, N13.1, N13.2, N13.30, N13.39, N14.0, N14.1, N14.2, N14.3, N14.4,
N15.0, N15.8, N15.9, N16, N17.0, N17.1, N17.2, N17.8, N17.9, N18.1, N18.2, N18.3,
N18.4, N18.5, N18.6, N18.9, N19, N25.0, N25.1, N25.81, N25.89, N25.9, N26.1, N26.9,
Q61.02, Q61.11, Q61.19, Q61.2, Q61.3, Q61.4, Q61.5, Q61.8, Q62.0, Q62.2, Q62.10,
Q62.11, Q62.12, Q62.31, Q62.32, Q62.39, R94.4
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[89]:


exec_str = f"""{condition_name}_remove_codes = [
201826,37016349,443238,442793,201254,4177206,
195771,254443,37016348,40484648,437992,436033,437515,4202383,4113451,4029420

]
before = len({condition_name}_standard_concepts)
{condition_name}_standard_concepts =  remove_codes({condition_name}_standard_concepts, {condition_name}_remove_codes)
display({condition_name}_standard_concepts.head(5))
print("Removal: ", before, " --> ", len({condition_name}_standard_concepts))
"""

exec(exec_str)


# In[90]:


# chronic_kidney_disease_standard_concepts[chronic_kidney_disease_standard_concepts.concept_id==4113451]


# ## Chronic Obstructive Pulmonary Disease

# In[91]:


# Chronic Pulmonary Disease Standard Concepts
condition_name = "chronic_pulmonary_disease"
exec_str = f"""
{condition_name}_icd9_str = '''
490, 491.0,
491.1, 491.8,
491.9, 492.0,
492.8, 491.20,
491.21, 491.22,
494.0, 494.1, 496 
'''
{condition_name}_icd10_str ='''
J40, J41.0, J41.1, J41.8, J42, J43.0, J43.1, J43.2, J43.8, J43.9, J44.0, J44.1, J44.9,
J47.0, J47.1, J47.9 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Dementia

# In[92]:


# Alzheimer's Standard Concepts
condition_name = "alzheimers"
exec_str = f"""
{condition_name}_icd9_str = '''
331.0, 331.11,
331.19, 331.2,
331.7, 290.0,
290.10, 290.11,
290.12, 290.13,
290.20, 290.21,
290.3, 290.40,
290.41, 290.42,
290.43, 294.0,
294.10, 294.11,
294.20, 294.21,
294.8, 797
'''
{condition_name}_icd10_str ='''
DX F01.50, F01.51, F02.80, F02.81, F03.90, F03.91, F04, G13.8, F05, F06.1, F06.8,
G30.0, G30.1, G30.8, G30.9, G31.1, G31.2, G31.01, G31.09, G94, R41.81, R54 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[93]:


condition_name = "alzheimers"
exec_str = f"""{condition_name}_remove_codes = [
372887,374009
]
before = len({condition_name}_standard_concepts)
{condition_name}_standard_concepts =  remove_codes({condition_name}_standard_concepts, {condition_name}_remove_codes)
display({condition_name}_standard_concepts.head(5))
print("Removal: ", before, " --> ", len({condition_name}_standard_concepts))
"""

exec(exec_str)


# ## Depression

# In[94]:


# Depression Standard Concepts
condition_name = "depression"
exec_str = f"""
{condition_name}_icd9_str = '''
296.20,
296.21, 296.22,
296.23, 296.24,
296.25, 296.26,
296.30, 296.31,
296.32, 296.33,
296.34, 296.35,
296.36, 296.51,
296.52, 296.53,
296.54, 296.55,
296.56, 296.60,
296.61, 296.62,
296.63, 296.64,
296.65, 296.66,
296.89, 298.0,
300.4, 309.1, 311 
'''
{condition_name}_icd10_str ='''
F31.30, F31.31, F31.32, F31.4, F31.5, F31.60, F31.61, F31.62, F31.63, F31.64,
F31.75, F31.76, F31.77, F31.78, F31.81, F32.0, F32.1, F32.2, F32.3, F32.4, F32.5, F32.9,
F33.0, F33.1, F33.2, F33.3, F33.40, F33.41, F33.42, F33.8, F33.9, F34.1, F43.21, F43.23
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Diabetes

# In[95]:


# Diabetes Standard Concepts
condition_name = "diabetes"
exec_str = f"""
{condition_name}_icd9_str = '''
249.00,
249.01, 249.10,
249.11, 249.20,
249.21, 249.30,
249.31, 249.40,
249.41, 249.50,
249.51, 249.60,
249.61, 249.70,
249.71, 249.80,
249.81, 249.90,
249.91, 250.00,
250.01, 250.02,
250.03, 250.10,
250.11, 250.12,
250.13, 250.20,
250.21, 250.22,
250.23, 250.30,
250.31, 250.32,
250.33, 250.40,
250.41, 250.42,
250.43, 250.50,
250.51, 250.52,
250.53, 250.60,
250.61, 250.62,
250.63, 250.70,
250.71, 250.72,
250.73, 250.80,
250.81, 250.82,
250.83, 250.90,

250.91, 250.92,
250.93, 357.2,
362.01, 362.02,
362.03, 362.04,
362.05, 362.06,
366.41
'''
{condition_name}_icd10_str ='''
E08.00, E08.01, E08.10, E08.11, E08.21, E08.22, E08.29, E08.311, E08.319, E08.321,
E08.3211, E08.3212, E08.3213, E08.3219, E08.329, E08.3291, E08.3292, E08.3293,
E08.3299, E08.331, E08.3311, E08.3312, E08.3313, E08.3319, E08.339, E08.3391,
E08.3392, E08.3393, E08.3399, E08.341, E08.3411, E08.3412, E08.3413, E08.3419,
E08.349, E08.3491, E08.3492, E08.3493, E08.3499, E08.351, E08.3511, E08.3512,
E08.3513, E08.3519, E08.3521, E08.3522, E08.3523, E08.3529, E08.3531, E08.3532,
E08.3533, E08.3539, E08.3541, E08.3542, E08.3543, E08.3549, E08.3551, E08.3552,
E08.3553, E08.3559, E08.359, E08.3591, E08.3592, E08.3593, E08.3599, E08.36,
E08.37X1, E08.37X2, E08.37X3, E08.37X9, E08.39, E08.40, E08.41, E08.42, E08.43,
E08.44, E08.49, E08.51, E08.52, E08.59, E08.610, E08.618, E08.620, E08.621, E08.622,
E08.628, E08.630, E08.638, E08.641, E08.649, E08.65, E08.69, E08.8, E08.9, E09.00,
E09.01, E09.10, E09.11, E09.21, E09.22, E09.29, E09.311, E09.319, E09.321, E09.3211,
E09.3212, E09.3213, E09.3219, E09.329, E09.3291, E09.3292, E09.3293, E09.3299,
E09.331, E09.3311, E09.3312, E09.3313, E09.3319, E09.339, E09.3391, E09.3392,
E09.3393, E09.3399, E09.341, E09.3411, E09.3412, E09.3413, E09.3419, E09.349,
E09.3491, E09.3492, E09.3493, E09.3499, E09.351, E09.3511, E09.3512, E09.3513,
E09.3519, E09.3521, E09.3522, E09.3523, E09.3529, E09.3531, E09.3532, E09.3533,
E09.3539, E09.3541, E09.3542, E09.3543, E09.3549, E09.3551, E09.3552, E09.3553,
E09.3559, E09.359, E09.3591, E09.3592, E09.3593, E09.3599, E09.36, E09.37X1,
E09.37X2, E09.37X3, E09.37X9, E09.39, E09.40, E09.41, E09.42, E09.43, E09.44,
E09.49, E09.51, E09.52, E09.59, E09.610, E09.618, E09.620, E09.621, E09.622,
E09.628, E09.630, E09.638, E09.641, E09.649, E09.65, E09.69, E09.8, E09.9, E10.10,
E10.11, E10.21, E10.22, E10.29, E10.311, E10.319, E10.321, E10.3211, E10.3212,
E10.3213, E10.3219, E10.329, E10.3291, E10.3292, E10.3293, E10.3299, E10.331,
E10.3311, E10.3312, E10.3313, E10.3319, E10.339, E10.3391, E10.3392, E10.3393,
E10.3399, E10.341, E10.3411, E10.3412, E10.3413, E10.3419, E10.349, E10.3491,
E10.3492, E10.3493, E10.3499, E10.351, E10.3511, E10.3512, E10.3513, E10.3519,
E10.359, E10.36, E10.37X1, E10.37X2, E10.37X3, E10.37X9, E10.39, E10.40, E10.41,
E10.42, E10.43, E10.44, E10.49, E10.51, E10.52, E10.59, E10.610, E10.618, E10.620,
E10.621, E10.622, E10.628, E10.630,

E10.638, E10.641, E10.649, E10.65, E10.69, E10.8, E10.9, E11.00, E11.01, E11.10,
E11.11, E11.21, E11.22, E11.29, E11.311, E11.319, E11.321, E11.3211, E11.3212,
E11.3213, E11.3219, E11.329, E11.3291, E11.3292, E11.3293, E11.3299, E11.331,
E11.3311, E11.3312, E11.3313, E11.3319, E11.339, E11.3391, E11.3392, E11.3393,
E11.3399, E11.341, E11.3411, E11.3412, E11.3413, E11.3419, E11.349, E11.3491,
E11.3492, E11.3493, E11.3499, E11.351, E11.3511, E11.3512, E11.3513, E11.3519,
E11.3521, E11.3522, E11.3523, E11.3529, E11.3531, E11.3532, E11.3533, E11.3539,
E11.3541, E11.3542, E11.3543, E11.3549, E11.3551, E11.3552, E11.3553, E11.3559,
E11.359, E11.3591, E11.3592, E11.3593, E11.3599, E11.36, E11.37X1, E11.37X2,
E11.37X3, E11.37X9, E11.39, E11.40, E11.41, E11.42, E11.43, E11.44, E11.49, E11.51,
E11.52, E11.59, E11.610, E11.618, E11.620, E11.621, E11.622, E11.628, E11.630,
E11.638, E11.641, E11.649, E11.65, E11.69, E11.8, E11.9, E13.00, E13.01, E13.10,
E13.11, E13.21, E13.22, E13.29, E13.311, E13.319, E13.321, E13.3211, E13.3212,
E13.3213, E13.3219, E13.329, E13.3291, E13.3292, E13.3293, E13.3299, E13.331,
E13.3311, E13.3312, E13.3313, E13.3319, E13.339, E13.3391, E13.3392, E13.3393,
E13.3399, E13.341, E13.3411, E13.3412, E13.3413, E13.3419, E13.349, E13.3491,
E13.3492, E13.3493, E13.3499, E13.351, E13.3511, E13.3512, E13.3513, E13.3519,
E13.3521, E13.3522, E13.3523, E13.3529, E13.3531, E13.3532, E13.3533, E13.3539,
E13.3541, E13.3542, E13.3543, E13.3549, E13.3551, E13.3552, E13.3553, E13.3559,
E13.359, E13.36, E13.39, E13.40, E13.41, E13.42, E13.43, E13.44, E13.49, E13.51,
E13.52, E13.59, E13.610, E13.618, E13.620, E13.621, E13.622, E13.628, E13.630,
E13.638, E13.641, E13.649, E13.65, E13.69, E13.8, E13.9 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[96]:


condition_name = "diabetes"
exec_str = f"""{condition_name}_remove_codes = [
4317258,134398,4032787
]
before = len({condition_name}_standard_concepts)
{condition_name}_standard_concepts =  remove_codes({condition_name}_standard_concepts, {condition_name}_remove_codes)
display({condition_name}_standard_concepts.head(5))
print("Removal: ", before, " --> ", len({condition_name}_standard_concepts))
"""

exec(exec_str)


# ## Hepatitis

# In[97]:


# Viral Hepatitis Standard Concepts
condition_name = "viral_hepatitis"
exec_str = f"""
{condition_name}_icd9_str = '''
070.0, 070.1, 070.2, 070.20, 070.21,
070.22, 070.23, 070.3, 070.30, 070.31,
070.32, 070.33, 070.4, 070.41, 070.42,
070.43, 070.49, 070.5, 070.51, 070.52,
070.53, 070.54, 070.59, 070.6, 070.7,
070.70, 070.71, 070.9, V02.6, V02.60,
V02.61, V02.62, V02.69
'''
{condition_name}_icd10_str ='''
 B15.0, B15.9, B16.0, B16.1, B16.2, B16.9, B17.0, B17.10,
B17.11, B17.2, B17.8, B17.9, B18.0, B18.1, B18.2, B18.8, B18.9,
B19.0, B19.10, B19.11, B19.20, B19.21, B19.9, Z22.50, Z22.51,
Z22.52, Z22.59
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[98]:


# Liver Disease / Cirrhosis Standard Concepts
condition_name = "liver_disease"
exec_str = f"""
{condition_name}_icd9_str = '''
570, 571, 571.0, 571.1, 571.2, 571.3,
571.5, 571.6, 571.8, 571.9, 572, 572.0,
572.1, 572.2, 572.3, 572.4, 572.8, 573,
573.0, 573.4, 573.5, 573.8, 573.9,
576.1, 789.1, V42.7,

42.91,
44.91, 54.91, 96.06
'''
{condition_name}_icd10_str ='''
K70.0, K70.10, K70.11, K70.2, K70.30, K70.31, K70.40,
K70.41, K70.9, K71.0, K71.11, K71.7, K71.8, K71.9, K72.00,
K72.01, K72.10, K72.11, K72.90, K72.91, K74.0, K74.1, K74.2,
K74.3, K74.4, K74.5, K74.60, K74.69, K75.0, K75.1, K75.81,
K75.89, K75.9, K76.0, K76.1, K76.2, K76.3, K76.5, K76.6, K76.7,
K76.81, K76.89, K76.9, K77, K80.30, K80.31, K80.32, K80.33,
K80.34, K80.35, K80.36, K80.37, K83.0, R16.0, R16.2, Z48.23,
Z94.4,

06L20ZZ, 06L23ZZ, 06L24ZZ, 06L30ZZ,
06L33ZZ, 06L34ZZ, 0DL57DZ, 0DL58DZ, 0D9S30Z, 0D9S3ZZ,
0D9S40Z, 0D9S4ZZ, 0D9T30Z, 0D9T3ZZ, 0D9T40Z, 0D9T4ZZ,
0D9V30Z, 0D9V3ZZ, 0D9V40Z, 0D9V4ZZ, 0D9W30Z, 0D9W3ZZ,
0D9W40Z, 0D9W4ZZ, 0W9F30Z, 0W9F3ZZ, 0W9F40Z, 0W9F4ZZ,
0W9G30Z, 0W9G3ZZ, 0W9G40Z, 0W9G4ZZ, 0W9J30Z, 0W9J3ZZ
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[99]:


# group hepatitis
list_of_hep_dfs = [viral_hepatitis_standard_concepts,liver_disease_standard_concepts]
hepatitis_standard_concepts = pd.concat(list_of_hep_dfs)
display(hepatitis_standard_concepts)


# ## HIV

# In[100]:


# HIV / AIDS Standard Concepts
condition_name = "HIV"
exec_str = f"""
{condition_name}_icd9_str = '''
042, 042.0, 042.1, 042.2, 042.9, 043,
043.1, 043.2, 043.3, 043.9, 044, 044.0,
044.9, 079.53, 795.71, V08
'''
{condition_name}_icd10_str ='''
B20, B97.35, R75, Z21'''

{condition_name}_drg_str ='''
488, 489, 490, 969, 970, 974, 975,
976, 977, 969, 970, 974, 975, 976
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str, {condition_name}_drg_str)
display({condition_name}_standard_concepts)
"""
exec(exec_str)


# In[ ]:





# ## Osteoporosis

# In[101]:


# Osteoporosis Standard Concepts
condition_name = "osteoporosis"
exec_str = f"""
{condition_name}_icd9_str = '''
733.00,
733.01, 733.02,
733.03, 733.09 
'''
{condition_name}_icd10_str ='''
M81.0, M81.6, M81.8
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Schizophrenia

# In[102]:


# Schizophrenia Standard Concepts
condition_name = "schizophrenia"
exec_str = f"""
{condition_name}_icd9_str = '''
295.00, 295.01, 295.02, 295.03,
295.04, 295.05, 295.10, 295.11, 295.12,
295.13, 295.14, 295.15, 295.20, 295.21,
295.22, 295.23, 295.24, 295.25, 295.30,
295.31, 295.32, 295.33, 295.34, 295.35,
295.40, 295.41, 295.42, 295.43, 295.44,
295.45, 295.50, 295.51, 295.52, 295.53,
295.54, 295.55, 295.60, 295.61, 295.62,
295.63, 295.64, 295.65, 295.70, 295.71,
295.72, 295.73, 295.74, 295.75, 295.80,
295.81, 295.82, 295.83, 295.84, 295.85,
295.90, 295.91, 295.92, 295.93, 295.94,
295.95
'''
{condition_name}_icd10_str ='''
F20.0, F20.1, F20.2, F20.3, F20.5, F20.81, F20.89, F20.9,
F25.0, F25.1, F25.8, F25.9 
'''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# ## Substance Abuse Disorders (Alcohol and Drugs)

# In[103]:


# Alcohol Standard Concepts
alcohol_use_disorders_icd9_str = """291.0, 291.1, 291.2, 291.3, 291.4,
291.5, 291.8, 291.81, 291.82, 291.89,
291.9, 303.00, 303.01, 303.02, 303.90,
303.91, 303.92, 305.00, 305.01, 305.02,
357.5, 425.5, 535.30, 535.31, 571.0,
571.1, 571.2, 571.3, 760.71, 980.0,
V65.42, V79.1, E860.0,
94.6, 94.61, 94.62, 94.63, 94.67, 94.68,
94.69"""

alcohol_use_disorders_icd10_str = '''
F10.10, F10.120, F10.121, F10.129, F10.14, F10.150, F10.151,
F10.159, F10.180, F10.181, F10.182, F10.188, F10.19, F10.20,
F10.220, F10.221, F10.229, F10.230, F10.231, F10.232, F10.239,
F10.24, F10.250, F10.251, F10.259, F10.26, F10.27, F10.280,
F10.281, F10.282, F10.288, F10.29, F10.920, F10.921, F10.929,
F10.94, F10.950, F10.951, F10.959, F10.96, F10.97, F10.980,
F10.981, F10.982, F10.988, F10.99, G62.1, I42.6, K29.20, K29.21,
K70.0, K70.10, K70.11, K70.2, K70.30, K70.31, K70.40, K70.41,
K70.9, P04.3, Q86.0, T51.0X1A, T51.0X2A, T51.0X3A, T51.0X4A,
Z71.41, Z71.42,
HZ2ZZZZ, HZ30ZZZ, HZ31ZZZ, HZ32ZZZ, HZ33ZZZ, HZ34ZZZ,
HZ35ZZZ, HZ36ZZZ, HZ37ZZZ, HZ38ZZZ, HZ39ZZZ, HZ3BZZZ,
HZ40ZZZ, HZ93ZZZ, HZ96ZZZ
'''

alcohol_use_disorders_standard_concepts = get_standard_codes(alcohol_use_disorders_icd9_str, alcohol_use_disorders_icd10_str )
display(alcohol_use_disorders_standard_concepts.head(5))


# In[104]:


alcohol_use_disorders_standard_concepts[alcohol_use_disorders_standard_concepts['concept_name'].str.contains('pregnancy')]


# In[105]:


# Drug Use Standard Concepts
condition_name = "drug_use"
exec_str = f"""
{condition_name}_icd9_str = ''' 
    292.0, 292.11, 292.12, 292.2,
    292.81, 292.82, 292.83, 292.84, 292.85,
    292.89, 292.9, 304.00, 304.01, 304.02,
    304.10, 304.11, 304.12, 304.2, 304.20,
    304.21, 304.22, 304.3, 304.30, 304.31,
    304.32, 304.4, 304.40, 304.41, 304.42,
    304.5, 304.50, 304.51, 304.52, 304.6,
    304.60, 304.61, 304.62, 304.7, 304.70,
    304.71, 304.72, 304.8, 304.80, 304.81,
    304.82, 304.9, 304.90, 304.91, 304.92,
    305.2, 305.20, 305.21, 305.22, 305.3,
    305.30, 305.31, 305.32, 305.4, 305.40,
    305.41, 305.42, 305.5, 305.50, 305.51,
    305.52, 305.6, 305.60, 305.61, 305.62,
    305.7, 305.70, 305.71, 305.72, 305.8,
    305.80, 305.81, 305.82, 305.9, 305.90,
    305.91, 305.92, 648.3, 648.30, 648.31,
    648.32, 648.33, 648.34, 655.5, 655.50,
    655.51, 655.53, 760.72, 760.73, 760.75,
    779.5, 965.0, 965.00, 965.01, 965.02,
    965.09, V65.42, E850.0, E850.1, E850.2,
    E854.1, E935.0, E935.1, 
    94.6, 94.64, 94.65, 94.66, 94.67, 94.68,
    94.69
        '''
{condition_name}_icd10_str ='''
    F12.90, F12.920, F12.921, F12.922, F12.929, F12.950, F12.951,
    F12.959, F12.980, F12.988, F12.99, F13.10, F13.120, F13.121,
    F13.129, F13.14, F13.150, F13.151, F13.159, F13.180, F13.181,
    F13.182, F13.188, F13.19, F13.20, F13.220, F13.221, F13.229,
    F13.230, F13.231, F13.232, F13.239, F13.24, F13.250, F13.251,
    F13.259, F13.26, F13.27, F13.280, F13.281, F13.282, F13.288,
    F13.29, F13.90, F13.920, F13.921, F13.929, F13.930, F13.931,
    F13.932, F13.939, F13.94, F13.950, F13.951, F13.959, F13.96,
    F13.97, F13.980, F13.981, F13.982, F13.988, F13.99, F14.10,
    F14.120, F14.121, F14.122, F14.129, F14.14, F14.150, F14.151,
    F14.159, F14.180, F14.181, F14.182, F14.188, F14.19, F14.20,
    F14.220, F14.221, F14.222, F14.229, F14.23, F14.24, F14.250,
    F14.251, F14.259, F14.280, F14.281, F14.282, F14.288, F14.29,
    F14.90, F14.920, F14.921, F14.922, F14.929, F14.94, F14.950,
    F14.951, F14.959, F14.980, F14.981, F14.982, F14.988, F14.99,
    F15.10, F15.120, F15.121, F15.122, F15.129, F15.14, F15.150,
    F15.151, F15.159, F15.180, F15.181, F15.182, F15.188, F15.19,
    F15.20, F15.220, F15.221, F15.222, F15.229, F15.23, F15.24,
    F15.250, F15.251, F15.259, F15.280, F15.281, F15.282, F15.288,
    F15.29, F15.90, F15.920, F15.921, F15.922, F15.929, F15.93,
    F15.94, F15.950, F15.951, F15.959, F15.980, F15.981, F15.982,
    F15.988, F15.99, F16.10, F16.120, F16.121, F16.122, F16.129,
    F16.14, F16.150, F16.151, F16.159, F16.180, F16.183, F16.188,
    F16.19, F16.20, F16.220, F16.221, F16.229, F16.24, F16.250,
    F16.251, F16.259, F16.280, F16.283, F16.288, F16.29, F16.90,
    F16.920, F16.921, F16.929, F16.94, F16.950, F16.951, F16.959,
    F16.980, F16.983, F16.988, F16.99, F17.203, F17.208, F17.209,
    F17.213, F17.218, F17.219, F17.223, F17.228, F17.229, F17.293,
    F17.298, F17.299, F18.10, F18.120, F18.121, F18.129, F18.14,
    F18.150, F18.151, F18.159, F18.17, F18.180, F18.188, F18.19,
    F18.20, F18.220, F18.221, F18.229, F18.24, F18.250, F18.251,
    F18.259, F18.27, F18.280, F18.288, F18.29, F18.90, F18.920,
    F18.921, F18.929, F18.94, F18.950, F18.951, 
    F18.959, F18.97, F18.980, F18.988, F18.99, F19.10, F19.120,
    F19.121, F19.122, F19.129, F19.14, F19.150, F19.151, F19.159,
    F19.16, F19.17, F19.180, F19.181, F19.182, F19.188, F19.19,
    F19.20, F19.220, F19.221, F19.222, F19.229, F19.230, F19.231,
    F19.232, F19.239, F19.24, F19.250, F19.251, F19.259, F19.26,
    F19.27, F19.280, F19.281, F19.282, F19.288, F19.29, F19.90,
    F19.920, F19.921, F19.922, F19.929, F19.930, F19.931, F19.932,
    F19.939, F19.94, F19.950, F19.951, F19.959, F19.96, F19.97,
    F19.980, F19.981, F19.982, F19.988, F19.99, F55.0, F55.1, F55.2,
    F55.3, F55.4, F55.8, O35.5XX0, O35.5XX1, O35.5XX2, O35.5XX3,
    O35.5XX4, O35.5XX5, O35.5XX9, T40.691A, T40.692A, T40.693A,
    T40.694A, O99.320, O99.321, O99.322, O99.323, O99.324,
    O99.325, P04.41, P04.49, P96.1, P96.2, T40.0X1A, T40.0X2A,
    T40.0X3A, T40.0X4A, T40.0X5A, T40.0X5S, T40.1X1A, T40.1X2A,
    T40.1X3A, T40.1X4A, T40.2X1A, T40.2X2A, T40.2X3A, T40.2X4A,
    T40.3X1A, T40.3X2A, T40.3X3A, T40.3X4A, T40.3X5A, T40.3X5S,
    T40.4X1A, T40.4X2A, T40.4X3A, T40.4X4A, T40.7X1A, T40.8X1A,
    T40.601A, T40.602A, T40.603A, T40.604A, T40.691A, T40.692A,
    T40.693A, T40.694A, T40.901A, T40.991A, Z71.41, Z71.42,
    Z71.51, Z71.52, Z71.6,
    HZ2ZZZZ, HZ30ZZZ, HZ31ZZZ, HZ32ZZZ, HZ33ZZZ, HZ34ZZZ,
    HZ35ZZZ, HZ36ZZZ, HZ37ZZZ, HZ38ZZZ, HZ39ZZZ, HZ3BZZZ,
    HZ40ZZZ, HZ93ZZZ, HZ96ZZZ
    '''
{condition_name}_standard_concepts = get_standard_codes({condition_name}_icd9_str, {condition_name}_icd10_str)
display({condition_name}_standard_concepts.head(5))
"""
exec(exec_str)


# In[108]:


drug_use_standard_concepts[drug_use_standard_concepts['concept_name'].str.contains('pregnancy')]


# In[109]:


# group substance abuse
list_of_substance_dfs = [alcohol_use_disorders_standard_concepts, drug_use_standard_concepts]
substance_abuse_standard_concepts = pd.concat(list_of_substance_dfs)
display(substance_abuse_standard_concepts)


# In[110]:


condition_name = "substance_abuse"
exec_str = f"""{condition_name}_remove_codes = [
4041280,43530950,36712695,74104
]
before = len({condition_name}_standard_concepts)
{condition_name}_standard_concepts =  remove_codes({condition_name}_standard_concepts, {condition_name}_remove_codes)
display({condition_name}_standard_concepts.head(5))
print("Removal: ", before, " --> ", len({condition_name}_standard_concepts))
"""

exec(exec_str)


# In[111]:


df = substance_abuse_standard_concepts.copy()
df = df[~df.concept_name.str.contains("trimester pregnancy")]
df = df[~df.concept_name.str.contains("Finding related to pregnancy")]
substance_abuse_standard_concepts = df
print(len(substance_abuse_standard_concepts))


# In[112]:


substance_abuse_standard_concepts


# ## Aggregate all comorbidities

# In[113]:


comorbidity_names = [
    "hypertension", "heart_failure", "ischemic_heart", "atrial_fibrillation", "hyperlipidemia",
    "stroke", "arthritis", "asthma", "autism", 'cancer', "chronic_kidney_disease", "chronic_pulmonary_disease",
    "alzheimers", "depression", "diabetes", "hepatitis", "HIV", "osteoporosis", "schizophrenia", "substance_abuse"
]
# create combined comorbidities
combined_dfs = []
for name in comorbidity_names:
    exec(f"""this_df = {name}_standard_concepts""")
    this_df["comorbidity"] = name
    this_df.rename(columns = {'concept_name':'condition_name', 'concept_id':"condition_concept_id"}, inplace=True)
    this_df = this_df[["comorbidity", "condition_name", "condition_concept_id", "domain_id", "vocabulary_id", "concept_code"]]
    combined_dfs.append(this_df)
all_comorbidities = pd.concat(combined_dfs)
all_comorbidities.drop_duplicates('condition_concept_id', inplace=True)


# In[119]:


all_comorbidities


# In[160]:


all_comorbidities[all_comorbidities['comorbidity']=='cancer']


# In[142]:


all_comorbidities[all_comorbidities['condition_name'].str.contains('pregnancy')]


# In[141]:


all_comorbidities[all_comorbidities['domain_id']=='Observation']


# In[114]:


# create download links
import base64
import pandas as pd
from IPython.display import HTML

def create_download_link( df, title = "Download CSV file", filename = "data.csv"):
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload,title=title,filename=filename)
    return HTML(html)


# In[144]:


hypertension_standard_concepts


# In[146]:


# save codes for each comorbidity
combined_dfs = []
links = dict()
for name in comorbidity_names:
    exec(f"""this_df = {name}_standard_concepts""")
    this_df["comorbidity"] = name
    this_df.rename(columns = {'concept_name':'condition_name', 'concept_id':"condition_concept_id"}, inplace=True)
    this_df = this_df[["comorbidity", "condition_name", "condition_concept_id", "domain_id", "vocabulary_id", "concept_code"]]
    display(this_df)
#     save_df(this_df, f"{name}_condition_codes")
    # create link
    links[name] = create_download_link(this_df, name, f"{name}.csv")
#     break


# In[120]:


hypertension_standard_concepts


# In[147]:


# create csv download links
for k, link in links.items():
    print(k)
    display(link)


# # Get Comorbidities from Conditions, Procedures, and Observations

# In[24]:


# download codes for each comorbidity : don't want to run comorbidities section again
comorbidity_names = [
    "hypertension", "heart_failure", "ischemic_heart", "atrial_fibrillation", "hyperlipidemia",
    "stroke", "arthritis", "asthma", "autism", 'cancer', "chronic_kidney_disease", "chronic_pulmonary_disease",
    "alzheimers", "depression", "diabetes", "hepatitis", "HIV", "osteoporosis", "schizophrenia", "substance_abuse"
]
combined_dfs = []
for name in comorbidity_names:
    exec(f"""
import_df('{name}_condition_codes')
combined_dfs.append({name}_condition_codes)
    """)


# In[25]:


all_comorbidities = pd.concat(combined_dfs)
all_comorbidities.drop_duplicates('condition_concept_id', inplace=True)


# In[26]:


all_comorbidities


# ## Create comorbidity indicators

# In[27]:


# Get comorbidities from conditions
# optimized by inserting temp table of comorbidities into sql query
def get_comorbidities_from_conditions(cohort_sql, all_comorbidities):
    all_comorbidities_condition_codes =  all_comorbidities[all_comorbidities['domain_id']=="Condition"]['condition_concept_id'].astype(str).values
    insert_values_sql0 = ",".join([f"({x})" for x in all_comorbidities_condition_codes[:1200]])
    insert_values_sql1 = ",".join([f"({x})" for x in all_comorbidities_condition_codes[1200:]])

    get_conditions_sql = f"""
        CREATE TEMP TABLE comorbidities
        (
          condition_concept_id INT64
        );
        INSERT INTO comorbidities
        VALUES {insert_values_sql0};
        INSERT INTO comorbidities
        VALUES {insert_values_sql1};

        --SELECT * FROM comorbidities;
        
        with {cohort_sql},
        with_conditions as (
            select cohort.person_id, cohort.drug_exposure_start_date, 
            conditions.condition_concept_id, 
            conditions.condition_start_date, conditions.condition_end_date
            from cohort
            inner join (
               select a.* from `{os.environ["WORKSPACE_CDR"]}`.condition_occurrence a 
               inner join (
                    select * from comorbidities
                ) b 
                 on a.condition_concept_id = b.condition_concept_id
            ) conditions
            on cohort.person_id = conditions.PERSON_ID
        ),
        with_condition_names as (
            select person.*, 
            condition_concept.concept_name as condition_name
            from with_conditions person
            LEFT JOIN
            `{os.environ["WORKSPACE_CDR"]}`.concept condition_concept 
            on person.condition_concept_id = condition_concept.CONCEPT_ID 
        )

        select * from with_condition_names
    """
    conditions = pd.read_gbq(get_conditions_sql, dialect="standard")

    # conditions with comorbidity
    conditions_with_comorbidity = conditions.merge(all_comorbidities, on='condition_concept_id', how='left', suffixes=[None, '_y'])
    conditions_with_comorbidity.drop('condition_name_y', axis=1, inplace=True)
    return conditions_with_comorbidity


# In[28]:


def get_comorbidities_in_cohort(cohort_sql, comorbidities_df):
    cohort_patients_sql = f"""
        with {cohort_sql}
        select distinct person_id from cohort
        """
    cohort = pd.read_gbq(cohort_patients_sql, dialect="standard")
    print("[get_comorbidities_in_cohort] cohort finished")
    cohort[comorbidity_names] = pd.DataFrame([len(comorbidity_names) * [0]], index=cohort.index)
    cohort['total_comorbidities'] = 0
    cohort.set_index('person_id', inplace=True)
    
    
    comorbidities = get_comorbidities_from_conditions(cohort_sql, comorbidities_df)
    print("[get_comorbidities_in_cohort] conditions finished")
    comorbidities = comorbidities[["person_id", "comorbidity"]]
    comorbidities = comorbidities.drop_duplicates()
    comorbidities['indicator'] = 1

    indicators = comorbidities.pivot(index='person_id', columns='comorbidity', values='indicator')
    indicators.fillna(0, inplace=True)
    indicators = indicators.astype(int)
    indicators = indicators[comorbidity_names]
    indicators['total_comorbidities'] = indicators[comorbidity_names].sum(axis=1)

    total = cohort.add(indicators, fill_value=0)
    total.reset_index(inplace=True)
    total = total.fillna(0)
    total = total.astype(int)

    print(len(cohort), len(indicators), len(total[total['total_comorbidities']==0]))
    # make sure patients that don't have any comorbidities is the difference between cohort and indicators
    assert len(cohort) - len(indicators) == len(total[total['total_comorbidities']==0])

    # Check if indicators work
    for comorbidity in comorbidity_names:
        assert len(indicators[indicators[comorbidity]==1]) == len(comorbidities[comorbidities['comorbidity']==comorbidity])
    
    return total


# In[ ]:


cohort_sql_strs = {"influenza": influenza_cohort_sql, 
                   "hepB": hepB_cohort_sql, 
                   "hpv": hpv_cohort_sql, 
                   "pneumococcal_under_65": pneumococcal_under_65_cohort_sql, 
                   "pneumococcal_65_and_over":pneumococcal_65_and_over_cohort_sql
                  }
comorbidities = dict()


# In[170]:


# create comorbidities df for all cohorts
for name, cohort_sql_str in cohort_sql_strs.items():
    print(f"comorbidities for {name} cohort started")
    comorbidities[name] = get_comorbidities_in_cohort(cohort_sql_str, all_comorbidities)
    print(f"comorbidities for {name} cohort finished")


# In[171]:


for name, df in comorbidities.items():
    save_df(df, f"{name}_comorbidities")


# In[30]:


# get comorbidities for all patients cohort
all_comorbidities_condition_codes =  all_comorbidities[all_comorbidities['domain_id']=="Condition"]['condition_concept_id'].astype(str).values
insert_values_sql0 = ",".join([f"({x})" for x in all_comorbidities_condition_codes[:1200]])
insert_values_sql1 = ",".join([f"({x})" for x in all_comorbidities_condition_codes[1200:]])

get_conditions_sql = f"""
CREATE TEMP TABLE comorbidities
(
  condition_concept_id INT64
);
INSERT INTO comorbidities
VALUES {insert_values_sql0};
INSERT INTO comorbidities
VALUES {insert_values_sql1};

--SELECT * FROM comorbidities;

with conditions as (
    select * from `{os.environ["WORKSPACE_CDR"]}`.condition_occurrence
    join (
        select * from comorbidities
    ) b
    on condition_occurrence.condition_concept_id = b.condition_concept_id
)
select * from conditions 
"""
all_conditions = pd.read_gbq(get_conditions_sql, dialect="standard")


# In[38]:


# add comorbidity column to all_conditions
all_conditions_with_comorbidity = all_conditions.merge(all_comorbidities, on='condition_concept_id', how='left', suffixes=[None, '_y'])


# In[39]:


all_conditions_with_comorbidity_limited = all_conditions_with_comorbidity[['person_id', 'comorbidity', 
                                                                           'condition_name', 'condition_concept_id',
                                                                           'condition_occurrence_id', 'condition_start_date',
                                                                           'condition_type_concept_id'
                                                                        ]]


# In[40]:


save_df_hdf(all_conditions_with_comorbidity_limited, "all_conditions_with_comorbidity_limited")


# In[41]:


# function to create indicators for all cohort
def create_indicators_for_all_cohort(comorbidities):
    
    cohort_sql = f"""
       -- select distinct person_id from `{os.environ["WORKSPACE_CDR"]}`.condition_occurrence
        select distinct person_id from `{os.environ["WORKSPACE_CDR"]}`.person person 
    """
    cohort = pd.read_gbq(cohort_sql, dialect="standard")
    
    cohort[comorbidity_names] = pd.DataFrame([len(comorbidity_names) * [0]], index=cohort.index)
    cohort['total_comorbidities'] = 0
    cohort.set_index('person_id', inplace=True)
    
    
    comorbidities = comorbidities[['person_id', 'comorbidity']]
    comorbidities = comorbidities.drop_duplicates()
    comorbidities['indicator'] = 1
    
    indicators = comorbidities.pivot(index='person_id', columns='comorbidity', values='indicator')
    indicators.fillna(0, inplace=True)
    indicators = indicators.astype(int)
    indicators = indicators[comorbidity_names]
    indicators['total_comorbidities'] = indicators[comorbidity_names].sum(axis=1)

    total = cohort.add(indicators, fill_value=0)
    total.reset_index(inplace=True)
    total = total.fillna(0)
    total = total.astype(int)

    print(len(cohort), len(indicators), len(total[total['total_comorbidities']==0]))
    # make sure patients that don't have any comorbidities is the difference between cohort and indicators
    assert len(cohort) - len(indicators) == len(total[total['total_comorbidities']==0])

    # Check if indicators work
    for comorbidity in comorbidity_names:
        assert len(indicators[indicators[comorbidity]==1]) == len(comorbidities[comorbidities['comorbidity']==comorbidity])

    return total


# In[42]:


all_cohort_comorbidities = create_indicators_for_all_cohort(all_conditions_with_comorbidity_limited[['person_id', 'comorbidity', 'condition_name', 'condition_concept_id']])


# In[3]:


all_cohort_comorbidities.total_comorbidities.describe()


# In[2]:


all_cohort_comorbidities.head()


# In[44]:


save_df_hdf(all_cohort_comorbidities, "all_patients_comorbidities")


# In[ ]:


# # %%script false --no-raise-error


# ## Find most-used condition codes 

# In[ ]:


# define function to find top comorbidity conditions
def get_top_comorbidity_conditions(cohort_sql, comorbidities_df):
    c = get_comorbidities_from_conditions(cohort_sql, comorbidities_df)
    comorbidities = c[['person_id', 'comorbidity', 'condition_name', 'condition_concept_id']]
    top_conditions = comorbidities.groupby(['condition_name', 'condition_concept_id', 'comorbidity']).size().to_frame('count')
    top_conditions.sort_values('count', ascending=False, inplace=True)
    return(top_conditions)


# In[ ]:


# # Get top comorbidities for all cohorts
# cohort_sql_strs = {"influenza": influenza_cohort_sql, 
#                    "hepB": hepB_cohort_sql, 
#                    "hpv": hpv_cohort_sql, 
#                    "pneumococcal_under_65": pneumococcal_under_65_cohort_sql, 
#                    "pneumococcal_65_and_over":pneumococcal_65_and_over_cohort_sql
#                   }
# top_conditions = dict()
# for name, cohort_sql_str in cohort_sql_strs.items():
#     print(f"top conditions for {name} cohort started")
#     top_conditions[name] = get_top_comorbidity_conditions(cohort_sql_str, all_comorbidities)
#     print(f"top conditions for {name} cohort finished")


# In[ ]:


# for name, cohort_sql_str in cohort_sql_strs.items():
#     print(name)
#     display(top_conditions[name].head(20))


# In[ ]:


# for name, df in top_conditions.items():
#     save_df(df, f"{name}_comorbidities_top_conditions")


# # Vaccination Rates

# ## Setup

# In[25]:


def agg_doses(doses_df):
    agg_doses_df = doses_df.groupby(['person_id']).size().to_frame('doses')
    agg_doses_df = agg_doses_df.sort_values('doses', axis=0, ascending=False)
    return agg_doses_df


# In[26]:


# remove doses within 26 days of each other
def remove_duplicate_doses(doses_df):
    days_bw = doses_df.copy()
    days_bw['days_between'] = doses_df.groupby(['person_id']).drug_exposure_start_date.diff().astype('timedelta64[D]')
    index_names =  days_bw[days_bw['days_between']<26].index
    days_bw.drop(index_names, inplace=True)
    return days_bw


# ## HepB Doses

# In[27]:


# HepB - Vaccination Doses
hepB_doses_sql = f"""
    with hepb_patients as (
        SELECT
           /* d_exposure.PERSON_ID,
            d_exposure.DRUG_EXPOSURE_START_DATE*/
            *
        from
            {os.environ["WORKSPACE_CDR"] }.drug_exposure d_exposure 
        WHERE
            (
                {hepB_CVX_drug_concepts_sql} OR {hepB_RxNorm_drugs_sql}
            ) 
    ),
    with_concept_name as (
        select b.concept_name as drug_name, b.vocabulary_id, a.* from hepb_patients a
        left join 
        (select * from {os.environ["WORKSPACE_CDR"] }.concept) b
        on a.drug_concept_id = b.concept_id
    ),
    with_drug_type_name as (
        select b.concept_name as drug_type, a.* from with_concept_name a
        left join 
        (select * from {os.environ["WORKSPACE_CDR"] }.concept) b
        on a.drug_type_concept_id = b.concept_id
    ),
    in_time_period as (
        select person_id, drug_concept_id, drug_name, drug_type, drug_exposure_start_date, vocabulary_id 
        from with_drug_type_name
        where drug_exposure_start_date <= '2018-12-31'
        group by person_id, drug_concept_id, drug_name, drug_type, drug_exposure_start_date, vocabulary_id
    )
    
    select * from in_time_period order by person_id, drug_exposure_start_date asc
"""
hepB_doses = pd.read_gbq(hepB_doses_sql, dialect="standard")


# In[28]:


hepB_w_duplicates_agg = agg_doses(hepB_doses)


# In[29]:


hepB_no_duplicates = remove_duplicate_doses(hepB_doses)


# In[30]:


hepB_no_duplicates_agg = agg_doses(hepB_no_duplicates)


# In[31]:


hepB_no_duplicates_agg


# In[32]:


#hepB doses
print("hepB doses")
display(hepB_w_duplicates_agg['doses'].describe())


# In[33]:


#hepB doses after removing within 26 days
print("hepB doses after removing within 26 days")
display(hepB_no_duplicates_agg['doses'].describe())


# In[34]:


# check doses for patient w/ hepB vaccine
hepB_no_duplicates[hepB_no_duplicates['person_id']==2528080]


# ## HPV Doses

# In[35]:


# HPV - Vaccination Doses
hpv_doses_sql = f"""
    with hpv_patients as (
        SELECT
           /* d_exposure.PERSON_ID,
            d_exposure.DRUG_EXPOSURE_START_DATE*/
            *
        from
            {os.environ["WORKSPACE_CDR"] }.drug_exposure d_exposure 
        WHERE
            (
                {hpv_CVX_drug_concepts_sql} OR {hpv_RxNorm_drugs_sql}
            ) 
    ),
    with_concept_name as (
        select b.concept_name as drug_name, b.vocabulary_id, a.* from hpv_patients a
        left join 
        (select * from {os.environ["WORKSPACE_CDR"] }.concept) b
        on a.drug_concept_id = b.concept_id
    ),
    with_drug_type_name as (
        select b.concept_name as drug_type, a.* from with_concept_name a
        left join 
        (select * from {os.environ["WORKSPACE_CDR"] }.concept) b
        on a.drug_type_concept_id = b.concept_id
    ),
    in_time_period as (
        select person_id, drug_concept_id, drug_name, drug_type, drug_exposure_start_date, vocabulary_id 
        from with_drug_type_name
        where drug_exposure_start_date <= '2018-12-31'
        group by person_id, drug_concept_id, drug_name, drug_type, drug_exposure_start_date, vocabulary_id
    )
    
    select * from in_time_period order by person_id, drug_exposure_start_date asc
"""
hpv_doses = pd.read_gbq(hpv_doses_sql, dialect="standard")


# In[36]:


hpv_doses


# In[37]:


hpv_w_duplicates_agg = agg_doses(hpv_doses)


# In[38]:


hpv_no_duplicates = remove_duplicate_doses(hpv_doses)


# In[39]:


hpv_no_duplicates_agg = agg_doses(hpv_no_duplicates)


# In[40]:


hpv_no_duplicates_agg


# In[41]:


# hpv doses
print("hpv doses")
display(hpv_w_duplicates_agg['doses'].describe())


# In[42]:


# hpv doses after removing within 26 days
print("hpv doses after removing within 26 days")
display(hpv_no_duplicates_agg['doses'].describe())


# In[43]:


# check doses for patient
hpv_no_duplicates[hpv_no_duplicates['person_id']==3149704]


# In[44]:


save_df(hpv_no_duplicates_agg, "hpv_doses")
save_df(hepB_no_duplicates_agg, "hepB_doses")


# In[ ]:





# In[ ]:




