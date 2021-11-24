#!/usr/bin/env python
# coding: utf-8

# # Setup

# In[1]:


# Import libraries and settings for analysis
# import pkg_resources
# pkg_resources.require("numpy==1.19.5") 
# pkg_resources.require("pandas==0.25.3")


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import math

pd.options.display.float_format = "{:.2f}".format


# In[2]:


# Import Libraries
import os
import subprocess
import numpy as np
import pandas as pd


# In[3]:


get_ipython().run_line_magic('pip', 'install tables')


# In[4]:


np.__version__


# In[5]:


# %pip uninstall numpy
get_ipython().run_line_magic('pip', 'install -Iv numpy==1.19.0 pandas==0.25.3')


# In[6]:


get_ipython().run_line_magic('pip', 'uninstall numpy -y')
get_ipython().run_line_magic('pip', 'install numpy==1.19.0')


# In[7]:


get_ipython().run_line_magic('pip', 'freeze')


# In[8]:


# items in bucket
# This code lists objects in your Google Bucket

def list_items():
    # Get the bucket name
    my_bucket = os.getenv('WORKSPACE_BUCKET')

    # List objects in the bucket
    print(subprocess.check_output(f"gsutil ls -r {my_bucket}", shell=True).decode('utf-8'))

# list_items()


# In[9]:


# Define import_df function
def import_df(filename):
    try:
        import_df_hdf(filename)
    except:
        print("h5 file not found")
        # Replace 'test.csv' with THE NAME of the file you're going to download from the bucket (don't delete the quotation marks)
        name_of_file_in_bucket = f'{filename}.csv'

        # get the bucket name
        my_bucket = os.getenv('WORKSPACE_BUCKET')

        # copy csv file from the bucket to the current working space
        os.system(f"gsutil cp '{my_bucket}/data/{name_of_file_in_bucket}' .")

        print(f'[INFO] {name_of_file_in_bucket} is successfully downloaded into your working space')
        # save dataframe in a csv file in the same workspace as the notebook
        temp_df = pd.read_csv(name_of_file_in_bucket, dtype=object)
        globals()[filename] = temp_df


# In[10]:


# Define import_df function
def import_df_hdf(filename):
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


# In[11]:


# Define function to create color palettes

def create_color_palette(*args):
    if len(args) == 1:
        assert args[0] > 0
        return sns.color_palette('Blues_d', args[0] )
    elif len(args) == 2:
        high, low = args[0], args[1]
        assert high >= low
        return sns.color_palette('Blues_d', math.ceil((high - low) * 1.45) )
    elif len(args) == 3:
        high, low, mult = args[0], args[1], args[2]
        assert high >= low
        return sns.color_palette('Blues_d', math.ceil((high - low) * mult) )
    else:
        raise Exception("wrong amount of params")
        


# In[12]:


# change columns to dates
def change_cols_to_date(dfs, cols):
    for df in dfs:
        df[cols] = df[cols].apply(pd.to_datetime)


# In[13]:


# change columns to int
def change_cols_to_int(dfs, cols):
    for df in dfs:
        df[cols] = df[cols].astype(int)


# In[14]:


# change columns with ids to int to string
def change_ids_to_str(dfs, cols):
    for df in dfs:
        df[cols] = df[cols].astype(str)


# In[15]:


# move row of index to top of df
def move_row_to_top(df, index_name):
    df["new"] = range(1,len(df)+1)
    df.loc[index_name,'new'] = 0
    return df.sort_values("new").drop('new', axis=1)


# In[16]:


#%%script false --no-raise-error


# In[17]:


# Set cohort sizes here to account for missing
cohort_sizes = {
    "influenza" : 15346,
    "hepB": 6323,
    "hpv": 2125,
    "pneumococcal(<65)": 15217,
    "pneumococcal(>=65)": 15100,
    "all": 315297
}
cohort_sizes_series = pd.Series([15346, 6323, 2125, 15217, 15100, 315297], 
                                index=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"])

def account_for_missing(df, total_row_name='count', output_row_name="missing" ):
    diff = cohort_sizes_series.subtract(df.loc[total_row_name].astype(int)).fillna(0)
#     if (diff.sum() > 0):
    df.loc[output_row_name] = diff
#         df = move_row_to_top(df, 'Missing')
    return df

def add_cohort_size_row(df):
    print("HERE")
    display(df.head(3))
    df.loc["Cohort Size"] = cohort_sizes_series
    df = move_row_to_top(df, 'Cohort Size')
    return df


# In[18]:


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


# In[19]:


# Create histograms for age in 2018
cohort_colors = {
    "influenza": "#d46868",
    "hepB": "#eda853",
    "hpv": "#71bf88",
    "pneumococcal(<65)": "#6198cf",
    "pneumococcal(>=65)": "#826cd9",
    "all": "#d96cce"
}
cohort_colors_list = list(cohort_colors.values())


# In[20]:


sns.set_palette(sns.color_palette(cohort_colors_list))


# # Demographics

# ## Import Datasets

# In[21]:


import_df("influenza_demographics")
import_df("hepB_demographics")
import_df("hpv_demographics")
import_df("pneumococcal_demographics_under_65")
import_df("pneumococcal_demographics_65_and_over")
import_df("all_patients_demographics")


# In[22]:


# Prepare dataframes for visualization
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]

change_cols_to_int(vac_demographic_dfs, ["age_at_vaccination"])
change_cols_to_int(demographic_dfs, ["age_in_2018"])


# In[27]:


hpv_demographics


# In[26]:


hpv_demographics[hpv_demographics['age_at_vaccination'] > 26]


# ## Analysis

# In[28]:


# Prepare dataframes for visualization
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]


# In[29]:


# Table for Pneumococcal Vaccine Types
# pneumococcal_demographics_under_65
# col_names = ['PCV13', 'PPSV23', "only_PPSV23"]
col_names = ['only_PCV13', 'only_PPSV23', "PCV13_and_PPSV23", "neither_PCV13_nor_PPSV23"]
pneumococcal_demographics_dfs = [pneumococcal_demographics_under_65, pneumococcal_demographics_65_and_over ]
pneumococcal_demographics_names = ["pneumococcal(<65)", "pneumococcal(>=65)"]
output_dfs = []

for i in range(len(pneumococcal_demographics_dfs)):
#     print(pneumococcal_demographics_dfs[i].PCV13.value_counts(sort=False))
#     print(pneumococcal_demographics_dfs[i].PPSV23.value_counts(sort=False))
#     print(pneumococcal_demographics_dfs[i].only_PPSV23.value_counts(sort=False))
    this_df = pneumococcal_demographics_dfs[i][col_names]
    this_df = this_df.apply(lambda x: x.value_counts(sort=False))
    this_df = this_df.T
#     print(this_df)
    this_df = this_df[[1]]
    
    this_df.columns =[pneumococcal_demographics_names[i]]
    total = len(pneumococcal_demographics_dfs[i].index)
    this_df['percent'] = this_df[pneumococcal_demographics_names[i]] * 100/ total
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df[pneumococcal_demographics_names[i]].astype(str) + " " + this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns = [pneumococcal_demographics_names[i]]
    output_dfs.append(this_df.stack())
    
    
pneumococcal_summary = pd.concat(output_dfs, axis=0).unstack() 
pneumococcal_summary.loc['Total'] = [len(df.index) for df in pneumococcal_demographics_dfs]
pneumococcal_summary.fillna('0 (0.0%)',inplace=True )
account_for_missing(pneumococcal_summary, 'Total', "Missing Patients")


# In[30]:


# Table for age in 2018
age_lists = [df.age_in_2018.describe().tolist() for df in demographic_dfs]
age_columns = [name + "_age (2018)" for name in demographic_df_names ]
age_summary = pd.DataFrame(list(zip(*age_lists)), columns=age_columns)
age_summary.set_index(all_patients_demographics.age_in_2018.describe().index, inplace=True)
account_for_missing(age_summary)


# In[26]:


# Table for age at vaccination
age_lists = [df.age_at_vaccination.describe().tolist() for df in vac_demographic_dfs]
age_columns = [name + "_age (vaccination)" for name in vac_demographic_df_names ]
age_summary = pd.DataFrame(list(zip(*age_lists)), columns=age_columns)
age_summary.set_index(influenza_demographics.age_in_2018.describe().index, inplace=True)
# display(age_summary)
account_for_missing(age_summary)


# Notes
#    - Average age is lower because we calculate the age from they first took the vaccine

# In[27]:


# Define function to create age histograms
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("white")
def create_age_at_vaccination_histogram(df, label, col_name, ax):
    bin_max = max(85, df[col_name].max())
    bins = [0, 18, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 80, bin_max]
    color = cohort_colors[label]
    dp =  sns.distplot(df[col_name], bins=bins, 
             kde=False, color=color, hist_kws={"alpha": 0.8}, axlabel = label + f" ({col_name})",
                        ax=ax)
    dp.set_xticks(bins)
    dp.yaxis.grid(True)
#     dp.xaxis.grid(True)
    return dp

def create_age_in_2018_histogram(df, label, col_name, ax):
    bin_max = max(85, df[col_name].max())
    bins = [0, 18, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 80, bin_max]
    color = cohort_colors[label]
    dp =  sns.distplot(df[col_name], bins=bins, 
             kde=False, color=color, hist_kws={"alpha": 0.8}, axlabel = label + f" ({col_name})",
                        ax=ax)
    dp.set_xticks(bins)
    dp.yaxis.grid(True)
#     dp.xaxis.grid(True)
    return dp


# In[28]:


# Create histograms for age at vaccination
width = 2
height = math.ceil(len(vac_demographic_dfs) / 2) 
fig, axes = plt.subplots(height, 2, figsize=(20, 20))
i = 0
for y in range(height):
    for x in range(width):
        if (i < len(vac_demographic_dfs)):
            create_age_at_vaccination_histogram(
                vac_demographic_dfs[i], vac_demographic_df_names[i], 
                "age_at_vaccination", axes[y, x])
            i = i + 1
        else:
            break
axes[2,1].set_visible(False)


# In[29]:


df = demographic_dfs[5]
len(df[df['age_in_2018']<18]) # number of patients who are under 18 in 2018


# In[30]:


# Create histograms for age in 2018
cohort_colors = {
    "influenza": "#6198cf",
    "hepB": "#71bf88",
    "hpv": "#d46868",
    "pneumococcal(<65)": "#eda853",
    "pneumococcal(>=65)": "#826cd9",
    "all": "#d96cce"
}

width = 2
height = math.ceil(len(demographic_dfs) / 2) 
fig, axes = plt.subplots(height, 2, figsize=(20, 20))
i = 0
for y in range(height):
    for x in range(width):
        if (i < len(demographic_dfs)):
            create_age_in_2018_histogram(demographic_dfs[i], demographic_df_names[i], "age_in_2018", axes[y, x])
            i = i + 1
        else:
            break


# In[31]:


# Prepare dataframes for visualization again
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]


# In[39]:


import scipy.stats as st
data = influenza_demographics['gender'].values
# st.t.interval(alpha=0.95, df=len(data)-1, loc=np.mean(data), scale=st.sem(data)) 
data


# In[41]:


import statsmodels.stats as stt


# In[43]:


ci_low, ci_upp = stt.proportion.proportion_confint(6106, 15346, alpha=0.05, method='normal')


# In[44]:


ci_low, ci_upp


# In[32]:


# Table for Gender
output_dfs = []
for i in range(len(demographic_dfs)):
    this_df = pd.DataFrame(demographic_dfs[i].gender.value_counts())
    total = len(demographic_dfs[i].index)
    this_df['percent'] = this_df['gender'] * 100/ total
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['gender'].astype(str) + " " + this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[demographic_df_names[i] + "_gender"]
    output_dfs.append(this_df.stack())
    
gender_summary = pd.concat(output_dfs, axis=0).unstack() 
gender_summary =  gender_summary[[i + "_gender" for i in demographic_df_names]]
num_of_rows = len(gender_summary.index)
gender_summary = gender_summary.reindex(['Female', 'Male', 'Not man only, not woman only, prefer not to answer, or skipped','No matching concept'])
assert ( len(gender_summary.index)== num_of_rows), "Reindex missing rows"

gender_summary.loc['Total Patients'] = [len(df.index) for df in demographic_dfs]
gender_summary.fillna('0 (0.0%)',inplace=True )
account_for_missing(gender_summary, "Total Patients", "Missing")


# In[33]:


# Table for Race
output_dfs = []
for i in range(len(demographic_dfs)):
    this_df = pd.DataFrame(demographic_dfs[i].race.value_counts())
    total = len(demographic_dfs[i].index)
    this_df['percent'] = this_df['race'] * 100/ total
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['race'].astype(str) + " " + this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[demographic_df_names[i]]
    output_dfs.append(this_df.stack())
    
race_summary = pd.concat(output_dfs, axis=0).unstack() 
race_summary =  race_summary[demographic_df_names]
num_of_rows = len(race_summary.index)
# print(race_summary.index)
race_summary = race_summary.reindex(['White', 'Black or African American', 'Asian', 'Another single population', 'More than one population', 'I prefer not to answer', 'PMI: Skip', 
                   'None Indicated', 'None of these', 'No matching concept'])
# print(race_summary.index)
assert len(race_summary.index) == num_of_rows, "Reindex not missing any rows"
race_summary.loc['Unique Patients'] = [len(df.person_id.unique()) for df in demographic_dfs]
race_summary = move_row_to_top(race_summary, 'Unique Patients')
race_summary = add_cohort_size_row(race_summary)
race_summary = account_for_missing(race_summary, "Unique Patients", "Missing")
display(create_download_link(race_summary))
display(race_summary)


# In[34]:


# Table for Ethnicity
output_dfs = []
for i in range(len(demographic_dfs)):
    this_df = pd.DataFrame(demographic_dfs[i].ethnicity.value_counts())
    total = len(demographic_dfs[i].index)
    this_df['percent'] = this_df['ethnicity'] * 100/ total
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['ethnicity'].astype(str) + " " + this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[demographic_df_names[i]]
    output_dfs.append(this_df.stack())
    
ethnicity_summary = pd.concat(output_dfs, axis=0).unstack() 
ethnicity_summary =  ethnicity_summary[demographic_df_names]
num_of_rows = len(ethnicity_summary.index)
# display(ethnicity_summary)
# print(ethnicity_summary.index)
ethnicity_summary.rename({'What Race Ethnicity: Race Ethnicity None Of These': "None of These"}, axis=0, inplace=True)
ethnicity_summary = ethnicity_summary.reindex(['Not Hispanic or Latino','Hispanic or Latino', 'None of These','PMI: Prefer Not To Answer','PMI: Skip' ])
# print(ethnicity_summary.index)
assert ( len(ethnicity_summary.index)== num_of_rows), "Reindex missing rows"
ethnicity_summary.loc['Unique Patients'] = [len(df.person_id.unique()) for df in demographic_dfs]
ethnicity_summary = move_row_to_top(ethnicity_summary, 'Unique Patients')
ethnicity_summary = add_cohort_size_row(ethnicity_summary)
ethnicity_summary = account_for_missing(ethnicity_summary, "Unique Patients", "Missing")
display(create_download_link(ethnicity_summary))
display(ethnicity_summary)


# In[35]:


# Table for States
output_dfs = []
for i in range(len(demographic_dfs)):
    this_df = pd.DataFrame(demographic_dfs[i].state.value_counts())
    this_df.reset_index(inplace=True)
    this_df.columns = ["state", 'count']
    total = this_df['count'].sum()
    this_df['percent'] = this_df['count'] * 100 / total
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['state'].astype(str) + " : " + this_df['count'].astype(str) + " " +this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[demographic_df_names[i] + "_states"]
    output_dfs.append(this_df.stack())
    
states_summary = pd.concat(output_dfs, axis=0).unstack() 
states_summary =  states_summary[[i + "_states" for i in demographic_df_names]]
states_summary.loc['Cohort Size'] = [len(df.index) for df in demographic_dfs]
states_summary = account_for_missing(states_summary, "Cohort Size", "Missing")
states_summary = move_row_to_top(states_summary, 'Missing')
states_summary = move_row_to_top(states_summary, 'Cohort Size')
states_summary.head(22)


# In[36]:


# Prepare dataframes for visualization again
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]


# In[37]:


# Bar Charts for Vaccination Year
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("whitegrid")
#sns.set_palette("flare")

# Bar graph by year for date of first vaccine exposure
def create_vaccination_year_bar(df, label, ax):
    df_with_vaccination_year = df.copy()
    df_with_vaccination_year["vaccine_year"] =  pd.DatetimeIndex(df_with_vaccination_year['drug_exposure_start_date']).year
    df_with_vaccination_year = (df_with_vaccination_year.groupby(['vaccine_year'])
                .size()
                .reset_index(name="patients"))
    pal = create_color_palette(df_with_vaccination_year['vaccine_year'].max(), df_with_vaccination_year['vaccine_year'].min())
    bp = sns.barplot(x='vaccine_year', y="patients", data=df_with_vaccination_year,
                       ax=ax, palette=pal)
    bp.set(xlabel=label)
    return bp

# Create bar for year of vaccination
# width = 2
# height = math.ceil(len(vac_demographic_dfs) / 2) 
# fig, axes = plt.subplots(height, 2, figsize=(20, 20))
# i = 0
# for y in range(height):
#     for x in range(width):
#         if (i < len(vac_demographic_dfs)):
#             create_vaccination_year_bar(vac_demographic_dfs[i], vac_demographic_df_names[i], axes[y, x])
#             i = i + 1
#         else:
#             break
height = len(vac_demographic_dfs)
fig, axes = plt.subplots(height, 1, figsize=(20, 40))
for y in range(height):
        create_vaccination_year_bar(vac_demographic_dfs[y], vac_demographic_df_names[y], axes[y])
    


# In[ ]:





# # BASICS Survey

# ## Import BASICS Survey

# In[26]:


import_df("influenza_basics")
import_df("hepB_basics")
import_df("hpv_basics")
import_df("pneumococcal_under_65_basics")
import_df("pneumococcal_65_and_over_basics")
import_df("all_patients_basics")


# ## Analysis

# In[65]:


# Aggregate BASICS dataframes
with_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, 
                   pneumococcal_under_65_basics, pneumococcal_65_and_over_basics, all_patients_basics]
basics_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, 
                   pneumococcal_under_65_basics, pneumococcal_65_and_over_basics]
vac_basics_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]
        
change_cols_to_date(vac_basics_dfs, ['drug_exposure_start_date', 'survey_date'])     


# In[66]:


# Create bar graphs for survey year
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("whitegrid")
#sns.set_palette("flare")

# Bar graph by year of survey response
def create_survey_year_bar(df, label, ax):
    df_with_survey_year = df.copy()
    df_with_survey_year["survey_year"] =  pd.DatetimeIndex(df_with_survey_year['survey_date']).year
    # if a patient has multiple surveys, get earliest year 
    df_with_survey_year = df_with_survey_year.groupby(['person_id'])['survey_year'].min().reset_index(name="survey_year")
    df_with_survey_year = df_with_survey_year.groupby(['survey_year']).size().reset_index(name="patients")
    assert df_with_survey_year['patients'].sum() == len(df['person_id'].unique())
    pal = create_color_palette(df_with_survey_year['survey_year'].max(), df_with_survey_year['survey_year'].min())
    bp= sns.barplot(x='survey_year', y="patients", data=df_with_survey_year,
                       ax=ax, palette=pal)
    bp.set(xlabel=label)
    return bp

# Create bar for year of survey
width = 2
height = math.ceil(len(with_basics_dfs) / 2) 
fig, axes = plt.subplots(height, 2, figsize=(20, 20))
i = 0
for y in range(height):
    for x in range(width):
        if (i < len(with_basics_dfs)):
            create_survey_year_bar(with_basics_dfs[i], basics_df_names[i], axes[y, x])
            i = i + 1
        else:
            break


# In[67]:


# Aggregate BASICS dataframes
with_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, 
                   pneumococcal_under_65_basics, pneumococcal_65_and_over_basics, all_patients_basics]
basics_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]

# with_basics_dfs


# In[68]:


len(hepB_basics.person_id.unique())


# In[69]:


# Table for Vaccination Date - Survey Date 
day_diff_lists = []
day_diff_description = []
for df in vac_basics_dfs:
    group_by_patients = df.copy()

    group_by_patients = group_by_patients.groupby(['person_id','drug_exposure_start_date'])['survey_date'].min().reset_index(name='survey_date')
    day_diff = (group_by_patients['drug_exposure_start_date'] - group_by_patients['survey_date']).dt.days.astype(int)
    day_diff_lists.append(day_diff.describe().tolist())
    day_diff_description = day_diff.describe().index
    
day_diff_columns = [name for name in vac_basics_df_names ]   
day_diff_summary = pd.DataFrame(list(zip(*day_diff_lists)), columns=day_diff_columns)
day_diff_summary.set_index(day_diff_description, inplace=True)
day_diff_summary
day_diff_summary = add_cohort_size_row(day_diff_summary)
account_for_missing(day_diff_summary)   


# In[70]:


# Create box plot for Vaccination Date - Survey Date
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("whitegrid")
#sns.set_palette("flare")


diff_df = pd.DataFrame(columns=['day_diff', 'cohort'])
for i in range(len(vac_basics_dfs)):
    group_by_patients = vac_basics_dfs[i].copy()
    group_by_patients = group_by_patients.groupby(['person_id','drug_exposure_start_date'])['survey_date'].min().reset_index(name='survey_date')
    day_diff = (group_by_patients['drug_exposure_start_date'] - group_by_patients['survey_date']).dt.days.astype(int)
    temp = pd.DataFrame(data=day_diff, columns=['day_diff'])
    temp['cohort'] = vac_basics_df_names[i]
    diff_df = diff_df.append(temp, ignore_index=True)
#     print(diff_df)
    
# diff_df
plt.figure(figsize=(20, 10))
box = sns.boxplot(y='day_diff', x='cohort', data=diff_df)


# In[28]:


# Aggregate BASICS dataframes
with_basics_dfs = [influenza_basics, hepB_basics, hpv_basics, 
                   pneumococcal_under_65_basics, pneumococcal_65_and_over_basics, all_patients_basics]
basics_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]

# with_basics_dfs


# In[29]:


# Create tables for all questions in BASICS Survey
questions = []
# questions_ = {question: pd.DataFrame(columns=basics_df_names) for question in list(hepB_basics_df['question'].unique())}
questions = dict()
questions_output_dfs = {question: [] for question in list(all_patients_basics['question'].unique())}
patient_counts = dict()

for i in range(len(with_basics_dfs)):
#     print(basics_df_names[i])
    temp_df = with_basics_dfs[i].copy()
    temp = temp_df.groupby(['question', 'answer']).size().reset_index(name="count")
    for (q), count_df in temp.groupby(['question']):
        t = count_df[['answer', 'count']].set_index('answer', drop=True)
        temp_df_q = temp_df[temp_df['question']==q]
        unique_patients = len(temp_df_q.person_id.unique())
        assert len(temp_df_q.index) ==  t['count'].sum()
#         print(len(temp_df_q.index), t['count'].sum())
        t = t[['count']]
        total_answers = t['count'].sum()
        assert total_answers >= unique_patients
        t.loc["Unique Patients"] = unique_patients
        t.loc['Total Answers'] = total_answers
        t.columns = [basics_df_names[i]]
        questions_output_dfs[q].append(t)

for key, list_of_dfs in questions_output_dfs.items():
    temp_df =  pd.concat(list_of_dfs, axis=1)
    temp_df = add_cohort_size_row(temp_df)
    temp_df = account_for_missing(temp_df, "Unique Patients", "Missing Patients")
    temp_df['mean'] = temp_df.mean(axis=1)
    temp_df = temp_df.sort_values(['mean'], ascending=[False])
#     display(temp_df)

    
    if (key == "Income: Annual Income"):
        temp_df.loc["Prefer not to answer, skipped, or missing"] = temp_df.loc["PMI: Prefer Not To Answer"] + temp_df.loc["PMI: Skip"] + temp_df.loc["Missing Patients"] 
#     display(temp_df)
    for col in temp_df.columns:
#         total = temp_df[col].loc['Total Answers']
        total = temp_df[col].loc['Cohort Size']

        total_patients = temp_df[col].loc['Unique Patients']
        

        temp_df[col + "_percent"] = temp_df[col] * 100 / total
        temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
        temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
        temp_df[col] =  temp_df[col + "_combined"]
        temp_df.at['Unique Patients', col] = total_patients
        temp_df = move_row_to_top(temp_df, "Unique Patients")
        temp_df.at['Total Answers', col] = total
        
        
    
    temp_df = temp_df[basics_df_names]
    questions[key] = temp_df

for key, table in questions.items():
    initial_index_count = len(table.index)
    if (key == "Income: Annual Income"):
#         table = table.reindex(['Cohort Size', 'Unique Patients','Total Answers', 'Annual Income: less 10k',
#             'Annual Income: 10k 25k', 'Annual Income: 25k 35k', 
#             'Annual Income: 35k 50k','Annual Income: 50k 75k',
#             'Annual Income: 75k 100k','Annual Income: 100k 150k', 
#            'Annual Income: 150k 200k',
#             'Annual Income: more 200k', 'PMI: Prefer Not To Answer', 'PMI: Skip'])
            table = table.reindex(['Cohort Size', 'Unique Patients','Total Answers', 'Annual Income: less 10k',
                'Annual Income: 10k 25k', 'Annual Income: 25k 35k', 
                'Annual Income: 35k 50k','Annual Income: 50k 75k',
                'Annual Income: 75k 100k','Annual Income: 100k 150k', 
               'Annual Income: 150k 200k',
                'Annual Income: more 200k', 'Prefer not to answer, skipped, or missing'])

    elif (key=="Health Insurance: Insurance Type Update"):
        continue
#     assert len(table.index) == initial_index_count
    print(key)
    table = add_cohort_size_row(table)
#     table = account_for_missing(table, "Unique Patients", "Missing Patients")
    display(table)
    display(create_download_link(table))
#     display(table)


# In[73]:


# BASICS Survey: Health Insurance: Insurance Type Update
question = "Health Insurance: Insurance Type Update"
insurance_groups = {
    "Private Health Insurance": ["Insurance Type Update: Purchased", 
                                 "Insurance Type Update: Employer Or Union"],
    "Medicare" : ["Insurance Type Update: Medicare"],
    "Medicaid":["Insurance Type Update: Medicaid", ],
    "Military Healthcare":["Insurance Type Update: VA",
                          "Insurance Type Update: Military"
                          ],
    "Indian Health Service":["Insurance Type Update: Indian"],
    "Other Type of Health Insurance":["Insurance Type Update: Other Health Plan"],
    "No Health Insurance": ["Insurance Type Update: None"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip"],
    "Total Answers" : ["Total Answers"],
    "Total Patients" : ["Total Patients"]
    
}
insurance_mapping = dict()
for k, v in insurance_groups.items():
    for x in v:
        insurance_mapping[x] = k
# print(insurance_mapping)
    
# print(get_group("Insurance Type Update: None"))
output_dfs = []
for i in range(len(with_basics_dfs)):
    temp = with_basics_dfs[i].copy()
    temp = temp.groupby(['question', 'answer']).size().reset_index(name="count")
    total_patients = len(with_basics_dfs[i]['person_id'].unique())
    for (q), count_df in temp.groupby(['question']):
        if (q == question):
            t = count_df[['answer', 'count']].set_index('answer', drop=True)
            t = t[['count']]
            total = t['count'].sum()
            t.loc['Total Answers'] = total
            t.loc['Total Patients'] = total_patients
            t.columns = [basics_df_names[i]]
            output_dfs.append(t)


temp_df =  pd.concat(output_dfs, axis=1)
# temp_df['mean'] = temp_df.mean(axis=1)
# temp_df = temp_df.sort_values(['mean'], ascending=[False])
temp_df = temp_df.groupby(by=insurance_mapping, axis=0).sum()
# print(temp_df)

for col in temp_df.columns:
    total = temp_df[col].loc['Total Answers']
    total_patients = temp_df[col].loc['Total Patients']
    
    temp_df[col + "_percent"] = temp_df[col] * 100 / total
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Total Answers', col] = total
    temp_df.at['Total Patients', col] = total_patients
    

temp_df = temp_df[basics_df_names]
# temp_df

initial_index_count = len(temp_df.index)
temp_df = temp_df.reindex(['Total Patients', 'Total Answers', 'Medicaid', 'Medicare','Private Health Insurance',
       'Military Healthcare','Indian Health Service',
       'Other Type of Health Insurance','No Health Insurance' , 'Invalid', 'Skip',
       ])
# print(temp_df.index)
# assert len(temp_df.index) == initial_index_count
# print(question)
# account_for_missing(temp_df, "Total Patients", "Missing Patients")
# display(temp_df)
 


# In[74]:


#%%script false --no-raise-error


# # Overall Health Survey

# ## Import Overall Health Survey

# In[75]:


import_df("influenza_overall_health")
import_df("hepB_overall_health")
import_df("hpv_overall_health")
import_df("pneumococcal_under_65_overall_health")
import_df("pneumococcal_65_and_over_overall_health")
import_df("all_patients_overall_health")


# In[76]:


pneumococcal_under_65_overall_health


# ## Analysis

# In[77]:


# Aggregate overall_health dataframes
with_overall_health_dfs = [influenza_overall_health, hepB_overall_health, hpv_overall_health, 
                   pneumococcal_under_65_overall_health, pneumococcal_65_and_over_overall_health, all_patients_overall_health]
overall_health_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
change_ids_to_str(with_overall_health_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
# with_overall_health_dfs


# In[78]:


# overall_health Survey: Organ Transplant
question = "Overall Health: Organ Transplant"
output_dfs = []
for i in range(len(with_overall_health_dfs)):
    temp = with_overall_health_dfs[i].copy()
    temp = temp.groupby(['question', 'answer']).size().reset_index(name="count")
    total = len(with_overall_health_dfs[i]['person_id'].unique())
    for (q), count_df in temp.groupby(['question']):
        if (q == question):
            t = count_df[['answer', 'count']].set_index('answer', drop=True)
            t = t[['count']]
            t.loc['Unique Patients'] = total
            t.columns = [overall_health_df_names[i]]
            t.index.name = 'patient_count'
            output_dfs.append(t)


temp_df =  pd.concat(output_dfs, axis=1)
organ_transplants_pts = temp_df.loc['Organ Transplant: Yes',:]

for col in temp_df.columns:
    total = temp_df[col].loc['Unique Patients']
    temp_df[col + "_percent"] = temp_df[col] * 100 / total
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Unique Patients', col] = total

temp_df = temp_df[overall_health_df_names]

initial_index_count = len(temp_df.index)
assert len(temp_df.index) == initial_index_count
print(question)
# display(temp_df)
add_cohort_size_row(temp_df)
temp_df = move_row_to_top(temp_df, "Unique Patients")
temp_df = move_row_to_top(temp_df, "Cohort Size")
temp_df = account_for_missing(temp_df, "Unique Patients", "Missing")
display(create_download_link(temp_df))
display(temp_df)


# In[79]:


print(organ_transplants_pts)


def tranplants_find_missing(df, total_row_name='count', output_row_name="missing" ):
    diff = organ_transplants_pts.subtract(df.loc[total_row_name].astype(int)).fillna(0)
#     if (diff.sum() > 0):
    df.loc[output_row_name] = diff
#         df = move_row_to_top(df, 'Missing')
    return df


# In[80]:


# overall_health Survey: Organ Transplant Description
question = "Organ Transplant: Organ Transplant Description"
output_dfs = []
for i in range(len(with_overall_health_dfs)):
    temp_df = with_overall_health_dfs[i].copy()
    temp_df = temp_df[temp_df['question']==question]
    temp_df = temp_df[temp_df['answer']!="PMI: Skip"]
#     print(len(temp_df.person_id.unique()))
#     print(temp_df['answer'].describe())
#     total = temp_df.answer.value_counts().to_frame('count')['count'].sum()
    
    temp = temp_df.groupby(['question', 'answer']).size().reset_index(name="count")
#     total = len(with_overall_health_dfs[i]['person_id'].unique())
    for (q), count_df in temp.groupby(['question']):
        t = count_df[['answer', 'count']].set_index('answer', drop=True)
        t = t[['count']]
#         t.drop('PMI: Skip', axis=0, inplace=True)
        total = t['count'].sum()
        t.loc['Unique Patients'] = len(temp_df.person_id.unique())
        t.loc['Total Transplants'] = total
#         print(len(temp_df.person_id.unique()), total)
        t.columns = [overall_health_df_names[i]]
        output_dfs.append(t)


temp_df =  pd.concat(output_dfs, axis=1,sort=True)
temp_df.loc['Answered Organ Transplant: Yes'] = organ_transplants_pts
temp_df = tranplants_find_missing(temp_df, "Unique Patients", "Missing Patients")
temp_df.fillna(0,inplace=True )
temp_df = temp_df.astype(int)
for col in temp_df.columns:
    total = temp_df[col].loc['Total Transplants']
    unique = temp_df[col].loc['Unique Patients']
    answered_yes = temp_df[col].loc['Answered Organ Transplant: Yes']
    
    temp_df[col + "_percent"] = temp_df[col] * 100 / answered_yes
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Total Transplants', col] = total
    temp_df.at['Unique Patients', col] = unique
    
temp_df = temp_df[overall_health_df_names]
temp_df = move_row_to_top(temp_df, 'Total Transplants' )
temp_df = move_row_to_top(temp_df, 'Unique Patients' )
temp_df.loc['Answered Organ Transplant: Yes'] = organ_transplants_pts
temp_df = move_row_to_top(temp_df, 'Answered Organ Transplant: Yes' )

initial_index_count = len(temp_df.index)
assert len(temp_df.index) == initial_index_count
print(question)
temp_df = temp_df.reindex(['Answered Organ Transplant: Yes', 'Unique Patients',
       'Total Transplants', 'Organ Transplant Description: Blood Vessels',
       'Organ Transplant Description: Bone',
       'Organ Transplant Description: Cornea',
       'Organ Transplant Description: Heart',
       'Organ Transplant Description: Intestine',
       'Organ Transplant Description: Kidney',
       'Organ Transplant Description: Liver',
       'Organ Transplant Description: Lung',  
       'Organ Transplant Description: Pancreas',
       'Organ Transplant Description: Skin',
       'Organ Transplant Description: Valve', 
      'Organ Transplant Description: Other Organ',
       'Organ Transplant Description: Other Tissue',
         'Missing Patients'])
 
display(temp_df)
display(create_download_link(temp_df))
# print(temp_df.index)


# In[81]:


#%%script false --no-raise-error


# # Lifestyle Survey

# ## Import Lifestyle Survey

# In[ ]:


import_df("influenza_lifestyle")
import_df("hepB_lifestyle")
import_df("hpv_lifestyle")
import_df("pneumococcal_under_65_lifestyle")
import_df("pneumococcal_65_and_over_lifestyle")
import_df("all_patients_lifestyle")


# ## Analysis

# In[ ]:


# Aggregate lifestyle dataframes
with_lifestyle_dfs = [influenza_lifestyle, hepB_lifestyle, hpv_lifestyle, 
                   pneumococcal_under_65_lifestyle, pneumococcal_65_and_over_lifestyle, all_patients_lifestyle]
lifestyle_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
change_ids_to_str(with_lifestyle_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
# with_lifestyle_dfs


# In[ ]:


# define create_tables

def create_tables(list_of_dfs, df_names):
    questions = []
    questions = dict()
    questions_output_dfs = {question: [] for question in list(list_of_dfs[-1]['question'].unique())}
    for i in range(len(list_of_dfs)):
        temp = list_of_dfs[i].copy()
        temp = temp.groupby(['question', 'answer']).size().reset_index(name="count")
#         total = len(list_of_dfs[i]['person_id'].unique())
        for (q), count_df in temp.groupby(['question']):
            t = count_df[['answer', 'count']].set_index('answer', drop=True)
            t = t[['count']]
            total = t['count'].sum()
            t.loc['Total Answers'] = total
            t.columns = [df_names[i]]
            questions_output_dfs[q].append(t)

    for key, list_of_dfs in questions_output_dfs.items():
        temp_df =  pd.concat(list_of_dfs, axis=1, sort=True)
        temp_df = add_cohort_size_row(temp_df)
        temp_df = account_for_missing(temp_df, "Total Answers", "Missing")
        if (key == "Smoking: 100 Cigs Lifetime" ):
            temp_df.loc["Prefer not to answer, Don't Know, Skipped, or Missing"] = temp_df.loc["PMI: Prefer Not To Answer"] + temp_df.loc["PMI: Dont Know"] + temp_df.loc["PMI: Skip"] + temp_df.loc["Missing"]
        elif(key in ["Alcohol: Drink Frequency Past Year", "Alcohol: Average Daily Drink Count"] ):
            temp_df.loc["Prefer not to answer, Skipped, or Missing"] = temp_df.loc["PMI: Prefer Not To Answer"] + temp_df.loc["PMI: Skip"] + temp_df.loc["Missing"]
#         display(temp_df)
        temp_df.fillna(0,inplace=True )
        temp_df = temp_df.astype(int)
        temp_df['mean'] = temp_df.mean(axis=1)
        temp_df = temp_df.sort_values(['mean'], ascending=[False])

        for col in temp_df.columns:
            total = temp_df[col].loc['Total Answers']
            cohort_size = temp_df[col].loc['Cohort Size']
            
            temp_df[col + "_percent"] = temp_df[col] * 100 / cohort_size
            temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
            temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
            temp_df[col] =  temp_df[col + "_combined"]
            temp_df.at['Total Answers', col] = total
            temp_df.at['Cohort Size', col] = cohort_size
            
            

        temp_df = temp_df[df_names]
        questions[key] = temp_df
    return questions
        


# In[ ]:


# Create tables for all questions in Lifestyle Survey
lifestyle_tables = create_tables(with_lifestyle_dfs, lifestyle_df_names)
for key, table in lifestyle_tables.items():
    initial_index_count = len(table.index)
    assert len(table.index) == initial_index_count
#     print(key)
#     display(table)


# ### Cigarettes

# In[ ]:


# Have you smoked 100 cigarettes?
# temp_index = ['Cohort Size', 'Total Answers','100 Cigs Lifetime: Yes', '100 Cigs Lifetime: No','PMI: Dont Know',
#        'PMI: Prefer Not To Answer', 'PMI: Skip', "Missing"]
temp_index = ['Cohort Size', 'Total Answers','100 Cigs Lifetime: Yes', '100 Cigs Lifetime: No', "Prefer not to answer, Don't Know, Skipped, or Missing"]
lifestyle_tables['Smoking: 100 Cigs Lifetime'] = lifestyle_tables['Smoking: 100 Cigs Lifetime'].reindex(temp_index)
df = lifestyle_tables['Smoking: 100 Cigs Lifetime']
# df = add_cohort_size_row(df)
# df = account_for_missing(df, "Total Answers", "Missing")
df = move_row_to_top(df, "Cohort Size")
display(df)
display(create_download_link(df))


# In[ ]:


# Cigarette Smoking Frequency
temp_index =['Total Answers', 'Smoke Frequency: Every Day',
       'Smoke Frequency: Some Days','Smoke Frequency: Not At All',
             'PMI: Dont Know', 'PMI: Prefer Not To Answer', 'PMI: Skip',]

lifestyle_tables['Smoking: Smoke Frequency'] = lifestyle_tables['Smoking: Smoke Frequency'].reindex(temp_index)
df = lifestyle_tables['Smoking: Smoke Frequency'] 
df = add_cohort_size_row(df)
df = account_for_missing(df, "Total Answers", "Missing")
df = move_row_to_top(df, "Cohort Size")
display(df)
display(create_download_link(df))


# ### Other Forms of Smoking

# In[ ]:


# Electronic Smoking
print('Electronic Smoking: Electric Smoke Participant')
display(lifestyle_tables['Electronic Smoking: Electric Smoke Participant'])
print('Electronic Smoking: Electric Smoke Frequency')
display(lifestyle_tables['Electronic Smoking: Electric Smoke Frequency'])


# In[ ]:


# Cigar Smoking
print('Cigar Smoking: Cigar Smoke Participant')
display(lifestyle_tables['Cigar Smoking: Cigar Smoke Participant'])
print('Cigar Smoking: Current Cigar Frequency')
display(lifestyle_tables['Cigar Smoking: Current Cigar Frequency'])


# In[ ]:


# Hookah Smoking
print('Hookah Smoking: Hookah Smoke Participant')
display(lifestyle_tables['Hookah Smoking: Hookah Smoke Participant'])
print('Hookah Smoking: Current Hookah Frequency')
display(lifestyle_tables['Hookah Smoking: Current Hookah Frequency'])


# In[ ]:


# Smokeless Tobacco
print('Smokeless Tobacco: Hookah Smoke Participant')
display(lifestyle_tables['Smokeless Tobacco: Smokeless Tobacco Participant'])
print('Smokeless Tobacco: Current Hookah Frequency')
display(lifestyle_tables['Smokeless Tobacco: Smokeless Tobacco Frequency'])


# ### Alcohol

# In[ ]:


# Alcohol
print('Alcohol: Alcohol Participant')
df = (lifestyle_tables['Alcohol: Alcohol Participant'])
df = add_cohort_size_row(df)
df = account_for_missing(df, "Total Answers", "Missing")
df = move_row_to_top(df, "Cohort Size")
display(df)
display(create_download_link(df))

print('Alcohol: Drink Frequency Past Year')
alc_freq = lifestyle_tables['Alcohol: Drink Frequency Past Year']
# alc_freq = alc_freq.reindex(["Total Answers", 'Drink Frequency Past Year: Monthly Or Less','Drink Frequency Past Year: 2 to 3 Per Week',
#            'Drink Frequency Past Year: 2 to 4 Per Month',
#            'Drink Frequency Past Year: 4 or More Per Week','Drink Frequency Past Year: Never', 'PMI: Prefer Not To Answer', 'PMI: Skip'])
alc_freq = alc_freq.reindex(["Total Answers", 'Drink Frequency Past Year: Monthly Or Less','Drink Frequency Past Year: 2 to 3 Per Week',
           'Drink Frequency Past Year: 2 to 4 Per Month',
           'Drink Frequency Past Year: 4 or More Per Week','Drink Frequency Past Year: Never', "Prefer not to answer, Skipped, or Missing"])

df = (alc_freq)
df = add_cohort_size_row(df)
# df = account_for_missing(df, "Total Answers", "Missing")
# df = move_row_to_top(df, "Cohort Size")
display(df)
display(create_download_link(df))

print('Alcohol: Average Daily Drink Count')
df = (lifestyle_tables['Alcohol: Average Daily Drink Count'])
df = add_cohort_size_row(df)
df = account_for_missing(df, "Total Answers", "Missing")
df = move_row_to_top(df, "Cohort Size")
# print(df.index) 
df = df.reindex(['Cohort Size', 'Total Answers', 'Average Daily Drink Count: 1 or 2',
     'Average Daily Drink Count: 3 or 4',
       'Average Daily Drink Count: 5 or 6',
     'Average Daily Drink Count: 7 to 9',
       'Average Daily Drink Count: 10 or More',  "Prefer not to answer, Skipped, or Missing"])
display(df)
display(create_download_link(df))


# In[ ]:


for i in lifestyle_tables.keys():
    print(i)


# In[ ]:


# Aggregate lifestyle dataframes
with_lifestyle_dfs = [influenza_lifestyle, hepB_lifestyle, hpv_lifestyle, 
                   pneumococcal_under_65_lifestyle, pneumococcal_65_and_over_lifestyle, all_patients_lifestyle]
lifestyle_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
# with_lifestyle_dfs


# In[ ]:


alc_participant_concept = '1586198'
alc_freq_concept = '1586201'
alc_drink_count_concept = '1586207'


# In[ ]:


def create_alcohol_summary(df):
    copy_df = df.copy()
    alc_participants = copy_df[copy_df['question_concept_id']==alc_participant_concept].copy()
    alc_freqs = copy_df[copy_df['question_concept_id']==alc_freq_concept].copy()
    alc_drink_counts = copy_df[copy_df['question_concept_id']==alc_drink_count_concept].copy()
    assert len(alc_freqs[alc_freqs['person_id'].duplicated()])==0
    assert len(alc_participants[alc_participants['person_id'].duplicated()])==0
    alc_freqs[['person_id', 'alc_freq_answer']] = alc_freqs[['person_id', 'answer']]
    alc_freqs = alc_freqs[['person_id', 'alc_freq_answer']]
    alc_drink_counts[['person_id', 'alc_count_answer']] = alc_drink_counts[['person_id', 'answer']]
    alc_drink_counts = alc_drink_counts[['person_id', 'alc_count_answer']]
    alc_participants[['person_id', 'alc_participant_answer']] = alc_participants[['person_id', 'answer']]
    alc_participants = alc_participants[['person_id', 'alc_participant_answer']]
    participant_tables = dict()
    for name, group in alc_participants.groupby(['alc_participant_answer']):
        drink_counts = alc_drink_counts[alc_drink_counts['person_id'].isin(group['person_id'].values)]
        describe_counts = drink_counts['alc_count_answer'].value_counts()
        describe_counts.at['Total']=  len(group)
        participant_tables[name] = describe_counts
    participant_summary = pd.concat(list(participant_tables.values()), axis=1, sort=True)
    participant_summary.fillna(0,inplace=True )
    participant_summary = participant_summary.astype(int)
    participant_summary.columns=list(participant_tables.keys())
    participant_summary = participant_summary.T
    participant_indices = ['Alcohol Participant: Yes', 'Alcohol Participant: No',
        'PMI: Prefer Not To Answer', 'PMI: Skip']                   
    participant_summary = participant_summary.reindex(participant_indices)
    participant_summary.index.name="answer"
    participant_summary['question'] = "Alcohol Participant"
    participant_summary.reset_index(inplace=True)
    participant_summary["answer"] = participant_summary["answer"].apply(lambda x: x.replace("Alcohol Participant: ", ""))
    yes_participant = participant_summary.iloc[:1, :].copy()
    yes_participant['question'] = "Alcohol Participant: Yes"
    other_participant = participant_summary.iloc[1:, :].copy()
    other_participant['question'] = "Alcohol Participant: Other"
    yes_participant= yes_participant.set_index(["question", "answer"])
    other_participant= other_participant.set_index(["question", "answer"])
    output_tables = dict()
    output_df = pd.DataFrame(index=alc_drink_counts['alc_count_answer'].unique(), columns=alc_freqs['alc_freq_answer'].unique())
    for name, group in alc_freqs.groupby(['alc_freq_answer']):
        drink_counts = alc_drink_counts[alc_drink_counts['person_id'].isin(group['person_id'].values)]
        describe_counts = drink_counts['alc_count_answer'].value_counts()
        describe_counts.at['Total']=  len(group)
        output_tables[name] = describe_counts
    freq_summary = pd.concat(list(output_tables.values()), axis=1, sort=True)
    freq_summary.fillna(0,inplace=True )
    freq_summary = freq_summary.astype(int)
    freq_summary.columns=list(output_tables.keys())
    freq_summary = freq_summary.T
    drink_freq_indices = ['Drink Frequency Past Year: Monthly Or Less', 'Drink Frequency Past Year: 2 to 3 Per Week',
           'Drink Frequency Past Year: 2 to 4 Per Month',
           'Drink Frequency Past Year: 4 or More Per Week','Drink Frequency Past Year: Never', 'PMI: Prefer Not To Answer', 'PMI: Skip']
    freq_summary = freq_summary.reindex(drink_freq_indices)
    freq_summary.index.name="answer"
    freq_summary['question'] = "Drink Frequency Past Year"
    freq_summary.reset_index(inplace=True)
    freq_summary["answer"] = freq_summary["answer"].apply(lambda x: x.replace("Drink Frequency Past Year: ", ""))
    freq_summary= freq_summary.set_index(["question", "answer"])
    alc_dfs = pd.concat([yes_participant, freq_summary, other_participant])
#     print(alc_dfs.columns)
    alc_dfs = alc_dfs[['Average Daily Drink Count: 1 or 2',
       'Average Daily Drink Count: 3 or 4',
       'Average Daily Drink Count: 5 or 6',
       'Average Daily Drink Count: 7 to 9', 
        'Average Daily Drink Count: 10 or More',
                       'PMI: Prefer Not To Answer',
       'PMI: Skip', 'Total']]
    display(alc_dfs)


# In[ ]:


def create_alcohol_graphs(df):
    copy_df = df.copy()
    alc_participants = copy_df[copy_df['question_concept_id']==alc_participant_concept].copy()
    alc_freqs = copy_df[copy_df['question_concept_id']==alc_freq_concept].copy()
    alc_drink_counts = copy_df[copy_df['question_concept_id']==alc_drink_count_concept].copy()
    assert len(alc_freqs[alc_freqs['person_id'].duplicated()])==0
    assert len(alc_participants[alc_participants['person_id'].duplicated()])==0
    alc_freqs[['person_id', 'alc_freq_answer']] = alc_freqs[['person_id', 'answer']]
    alc_freqs = alc_freqs[['person_id', 'alc_freq_answer']]
    alc_drink_counts[['person_id', 'alc_count_answer']] = alc_drink_counts[['person_id', 'answer']]
    alc_drink_counts = alc_drink_counts[['person_id', 'alc_count_answer']]
    alc_participants[['person_id', 'alc_participant_answer']] = alc_participants[['person_id', 'answer']]
    alc_participants = alc_participants[['person_id', 'alc_participant_answer']]
    participant_tables = dict()
    for name, group in alc_participants.groupby(['alc_participant_answer']):
        drink_counts = alc_drink_counts[alc_drink_counts['person_id'].isin(group['person_id'].values)]
        describe_counts = drink_counts['alc_count_answer'].value_counts()
        describe_counts.at['Total']=  len(group)
        participant_tables[name] = describe_counts
    participant_summary = pd.concat(list(participant_tables.values()), axis=1, sort=True)
    participant_summary.fillna(0,inplace=True )
    participant_summary = participant_summary.astype(int)
    participant_summary.columns=list(participant_tables.keys())
    participant_summary = participant_summary.T
    participant_indices = ['Alcohol Participant: Yes', 'Alcohol Participant: No',
        'PMI: Prefer Not To Answer', 'PMI: Skip']                   
    participant_summary = participant_summary.reindex(participant_indices)
    participant_summary.index.name="answer"
    participant_summary['question'] = "Alcohol Participant"
    participant_summary.reset_index(inplace=True)
    participant_summary["answer"] = participant_summary["answer"].apply(lambda x: x.replace("Alcohol Participant: ", ""))
    yes_participant = participant_summary.iloc[:1, :].copy()
    yes_participant['question'] = "Alcohol Participant: Yes"
    other_participant = participant_summary.iloc[1:, :].copy()
    other_participant['question'] = "Alcohol Participant: Other"
    yes_participant= yes_participant.set_index(["question", "answer"])
    other_participant= other_participant.set_index(["question", "answer"])
    output_tables = dict()
    output_df = pd.DataFrame(index=alc_drink_counts['alc_count_answer'].unique(), columns=alc_freqs['alc_freq_answer'].unique())
    for name, group in alc_freqs.groupby(['alc_freq_answer']):
        drink_counts = alc_drink_counts[alc_drink_counts['person_id'].isin(group['person_id'].values)]
        describe_counts = drink_counts['alc_count_answer'].value_counts()
        describe_counts.at['Total']=  len(group)
        output_tables[name] = describe_counts
    freq_summary = pd.concat(list(output_tables.values()), axis=1, sort=True)
    freq_summary.fillna(0,inplace=True )
    freq_summary = freq_summary.astype(int)
    freq_summary.columns=list(output_tables.keys())
    freq_summary = freq_summary.T
    drink_freq_indices = ['Drink Frequency Past Year: Monthly Or Less',
           'Drink Frequency Past Year: 2 to 4 Per Month','Drink Frequency Past Year: 2 to 3 Per Week',
           'Drink Frequency Past Year: 4 or More Per Week','Drink Frequency Past Year: Never', 'PMI: Prefer Not To Answer', 'PMI: Skip']
    freq_summary = freq_summary.reindex(drink_freq_indices)
    freq_summary.index.name="answer"
    freq_summary['question'] = "Drink Frequency Past Year"
    freq_summary.reset_index(inplace=True)
    freq_summary["answer"] = freq_summary["answer"].apply(lambda x: x.replace("Drink Frequency Past Year: ", ""))
    freq_summary= freq_summary.set_index(["question", "answer"])
#     alc_dfs = pd.concat([yes_participant, freq_summary, other_participant])
    alc_dfs = freq_summary
#     print(alc_dfs.columns)
    alc_dfs = alc_dfs[['Average Daily Drink Count: 1 or 2',
       'Average Daily Drink Count: 3 or 4',
       'Average Daily Drink Count: 5 or 6',
       'Average Daily Drink Count: 7 to 9', 
        'Average Daily Drink Count: 10 or More',
                       'PMI: Prefer Not To Answer',
       'PMI: Skip', 'Total']]
#     display(alc_dfs.T)
    temp_df = alc_dfs.T
    
#     temp_df=move_row_to_top(temp_df, "Total")
#     temp_df.loc[:, 'Total'] = temp_df.sum(axis=1)
#     display(temp_df)
#     display(temp_df)
#     print(temp_df.sum(axis=1))
    temp_df.drop("Total", inplace=True)
#     sns.boxenplot(x="question", y="Drink Frequency Past Year", scale="linear", data=alc_dfs)
    fig, ax = plt.subplots(figsize=(11, 9))
    display(temp_df)
    print(temp_df.values.max())
#     pal = create_color_palette(temp_df.values.max(), temp_df.values.min(), 2)
    sns.heatmap(temp_df, cmap="mako", robust=True, annot=True, fmt=".0f", center=temp_df.values.mean())
    plt.show()
    


# In[ ]:


for df, name in zip(with_lifestyle_dfs, lifestyle_df_names):
    print(name)
#     create_alcohol_summary(df)
    create_alcohol_graphs(df)


# In[ ]:


# #%%script false --no-raise-error


# # Utilization Survey

# ## Import Utilization Survey

# In[ ]:


import_df("influenza_utilization")
import_df("hepB_utilization")
import_df("hpv_utilization")
import_df("pneumococcal_under_65_utilization")
import_df("pneumococcal_65_and_over_utilization")
import_df("all_patients_utilization")


# ## Analysis

# In[ ]:


# Aggregate utilization dataframes
with_utilization_dfs = [influenza_utilization, hepB_utilization, hpv_utilization, 
                   pneumococcal_under_65_utilization, pneumococcal_65_and_over_utilization, all_patients_utilization]
utilization_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
change_ids_to_str(with_utilization_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
# with_utilization_dfs


# In[ ]:


# define create_tables

def create_tables(list_of_dfs, df_names):
    questions = []
    questions = dict()
    questions_output_dfs = {question: [] for question in list(list_of_dfs[-1]['question'].unique())}
    for i in range(len(list_of_dfs)):
        temp = list_of_dfs[i].copy()
        unique_patients = len(temp['person_id'].unique())
        temp = temp.groupby(['question', 'answer']).size().reset_index(name="count")
#         total = len(list_of_dfs[i]['person_id'].unique())
        for (q), count_df in temp.groupby(['question']):
            t = count_df[['answer', 'count']].set_index('answer', drop=True)
            t = t[['count']]
            t.loc['Total Answers'] = t['count'].sum()
#             t.loc['Unique Patients'] = unique_patients
            t.columns = [df_names[i]]
            questions_output_dfs[q].append(t)

    for key, list_of_dfs in questions_output_dfs.items():
        temp_df =  pd.concat(list_of_dfs, axis=1, sort=True)
        temp_df.fillna(0,inplace=True )
        temp_df = temp_df.astype(int)
        temp_df['mean'] = temp_df.mean(axis=1)
        temp_df = temp_df.sort_values(['mean'], ascending=[False])

        for col in temp_df.columns:
            total = temp_df[col].loc['Total Answers']
#             total = temp_df[col].loc['Unique Patients']
            
            temp_df[col + "_percent"] = temp_df[col] * 100 / total
            temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
            temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
            temp_df[col] =  temp_df[col + "_combined"]
            temp_df.at['Total Answers', col] = total
#             temp_df.at['Unique Patients', col] = total
            

        temp_df = temp_df[df_names]
        questions[key] = temp_df
    return questions
        


# In[ ]:


# Create tables for all questions in utilization Survey
utilization_tables = create_tables(with_utilization_dfs, utilization_df_names)
for key, table in utilization_tables.items():
    initial_index_count = len(table.index)
    assert len(table.index) == initial_index_count
    print(key)
    table = account_for_missing(table, "Total Answers", "Missing")
    display(table)


# In[ ]:


#%%script false --no-raise-error


# # Insurance (from Observations)

# ## Import Datasets

# In[38]:


import_df("influenza_insurance")
import_df("hepB_insurance")
import_df("hpv_insurance")
import_df("pneumococcal_under_65_insurance")
import_df("pneumococcal_65_and_over_insurance")
import_df("all_patients_insurance")


# ## Analysis

# In[39]:


# Aggregate insurance dataframes
# with_insurance_dfs = [influenza_insurance, hepB_insurance, hpv_insurance, 
#                    pneumococcal_under_65_insurance, pneumococcal_65_and_over_insurance, all_patients_insurance]
insurance_dfs={"influenza":influenza_insurance,
                    "hepB":hepB_insurance, 
                    "hpv":hpv_insurance, 
                    "pneumococcal(<65)":pneumococcal_under_65_insurance, 
                    "pneumococcal(>=65)":pneumococcal_65_and_over_insurance, 
                    "all":all_patients_insurance}
# change_ids_to_str(with_insurance_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
# with_insurance_dfs


# In[40]:


len(influenza_insurance.person_id.unique())


# In[41]:


# Check if patients in both 
og_patients = all_patients_insurance[all_patients_insurance['standard_concept_name']=="Health Insurance: Health Insurance Type"]['person_id'].unique()
update = all_patients_insurance[all_patients_insurance['standard_concept_name']=="Health Insurance: Insurance Type Update"]
# assert len(update[update['person_id'].isin(og_patients)]) == 0

update_patients = all_patients_insurance[all_patients_insurance['standard_concept_name']=="Health Insurance: Insurance Type Update"]['person_id'].unique()
og = all_patients_insurance[all_patients_insurance['standard_concept_name']=="Health Insurance: Health Insurance Type"]
# assert len(og[og['person_id'].isin(update_patients)]) == 0

# print(len(og_patients), len(update_patients))
# og[og['person_id'].isin(update_patients)]


# In[42]:


# og[og['person_id'].duplicated(keep=False)].sort_values('person_id')
# update[update['person_id'].duplicated(keep=False)].sort_values('person_id')


# In[43]:


# define create_tables

def create_tables(list_of_dfs, df_names):
#     questions = []
    questions = dict()
    question_col = "standard_concept_name"
    answer_col = "value_as_concept_name"
    questions_output_dfs = {question: [] for question in list(list_of_dfs[-1][question_col].unique())}
    for i in range(len(list_of_dfs)):
        temp = list_of_dfs[i].copy()
        temp = temp.groupby([question_col, answer_col]).size().reset_index(name="count")
        for (q), count_df in temp.groupby([question_col]):
            t = count_df[[answer_col, 'count']].set_index(answer_col, drop=True)
            t = t[['count']]
            total = t['count'].sum()
            t.loc['Total'] = total
            t.columns = [df_names[i]]
            questions_output_dfs[q].append(t)

    for key, list_of_dfs in questions_output_dfs.items():
        temp_df =  pd.concat(list_of_dfs, axis=1, sort=True)
        temp_df.fillna(0,inplace=True )
        temp_df = temp_df.astype(int)
        temp_df['mean'] = temp_df.mean(axis=1)
        temp_df = temp_df.sort_values(['mean'], ascending=[False])

        for col in temp_df.columns:
            total = temp_df[col].loc['Total']
            temp_df[col + "_percent"] = temp_df[col] * 100 / total
            temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
            temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
            temp_df[col] =  temp_df[col + "_combined"]
            temp_df.at['Total', col] = total

        temp_df = temp_df[df_names]
        questions[key] = temp_df
    return questions
        


# In[44]:


# Create tables for all questions in insurance Survey
# insurance_tables = create_tables(insurance_dfs, insurance_df_names)
# for key, table in insurance_tables.items():
#     initial_index_count = len(table.index)
#     assert len(table.index) == initial_index_count
# #     print(key)
#     print(table.index)
#     display(table)
#     break


# In[45]:


# Map to output categories: Health Insurance Type
# Health Insurance Type 
insurance_question = "Health Insurance: Health Insurance Type"
insurance_groups = {
    "Private Health Insurance": ["Private health insurance", "Medi-Gap"],
    "Medicare" : ["Medicare"],
    "Medicaid":["Medicaid (if available, display plan name)", 
                "State-sponsored health plan (if available, display state plan name)", 
                "Other government program"],
    "Military Healthcare":["Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["SCHIP (CHIP/Children's health insurance program)", 
                                      "Single service plan (e.g. dental, vision, prescriptions)",
                                     ],
    "No Health Insurance / Self-Pay": ["No coverage of any type"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip", "Don't know", "I prefer not to answer"],
    "Total" : ["Total"]
}
insurance_mapping = dict()
for k, v in insurance_groups.items():
    for x in v:
        insurance_mapping[x] = k
         
        
# Health Insurance: Insurance Type Update
insurance_update_question = "Health Insurance: Insurance Type Update"
insurance_update_groups = {
    "Private Health Insurance": ["Insurance Type Update: Purchased", 
                                 "Insurance Type Update: Employer Or Union"],
    "Medicare" : ["Insurance Type Update: Medicare"],
    "Medicaid":["Insurance Type Update: Medicaid", ],
    "Military Healthcare":["Insurance Type Update: VA",
                          "Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["Insurance Type Update: Other Health Plan"],
    "No Health Insurance / Self-Pay": ["Insurance Type Update: None", "Self-pay"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip"],
    "Total" : ["Total"]
}
insurance_update_mapping = dict()
for k, v in insurance_update_groups.items():
    for x in v:
        insurance_update_mapping[x] = k


# In[46]:


# question = "Health Insurance: Health Insurance Type"
# input_dfs = insurance_dfs
# # input_name_dfs = insurance_df_names
# for name, df in input_dfs.items():
#     temp = df.copy()
#     temp = temp[temp['standard_concept_name']==question]
#     temp["output_group"] = temp.value_as_concept_name.map(insurance_mapping)
#     display(temp.output_group.describe())


# In[47]:


def map_to_output_categories_insurance_type(df_dict, question, mapping):
    output_dfs = dict()
    for name, df in df_dict.items():
        temp = df.copy()
        temp = temp[temp['standard_concept_name']==question]
        temp["output_group"] = temp.value_as_concept_name.map(mapping)
        output_dfs[name] = temp
    return output_dfs


# In[48]:


# combine og insurance and update

# insurance_a = map_to_output_categories_insurance_type(insurance_dfs, insurance_update_question, insurance_update_mapping)
# insurance_b = map_to_output_categories_insurance_type(insurance_dfs, insurance_question, insurance_mapping)
# combined_insurance_with_group = dict()
# for name, df in insurance_a.items():
#     temp_df = pd.concat([df, insurance_b[name]])
#     # if patient has original insurance and update
#     combined_insurance_with_group[name] = temp_df
# # pd.concat([insurance_a, insurance_b])


# In[49]:


# insurance_a


# In[50]:


# combined_insurance_with_group['influenza'].groupby('person_id').size().to_frame("entry_count").sort_values('entry_count',ascending=False)


# In[51]:


# test_df = combined_insurance_with_group['influenza']
# test_df[test_df['person_id']==2624905]


# In[52]:


# Health Insurance Type 
question = "Health Insurance: Health Insurance Type"
insurance_groups = {
    "Private Health Insurance": ["Private health insurance", "Medi-Gap"],
    "Medicare" : ["Medicare"],
    "Medicaid":["Medicaid (if available, display plan name)", 
                "State-sponsored health plan (if available, display state plan name)", 
                "Other government program"],
    "Military Healthcare":["Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["SCHIP (CHIP/Children's health insurance program)", 
                                      "Single service plan (e.g. dental, vision, prescriptions)",
                                     ],
    "No Health Insurance / Self-Pay": ["No coverage of any type"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip", "Don't know", "I prefer not to answer"],
    "Total Answers" : ["Total Answers"],
    "Unique Patients" : ["Unique Patients"]
}
insurance_mapping = dict()
for k, v in insurance_groups.items():
    for x in v:
        insurance_mapping[x] = k
        
question_col = "standard_concept_name"
answer_col = "value_as_concept_name"
output_dfs = []
input_dfs = insurance_dfs
for name, df in input_dfs.items():
    temp = df.copy()
    temp = temp[temp[question_col]==question]
    total_patients = len(temp['person_id'].unique())
    temp = temp.groupby([question_col, answer_col]).size().reset_index(name="count")
    for (q), count_df in temp.groupby([question_col]):
        if (q == question):
            t = count_df[[answer_col, 'count']].set_index(answer_col, drop=True)
            t = t[['count']]
            t.loc['Total Answers'] = t['count'].sum()
            t.loc['Unique Patients'] = total_patients
            t.columns = [name]
            output_dfs.append(t)


temp_df =  pd.concat(output_dfs, axis=1, sort=True)
temp_df = temp_df.groupby(by=insurance_mapping, axis=0).sum()
temp_df = temp_df.astype(int)

for col in temp_df.columns:
    total = temp_df[col].loc['Total Answers']
    total_pat = temp_df[col].loc['Unique Patients']
    
    temp_df[col + "_percent"] = temp_df[col] * 100 / total
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Total Answers', col] = total
    temp_df.at['Unique Patients', col] = total_pat
    

temp_df = temp_df[input_dfs.keys()]
initial_index_count = len(temp_df.index)
# print(temp_df.index)

# print(len(temp_df.index))
temp_df = temp_df.reindex(["Unique Patients", 'Total Answers' , 'Medicaid', 'Medicare','Private Health Insurance',
       'Military Healthcare','Indian Health Service',
       'Other Type of Health Insurance','No Health Insurance / Self-Pay' , 'Invalid', 'Skip',
       ], fill_value="0 (0.0%)")

# assert len(temp_df.index) == initial_index_count
print(question)
insurance_type = temp_df
insurance_type = add_cohort_size_row(insurance_type)
insurance_type = account_for_missing(insurance_type, "Unique Patients", "Missing Patients")

insurance_type = account_for_missing(insurance_type, "Unique Patients", "Missing Patients")
display(insurance_type)
create_download_link(insurance_type)


# In[53]:


# Health Insurance: Insurance Type Update
question = "Health Insurance: Insurance Type Update"
insurance_groups = {
    "Private Health Insurance": ["Insurance Type Update: Purchased", 
                                 "Insurance Type Update: Employer Or Union"],
    "Medicare" : ["Insurance Type Update: Medicare"],
    "Medicaid":["Insurance Type Update: Medicaid", ],
    "Military Healthcare":["Insurance Type Update: VA",
                          "Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["Insurance Type Update: Other Health Plan"],
    "No Health Insurance / Self-Pay": ["Insurance Type Update: None", "Self-pay"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip"],
    "Total Answers" : ["Total Answers"],
    "Unique Patients" : ["Unique Patients"]
    
}
insurance_mapping = dict()
for k, v in insurance_groups.items():
    for x in v:
        insurance_mapping[x] = k

        
question_col = "standard_concept_name"
answer_col = "value_as_concept_name"
output_dfs = []
input_dfs = insurance_dfs
# for i in range(len(input_dfs)):
for name, df in input_dfs.items():
    temp = df.copy()
    temp = temp[temp[question_col]==question]
    total_patients = len(temp['person_id'].unique())
    temp = temp.groupby([question_col, answer_col]).size().reset_index(name="count")
    for (q), count_df in temp.groupby([question_col]):
        if (q == question):
            t = count_df[[answer_col, 'count']].set_index(answer_col, drop=True)
            t = t[['count']]
            t.loc['Total Answers'] = t['count'].sum()
            t.loc['Unique Patients'] = total_patients
            t.columns = [name]
            output_dfs.append(t)


temp_df =  pd.concat(output_dfs, axis=1)
temp_df = temp_df.groupby(by=insurance_mapping, axis=0).sum()

for col in temp_df.columns:
    total = temp_df[col].loc['Total Answers']
    total_pat = temp_df[col].loc['Unique Patients']
    
    temp_df[col + "_percent"] = temp_df[col] * 100 / total
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Total Answers', col] = total
    temp_df.at['Unique Patients', col] = total_pat
    

temp_df = temp_df[input_dfs.keys()]

initial_index_count = len(temp_df.index)
# print(temp_df.index)
temp_df = temp_df.reindex(["Unique Patients", 'Total Answers' , 'Medicaid', 'Medicare','Private Health Insurance',
       'Military Healthcare','Indian Health Service',
       'Other Type of Health Insurance','No Health Insurance / Self-Pay' , 'Invalid', 'Skip',
       ], fill_value="0 (0.0%)")
# print(temp_df.index)
# assert len(temp_df.index) == initial_index_count
print(question)
# display(temp_df)
insurance_type_update = temp_df
insurance_type_update = add_cohort_size_row(insurance_type_update)
insurance_type_update = account_for_missing(insurance_type_update, "Unique Patients", "Missing Patients")
display(insurance_type_update)
create_download_link(insurance_type_update)
# temp_df


# In[54]:


# Map to output categories: Health Insurance Type
# Health Insurance Type 
insurance_question = "Health Insurance: Health Insurance Type"
insurance_groups = {
    "Private Health Insurance": ["Private health insurance", "Medi-Gap"],
    "Medicare" : ["Medicare"],
    "Medicaid":["Medicaid (if available, display plan name)", 
                "State-sponsored health plan (if available, display state plan name)", 
                "Other government program"],
    "Military Healthcare":["Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["SCHIP (CHIP/Children's health insurance program)", 
                                      "Single service plan (e.g. dental, vision, prescriptions)",
                                     ],
    "No Health Insurance / Self-Pay": ["No coverage of any type"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip", "Don't know", "I prefer not to answer"],
    "Total" : ["Total"]
}
insurance_mapping = dict()
for k, v in insurance_groups.items():
    for x in v:
        insurance_mapping[x] = k
         
        
# Health Insurance: Insurance Type Update
insurance_update_question = "Health Insurance: Insurance Type Update"
insurance_update_groups = {
    "Private Health Insurance": ["Insurance Type Update: Purchased", 
                                 "Insurance Type Update: Employer Or Union"],
    "Medicare" : ["Insurance Type Update: Medicare"],
    "Medicaid":["Insurance Type Update: Medicaid", ],
    "Military Healthcare":["Insurance Type Update: VA",
                          "Military health care (tricare/VA, Champ-VA)"
                          ],
    "Indian Health Service":["Indian health service"],
    "Other Type of Health Insurance":["Insurance Type Update: Other Health Plan"],
    "No Health Insurance / Self-Pay": ["Insurance Type Update: None", "Self-pay"],
    "Invalid" : ["Invalid"],
    "Skip" : ["PMI: Skip"],
    "Total" : ["Total"]
}
insurance_update_mapping = dict()
for k, v in insurance_update_groups.items():
    for x in v:
        insurance_update_mapping[x] = k


# In[55]:


def map_to_output_categories_insurance_type(df_dict, question, mapping):
    output_dfs = dict()
    for name, df in df_dict.items():
        temp = df.copy()
        temp = temp[temp['standard_concept_name']==question]
        temp["output_group"] = temp.value_as_concept_name.map(mapping)
        output_dfs[name] = temp
    return output_dfs


# In[56]:


# combine insurance groups pt. 1
insurance_a = map_to_output_categories_insurance_type(insurance_dfs, insurance_update_question, insurance_update_mapping)
insurance_b = map_to_output_categories_insurance_type(insurance_dfs, insurance_question, insurance_mapping)
combined_insurance_with_group = dict()
for cohort_name, df in insurance_a.items():
    insurance_update_pts = df.person_id.unique()
    og_insurance_df = insurance_b[cohort_name]
    og_insurance_df = og_insurance_df[~og_insurance_df.person_id.isin(insurance_update_pts) ] # only keep pts not in update already
    temp_df = pd.concat([df, og_insurance_df]).astype(str)
    temp_df = temp_df[['person_id', 'output_group']]
    temp_df.drop_duplicates(inplace=True)
    combined_insurance_with_group[cohort_name] = temp_df


# In[58]:


# combine insurance groups pt. 2
input_dfs = combined_insurance_with_group
output_dfs = []
# for i in range(len(input_dfs)):
for name, df in input_dfs.items():
    temp = df.copy()
    unique_patients = len(temp['person_id'].unique())
    temp = temp.groupby(['output_group']).size().reset_index(name="count")
#     display(temp)
    temp.set_index("output_group", inplace=True)
#     del temp.index.name
    temp.loc['Total Answers'] = temp['count'].sum()
    temp.loc['Unique Patients'] = unique_patients
#     temp = add_cohort_size_row(temp)
    temp.columns = [name]
    output_dfs.append(temp)
#     break
    
temp_df =  pd.concat(output_dfs, axis=1)
temp_df = add_cohort_size_row(temp_df)
temp_df = account_for_missing(temp_df, "Unique Patients", "Missing Patients")
temp_df.loc["Skipped or Missing"] = temp_df.loc["Skip"] + temp_df.loc["Missing Patients"]
# display(temp_df)
for col in temp_df.columns:
    total = temp_df[col].loc['Total Answers']
    total_pat = temp_df[col].loc['Unique Patients']
    cohort_size =  temp_df[col].loc['Cohort Size']
#     print(temp_df)
    
    temp_df[col + "_percent"] = temp_df[col] * 100 / cohort_size
    temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
    temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
    temp_df[col] =  temp_df[col + "_combined"]
    temp_df.at['Total Answers', col] = total
    temp_df.at['Unique Patients', col] = total_pat
    temp_df.at['Cohort Size', col] = cohort_size
    
    
temp_df = temp_df[input_dfs.keys()]

# temp_df = temp_df.reindex(["Unique Patients", 'Total Answers' , 'Medicaid', 'Medicare','Private Health Insurance',
#        'Military Healthcare','Indian Health Service',
#        'Other Type of Health Insurance','No Health Insurance / Self-Pay' , 'Invalid', 'Skip',
#        ], fill_value="0 (0.0%)")
# use skip and missing
temp_df = temp_df.reindex(["Cohort Size", "Unique Patients", 'Total Answers' , 'Medicaid', 'Medicare','Private Health Insurance',
       'Military Healthcare','Indian Health Service',
       'Other Type of Health Insurance','No Health Insurance / Self-Pay', 'Skipped or Missing',
       ], fill_value="0 (0.0%)")


combined_insurance = temp_df
# combined_insurance = add_cohort_size_row(combined_insurance)
# combined_insurance = account_for_missing(combined_insurance, "Unique Patients", "Missing Patients")
print("Combined Insurance (Checking update first, then original)")
display(combined_insurance)
create_download_link(combined_insurance)


# In[ ]:


pd.__version__


# In[ ]:


np.__version__


# In[ ]:


#%%script false --no-raise-error


# # PROMIS_health Survey

# ## Import PROMIS_health Survey

# In[19]:


import_df("influenza_PROMIS_health")
import_df("hepB_PROMIS_health")
import_df("hpv_PROMIS_health")
import_df("pneumococcal_under_65_PROMIS_health")
import_df("pneumococcal_65_and_over_PROMIS_health")
import_df("all_patients_PROMIS_health")


# In[20]:


influenza_PROMIS_health


# ## Analysis

# ### Processing

# In[21]:


# Aggregate PROMIS_health dataframes
with_PROMIS_health_dfs = [influenza_PROMIS_health, hepB_PROMIS_health, hpv_PROMIS_health, 
                   pneumococcal_under_65_PROMIS_health, pneumococcal_65_and_over_PROMIS_health, all_patients_PROMIS_health]
PROMIS_health_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
change_ids_to_str(with_PROMIS_health_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
#with_PROMIS_health_dfs


# In[22]:


# define create_tables

def create_tables(list_of_dfs, df_names):
    questions = []
    questions = dict()
    questions_output_dfs = {question: [] for question in list(list_of_dfs[-1]['question'].unique())}
    for i in range(len(list_of_dfs)):
        temp = list_of_dfs[i].copy()
        temp = temp.groupby(['question', 'answer']).size().reset_index(name="count")
#         total = len(list_of_dfs[i]['person_id'].unique())
        for (q), count_df in temp.groupby(['question']):
            t = count_df[['answer', 'count']].set_index('answer', drop=True)
            t = t[['count']]
            total = t['count'].sum()
#             print(total.dtype)
            t.loc['Total',] = total
            t.columns = [df_names[i]]
            questions_output_dfs[q].append(t)

    for key, list_of_dfs in questions_output_dfs.items():
        temp_df =  pd.concat(list_of_dfs, axis=1, sort=True)
        temp_df.fillna(0,inplace=True )
        temp_df = temp_df.astype(int)
        temp_df['mean'] = temp_df.mean(axis=1)
        temp_df = temp_df.sort_values(['mean'], ascending=[False])

        for col in temp_df.columns:
            total = temp_df[col].loc['Total']
            temp_df[col + "_percent"] = temp_df[col] * 100 / total
            temp_df[col + "_percent"] =   temp_df[col + "_percent"].map('({:,.1f}%)'.format)
            temp_df[col + "_combined"] = temp_df[col].astype(str) + " " + temp_df[col + "_percent"].astype(str)
            temp_df[col] =  temp_df[col + "_combined"]
            temp_df.at['Total', col] = total

        temp_df = temp_df[df_names]
        questions[key] = temp_df
    return questions
        


# In[23]:


# Create tables for all questions in PROMIS_health Survey
PROMIS_health_tables = create_tables(with_PROMIS_health_dfs, PROMIS_health_df_names)
for key, table in PROMIS_health_tables.items():
    initial_index_count = len(table.index)
    assert len(table.index) == initial_index_count
    print(key)
    display(table)


# In[24]:


# Define raw score mapping
physical_health_answer_scores = {
    5: ["1585724", "1585742", "1585749"],
    4: ["1585725", "1585743", "1585750"],
    3: ["1585726", "1585744", "1585751"],
    2: ["1585727", "1585745", "1585752"],
    1: ["1585728", "1585746", "1585753"]
}
avg_pain_scores = [5,4,4,4,3,3,3,2,2,2,1]
physical_health_questions = ["1585723","1585741","1585747","1585748"]

mental_health_answer_scores = {
    5: ["1585718", "1585730", "1585736", "1585761"],
    4: ["1585719", "1585731", "1585737", "1585762"],
    3: ["1585720", "1585732", "1585738", "1585763"],
    2: ["1585721", "1585733", "1585739", "1585764"],
    1: ["1585722", "1585734", "1585740", "1585765"]
}
mental_health_questions = ["1585717","1585729","1585735","1585760"]


# In[25]:


# Define T-score mapping
# Physical
physical_t_score_mapping = {
    "4":16.2,
    "5":19.9,
    "6":23.5,
    "7":26.7,
    "8":29.6,
    "9":32.4,
    "10":34.9,
    "11":37.4,
    "12":39.8,
    "13":42.3,
    "14":44.9,
    "15":47.7,
    "16":50.8,
    "17":54.1,
    "18":57.7,
    "19":61.9,
    "20":67.7
}
physical_se_mapping = {
    "4":4.8,
    "5":4.7,
    "6":4.5,
    "7":4.3,
    "8":4.2,
    "9":4.2,
    "10":4.1,
    "11":4.1,
    "12":4.1,
    "13":4.2,
    "14":4.3,
    "15":4.4,
    "16":4.6,
    "17":4.7,
    "18":4.9,
    "19":5.2,
    "20":5.9
}
# Mental
mental_t_score_mapping = {
    "4":21.2,
    "5":25.1,
    "6":28.4,
    "7":31.3,
    "8":33.8,
    "9":36.3,
    "10":38.8,
    "11":41.1,
    "12":43.5,
    "13":45.8,
    "14":48.3,
    "15":50.8,
    "16":53.3,
    "17":56.0,
    "18":59.0,
    "19":62.5,
    "20":67.6,
}
mental_se_mapping = {
    "4":4.6,
    "5":4.1,
    "6":3.9,
    "7":3.7,
    "8":3.7,
    "9":3.7,
    "10":3.6,
    "11":3.6,
    "12":3.6,
    "13":3.6,
    "14":3.7,
    "15":3.7,
    "16":3.7,
    "17":3.8,
    "18":3.9,
    "19":4.2,
    "20":5.3
}


# In[26]:


# define calcuate_T_score function
def calcuate_mental_raw_score(df, questions, answer_scores):
    temp_mapping = dict()
    for k, v in answer_scores.items():
        for x in v:
            temp_mapping[x] = k
    temp_df = df.copy()
    temp_df = temp_df[temp_df['question_concept_id'].isin(questions)]
    temp_df["raw_score"] = temp_df['answer_concept_id'].map(temp_mapping)
    temp_df = temp_df.dropna(axis=0)
    temp_df = temp_df[temp_df.groupby(['person_id'])['question_concept_id'].transform('count')==4]
    scores = temp_df.groupby(['person_id'])['raw_score'].sum().reset_index()
    scores['raw_score'] =  scores['raw_score'].astype(int)
#     display(scores)
#     display(temp_df)
    return scores


# In[27]:


# define calcuate_physical_T_score function
def map_avg_pain(i):
    if (i == "PMI: Skip"):
        return float('NaN')
    int_i = int(i)
    if (0 <= int_i <= 10):
        return avg_pain_scores[int_i]
        
        
def calcuate_physical_raw_score(df, questions, answer_scores, avg_pain):
    temp_mapping = dict()
    for k, v in answer_scores.items():
        for x in v:
            temp_mapping[x] = k
    temp_df = df.copy()
    temp_df = temp_df[temp_df['question_concept_id'].isin(questions)]
    temp_df["raw_score"] = temp_df['answer_concept_id'].map(temp_mapping)
    temp_df.loc[temp_df['question_concept_id'] == '1585747', "raw_score"] = temp_df[temp_df['question_concept_id'] == '1585747']['answer'].map(map_avg_pain)
    display(temp_df)
    scores = pd.DataFrame(columns=['person_id', 'raw_score'])
    display(scores)
    temp_df = temp_df.dropna(axis=0)
    temp_df = temp_df[temp_df.groupby(['person_id'])['question_concept_id'].transform('count')==4]
    scores = temp_df.groupby(['person_id'])['raw_score'].sum().reset_index()
    scores['raw_score'] =  scores['raw_score'].astype(int)

    return scores


# In[38]:


# Generate T_scores dfs for all cohorts
physical_T_score_desc = []
physical_se_desc = []
mental_T_score_desc = []
mental_se_desc = []
desc_index = []

physical_t_scores_dict = dict()
mental_t_scores_dict = dict()

physical_se_dict = dict()
mental_se_dict = dict()

for i, df in enumerate(with_PROMIS_health_dfs):
#     print(f"Creating summary for: {PROMIS_health_df_names[i]}")
    mental_scores = calcuate_mental_raw_score(df, mental_health_questions, mental_health_answer_scores)
    mental_scores['T_score']=  mental_scores['raw_score'].astype(str).map(mental_t_score_mapping)
    mental_scores['se']=  mental_scores['raw_score'].astype(str).map(mental_se_mapping)
    mental_T_score_desc.append(mental_scores['T_score'].describe().tolist())
    mental_se_desc.append(mental_scores['se'].describe().tolist())
    
    physical_scores = calcuate_physical_raw_score(df, physical_health_questions, physical_health_answer_scores, avg_pain_scores)
    physical_scores['T_score']=  physical_scores['raw_score'].astype(str).map(physical_t_score_mapping)
    physical_scores['se']=  physical_scores['raw_score'].astype(str).map(physical_se_mapping)
    physical_T_score_desc.append(physical_scores['T_score'].describe().tolist())
    physical_se_desc.append(physical_scores['se'].describe().tolist())
    
    
    mental_t_scores_dict[PROMIS_health_df_names[i]] =  mental_scores[['person_id', 'T_score']]
    mental_se_dict[PROMIS_health_df_names[i]] =  mental_scores[['person_id', 'se']]
    
    physical_t_scores_dict[PROMIS_health_df_names[i]] =  physical_scores[['person_id', 'T_score']]
    physical_se_dict[PROMIS_health_df_names[i]] =  physical_scores[['person_id', 'se']]
    # add for mental
#     physical_t_scores_dict[PROMIS_health_df_names[i]] = physical_T_scores
    desc_index = physical_scores['T_score'].describe().index


# ### No Group

# In[29]:


# Physical T_scores
physical_T_scores_summary = pd.DataFrame(list(zip(*physical_T_score_desc)), columns=PROMIS_health_df_names)
physical_T_scores_summary.set_index(desc_index, inplace=True)
print("Physical T Scores Description")
physical_T_scores_summary
physical_T_scores_summary = add_cohort_size_row(physical_T_scores_summary)
physical_T_scores_summary = account_for_missing(physical_T_scores_summary)
physical_T_scores_summary.loc["iqr"] = physical_T_scores_summary.loc["75%"] - physical_T_scores_summary.loc["25%"]
display(physical_T_scores_summary)
display(create_download_link(physical_T_scores_summary))


# In[30]:


# Physical se
physical_se_summary = pd.DataFrame(list(zip(*physical_se_desc)), columns=PROMIS_health_df_names)
physical_se_summary.set_index(desc_index, inplace=True)
print("Physical SE Description")
physical_se_summary
physical_se_summary = add_cohort_size_row(physical_se_summary)
account_for_missing(physical_se_summary)


# In[31]:


# mental T_scores
mental_T_scores_summary = pd.DataFrame(list(zip(*mental_T_score_desc)), columns=PROMIS_health_df_names)
mental_T_scores_summary.set_index(desc_index, inplace=True)
print("mental T Scores Description")
mental_T_scores_summary
mental_T_scores_summary = add_cohort_size_row(mental_T_scores_summary)

mental_T_scores_summary = account_for_missing(mental_T_scores_summary)
mental_T_scores_summary.loc["iqr"] = mental_T_scores_summary.loc["75%"] - mental_T_scores_summary.loc["25%"]
display(mental_T_scores_summary)
display(create_download_link(mental_T_scores_summary))


# In[32]:


# mental se
mental_se_summary = pd.DataFrame(list(zip(*mental_se_desc)), columns=PROMIS_health_df_names)
mental_se_summary.set_index(desc_index, inplace=True)
print("mental SE Description")
mental_se_summary = add_cohort_size_row(mental_se_summary)

account_for_missing(mental_se_summary)


# In[33]:


# Define box plot func for t-scores
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("whitegrid")
sns.set_palette(sns.color_palette(cohort_colors_list))
#sns.set_palette("flare")

def create_box_plot_for_T_score(df_dict):
    t_scores_df = pd.DataFrame(columns=['T_score', 'cohort'])
    for k, v in df_dict.items():
        temp = v[['T_score']]
        temp['cohort'] = k
        t_scores_df = t_scores_df.append(temp, ignore_index=True)
    box = sns.boxplot(y='T_score', x='cohort', data=t_scores_df)
    return box

def create_box_plot_for_SE_score(df_dict):
    t_scores_df = pd.DataFrame(columns=['se', 'cohort'])
    for k, v in df_dict.items():
        temp = v[['se']]
        temp['cohort'] = k
        t_scores_df = t_scores_df.append(temp, ignore_index=True)
    box = sns.boxplot(y='se', x='cohort', data=t_scores_df)
    return box


# In[34]:


# Box plot for physical T scores
plt.figure(figsize=(20, 10))
print("Physical T Scores")
plot = create_box_plot_for_T_score(physical_t_scores_dict)


# In[35]:


# Box plot for physical SE scores
plt.figure(figsize=(20, 10))
print("Physical SE Scores")
plot = create_box_plot_for_SE_score(physical_se_dict)


# In[ ]:


# Box plot for mental T scores
plt.figure(figsize=(20, 10))
print("Mental T Scores")
plot = create_box_plot_for_T_score(mental_t_scores_dict)


# In[ ]:


# Box plot for mental SE scores
plt.figure(figsize=(20, 10))
print("Mental SE Scores")
plot = create_box_plot_for_SE_score(mental_se_dict)


# ### Grouped

# In[62]:


# create groupings for physical T-score
def group_physical_t_score(df):
    if (df['T_score']>=58):
        return "Excellent"
    elif 50 <= df['T_score'] < 58:
        return "Very Good"
    elif 42 <= df['T_score'] < 50:
        return "Good"
    elif 35 <= df['T_score'] < 42:
        return "Fair"
    elif df['T_score'] < 35:
        return "Poor"
    
grouped_physical_t_scores = dict()
for (name, this_df) in physical_t_scores_dict.items():
    df = this_df.copy()
    df['group'] = df.apply(lambda row: group_physical_t_score(row), axis=1)
    grouped_physical_t_scores[name] = df
#     display(df)


# In[107]:


# create groupings for mental T-score
def group_mental_t_score(df):
    if (df['T_score']>=57):
        return "Excellent"
    elif 48 <= df['T_score'] < 57:
        return "Very Good"
    elif 40 <= df['T_score'] < 48:
        return "Good"
    elif 29 <= df['T_score'] < 40:
        return "Fair"
    elif df['T_score'] < 29:
        return "Poor"
    
grouped_mental_t_scores = dict()
for (name, this_df) in mental_t_scores_dict.items():
    df = this_df.copy()
    df['group'] = df.apply(lambda row: group_mental_t_score(row), axis=1)
    grouped_mental_t_scores[name] = df
#     display(df)


# In[117]:


# grouped physical T-scores
cohort_sizes={"influenza":15346,
             "hepB":6323,
            "hpv":2125,
            "pneumococcal(<65)":15217,
            "pneumococcal(>=65)":15100,
            "all":315297
             }
output_dfs = []
for (name, this_df) in grouped_physical_t_scores.items():
    unique_patients = len(this_df.person_id.unique())
    df = pd.DataFrame(this_df['group'].value_counts())
    df.columns = ["count"]
    total_answers = df['count'].sum()
    df.loc["Unique Patients"] = unique_patients
    df.loc['Total Answers'] = total_answers
    df.loc['Cohort Size'] = cohort_sizes[name]
    
    
#     df = add_cohort_size_row(df)
#     display(df)
    df.loc["Missing"] = cohort_sizes[name] - unique_patients
#     display(df)

    df['percent'] = df['count'] * 100/ cohort_sizes[name]
    df['percent'] =  df['percent'].map('({:,.1f}%)'.format)
    df['combined'] = df['count'].astype(str) + " " + df['percent'].astype(str)
    df = df[['combined']]
    df.loc['Cohort Size'] = cohort_sizes[name]
    df.columns =[name]
    output_dfs.append(df.stack())
    
grouped_physical_t_scores_summary = pd.concat(output_dfs, axis=0).unstack() 
grouped_physical_t_scores_summary = grouped_physical_t_scores_summary[grouped_physical_t_scores.keys()]
grouped_physical_t_scores_summary = grouped_physical_t_scores_summary.reindex(['Cohort Size', 'Excellent', 'Very Good', 'Good', 'Fair', 'Poor', 'Missing'])
display(grouped_physical_t_scores_summary)
create_download_link(grouped_physical_t_scores_summary, "grouped_physical_t_scores_summary", "grouped_physical_t_scores_summary")


# In[118]:


# grouped mental T-scores
cohort_sizes={"influenza":15346,
             "hepB":6323,
            "hpv":2125,
            "pneumococcal(<65)":15217,
            "pneumococcal(>=65)":15100,
            "all":315297
             }
output_dfs = []
for (name, this_df) in grouped_mental_t_scores.items():
    unique_patients = len(this_df.person_id.unique())
    df = pd.DataFrame(this_df['group'].value_counts())
    df.columns = ["count"]
    total_answers = df['count'].sum()
    df.loc["Unique Patients"] = unique_patients
    df.loc['Total Answers'] = total_answers
    df.loc['Cohort Size'] = cohort_sizes[name]
    
    
#     df = add_cohort_size_row(df)
#     display(df)
    df.loc["Missing"] = cohort_sizes[name] - unique_patients
#     display(df)

    df['percent'] = df['count'] * 100/ cohort_sizes[name]
    df['percent'] =  df['percent'].map('({:,.1f}%)'.format)
    df['combined'] = df['count'].astype(str) + " " + df['percent'].astype(str)
    df = df[['combined']]
    df.loc['Cohort Size'] = cohort_sizes[name]
    df.columns =[name]
    output_dfs.append(df.stack())
    
grouped_mental_t_scores_summary = pd.concat(output_dfs, axis=0).unstack() 
grouped_mental_t_scores_summary = grouped_mental_t_scores_summary[grouped_mental_t_scores.keys()]
grouped_mental_t_scores_summary = grouped_mental_t_scores_summary.reindex(['Cohort Size', 'Excellent', 'Very Good', 'Good', 'Fair', 'Poor', 'Missing'])
display(grouped_mental_t_scores_summary)
create_download_link(grouped_mental_t_scores_summary, "grouped_mental_t_scores_summary", "grouped_mental_t_scores_summary")


# In[ ]:


#%%script false --no-raise-error


# # Comorbidities for Cohorts

# ## Import Datasets

# In[ ]:


import_df("influenza_comorbidities")
import_df("hepB_comorbidities")
import_df("hpv_comorbidities")
import_df("pneumococcal_under_65_comorbidities")
import_df("pneumococcal_65_and_over_comorbidities")
import_df_hdf("all_patients_comorbidities")


# ## Analyze Datasets

# In[ ]:


# Aggregate comorbidities dataframes
comorbidities_dfs = [influenza_comorbidities, hepB_comorbidities,
                          hpv_comorbidities, pneumococcal_under_65_comorbidities, 
                          pneumococcal_65_and_over_comorbidities
                          , all_patients_comorbidities
                         ]
comorbidities_df_names = ["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"
                          , "all"
                         ]
comorbidities_dict = {
    "influenza": influenza_comorbidities, 
    "hepB" : hepB_comorbidities, 
    "hpv": hpv_comorbidities, 
    "pneumococcal(<65)" : pneumococcal_under_65_comorbidities, 
    "pneumococcal(>=65)": pneumococcal_65_and_over_comorbidities,
    "all" : all_patients_comorbidities
}

column_names = [
    "hypertension", "heart_failure", "ischemic_heart", "atrial_fibrillation", "hyperlipidemia",
    "stroke", "arthritis", "asthma", "autism", 'cancer', "chronic_kidney_disease", "chronic_pulmonary_disease",
    "alzheimers", "depression", "diabetes", "hepatitis", "HIV", "osteoporosis", "schizophrenia", "substance_abuse"
]
# change_ids_to_str(with_comorbidities_dfs, ["person_id", "question_concept_id", "answer_concept_id"])
# with_comorbidities_dfs


# In[ ]:


len(hepB_comorbidities.person_id.unique())


# In[ ]:


# move row of index to top of df
def move_row_to_top(df, index_name):
    df["new"] = range(1,len(df)+1)
    df.loc[index_name,'new'] = 0
    return df.sort_values("new").drop('new', axis=1)


# In[ ]:


# get top comorbidities per cohort
output_dfs = []
for name, df in comorbidities_dict.items():
#     print(name, len(df))
    this_df = df[column_names].astype(int).sum().to_frame('patient_count').sort_values("patient_count", ascending=False)
    this_df.reset_index(inplace=True)
    this_df['percent'] = this_df['patient_count'] * 100 / len(df)
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['index'].astype(str) + " : " + this_df['patient_count'].astype(str) + " " +this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[name] 
    output_dfs.append(this_df.stack())
    
top_comorbidities = pd.concat(output_dfs, axis=0).unstack() 
# display(top_comorbidities)
top_comorbidities =  top_comorbidities[comorbidities_df_names]
top_comorbidities.loc['Total'] = [len(df.index) for df in comorbidities_dict.values()]
top_comorbidities = account_for_missing(top_comorbidities, "Total", "Missing Patients")
top_comorbidities = move_row_to_top(top_comorbidities, "Missing Patients")

# move total to top
top_comorbidities = move_row_to_top(top_comorbidities, 'Total')
display(top_comorbidities)
create_download_link (top_comorbidities, "Download Top Comorbidities as CSV", "top_comorbidities.csv")


# In[ ]:


# Table for comorbidities
pd.options.display.float_format = "{:.2f}".format

comorbidities_lists = [df.total_comorbidities.astype(int).describe().tolist() for df in comorbidities_dfs]
comorbidities_columns = [name + "_comorbidities" for name in comorbidities_df_names ]
comorbidities_summary = pd.DataFrame(list(zip(*comorbidities_lists)), columns=comorbidities_columns)
comorbidities_summary.set_index(influenza_comorbidities.total_comorbidities.astype(int).describe().index, inplace=True)
account_for_missing(comorbidities_summary)
display(comorbidities_summary)
create_download_link (comorbidities_summary, "Download comorbidities_summary as CSV", "comorbidities_summary.csv")


# In[ ]:


# get comorbidities per cohort (for dummy tables)
output_dfs = []
for name, df in comorbidities_dict.items():
#     print(name, len(df))
    this_df = df[column_names].astype(int).sum().to_frame('patient_count').sort_values("patient_count", ascending=False)
#     display(this_df)
#     if name=="all":
#         print(this_df.index)
#     this_df.reset_index(inplace=True)
    this_df['percent'] = this_df['patient_count'] * 100 / len(df)
    this_df['percent'] =  this_df['percent'].map('({:,.1f}%)'.format)
    this_df['combined'] = this_df['patient_count'].astype(str) + " " +this_df['percent'].astype(str)
    this_df = this_df[['combined']]
    this_df.columns =[name] 
#     display(this_df)
    this_df = this_df.reindex(['hypertension', 'hyperlipidemia', 'arthritis', 'depression', 'diabetes',
           'substance_abuse', 'asthma', 'chronic_kidney_disease', 'hepatitis',
           'ischemic_heart', 'chronic_pulmonary_disease', 'cancer', 'osteoporosis',
           'stroke', 'heart_failure', 'atrial_fibrillation', 'schizophrenia',
           'HIV', 'alzheimers', 'autism'], fill_value="0 (0.0%)")
    output_dfs.append(this_df.stack())
    
top_comorbidities = pd.concat(output_dfs, axis=0).unstack() 
# display(top_comorbidities)
top_comorbidities =  top_comorbidities[comorbidities_df_names]
top_comorbidities= top_comorbidities.reindex(['hypertension', 'hyperlipidemia', 'arthritis', 'depression', 'diabetes',
           'substance_abuse', 'asthma', 'chronic_kidney_disease', 'hepatitis',
           'ischemic_heart', 'chronic_pulmonary_disease', 'cancer', 'osteoporosis',
           'stroke', 'heart_failure', 'atrial_fibrillation', 'schizophrenia',
           'HIV', 'alzheimers', 'autism'], fill_value="0 (0.0%)")
top_comorbidities.loc['Total'] = [len(df.index) for df in comorbidities_dict.values()]
# top_comorbidities = account_for_missing(top_comorbidities, "Total", "Missing Patients")
# top_comorbidities = move_row_to_top(top_comorbidities, "Missing Patients")
# move total to top
top_comorbidities = move_row_to_top(top_comorbidities, 'Total')
display(top_comorbidities)
create_download_link (top_comorbidities, "Download Top Comorbidities as CSV", "top_comorbidities.csv")


# In[ ]:


# Aggregate comorbidities dataframes
comorbidities_dict = {"influenza": influenza_comorbidities, 
                      "hepB": hepB_comorbidities,
                      "hpv": hpv_comorbidities, 
                      "pneumococcal(<65)":pneumococcal_under_65_comorbidities, 
                      "pneumococcal(>=65)":pneumococcal_65_and_over_comorbidities,
                      "all":all_patients_comorbidities
                     }


# In[ ]:


comorbidities_dict["influenza"]


# In[ ]:


# Define box plot func for comorbidity counts

sns.set(rc={'figure.figsize':(20,10)})
sns.set_style("whitegrid")
sns.set_palette(sns.color_palette(cohort_colors_list))
#sns.set_palette("flare")

def create_box_plot_for_comorbidities(df_dict):
    combined_df = pd.DataFrame(columns=['number_of_comorbidities', 'cohort'])
    for k, v in df_dict.items():
        temp = v[['total_comorbidities']].copy()
        temp.rename({'total_comorbidities':'number_of_comorbidities'}, axis=1, inplace=True)
        temp['number_of_comorbidities'] = temp['number_of_comorbidities'].astype(int)
        #print(temp)
        temp['cohort'] = k
        combined_df = combined_df.append(temp, ignore_index=True)
    #print(combined_df)
    box = sns.boxplot(y='number_of_comorbidities', x='cohort', data=combined_df)
    #return box


# In[ ]:


# Box plot for comorbidity counts in each cohort
create_box_plot_for_comorbidities(comorbidities_dict)


# In[ ]:


# Table for distribution of comorbidity counts
output_dfs = []
for k,df in comorbidities_dict.items():
    this_df = df.copy()
    unique_pts = len(this_df.person_id.unique())
    total = len(this_df.index)
    counts = this_df.groupby("total_comorbidities").size().to_frame("patients")
    counts.index = counts.index.astype(int)
    counts.sort_index(inplace=True, axis=0)
#     display(counts)
    counts['percent'] = counts['patients'] * 100 / total
    counts['percent'] =  counts['percent'].map('({:,.1f}%)'.format)
    counts['combined'] = counts['patients'].astype(str) + " " + counts['percent'].astype(str)
    counts = counts[['combined']]
#     counts.loc['Unique Patients'] = unique_pts
    counts.columns = [k]
    output_dfs.append(counts.stack())
    
    
comorbidities_count_dist = pd.concat(output_dfs, axis=0).unstack()
comorbidities_count_dist.fillna("0 (0.0%)",inplace=True )
comorbidities_count_dist.loc[19] = "0 (0.0%)"
comorbidities_count_dist.loc[20] = "0 (0.0%)"
comorbidities_count_dist = comorbidities_count_dist[ comorbidities_dict.keys()]
print("comorbidity counts")
comorbidities_count_dist = add_cohort_size_row(comorbidities_count_dist)
# comorbidities_count_dist = account_for_missing(comorbidities_count_dist, "Unique Patients", "Missing")
display(comorbidities_count_dist)
create_download_link (comorbidities_count_dist, "Download Comorbidities Count Distrib. as CSV", "comorbidities_count_dist.csv")


# In[ ]:


# Define function to create age histograms
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("white")
def create_comorbidity_count_histogram(df, label, col_name, ax):
#     bin_max = max(85, df[col_name].max())
#     bins = [0, 18, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 80, bin_max]
    bins = range(0, 21)
    color = cohort_colors[label]
    dp =  sns.distplot(df[col_name], bins=bins,
             kde=True, color=color, hist_kws={"alpha": 0.8}, axlabel = label + f" ({col_name})",
                        kde_kws={"lw": 3, "label": "KDE", "kernel": "tri", "bw":1},
                        ax=ax)
    dp.set_xticks(bins)
    dp.yaxis.grid(True)
#     dp.xaxis.grid(True)
    return dp

# Create histograms for comorbidities count distribution
width = 2
# height = math.ceil(len(vac_demographic_dfs) / 2) 
height = 3
fig, axes = plt.subplots(height, 2, figsize=(20, 20))
i = 0
for key, df in comorbidities_dict.items():
    create_comorbidity_count_histogram(df, key, "total_comorbidities", axes[i // 2, i % 2])
    i += 1


# In[ ]:


# Define function to create combined KDES
sns.set(rc={'figure.figsize':(10,6)})
sns.set_style("white")
def create_comorbidity_count_histogram(df, label, col_name, ax):
#     bin_max = max(85, df[col_name].max())
#     bins = [0, 18, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 80, bin_max]
    bins = range(0, 21)
    color = cohort_colors[label]
    dp = sns.distplot(df[col_name], bins=bins,
             kde=True, color=color, hist_kws={"alpha": 0.8}, axlabel = label + f" ({col_name})",
                        kde_kws={"lw": 3, "label": "KDE", "kernel": "tri", "bw":1},
                        ax=ax)
    dp.set_xticks(bins)
    dp.yaxis.grid(True)
#     dp.xaxis.grid(True)
    return dp

# fig, axes = plt.plot(20, 20)
ax = plt.gca()
i = 0
label = "total_comorbidities"
bins = range(0, 21)
for key, df in comorbidities_dict.items():
    color = cohort_colors[key]
    sns.distplot(df[label], bins=bins, hist=False,
             kde=True, color=color, hist_kws={"alpha": 0.6, "histtype":"step", "lw":3, "density":True}, axlabel = label,
                        kde_kws={"lw": 3, "label": key, "kernel": "tri", "bw":1, "cut":0}, ax=ax)
#     display(df)
#     break;
    i += 1

plt.rcParams["figure.figsize"] = (20,10)
ax.set_xticks(bins)
ax.yaxis.grid(True)


# In[ ]:


#%%script false --no-raise-error


# # Top Condition Codes for Comorbidities

# ## Download Dataset

# In[ ]:


import_df_hdf('all_conditions_with_comorbidity_limited')


# ## Get Top Codes via CSV Download

# In[ ]:


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


# In[ ]:


# make copy
all_cohort_comorbidities = all_conditions_with_comorbidity_limited.copy()
# remove extra columns
group_all_comorbidities = all_cohort_comorbidities[['person_id', 'comorbidity', 'condition_name', 'condition_concept_id']]
removed_duplicates = group_all_comorbidities.drop_duplicates()


# In[ ]:


# top condition codes for all comorbidities
def get_top_condition_codes(group):
    top_conditions = group.groupby(['comorbidity', 'condition_concept_id', 'condition_name']).size().to_frame('patient_count')
    top_conditions= top_conditions.sort_values('patient_count', ascending=False)
    return top_conditions

get_top_condition_codes(removed_duplicates).head(20)


# In[ ]:


output_dfs = {}
for comorbidity, group in removed_duplicates.groupby('comorbidity'):
    top_conditions = group.groupby(['comorbidity', 'condition_concept_id', 'condition_name']).size().to_frame('patient_count')
    top_conditions= top_conditions.sort_values('patient_count', ascending=False)
    output_dfs[comorbidity] = top_conditions


# In[ ]:


# create links for top condition codes for each comorbidity
for comorbidity, df in output_dfs.items():
    html = create_download_link(df, comorbidity, filename=f"{comorbidity}.csv")
    display(html)


# In[ ]:


# End of Section 10
#%%script false --no-raise-error


# # Doses for HepB and HPV

# ## Import Datasets

# In[ ]:


import_df("hpv_doses")
import_df("hepB_doses")


# In[ ]:


doses_dict = {
    "hpv" :hpv_doses,
    "hepB": hepB_doses
}


# In[ ]:


hpv_doses


# In[ ]:


# Table for counts
output_dfs = []
for k,this_df in doses_dict.items():
    total = len(this_df.index)
    counts = this_df.groupby("doses").size().to_frame("patients")
    counts.index = counts.index.astype(int)
    counts.sort_index(inplace=True, axis=0)
#     display(counts)
    counts['percent'] = counts['patients'] * 100 / total
    counts['percent'] =  counts['percent'].map('({:,.1f}%)'.format)
    counts['combined'] = counts['patients'].astype(str) + " " + counts['percent'].astype(str)
    counts = counts[['combined']]
    counts.columns =[k + "_doses_count"]
    counts.loc['total'] = total
    output_dfs.append(counts.stack())
    
    
doses = pd.concat(output_dfs, axis=0).unstack()
doses.fillna("0 (0.0%)",inplace=True )
#doses.loc[19] = "0 (0.0%)"
#doses.loc[20] = "0 (0.0%)"
# doses
account_for_missing(doses, "total")


# In[ ]:


create_download_link(doses)


# # Correlation
# 

# In[ ]:


import_df("influenza_demographics")
import_df("hepB_demographics")
import_df("hpv_demographics")
import_df("pneumococcal_demographics_under_65")
import_df("pneumococcal_demographics_65_and_over")
import_df("all_patients_demographics")

# Prepare dataframes for visualization
demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                   pneumococcal_demographics_65_and_over, all_patients_demographics]
vac_demographic_dfs = [influenza_demographics, hepB_demographics, hpv_demographics, pneumococcal_demographics_under_65, 
                            pneumococcal_demographics_65_and_over]
demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)", "all"]
vac_demographic_df_names=["influenza", "hepB", "hpv", "pneumococcal(<65)", "pneumococcal(>=65)"]

change_cols_to_int(vac_demographic_dfs, ["age_at_vaccination"])
change_cols_to_int(demographic_dfs, ["age_in_2018"])


# In[ ]:


df = influenza_demographics.copy()
df = df[df['race']=="Black or African American"]
df


# In[ ]:


df.race.value_counts()

