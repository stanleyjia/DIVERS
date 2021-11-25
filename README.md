# DIVERS
### Patient Characteristics Across Adult Routine Vaccinations: A Descriptive Analysis of the All of Us Research Program

## Overview
This code was used to query and run analysis on adult vaccination cohorts in the All of Us database, resulting in a research paper (currently in peer review) and a published poster at IDweek 2021.

## IDweek 2021 Poster
 An analysis of the National Institutes of Health All of Us research database: sociodemographic disparities among patients who received vaccinations
 
 ![Poster](https://user-images.githubusercontent.com/17485488/143495625-09d4c81a-1c0a-461c-9219-6d22788968fe.jpeg)

 
Download IDweek poster here: [AoUVaccinationStudy_Sep14.pdf](https://github.com/stanleyjia/DIVERS/files/7605047/AoUVaccinationStudy_Sep14.pdf)


## How it works
Due to the sheer size of the database (450,000+ participants), running the queries for each analysis is too time-consuming. I came up with a solution: 

### Solution
(1) Only download the necessary and relavant data for each category of analysis (demographics, comorbidities, medication usage, etc). 

(2) Run these queries and then locally store these smaller subsets of data as separate datasets in the workbench. 

(3) Run analysis in a separate notebook 

### Files
In the repository, "CREATE_DATASET_01" is used to create these datasets while "ANALYSIS_01" is used to run statistical analysis and visualization of those datasets. As a result, "CREATE_DATASET_01" is composed of mainly Google BigQuery SQL code while "ANALYSIS_01" uses Python pandas and numpy.


## How to use
Unfortunately, under the All of Us Researcher guidelines, we are not allowed to access and display participant data outside of the researcher workbench. Consequently, the Python and SQL code in this repository isn't executable. However, it still demonstrates how users can query, analyze, and visualize data from a OMOP CDM database.
