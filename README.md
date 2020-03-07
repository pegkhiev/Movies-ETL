# Movies-ETL

## Background
The challenge is to write a function that automates the Extraction, Transformation, and Load of new data without supervision, 
and try to catch any errors that might arise, with the proper assumptions. 

## Files included in the repo: 
- challenge.ipynb 
- challenge.py (code only) 
### Note: the original wikipedia json file, the kaggle csv file, and ratings csv file are not uploaded to the repo as it is not required in the challenge rubric. The config.py file that contains PostgreSQL passeword is not not uploaded to repo for privacy. 

## Function
The def function takes in three arguments: wiki, kaggle, and rating.  The values are the respective file names when calling the function.  

## Assumption 1: 
In Extraction stage, it is assumed that the file names and the path names may not be saved correctly.  A try-except block has been added to catch any error in case the file name or the path name is not accurately saved. Three try-except blocks added for Wikipedia data, Kaggle, and Ratings data respectively  

## Assumption 2: 
In parsing the Wikipedia data with numerical values, including "Box office", "Budget", it is assumed that new files may not have these columns or that the names might be recorded differently.  Two try-except blocks are added to catch errors in case the names are not existent or different. 

## Assumption 3: 
In parsing the Wikipedia date for "Release date" and "Running time",  it is assumed that new files may not have these columns or that the names might be recorded differently.  Two try-except blocks are added to catch errors in case the names are not existent or different. 

## Assumption 4
In cleaning Kaggle data, in the original exploratory steps, only "video" column had one value, and was dropped.  In the automated function, it is assumed that there may be multiple columns with only one value.  A for-loop in added to drop any column that has only 1 value. 

## Assumption 5
After merging Wikipedia and Kaggle dataframes, the step of choosing which columns to keep assumed that not every column would be in the new dataframe because the original dataframes might be missing one or more columns.  A try-except block is added to catch errors in case one or more columns is not available. 

## Loading the data to SQL 
1) TRUNCATE TABLE command was used to remove all data in the SQL tables but keeping the original tables 
2) In loading both the movies_df dataframe and ratings dataframe, "if_exists = "append"" is added to add data into the empty tables 

<img alt = "SQL screenshot" src = "https://github.com/pegkhiev/Movies-ETL/blob/master/images/Screen%20Shot%202020-03-07%20at%2010.50.04%20AM.png">
