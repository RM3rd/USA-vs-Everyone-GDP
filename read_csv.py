import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
import numpy as np
from datetime import datetime

tmpfile    = "gdp_temp.tmp"               # file used to store all extracted data
logfile    = "gdp_logfile.txt"            # all event logs will be stored in this file
targetfile = "gdp_transformed_data.csv"   # file where transformed data is stored

# function to extract data from GPD csv file
def extract_from_csv(gdp_file):

# assigning variables to both my file paths
    gdp_file = "/Users/ralphmajetteiii/Desktop/USA-vs-Everyone-GDP-/GPD by Country.csv"

# assigning files to data frames
    gdp_df = pd.read_csv(gdp_file)
    return gdp_df

# transforming the data 
def transform_data(gdp_df):
    gdp_df['GDP'] = round(gdp_df.GDP, 2)
    gdp_df['GDP per Capita'] = round(gdp_df.GDP_per_Capita, 2)
    gdp_df['Country'].upper()
    return gdp_df

# loading data into target file
def gdp_load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile) 

# adding logging 
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')  

# Log that you have started the ETL process
log("ETL Job Started")

# Log that you have started the Extract step
log("Extract phase Started")

# Call the Extract function
extracted_data = extract_from_csv()
log("Extract phase Ended")

# Log that you have completed the Extract step
log("Extract phase Started")

# Log that you have started the Transform step
log("Transform phase Started")

# Call the Transform function
transformed_data = transform_data(extracted_data)

# Log that you have completed the Transform step
log("Transform phase Ended")

# Log that you have started the Load step
log("Load phase Started")

# Call the Load function
gdp_load(targetfile,transformed_data)

# Log that you have completed the Load step
log("Load phase Ended")

# Log that you have completed the ETL process
log("ETL Job Ended") 









