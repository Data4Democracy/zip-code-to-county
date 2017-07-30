import requests
import os
import time
import pandas as pd
import glob
import datetime as dt

def verify_dirs_exist():
    #This notebook requires a few directories
    dirs = ["download", "download\csv", "download\excel"]
    for d in dirs:
        curpath = os.path.abspath(os.curdir) # get current working directory
        full_path = os.path.join(curpath, d) # join cwd with proposed d
        create_dir_if_not_exists(full_path)

def create_dir_if_not_exists(full_path):
    # expects a full path to the directory to test against or to create.
    if not os.path.exists(full_path):
        os.makedirs(full_path)
        print("Created directory ", full_path)
    else:
        print("Directory ", full_path, " already existed")

def generate_file_name_from_url(url):
    month_year = url.split("\\")[-1].split("_")[-1].split(".")[0]
    month = month_year[:2]
    year = month_year[2:]
    new_file_name = "ZIP-COUNTY-FIPS_"+year + "-" + month
    return new_file_name

def get_file_path(url, csv_file=False):
    "Takes in the full url and returns the full file path"
    "File names are ZIP_COUNTY_032010.xlsx"
    curpath = os.path.abspath(os.curdir) #get current working directory
    full_path = ''
    if csv_file:
        #If we are passing in a csv, change xlsx to .csv
        csv_file_name = url.split("\\")[-1][:-5] + ".csv"
        full_path = os.path.join(curpath, "download\csv", csv_file_name)
    else:
        url_name = url.split('/')[-1] # gets file name
        #switching file names to be YYYY-MM for better file management
        url_file_name = generate_file_name_from_url(url) + ".xlsx"
        full_path = os.path.join(curpath, "download\excel", url_file_name)
    return full_path

def download_file(url):
    #With a full url, downloads the full file in chunks.
    #Able to handle large files.
    full_file_path = get_file_path(url)
    r = requests.get(url)
    with open(full_file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    return full_file_path

"""
Census State & County FIPS Data
HUD uses ZIP & FIPS data. We need to grab the FIPS to county
name data to be able to merge and create the cross lookup
"""

census_fips_url = "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"
#the FIPS data does not come with column names
census_col_names = ["STATE","STATEFP","COUNTYFP","COUNTYNAME","CLASSFP"]

# Open url, read in the data with the column names, and convert specific columns to str.
# When Pandas reads these columns, it automatilcally intrepets them as INTS
fips_df = pd.read_table(
    census_fips_url,
    sep=",",
    names=census_col_names,
    converters={'STATEFP': str,'COUNTYFP': str,'CLASSFP': str}
)

# Combine State & County FP to generate the full FIPS code for easier lookup
fips_df["STCOUNTYFP"] = fips_df["STATEFP"] + fips_df["COUNTYFP"]
#Dropping STATFP & COUNTYFP as we no longer need them
fips_df = fips_df[["STCOUNTYFP", "STATE" ,"COUNTYNAME", "CLASSFP"]]

# Get current year to handle future runs of this file
now = dt.datetime.now()
cur_year = now.year

def get_files_url(month, year):
    monthyear = month + str(year)
    return "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_{}.xlsx".format(monthyear)

"""
Main loop
"""
def main()
    # from the beginning of hud year data to current year
    for year in range(2010, cur_year+1):
        #hud files are based on quarters
        for month in ["03", "06", "09", "12"]:
            #generate the HUDs url
            url = get_files_url(month, year)
            #download the file
            full_file_path = download_file(url)
            #open and get the excel dataframe
            excel_df = process_excel_file(full_file_path)
            #merge the excel file with the fips data
            merged_df = fips_df.merge(excel_df)
            #reduce the dataframe down to specific columns
            merged_df = merged_df[["ZIP", "COUNTYNAME", "STATE", "STCOUNTYFP", "CLASSFP"]]
            #generate a csv file path
            csv_path = get_file_path(full_file_path, True)
            print(csv_path)
            try:
                merged_df.to_csv(csv_path, encoding='utf-8', index=False)
            except:
                #once we get to a Q that hasn't happened yet, we'll get an XLDRerror
                print("Operation has completed")
                break

            # prevent from overloading the HUD site and to be a nice visitor
            time.sleep(1)
            print("Completed ", csv_path)


if __name__ == '__main__':
    verify_dirs_exist()
    main()
