import xml.etree.ElementTree as ET
import requests, zipfile, io
import pandas as pd
import os
import json
import time
from datetime import datetime 
import logging

def setup_logger(log_file):
  """Sets up a logger to write messages to a file."""

  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

  file_handler = logging.FileHandler(log_file)
  file_handler.setFormatter(formatter)

  logger.addHandler(file_handler)
  return logger

logger = setup_logger("my_log_file.txt")

column_name = ['name','height','weight']
d = {}

def extract_data_from_zip():
  logger.info("Download file using URL...")
  start_time = datetime.now()

  try:
    zip_file_url = input("Enter the URL ")
    r = requests.get(zip_file_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    a = os.getcwd()
    z.extractall(a)

    logger.info("Download file fROM URL completed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")
  except:
    logger.error("Downloading file form url failed")
    logger.info("Check URL...")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def data_extract(column_name,filename):
  try:
    n = []
    tree = ET.parse(filename)
    root = tree.getroot()
    for i in root.iter(column_name):
        n.append(i.text) 
    return n
  except:
    logger.error("Data extraction from xml failed")

def data_extract_xml(filename):
  logger.info("Extract data from xml file")
  start_time = datetime.now()
  try:
    for col_name in column_name:
      data = data_extract(col_name,filename)
      d.update({col_name+"_"+filename:data})

    logger.info("Data Extracted from XML")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

  except:
    logger.error("Invalid Column Name or empty data")
    logger.info("Extract data from xml failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def data_extract_csv(filename):
  logger.info("Extract data from csv file")
  start_time = datetime.now()
  try:
    df = pd.read_csv(filename)
    for col_name in column_name:
      data = df[col_name].tolist()
      d.update({col_name+"_"+filename:data})

    logger.info("Data Extracted from CSV")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

  except:
    logger.error("Invalid Column Name or empty data")
    logger.info("Extract data from csv failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def data_extract_json(filename):
  logger.info("extract data from json")
  start_time = datetime.now()
  try:
    data = []
    with open(filename) as f:
      name = []
      height = []
      weight = []
      for i in f:
        res = json.loads(i)
        name.append(res['name'])
        height.append(res['height'])
        weight.append(res['weight'])
      data.append(name)
      data.append(height)
      data.append(weight)
      for i in range(len(column_name)):
        d.update({(str(column_name[i])+"_"+filename):data[i]})
    
    logger.info("Data Extracted from JSON")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

  except:
    logger.error("Invalid Column Name or error loading json file")
    logger.info("Extract data from json failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def data_gather(d):
  logger.info("Data gathering starting...")
  start_time = datetime.now()
  try:
    name = []
    height = []
    weight = []
    data = {}

    for i in column_name:
        for l,m in d.items():
            if i in l:
                if "name" == i:
                    name.extend(m)
                if "height" == i:
                    height.extend(m)
                if "weight" == i:
                    weight.extend(m)
    for i in column_name:
        if "name" == i:
            data.update({i:name})
        if "height" == i:
            data.update({i:height})
        if "weight" == i:
            data.update({i:weight})

    logger.info("Data Gathering Completed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")
    
    return data
  except:
    logger.error("Invalid Column Name or data gathering issue")
    logger.info("Data gathering failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def make_dataframe(data):
  logger.info("Transform data into dataframe")
  start_time = datetime.now()
  try:
    df = pd.DataFrame.from_dict(data)

    logger.info("Transform data into dataframe completed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

    return df
  except:
    logger.error("Dictionary to dataframe conversion issue")
    logger.info("Transform data into dataframe failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def transform_data(df):
  logger.info("Starting adding column in ETL process...")
  start_time = datetime.now()
  try:
    df['height'] = df['height'].astype(float)
    df['weight'] = df['weight'].astype(float)
    df['height_inches'] = df['height'] * 0.0254
    df['weight_kilograms'] = df['weight'] * 0.453592
    #df.drop(['height','weight'],axis=1,inplace=True)

    logger.info("Column successfully added")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")
    
    return df
  except:
    logger.error("Values in column are not present")
    logger.info("Adding column in ETL failed")
    time_elapsed = datetime.now() - start_time 
    logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

def main():
    logger.info("Starting ETL process...")
    start_time = datetime.now() 

    try:
      extract_data_from_zip()
      time.sleep(2)
      files = [f for f in os.listdir() if os.path.isfile(f)]
      for i in files:
        if i.endswith('.xml'):
          data_extract_xml(i)
        elif i.endswith('.csv'):
          data_extract_csv(i)
        elif i.endswith('.json'):
          data_extract_json(i)

      data = data_gather(d)
      df = make_dataframe(data)
      unique_df = df.drop_duplicates()
      transformed_data = transform_data(unique_df)
      #print(transformed_data)
      unique_df.to_csv('transformed_data.csv')

      logger.info("Data transformation completed")
      time_elapsed = datetime.now() - start_time 
      logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")

    except:

      logger.error("Invalid URL")
      logger.info("Data transformation Failed")
      time_elapsed = datetime.now() - start_time 
      logger.info(f"Time elapsed (hh:mm:ss.ms) {time_elapsed}")
    #...

if __name__ == "__main__":
    main()

