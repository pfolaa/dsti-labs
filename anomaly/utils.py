import datetime
import json
import re
import tqdm
import regex
import os
import glob2
import pandas as pd


def convertToTimestamp(str, date_format= "%Y-%m-%dT%H:%M:%S.%fZ"):
  '''
  function to convert date to Timestamp
  '''
  element = datetime.datetime.strptime(str, date_format)
  return datetime.datetime.timestamp(element)


def read_json_insert_csv(root_path, json_file, file_csv):
  data = json.load(json_file)
  df = pd.DataFrame.from_records(data)
  # convert file to csv
  df.to_csv(f'{root_path}/{file_csv}', 
            sep='|', 
            index= None)

  # return 1 fichier csv fer json file
  return df 


def process_json( path_file_json="./nirra-log-bot", dest_path="./liberta_leasing"):
  # créer toute l'aborescence du fichier, crée le chemin
  os.makedirs(dest_path, exist_ok=True) 
  # read all json files

  json_files = glob2.glob(os.path.join(path_file_json,'*.json'))
  for file_name in tqdm.tqdm(json_files):
    with open(file_name) as json_file:
      path_file_csv = file_name.replace(".json", ".csv").split("/")[-1]
      read_json_insert_csv(dest_path, json_file, path_file_csv)
      
