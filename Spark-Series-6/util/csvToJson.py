"""
 CSV ingestion in a dataframe.
 Source of file: https://data.gov/
 @author Aruna Das
"""
import json
import csv
import os

current_dir = os.path.dirname(__file__)
relative_path = "../data/Street_Names.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# create a dictionary
jsonArray = []
with open(absolute_file_path,encoding='utf-8') as csvfile:
    csvReader = csv.DictReader(csvfile)

    # conver each row to dictionar and add to data
    for row in csvReader:
        jsonArray.append(row)

#json_file_name = 
json_file_path = os.path.join(current_dir,"../data/Street_Names.json")

with open(json_file_path,'w', encoding='utf-8') as jsonfile:
    jsonfile.write(json.dumps(jsonArray,indent=4))        

