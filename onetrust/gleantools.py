import time, json, re, csv, os, ast

def writeToCSV(data, filename):
        if not data:
            print(f"No rows to write to {filename}; skipping.")
            return
        fieldnames = data[0].keys()
        with open(filename, 'w', newline='') as file:
             writer = csv.DictWriter(file, fieldnames)
             writer.writeheader()
             writer.writerows(data)

def convert_str_map(str):
    try:
        mapping = ast.literal_eval(str.replace("\n",""))
        if isinstance(mapping, dict):
             return mapping
    except (SyntaxError, ValueError):
        print("Error in str to map conversion => ", str)

def csv_to_dict(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data
