import csv
import ast
from datetime import datetime 
import gleanConstants as Constants
CONST = Constants.Constants()

def nowString():
    """Builds a string from the current time

    Returns:
        A formatted string to use for things like uploadId
    """
    now = datetime.now() # current date and time
    return now.strftime("%Y%m%d%H%M%S")


log_file_name = "coupa_crawler_" + nowString() + ".log"

# def get_logger():
#     logger = logging.getLogger('log')
#     log_file_handler = logging.FileHandler(log_file_name)
#     log_file_handler.setLevel(logging.DEBUG)
#     log_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     log_file_handler.setFormatter(log_file_formatter)
#     logger.addHandler(log_file_handler)
#     return logger


def writeToCSV(data, filename):
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


def convert_date(date):
    dateformat = "%Y-%m-%dT%H:%M:%S"
    return int(datetime.strptime(date,dateformat).timestamp())