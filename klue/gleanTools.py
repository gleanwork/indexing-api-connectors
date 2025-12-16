# pylint: disable-all
import re, time, csv, binascii, base64, json, glob, os
from datetime import datetime, timedelta
from calendar import timegm

import gleanConstants as Constants

CONST = Constants.Constants()



def base64encode(sourceString):
    bytes = sourceString.encode('utf-8')
    base64_bytes = base64.b64encode(bytes)
    base64_string = base64_bytes.decode('utf-8')
    return base64_string



def checksum(text):
    checksum = binascii.crc32(text.encode())
    return checksum



def cleanString(myStr):
    charsToKeep = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghiklmnopqrstuvwxyz1234567890"
    myStr = myStr.strip()
    cleanString = ''
    for char in myStr:
        if char == " " or char == ".":
            cleanString = cleanString + "+"
        elif charsToKeep.find(char) > -1:
            cleanString = cleanString + char
    return cleanString

def extractDateFromFilename(filename, extension = "json"):
    return filename.split('_')[1].split(f'.{extension}')[0]



def find_in_dict(data, target):
    for key, value in data.items():
        if key.lower() == target.lower():
            return {key: value}
        elif isinstance(value, dict):
            result = find_in_dict(value, target)
            if result is not None:
                return result
        elif isinstance(value, list):
            for possibleDict in value:
                if isinstance(possibleDict, dict):
                    result = find_in_dict(possibleDict, target)
                    if result is not None:
                        return result
    return None



def getMostRecentFile(path = None, prefix = "team-dump_", extension = "json"):
    if not path:
        resolvedPath = os.path.join(CONST.CACHE_DIR, prefix)
    else:
        resolvedPath = os.path.join(path, prefix)
    files = glob.glob(f"{resolvedPath}*.{extension}")
    if not files:
        return None
    files.sort(key = lambda x: extractDateFromFilename(x, extension), reverse=True)
    print(f"{prefix} dump files: {files}")
    return files[0]



def getSecondsFromTimestamp(timeStamp):
    timeStamp = timeStamp.replace('Z', '').replace('.', '')
    timeStamp = datetime.strptime(timeStamp, "%Y%m%d%H%M%S%f")
    now = datetime.utcnow()
    diff = now - timeStamp
    return diff.total_seconds()



def getTimestamp(**delta):
    """Builds a timestamp that VDS uses

       20230627205010.035Z
    """
    returnStamp = datetime.utcnow()
    if delta and isinstance(delta, dict):
         try:
             returnStamp = returnStamp - timedelta(**delta)
         except:
            pass
    returnStamp = returnStamp.strftime("%Y%m%d%H%M%S.%f")[:-3] + "Z"

    return returnStamp



def logTime():
    now = datetime.now()
    formattedTime = now.strftime("%Y-%m-%d %H:%M:%S")

    return formattedTime



def maskString(obj, length=5):
    returnString = "..."

    try:
        theString = str(obj)
        strLen = len(theString)
        if strLen > 8 and strLen > length:
            returnString = f"{theString[:length]}..."
        else:
            returnString = f"{theString[:1]}..."
    except:
        pass

    return returnString



def nowString():
    """Builds a string from the current time

    Returns:
        A formatted string to use for things like uploadId
    """
    now = datetime.now() # current date and time
    return now.strftime("%Y%m%d%H%M%S")



def readFileData(fileName):
    try:
        with open(fileName, 'r') as file:
            data = file.read()
        return json.loads(data)
    except Exception as e:
        print(f"Error reading from {fileName}: {e}")
        return None



def writeFileData(data, filename): 
    try:
        with open(filename, 'w') as file: 
            json.dump(data, file) 
    except Exception as e: 
        print(f"Error writing to {filename}: {e}") 
        return False
    return True
