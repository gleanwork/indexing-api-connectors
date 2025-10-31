"""Miscellaneous tools

A set of useful functions to help with repetative tasks 

"""
import time, json, re
from datetime import datetime
from calendar import timegm

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            # return obj.decode()
            return "bytes of length: " + str(len(obj))
        return json.JSONEncoder.default(self, obj)


def cleanString(myStr):
    charsToKeep = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghiklmnopqrstuvwxyz1234567890"
    myStr = myStr.strip()
    cleanString = ''
    for char in myStr:
        if char == " ":
            cleanString = cleanString + "+"
        elif charsToKeep.find(char) > -1:
            cleanString = cleanString + char
    return cleanString


def dump(obj):
    return json.dumps(obj, cls=BytesEncoder)


def dateToEpoch(myDateString):
    try:
        # If formated like: "Fri, 09 Sep 2022 19:28:33 GMT"
        utc_time = time.strptime(myDateString, "%a, %d %b %Y %H:%M:%S GMT")
    except:
        myDateString = "Fri, 21 Sep 2021 21:21:21 GMT"
        utc_time = time.strptime(myDateString, "%a, %d %b %Y %H:%M:%S GMT")
    return timegm(utc_time)


def nowString():
    """Builds a string from the current time

    Returns:
        A formatted string to use for things like uploadId
    """
    now = datetime.now() # current date and time
    return now.strftime("%Y%m%d%H%M%S")


def removeTags(tags, myStr):
    for tag in tags:
        pattern = r'<[ ]*' + tag + '.*?\/[ ]*' + tag + '[ ]*>'
        if myStr.find(tag) > -1:
            myStr = re.sub(pattern, '', myStr, flags=(re.IGNORECASE | re.MULTILINE | re.DOTALL))
    return myStr