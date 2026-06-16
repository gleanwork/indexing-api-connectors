import os, sys, getopt
class Constants:
    """A Class for storing runtime configuration data"""

    # Defining here for IDE autocomplete
    DATASOURCE_CATEGORY = str
    DATASOURCE_DISPLAYNAME = str
    DATASOURCE_HOMEURL = str
    DATASOURCE_ICON = str
    DATASOURCE_NAME = str
    DATASOURCE_URLREGEX = str
    DEBUG = False
    GLEAN_INSTANCE = str
    GLEAN_PUSH_API_TOKEN = str
    ONETRUST_BASE_URL = str
    ONETRUST_CLIENT_ID = str
    ONETRUST_CLIENT_SECRET = str
    VERBOSE = False
    BULK_INDEX = False

    requiredKeys = [
        "DATASOURCE_CATEGORY",
        "DATASOURCE_DISPLAYNAME",
        "DATASOURCE_HOMEURL",
        "DATASOURCE_ICON",
        "DATASOURCE_NAME",
        "DATASOURCE_URLREGEX",
        "DEBUG",
        "GLEAN_INSTANCE",
        "GLEAN_PUSH_API_TOKEN",
        "ONETRUST_BASE_URL",
        "ONETRUST_CLIENT_ID",
        "ONETRUST_CLIENT_SECRET",
        "BULK_INDEX"
    ]


    def __init__(self) -> None:
        allVars = True

        for requiredKey in self.requiredKeys:
            try:
                self.__dict__[requiredKey] = os.environ[requiredKey]

            except:
                allVars = False
                print(f"key {requiredKey} NOT in environment")

        if allVars == False:
            print('Not all vars set. Exiting.')
            sys.exit()

        self.setDebug()
        self.setBulkIndex()

        argv = sys.argv[1:]
        try:
            opts, args = getopt.getopt(argv, "d:v", ["debug=", "verbose"])

            for opt, arg in opts:
                if opt in ("-d", "--debug"):
                    self.setDebug(arg)
                elif opt in ("-v", "--verbose"):
                    self.VERBOSE = True

        except getopt.GetoptError:
            print('usage: onetrust.py [-d true|false] [-v]')
            pass


    def isVerbose(self):
        return self.VERBOSE == True

    def isBulkIndex(self) -> bool:
        return self.BULK_INDEX

    def setBulkIndex(self):
        val = os.environ["BULK_INDEX"].lower()
        if  str(val) == "false":
            self.BULK_INDEX = False
        elif str(val) == "true":
            self.BULK_INDEX = True
        else:
            print("Error: invalid BULK_INDEX env value")
            sys.exit()

    def getDebug(self) -> bool:
        return self.DEBUG

    def setDebug(self, debug = None):

        trueVals = ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']

        if debug != None:
            try:
                self.DEBUG = str(debug).lower() in trueVals
            except:
                self.DEBUG = False
        else:
            try:
                self.DEBUG = os.environ["DEBUG"].lower() in trueVals
            except:
                self.DEBUG = False

        return self.DEBUG
