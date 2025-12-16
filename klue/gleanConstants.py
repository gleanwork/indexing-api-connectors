# pylint: disable-all
import os, sys, getopt
class Constants:
    """A Class for storing runtime configuration data"""
    # Defining here for IDE autocomplete
    BATCH_SIZE = 100
    BULK_INDEX = True
    CRAWL_TYPE = "FULL"
    DATASOURCE_CATEGORY = ""
    DATASOURCE_DISPLAYNAME = ""
    DATASOURCE_HOMEURL = ""
    DATASOURCE_ICON = ""
    DATASOURCE_NAME = ""
    DATASOURCE_URLREGEX = ""
    DATASOURCE_VIEWURLBASE = ""
    DEBUG = False  
    DIRECT_ID = ""
    GLEAN_INDEX_API = ""
    GLEAN_INSTANCE = ""
    GLEAN_PUSH_API_TOKEN = ""
    KLUE_API_KEY = ""
    MAX_IDX = 500000
    MAX_THREADS = 10
    MIN_IDX = 0
    VERBOSE = False

    requiredKeys = [ 
        "DATASOURCE_CATEGORY",  
        "DATASOURCE_DISPLAYNAME",  
        "DATASOURCE_HOMEURL",  
        "DATASOURCE_ICON",  
        "DATASOURCE_NAME",  
        "DATASOURCE_URLREGEX",  
        "DATASOURCE_VIEWURLBASE",
        "DEBUG",  
        "GLEAN_INSTANCE",  
        "GLEAN_PUSH_API_TOKEN",
        "KLUE_API_KEY", 
    ]  
   
    otherKeys = [  
        "BATCH_SIZE",
        "BULK_INDEX"
    ]  
   
    trueVals = ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']  
   
   
    def __init__(self) -> None:  
        allVars = True  
   
        for requiredKey in self.requiredKeys:  
            try:  
                self.__dict__[requiredKey] = os.environ[requiredKey]  
            except:  
                allVars = False  
                print(f"key {requiredKey} NOT in environment")  
   
        for otherKey in self.otherKeys:  
            try:  
                self.__dict__[otherKey] = os.environ[otherKey]  
            except:  
                pass  
   
        if allVars == False:  
            print('Not all vars set. Exiting.')  
            sys.exit()  
   
        self.BATCH_SIZE = int(self.BATCH_SIZE)  
   
        self.GLEAN_INDEX_API = f"https://{self.GLEAN_INSTANCE}-be.glean.com/api/index/v1" 
        self.setDebug() 

        self.BULK_INDEX = True if self.BULK_INDEX.lower() in self.trueVals else False
   
        argv = sys.argv[1:]  
        try:  
            opts, args = getopt.getopt(argv,"b:c:d:e:f:i:l:m:pr:s:t:vx:", [   
                                                                          "batch-size=",  
                                                                          "crawl-type=",   
                                                                          "debug=",  
                                                                          "id=",  
                                                                          "min-idx=",  
                                                                          "max-idx=",  
                                                                          "verbose"  
                                                                    ])  
   
            for opt, arg in opts:  
                if   opt in ("-b", "--batch-size"):  
                    self.BATCH_SIZE = int(arg) 
                elif opt in ("-c", "--crawl-type"):  
                    self.setCrawlType(arg)  
                elif opt in ("-d", "--debug"):  
                    self.setDebug(arg)  
                elif opt in ("-i", "--id"):  
                    self.DIRECT_ID = arg  
                elif opt in ("-m", "--min-idx"):  
                    self.MIN_IDX = int(arg)
                elif opt in ("-x", "--max-idx"):  
                    self.MAX_IDX = int(arg)  
                elif opt in ("-v", "--verbose"):  
                    self.VERBOSE = True  
   
        except getopt.GetoptError:  
            print('crawl.py -v -d false --min-idx 10 --max-idx 11')  
            pass  
   
   
    def isVerbose(self):  
        return self.VERBOSE == True  
   
    def getKeys(self) -> dict:  
        return self.__dict__.keys()  
   
    def setCrawlType(self, inputType):  
        if inputType.lower() == 'full' or inputType == "":  
            self.CRAWL_TYPE = "FULL"  
        elif inputType.lower() == 'incremental':  
            self.CRAWL_TYPE = "INCREMENTAL"  
        elif inputType.lower() == 'none':  
            self.CRAWL_TYPE = "NONE"  
        else:  
            print("Error: unknown crawl type: ", inputType)  
            sys.exit()  
   
    def getDebug(self) -> bool:  
        return self.DEBUG  
   
    def setDebug(self, debug = None):  
   
        if debug != None:  
            try:  
                self.DEBUG = str(debug).lower() in self.trueVals  
            except:   
                self.DEBUG = False  
        else:  
            try:  
                self.DEBUG = os.environ["DEBUG"].lower() in self.trueVals  
            except:  
                self.DEBUG = False  
   
        return self.DEBUG  
 
