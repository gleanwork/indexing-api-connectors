import os, sys, getopt
class Constants:
    """A Class for storing runtime configuration data"""

    # Defining here for IDE autocomplete 
    CRAWL_TYPE = "FULL"
    DATASOURCE_CATEGORY = ""
    DATASOURCE_DISPLAYNAME = ""
    DATASOURCE_EMAIL = ""
    DATASOURCE_HOMEURL = ""
    DATASOURCE_ICON = ""
    DATASOURCE_NAME = ""
    DATASOURCE_URLREGEX = ""
    DATASOURCE_USER = ""
    DATASOURCE_VIEWURLBASE = ""
    DEBUG = False
    DIRECT_ID = None
    GLEAN_INSTANCE = ""
    GLEAN_PUSH_API_TOKEN = ""
    MIN_IDX = 0
    MAX_IDX = 500000
    MAX_THREADS = 10
    BENCHLING_ADMIN_USER = ""
    BENCHLING_KEY = ""
    BENCHLING_SECRET = ""
    BENCHLING_URL = ""
    PUBLIC_ONLY = False
    DELAY = 0.2
    VERBOSE = False

    requiredKeys = [
        "DATASOURCE_CATEGORY",
        "DATASOURCE_DISPLAYNAME",
        "DATASOURCE_EMAIL",
        "DATASOURCE_HOMEURL",
        "DATASOURCE_ICON",
        "DATASOURCE_NAME",
        "DATASOURCE_URLREGEX",
        "DATASOURCE_USER",
        "DATASOURCE_VIEWURLBASE",
        "DEBUG",
        "GLEAN_INSTANCE",
        "GLEAN_PUSH_API_TOKEN",
        "BENCHLING_ADMIN_USER",
        "BENCHLING_KEY",
        "BENCHLING_SECRET",
        "BENCHLING_URL"
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

        self.setCrawlType("FULL")

        argv = sys.argv[1:]
        try:
            opts, args = getopt.getopt(argv,"c:d:i:m:pr:t:vx:", [ "crawl-type=", 
                                                                "debug=",
                                                                "id=",
                                                                "min-idx=",
                                                                "max-idx=",
                                                                "public-only",
                                                                "rate=",
                                                                "threads",
                                                                "verbose"
                                                              ])

            for opt, arg in opts:
                if   opt in ("-c", "--crawl-type"):
                    self.setCrawlType(arg)
                elif opt in ("-d", "--debug"):
                    self.setDebug(arg)
                elif opt in ("-i", "--id"):
                    self.DIRECT_ID = arg
                elif opt in ("-m", "--min-idx"):
                    self.MIN_IDX = int(arg)
                elif opt in ("-p", "--public-only"):
                    self.PUBLIC_ONLY = True
                elif opt in ("-r", "--rate"):
                    self.DELAY = 1 / float(arg)
                elif opt in ("-t", "--threads"):
                    self.MAX_THREADS = int(arg)
                elif opt in ("-v", "--verbose"):
                    self.VERBOSE = True
                elif opt in ("-x", "--max-idx"):
                    self.MAX_IDX = int(arg)

        except getopt.GetoptError:
            print('crawl.py -v -d false --min-idx 10 --max-idx 11 --rate .1')
            pass


    def isVerbose(self):
        return self.VERBOSE == True

    def getKeys(self) -> dict:
        return self.__dict__.keys()

    def setCrawlType(self, inputType):
        if inputType.lower() == 'full' or inputType == "":
            self.CRAWL_TYPE = "FULL"
        elif inputType.lower() == 'entity':
            self.CRAWL_TYPE = "ENTITY"
        elif inputType.lower() == 'content':
            self.CRAWL_TYPE = "CONTENT"
        elif inputType.lower() == 'none':
            self.CRAWL_TYPE = "NONE"
        else:
            print("Error: unknown crawl type: ", inputType)
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

    def setUserDomains(self, domains):
        self.DATASOURCE_USERDOMAINS = []
        for domain in domains.lower().split(","):
            self.DATASOURCE_USERDOMAINS.append(domain.strip())
