from scripts.importList import *
def instantiate_basics(BASE_DIR):
    # Determine assets path, works in PyInstaller bundle or script
    global ASSETS_DIR
    if getattr(sys, 'frozen', False):
        ASSETS_DIR = os.path.join(BASE_DIR, '_internal','assets')
    else:
        ASSETS_DIR = os.path.join(BASE_DIR, 'assets')
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    global DATABASE_PATH
    global TRAN_DATABASE_PATH
    global HELP_PATH
    global executor
    global APIexecutor
    global gui_queue
    DATABASE_PATH = os.path.join(ASSETS_DIR, 'Acc_Tran.db')
    TRAN_DATABASE_PATH = os.path.join(ASSETS_DIR, 'tranCalc.db')
    HELP_PATH = os.path.join(ASSETS_DIR,"helpInfo.txt")

    executor = ThreadPoolExecutor()
    APIexecutor = ThreadPoolExecutor(max_workers=5) #limits overcalling
    gui_queue = queue.Queue()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=ASSETS_DIR + "/systemLogs.log",
        filemode="a"
    )
    if getattr(sys, 'frozen', False): #Executables will log print statements
        class PrintToLogger:
            def write(self, msg):
                msg = msg.strip()
                if msg:
                    logging.info(msg)

            def flush(self):
                pass

        sys.stdout = PrintToLogger()


    warnings.simplefilter("error",RuntimeWarning)