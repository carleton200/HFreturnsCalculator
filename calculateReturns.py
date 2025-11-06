from scripts.importList import *
from scripts.commonValues import *
from scripts.instantiate_basics import *
instantiate_basics(BASE_DIR= os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))) #prepares values needed for other class functionality and imports

from classes.DatabaseManager import DatabaseManager
from classes.transactionApp import transactionApp
from classes.returnsApp import returnsApp
from classes.windowClasses import *
from classes.tableWidgets import *
from classes.widgetClasses import *

from scripts.processPool import processPool
from scripts.basicFunctions import *
from scripts.exportTableToExcel import exportTableToExcel
from scripts.loggingFuncs import *


if __name__ == '__main__':
    freeze_support()
    key = os.environ.get(dynamoAPIenvName)
    ok = key
    app = QApplication(sys.argv)
    queueTimer = QTimer()
    queueTimer.timeout.connect(poll_queue)
    queueTimer.start(500)

    w = returnsApp(start_index=0 if not ok else 1)
    
    if ok: w.api_key = key
    w.show()
    if ok:
        w.init_data_processing()
    else:
        w.stack.setCurrentIndex(0)
    bgWatch = QTimer()
    bgWatch.timeout.connect(w.watchForUpdateTime)
    hours = 0.8
    bgWatch.start(int(hours * 60 * 60 * 1000))
    sys.exit(app.exec_())
