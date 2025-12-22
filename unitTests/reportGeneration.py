# ----TESTING for report generation excels ------
import traceback
from classes.returnsApp import returnsApp
from scripts.reportWorkbooks import portfolioSnapshot
from PyQt5.QtWidgets import QApplication
import sys

def pSnap():
    try:
        workbook = portfolioSnapshot([],testApp)
    except:
        print(traceback.format_exc())
        return False
    return True