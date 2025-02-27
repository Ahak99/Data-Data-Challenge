import os
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from data_extraction.data_extraction import data_extraction
from data_transformation.data_transformation import data_transformation



if __name__ == "__main__":
    data_extraction()
    data_transformation()
