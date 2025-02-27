import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import lunch_data_preprocess
from log_handler import setup_logging, log_error
import pandas as pd
from utils import create_output_directory, save_data, get_latest_folder


def data_transformation(): 
    try:
        # Initialize logging
        log_filename = f'logs/transformation_glitches_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
        setup_logging(log_filename)
        prefix = "data/bronze/"
        file_path = get_latest_folder(prefix) + "/all_watches_2025.csv"
        dataframe = pd.read_csv(file_path)
        CURRENCIES_CODE = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}

        transformed_df = lunch_data_preprocess(dataframe, CURRENCIES_CODE)
        
        silver_dir = create_output_directory(stage="silver")
        save_data(transformed_df, f"PANERAI_DATA_{datetime.now().year}", silver_dir)
        gold_dir = create_output_directory(stage="gold")
        save_data(transformed_df, f"PANERAI_DATA_{datetime.now().year}", gold_dir)
        print(transformed_df.head())
    
    except FileNotFoundError:
        log_error("The file 'all_watches_2025.csv' was not found.")
    except pd.errors.EmptyDataError:
        log_error("The file 'all_watches_2025.csv' is empty.")
    except pd.errors.ParserError:
        log_error("Error parsing the file 'all_watches_2025.csv'.")
    except Exception as e:
        log_error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    data_transformation()

