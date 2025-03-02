import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import os
from datetime import datetime
import sys
import pandas as pd
from scraper.utils import launch_data_preprocess, create_output_directory, save_data, get_latest_folder
from log_handler import setup_logging, log_error


class DataTransformation:
    def __init__(self, input_file: str = None, output_file: str = None, destinations: list = None):
        """
        If no input_file is provided, the default behavior is to look in the latest folder under 'data/bronze/'.
        If no destinations are provided, defaults to ['silver', 'gold'].
        """
        self.log_filename = f'logs/transformation_glitches_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
        setup_logging(self.log_filename)
        self.current_year = datetime.now().year
        self.input_file = input_file
        self.output_file = output_file
        self.destinations = destinations if destinations else ["silver"]

    def get_input_file_path(self, prefix: str) -> str:
        """
        Retrieves the path of the CSV file from the latest folder under the given prefix.
        This is only used if an input file is not provided.
        """
        latest_folder = get_latest_folder(prefix)
        if not latest_folder:
            raise FileNotFoundError(f"No folder found under prefix: {prefix}")
        return os.path.join(latest_folder, f"all_watches_{self.current_year}.csv")

    def run(self) -> None:
        """
        Main transformation process:
          - Reads the provided CSV file (or retrieves it from the default bronze folder).
          - Applies cleaning and currency conversion.
          - Saves the transformed data into each of the selected output directories.
        """
        try:
            if self.input_file:
                file_path = self.input_file
            else:
                prefix = "data/bronze/"
                file_path = self.get_input_file_path(prefix)
            print(file_path)
            dataframe = pd.read_csv(file_path)
            CURRENCIES_CODE = {"USA": "USD", "France": "EUR", "UK": "GBP", "Japan": "JPY"}
            
            transformed_df = launch_data_preprocess(dataframe, CURRENCIES_CODE)
            
            if self.output_file:
                output_file_name = self.output_file
            else:
                output_file_name = f"PANERAI_DATA_{self.current_year}"
            for dest in self.destinations:
                dest_dir = create_output_directory(dest)
                save_data(transformed_df, output_file_name, dest_dir)
            print(output_file_name)
            print(dest_dir)
        except FileNotFoundError as fnf_error:
            log_error(f"File not found: {str(fnf_error)}")
        except pd.errors.EmptyDataError:
            log_error("The input CSV file is empty.")
        except pd.errors.ParserError:
            log_error("Error parsing the input CSV file.")
        except Exception as e:
            log_error(f"An error occurred during data transformation: {str(e)}")

