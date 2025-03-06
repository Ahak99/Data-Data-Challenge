import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
from datetime import datetime
from scraper.utils import start_webdriver, create_output_directory, close_webdriver, launch_extraction, save_data
from log_handler import setup_logging, log_error

class DataExtraction:
    COUNTRIES = {"USA": "us/en", "France": "fr/fr", "UK": "gb/en", "Japan": "jp/ja"}
    COLLECTIONS = ['RADIOMIR', 'LUMINOR', 'SUBMERSIBLE', 'LUMINOR-DUE']
    BASE_URL = "https://www.panerai.com/{}/collections/watch-collection/{}.html"
    
    def __init__(self):
        self.log_filename = f'logs/extraction_glitches_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
        setup_logging(self.log_filename)
        self.driver = None
        self.all_products_data = []

    def _extract_products_for_country(self, country: str, country_url: str) -> list:
        """
        Extract products for a given country by iterating through all collections.
        """
        country_products = []
        for collection in self.COLLECTIONS:
            products = launch_extraction(self.driver, country, country_url, collection, self.BASE_URL)
            country_products.extend(products)
        return country_products

    def run(self) -> None:
        """
        Main extraction process:
          - Initializes the WebDriver.
          - Iterates over each country to extract and save product data.
          - Aggregates all data into a single CSV file.
        """
        try:
            self.driver = start_webdriver()
            bronze_dir = create_output_directory("bronze")
            
            for country, country_url in self.COUNTRIES.items():
                country_products = self._extract_products_for_country(country, country_url)
                if country_products:
                    df_country = pd.DataFrame(country_products)
                    save_data(df_country, f"{country}_watches_{datetime.now().year}", bronze_dir)
                self.all_products_data.extend(country_products)
            
            if self.all_products_data:
                df_all = pd.DataFrame(self.all_products_data)
                save_data(df_all, f"all_watches_{datetime.now().year}", bronze_dir)
        except Exception as e:
            log_error(f"Unexpected error in DataExtraction.run: {str(e)}")
        finally:
            if self.driver:
                close_webdriver(self.driver)

if __name__ == "__main__":
    extractor = DataExtraction()
    extractor.run()
