from utils import start_webdriver, create_output_directory, close_webdriver, launch_extraction, save_data
from log_handler import setup_logging, log_error
from datetime import datetime
import pandas as pd

def data_extraction():
    # Initialize logging
    log_filename = f'logs/extraction_glitches_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    setup_logging(log_filename)

    countries = {"USA": "us/en", "France": "fr/fr", "UK": "gb/en", "Japan": "jp/ja"}
    collections = ['RADIOMIR', 'LUMINOR', 'SUBMERSIBLE', 'LUMINOR-DUE']
    base_url = "https://www.panerai.com/{}/collections/watch-collection/{}.html"

    driver = None
    all_data = []
    try:
        driver = start_webdriver()
        bronze_dir = create_output_directory("bronze")
        
        for country, country_url in countries.items():
            all_products = []
            for collection in collections:
                product_info = launch_extraction(driver, country, country_url, collection, base_url)
                all_products.extend(product_info)
                all_data.extend(product_info)
            df_all_products = pd.DataFrame(all_products)
            save_data(df_all_products, f"{country}_watches_{datetime.now().year}", bronze_dir)
        
        # Save all data into one CSV file
        df_all_data = pd.DataFrame(all_data)
        save_data(df_all_data, f"all_watches_{datetime.now().year}", bronze_dir)
    except Exception as e:
        log_error(f"Unexpected error in main script: {str(e)}")
    finally:
        if driver:
            close_webdriver(driver)

