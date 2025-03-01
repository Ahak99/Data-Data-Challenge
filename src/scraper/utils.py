import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
import json
import glob
import os
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from log_handler import log_error  # Import the logging handler

def start_webdriver():
    """Initialize the Chrome WebDriver with the specified service and options."""
    try:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        log_error(f"Failed to start WebDriver: {str(e)}")
        raise

def close_webdriver(driver):
    """Quit the WebDriver."""
    try:
        if driver:
            driver.quit()
    except Exception as e:
        log_error(f"Failed to close WebDriver: {str(e)}")
        raise

def extract_product_info(card):
    """Extract the product's data from the 'data-tracking-product' attribute."""
    try:
        product_link_element = card.find_element(By.CLASS_NAME, "pan-prod-ref-link-v2")
        data_tracking = product_link_element.get_attribute("data-tracking-product")
        if data_tracking:
            data_tracking = json.loads(data_tracking.replace("&quot;", '"'))
            product_info = {
                'name': data_tracking.get('name', 'N/A'),
                'reference': data_tracking.get('reference', 'N/A'),
                'collection': data_tracking.get('collection', 'N/A'),
                'brand': data_tracking.get('brand', 'N/A'),
                'price': data_tracking.get('price', 'N/A'),
                'currency': data_tracking.get('currency', 'N/A'),
                'availability': "Available" if data_tracking.get('isAvailable', 'false') == 'true' else "Out of Stock",
                'product_url': product_link_element.get_attribute('href'),
                'image_url': extract_image_url(card)  # Added image URL extraction
            }
            return product_info
        else:
            return None
    except Exception as e:
        log_error(f"Error processing product: {str(e)}")
        return None

def extract_image_url(card):
    """Extract the main product image URL."""
    try:
        img_element = WebDriverWait(card, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".pan-prod-ref-front-image-v2 img"))
        )
        main_image = img_element.get_attribute("data-src") or img_element.get_attribute("src")
        if main_image and "transform" in main_image:
            main_image = main_image.split(".transform")[0]
        image_url = "https://www.panerai.com" + main_image if main_image else "N/A"
        return image_url
    except Exception as e:
        log_error(f"Error extracting image URL: {str(e)}")
        return "N/A"

def create_output_directory(stage):
    """Create and return a timestamped output directory."""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_dir = os.path.join(f"data/{stage}/", timestamp)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def save_data(df, filename, output_dir):
    """Save extracted data into a CSV file in the given directory."""
    if df.empty:
        print(f"No data to save for {filename}.")
        return None
    
    file_path = os.path.join(output_dir, f"{filename}.csv")
    df.to_csv(file_path, index=False)
    print(f"Saved data for {filename} to {file_path}")
    return df

def launch_extraction(driver, country, country_url, collection, base_url):
    try:
        collection_lower = collection.lower()
        url = base_url.format(country_url, collection_lower)
        driver.get(url)
        print(f"Scraping: {url}")

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "pan-prod-ref-card-v2"))
        )
        product_cards = driver.find_elements(By.CLASS_NAME, "pan-prod-ref-card-v2")
        print(f"Found {len(product_cards)} products for collection: {collection}")

        product_infos = []
        for card in product_cards[:len(product_cards)//2]:
            product_info = extract(country, card)
            if product_info:
                product_infos.append(product_info)
        return product_infos
    except Exception as e:
        log_error(f"Error processing {collection} in {country}: {str(e)}")
        return []
    
def extract(country, card):
    try:
        product_info = extract_product_info(card)
        image_url = extract_image_url(card)
        # product_info["image"] = image_url
        product_info["country"] = country
        product_info["year"] = datetime.now().year
        return product_info
    except Exception as e:
        log_error(f"Error in data_extraction for {country}: {str(e)}")
        return None
    
def get_exchange_rate(from_currency: str, to_currency: str) -> float:
    """
    Fetches real-time exchange rates using ExchangeRate-API.
    
    Args:
        from_currency (str): Base currency (e.g., 'USD').
        to_currency (str): Target currency (e.g., 'EUR').

    Returns:
        float: Exchange rate from 'from_currency' to 'to_currency', or None if an error occurs.
    """
    if from_currency.upper() == to_currency.upper():
        return 1.0
        
    load_dotenv()

    EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY")
    if not EXCHANGE_API_KEY:
        log_error("Missing API key")
        return None

    url = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_API_KEY}/latest/{from_currency.upper()}"
    print(url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "conversion_rates" not in data:
            raise ValueError("Invalid API response: missing 'conversion_rates'")

        rate = data["conversion_rates"].get(to_currency.upper())
        if rate is None:
            raise ValueError(f"Exchange rate for {to_currency.upper()} not found in API response")

        return rate

    except requests.exceptions.RequestException as e:
        log_error(f"Network error: {e}")
    except ValueError as e:
        log_error(f"Data error: {e}")
    except Exception as e:
        log_error(f"Unexpected error: {e}")

    return None  # Return None in case of an error

def clean_data(dataframe):
    """
    Cleans the dataset with robust error handling and validation.
    
    Args:
        dataframe (pd.DataFrame): Input DataFrame containing watch product data.
        
    Returns:
        pd.DataFrame: Cleaned DataFrame. Returns empty DataFrame on critical errors.
    """
    try:
        # Validate input type
        if not isinstance(dataframe, pd.DataFrame):
            log_error("clean_data: Input is not a pandas DataFrame")
            return pd.DataFrame()

        if dataframe.empty:
            return dataframe.copy()

        df = dataframe.copy()
        
        # Handle missing values
        try:
            initial_count = len(df)
            df.dropna(subset=['reference', 'price'], inplace=True)
            if (diff := initial_count - len(df)) > 0:
                log_error(f"clean_data: Removed {diff} rows with missing values")
        except Exception as e:
            log_error(f"clean_data: Error handling missing values - {str(e)}")
        
        # Deduplication
        try:
            dup_count = df.duplicated(subset=['reference', 'country']).sum()
            if dup_count > 0:
                log_error(f"clean_data: Removing {dup_count} duplicates")
                df.drop_duplicates(subset=['reference', 'country'], keep='first', inplace=True)
        except Exception as e:
            log_error(f"clean_data: Deduplication failed - {str(e)}")
        
        # Price cleaning with validation
        try:
            # Remove currency symbols and convert to float values
            if df["price"].dtype == 'object':
                df['price'] = df['price'].str.replace(r'[￥$£€, \u00A0]', '', regex=True).astype(int)
            
            # Validate numeric conversion
            if (invalid_prices := df['price'].isna().sum()) > 0:
                log_error(f"clean_data: {invalid_prices} invalid price values")
                df = df.dropna(subset=['price'])
        except Exception as e:
            log_error(f"clean_data: Price cleaning failed - {str(e)}")
            return df if 'price' in df.columns else pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"clean_data: Critical error - {str(e)}")
        return pd.DataFrame()


def transform_data(dataframe, CURRENCIES_CODE):
    """
    Transforms data with currency conversions and error handling.
    
    Args:
        dataframe (pd.DataFrame): Cleaned DataFrame
        CURRENCIES_CODE (dict): Currency mapping dictionary
        
    Returns:
        pd.DataFrame: Transformed DataFrame. Returns input on error.
    """
    try:
        # Validate inputs
        if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
            log_error("transform_data: Invalid input DataFrame")
            return dataframe
        
        if not isinstance(CURRENCIES_CODE, dict) or not CURRENCIES_CODE:
            log_error("transform_data: Invalid currency code mapping")
            return dataframe

        df = dataframe.copy()

        # Currency code mapping
        try:
            df["currency_code"] = df["country"].map(CURRENCIES_CODE)
            if (missing_codes := df["currency_code"].isna().sum()) > 0:
                log_error(f"transform_data: {missing_codes} missing currency mappings")
        except Exception as e:
            log_error(f"transform_data: Currency mapping failed - {str(e)}")
            return df

        # # Currency conversion
        try:
            valid_currencies = set(CURRENCIES_CODE.values())
            # Extract unique source currencies that are valid
            unique_source_currencies = df.loc[df['currency_code'].isin(valid_currencies), 'currency_code'].unique()

            # Precompute exchange rates for each (source, target) pair
            exchange_rates = {}
            for source_currency in unique_source_currencies:
                for target_currency in valid_currencies:
                    rate = get_exchange_rate(source_currency, target_currency)
                    if rate is None:
                        log_error(f"transform_data: Failed rate fetch for {source_currency}->{target_currency}")
                    exchange_rates[(source_currency, target_currency)] = rate
            
            # For each target currency, build a mapping from source currency to exchange rate
            for target_currency in valid_currencies:
                col_name = f"price_{target_currency}"
                rate_mapping = {
                    source: exchange_rates[(source, target_currency)]
                    for source in unique_source_currencies
                    if exchange_rates[(source, target_currency)] is not None
                }
                # Use vectorized mapping to apply the conversion
                df[col_name] = df['price'] * df['currency_code'].map(rate_mapping)
        except Exception as e:
            log_error(f"transform_data: Currency conversion failed - {str(e)}")
            return df

        
        # Validate required columns
        required_columns = {'brand', 'product_url', 'image_url', 'collection',
                           'reference', 'price', 'currency', 'country', 'year'}
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            log_error(f"clean_data: Missing required columns {missing_cols}")
            return pd.DataFrame()
        
        try:
            # Column reordering with fallback for missing columns
            column_list_ref = list(required_columns)
            remaining_columns = [col for col in df.columns if col not in required_columns]
            df = df[column_list_ref + remaining_columns]
        except KeyError as e:
            log_error(f"clean_data: Column reordering failed - {str(e)}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"transform_data: Critical error - {str(e)}")
        return dataframe

def launch_data_preprocess(dataframe, CURRENCIES_CODE):
    """
    Cleans and transforms the dataset.
    
    Args:
        dataframe (pd.DataFrame): Input DataFrame containing watch product data.
        CURRENCIES_CODE (dict): Dictionary mapping currency symbols to standard codes.
        
    Returns:
        pd.DataFrame: Processed DataFrame with standardized and converted prices.
    """
    # First clean the data
    cleaned_df = clean_data(dataframe)
    
    # Then transform the data
    transformed_df = transform_data(cleaned_df, CURRENCIES_CODE)
    
    return cleaned_df

def get_latest_folder(path):
    # Get a list of all directories in the given path
    folders = [f for f in glob.glob(os.path.join(path, f"{datetime.now().year}-*")) if os.path.isdir(f)]
    
    # Sort folders based on modification time (newest first) and return the first one
    if folders:
        latest_folder = max(folders, key=os.path.getmtime)
        return latest_folder
    else:
        return None
