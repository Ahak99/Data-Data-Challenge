import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import os
import time
from datetime import datetime
import sys
import logging
import json
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Disable logging of error messages during tests
logging.disable(logging.ERROR)

import pandas as pd

# Import functions from your modules (adjust the import paths as needed)
from scraper.utils import (
    create_output_directory,
    save_data,
    get_latest_folder,
    get_exchange_rate,
    clean_data,
    transform_data,
    launch_data_preprocess,
    extract_product_info,
    extract_image_url,
    start_webdriver,
    close_webdriver,
    launch_extraction,
    extract,
)
from src.log_handler import setup_logging, log_error


class TestUtils(unittest.TestCase):
    def test_create_output_directory(self):
        stage = "test"
        dir_path = create_output_directory(stage)
        self.assertTrue(os.path.exists(dir_path))
        self.assertIn(stage, dir_path)
        # Clean up after test
        shutil.rmtree(dir_path)

    def test_save_data(self):
        df = pd.DataFrame({"col": [1, 2, 3]})
        with tempfile.TemporaryDirectory() as tmpdirname:
            result = save_data(df, "test_file", tmpdirname)
            file_path = os.path.join(tmpdirname, "test_file.csv")
            self.assertTrue(os.path.exists(file_path))
            loaded_df = pd.read_csv(file_path)
            pd.testing.assert_frame_equal(df, loaded_df)

    def test_get_latest_folder(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Create two dummy folders with different modification times
            folder1 = os.path.join(tmpdirname, f"{datetime.now().year}-folder1")
            folder2 = os.path.join(tmpdirname, f"{datetime.now().year}-folder2")
            os.makedirs(folder1)
            os.makedirs(folder2)
            # Set folder1's modification time to an older timestamp
            old_time = datetime(2020, 1, 1).timestamp()
            os.utime(folder1, (old_time, old_time))
            latest = get_latest_folder(tmpdirname)
            self.assertEqual(latest, folder2)

    @patch("scraper.utils.requests.get")
    @patch("scraper.utils.load_dotenv")
    def test_get_exchange_rate(self, mock_load_dotenv, mock_get):
        # Patch os.getenv to return a fake API key
        with patch("scraper.utils.os.getenv", return_value="FAKE_API_KEY"):
            fake_response = MagicMock()
            fake_response.json.return_value = {"conversion_rates": {"USD": 1.0, "EUR": 0.8}}
            fake_response.raise_for_status = lambda: None
            mock_get.return_value = fake_response
            rate = get_exchange_rate("USD", "EUR")
            self.assertEqual(rate, 0.8)

    def test_clean_data_valid(self):
        # Create a DataFrame with all required columns and proper values
        data = {
            "brand": ["PANERAI", "PANERAI"],
            "product_url": ["http://example.com", "http://example.com"],
            "image_url": ["http://img.com/1", "http://img.com/2"],
            "collection": ["Luminor Due", "Radiomir"],
            "reference": ["PAM01329", "PAM01570"],
            "price": ["$39,200", "$6,100"],
            "currency": ["$", "€"],
            "country": ["USA", "France"],
            "year": [2025, 2025],
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_data(df)
        self.assertFalse(cleaned_df.empty)
        # Check that the price column has been converted to a numeric type
        self.assertTrue(pd.api.types.is_numeric_dtype(cleaned_df["price"]))

    @patch("scraper.utils.get_exchange_rate", return_value=1.0)
    def test_transform_data(self, mock_get_exchange_rate):
        data = {
            "brand": ["PANERAI"],
            "product_url": ["http://example.com"],
            "image_url": ["http://img.com/1"],
            "collection": ["Luminor Due"],
            "reference": ["PAM01329"],
            "price": [39200],
            "currency": ["$"],
            "country": ["USA"],
            "year": [2025],
        }
        df = pd.DataFrame(data)
        currencies_code = {"USA": "USD", "France": "EUR", "UK": "GBP", "Japan": "JPY"}
        transformed_df = transform_data(df, currencies_code)
        # Check that a new column for currency code is added
        self.assertIn("currency_code", transformed_df.columns)
        self.assertEqual(transformed_df["currency_code"].iloc[0], "USD")
        # Since get_exchange_rate is mocked to return 1.0, the conversion price should match the original
        self.assertEqual(transformed_df["price_USD"].iloc[0], 39200)

    @patch("scraper.utils.get_exchange_rate", return_value=1.0)
    def test_launch_data_preprocess(self, mock_get_exchange_rate):
        data = {
            "brand": ["PANERAI"],
            "product_url": ["http://example.com"],
            "image_url": ["http://img.com/1"],
            "collection": ["Luminor Due"],
            "reference": ["PAM01329"],
            "price": ["$39,200"],
            "currency": ["$"],
            "country": ["USA"],
            "year": [2025],
        }
        df = pd.DataFrame(data)
        currencies_code = {"USA": "USD", "France": "EUR", "UK": "GBP", "Japan": "JPY"}
        processed_df = launch_data_preprocess(df, currencies_code)
        self.assertFalse(processed_df.empty)
        self.assertIn("currency_code", processed_df.columns)

    def test_extract_product_info(self):
        # Create a dummy card (a stand-in for a Selenium WebElement)
        card = MagicMock()
        product_link_element = MagicMock()
        # Simulate the value returned from the "data-tracking-product" attribute
        tracking_data = {
            "name": "Luminor Due Platinumtech™",
            "reference": "PAM01329",
            "collection": "Luminor Due",
            "brand": "PANERAI",
            "price": "$39,200",
            "currency": "$",
            "isAvailable": "true",
        }
        # Replace quotes to simulate the &quot; replacement in the HTML attribute
        fake_attr = json.dumps(tracking_data).replace('"', '&quot;')
        product_link_element.get_attribute.return_value = fake_attr
        card.find_element.return_value = product_link_element
        # Patch extract_image_url so that it returns a dummy image URL without needing to interact with Selenium
        with patch("scraper.utils.extract_image_url", return_value="http://img.com/test.png"):
            product_info = extract_product_info(card)
            self.assertIsNotNone(product_info)
            self.assertEqual(product_info["name"], "Luminor Due Platinumtech™")
            self.assertEqual(product_info["availability"], "Available")

    def test_extract_image_url(self):
        # Create a dummy card element
        card = MagicMock()
        img_element = MagicMock()
        # Simulate that get_attribute returns a URL containing ".transform"
        img_element.get_attribute.side_effect = lambda attr: "test.transform" if attr == "data-src" else None
        # Patch WebDriverWait.until to immediately return our dummy image element
        with patch("scraper.utils.WebDriverWait") as mock_wait:
            instance = mock_wait.return_value
            instance.until.return_value = img_element
            image_url = extract_image_url(card)
            self.assertIn("https://www.panerai.com", image_url)

    def test_close_webdriver(self):
        dummy_driver = MagicMock()
        close_webdriver(dummy_driver)
        dummy_driver.quit.assert_called_once()

    @patch("scraper.utils.ChromeDriverManager.install", return_value="/fake/path")
    @patch("scraper.utils.webdriver.Chrome")
    def test_start_webdriver(self, mock_chrome, mock_install):
        dummy_driver = MagicMock()
        mock_chrome.return_value = dummy_driver
        driver = start_webdriver()
        self.assertEqual(driver, dummy_driver)

    def test_launch_extraction(self):
        # Create a dummy driver
        driver = MagicMock()
        driver.get.return_value = None
        # Patch WebDriverWait to simulate waiting for the element to appear
        with patch("scraper.utils.WebDriverWait") as mock_wait:
            instance = mock_wait.return_value
            instance.until.return_value = True
            # Create a dummy card element that will be returned by find_elements
            dummy_card = MagicMock()
            # Patch the extract() function (called inside launch_extraction) to return dummy data
            with patch("scraper.utils.extract", return_value={"name": "dummy", "country": "USA", "year": 2025}):
                # Simulate two product cards in the list (the function only processes half)
                driver.find_elements.return_value = [dummy_card, dummy_card]
                products = launch_extraction(
                    driver,
                    "USA",
                    "us/en",
                    "Luminor Due",
                    "http://example.com/{}/collections/watch-collection/{}.html",
                )
                # Since slicing is done as [:len(cards)//2], only one product is processed
                self.assertEqual(len(products), 1)

    def test_extract_wrapper(self):
        card = MagicMock()
        with patch("scraper.utils.extract_product_info", return_value={"name": "dummy"}):
            with patch("scraper.utils.extract_image_url", return_value="http://img.com"):
                product = extract("USA", card)
                self.assertEqual(product["country"], "USA")
                self.assertEqual(product["year"], datetime.now().year)


class TestLogHandler(unittest.TestCase):
    def test_log_error(self):
        # Ensure that log_error does not raise an exception when logging an error message.
        try:
            log_error("Test error message")
        except Exception:
            self.fail("log_error raised an Exception unexpectedly!")


if __name__ == "__main__":
    unittest.main()
