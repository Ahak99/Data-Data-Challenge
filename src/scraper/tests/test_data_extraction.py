import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

from scraper.data_extraction.data_extraction import DataExtraction

class TestDataExtraction(unittest.TestCase):
    @patch("scraper.data_extraction.data_extraction.close_webdriver")
    @patch("scraper.data_extraction.data_extraction.save_data")
    @patch("scraper.data_extraction.data_extraction.launch_extraction")
    @patch("scraper.data_extraction.data_extraction.create_output_directory")
    @patch("scraper.data_extraction.data_extraction.start_webdriver")
    def test_run_extraction(self, mock_start_webdriver, mock_create_output_directory, mock_launch_extraction, mock_save_data, mock_close_webdriver):
        # Set up a dummy driver and output directory.
        dummy_driver = MagicMock()
        mock_start_webdriver.return_value = dummy_driver
        mock_create_output_directory.return_value = "dummy_bronze_dir"
        
        # Simulate that for each call, launch_extraction returns a single fake product.
        fake_product = {"name": "Fake Watch", "country": "USA", "year": datetime.now().year}
        mock_launch_extraction.return_value = [fake_product]

        extractor = DataExtraction()
        extractor.run()

        # Verify that the WebDriver is started and later closed.
        mock_start_webdriver.assert_called_once()
        mock_close_webdriver.assert_called_once_with(dummy_driver)

        # Verify that an output directory was created with the "bronze" stage.
        mock_create_output_directory.assert_called_with("bronze")

        # There are 4 countries and 4 collections, so launch_extraction should be called 16 times.
        expected_calls = len(DataExtraction.COUNTRIES) * len(DataExtraction.COLLECTIONS)
        self.assertEqual(mock_launch_extraction.call_count, expected_calls)

        # Expect save_data to be called for each country (when data is found) plus one final aggregated save.
        self.assertEqual(mock_save_data.call_count, len(DataExtraction.COUNTRIES) + 1)
    
    @patch("scraper.data_extraction.data_extraction.launch_extraction")
    def test_extract_products_for_country(self, mock_launch_extraction):
        # Simulate different product lists for each collection.
        fake_product1 = {"name": "Watch1", "country": "TestCountry", "year": 2025}
        fake_product2 = {"name": "Watch2", "country": "TestCountry", "year": 2025}
        mock_launch_extraction.side_effect = [
            [fake_product1],
            [fake_product2],
            [],
            [fake_product1, fake_product2]
        ]
        extractor = DataExtraction()
        extractor.driver = MagicMock()  # Provide a dummy driver
        products = extractor._extract_products_for_country("TestCountry", "test_url")
        # Expected: 1 + 1 + 0 + 2 = 4 products
        self.assertEqual(len(products), 4)

    @patch("scraper.data_extraction.data_extraction.log_error")
    @patch("scraper.data_extraction.data_extraction.close_webdriver")
    @patch("scraper.data_extraction.data_extraction.launch_extraction")
    @patch("scraper.data_extraction.data_extraction.create_output_directory")
    @patch("scraper.data_extraction.data_extraction.start_webdriver")
    def test_run_extraction_with_exception(self, mock_start_webdriver, mock_create_output_directory, mock_launch_extraction, mock_close_webdriver, mock_log_error):
        # Set up a dummy driver and output directory.
        dummy_driver = MagicMock()
        mock_start_webdriver.return_value = dummy_driver
        mock_create_output_directory.return_value = "dummy_bronze_dir"
        
        # Simulate an exception in launch_extraction on the first call.
        mock_launch_extraction.side_effect = Exception("Test Exception")

        extractor = DataExtraction()
        extractor.run()

        # Verify that log_error was called to log the exception.
        mock_log_error.assert_called()
        # Verify that the WebDriver is closed even after an exception.
        mock_close_webdriver.assert_called_once_with(dummy_driver)

if __name__ == "__main__":
    unittest.main()
