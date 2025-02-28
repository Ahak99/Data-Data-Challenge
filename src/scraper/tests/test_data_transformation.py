import os
import time
from datetime import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from scraper.data_transformation.data_transformation import DataTransformation

class TestDataTransformation(unittest.TestCase):

    @patch("scraper.data_transformation.data_transformation.get_latest_folder")
    @patch("scraper.data_transformation.data_transformation.save_data")
    @patch("scraper.data_transformation.data_transformation.launch_data_preprocess")
    @patch("scraper.data_transformation.data_transformation.create_output_directory")
    def test_run_transformation(self, mock_create_output_directory, mock_launch_preprocess, mock_save_data, mock_get_latest_folder):
        # Create a temporary directory to mimic a bronze folder and CSV file.
        with tempfile.TemporaryDirectory() as tmpdirname:
            bronze_folder = os.path.join(tmpdirname, "bronze_folder")
            os.makedirs(bronze_folder)
            csv_path = os.path.join(bronze_folder, "all_watches_2025.csv")
            sample_data = {
                "brand": ["PANERAI"],
                "product_url": ["http://example.com"],
                "image_url": ["http://example.com/image.png"],
                "collection": ["Luminor Due"],
                "reference": ["PAM01329"],
                "price": ["$39,200"],
                "currency": ["$"],
                "country": ["USA"],
                "year": [2025]
            }
            df_sample = pd.DataFrame(sample_data)
            df_sample.to_csv(csv_path, index=False)

            # Mock the functions
            mock_get_latest_folder.return_value = bronze_folder
            processed_df = df_sample.copy()
            processed_df["currency_code"] = "USD"
            processed_df['price'] = processed_df['price'].astype(str).str.replace(r'[^\d.,]', '', regex=True)
            processed_df['price'] = (processed_df['price']
                .str.replace(',', '', regex=False)
                .str.replace(r'[^0-9.]', '', regex=True)
                .apply(pd.to_numeric, errors='coerce')
            )
            mock_launch_preprocess.return_value = processed_df
            
            # Simulate the create_output_directory function to return the same directory path for all destinations
            mock_create_output_directory.return_value = tmpdirname
            
            # Set the input file and destinations
            input_file = csv_path
            output_file = "output.csv"
            destinations = ["silver", "gold"]

            # Instantiate the DataTransformation class with the mocked input file and destinations
            transformer = DataTransformation(input_file=input_file, ouput_file=output_file, destinations=destinations)

            # Run the transformation
            transformer.run()

            # Verify that launch_data_preprocess was called.
            mock_launch_preprocess.assert_called()

            # Verify that save_data was called for each destination ('silver', 'gold')
            self.assertEqual(mock_save_data.call_count, len(destinations), "save_data should be called for each destination")

if __name__ == "__main__":
    unittest.main()
