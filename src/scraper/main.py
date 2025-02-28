from data_extraction.data_extraction import DataExtraction
from data_transformation.data_transformation import DataTransformation


if __name__ == "__main__":
    
    # print("Start Data Scraping ...\n")
    # extractor = DataExtraction()
    # extractor.run()
    # print("\nData Scraping Completed! ...\n")
    
    print("\nStart Data Transformation ...\n")
    destinations = ["silver", "gold"]
    transformer = DataTransformation(destinations=destinations)
    transformer.run()
    print("Data Transformation Completed! ...\n")
