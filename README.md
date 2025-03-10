# 🏆 Panerai Luxury Watch Price Analysis

## 📌 Project Overview

Luxury watch prices fluctuate due to market dynamics and currency variations. This project aims to track and analyze **Panerai's watch prices** over time (from 2021 to 2025) to uncover trends, pricing strategies, and investment opportunities.

## 🌍 Data Source

We retrieve official pricing data directly from **[Panerai.com](https://www.panerai.com/)**, ensuring accuracy and up-to-date insights.

## 🔄 Data Pipeline - The Medallion Architecture

To maintain data integrity and usability, we structure it using the **Medallion Architecture**:

💾 **Bronze Layer:** Raw extracted data from Panerai’s website.  
🔧 **Silver Layer:** Cleaned and transformed data, structured for analysis.  
📊 **Gold Layer:** Insightful, enriched data for business decisions.

## 🚀 Workflow Automation

🔗 **Apache Airflow** powers this pipeline, automating data extraction, and transformation.

## 📊 Analytical Approach

### 🔎 Exploratory Data Analysis (EDA):

- Understanding data structure: **columns, types, missing values, duplicates**.
- Summary statistics: **number of brands, collections, and unique product references**.

### 🌎 Country-Based Market Insights:

- **Region-Specific Data Splits:** Analyzing pricing trends in **France (EUR), USA (USD), UK (GBP), and Japan (JPY)**.
- **Descriptive Statistics & Visualizations:**
  - Unique collections & top references.
  - Number of watches per collection.

### 📈 Business Intelligence:

- **Finding the Cheapest & Most Expensive Models:**
  - Across the entire dataset & per collection.
  - Identifying which **country offers the lowest price**.
- **Best Selling Option:**
  - Where the watch sells at the **highest price**.
  - Computing **potential profit margins** between highest and lowest prices.
- **Total Profitability Analysis by Country.**

### 📆 Price Evolution & Trend Analysis:

- Yearly **price increase rates** are compared across models & collections.
- Identifying the most inflation-resistant luxury watches.

### 🚀 Extended Analytical Tasks:

- **Available Product in Stock (2025):**
  - Analyze all available products in stock across all countries as of 2025.
- **Best Sell Price with Profit Analysis (2021 & 2025):**
  - Evaluate the best sell price and corresponding profit margins for the years 2021 and 2025.
- **Price Evaluation Metrics:**
  - Calculate metrics based on increase rate and Compound Annual Growth Rate (CAGR) to assess price evolution.
- **Watches on Sale (2021 - 2025):**
  - Analyze the inventory of watches that have been on sale continuously from 2021 until 2025.
- **Market Discontinuation Analysis:**
  - Generate a list of products that were available in the market in 2021 but are no longer available in 2025.

## 🛠️ Tech Stack

This project is powered by **cutting-edge tools & frameworks**:

🐍 **Python** - The backbone of our data pipeline.  
🕵️‍♂️ **Selenium & Webdriver-Manager** - Dynamic web scraping automation.  
📊 **Pandas** - Data wrangling and manipulation.  
🌐 **Requests** - Fetching additional data when needed.  
🔐 **python-dotenv** - Secure environment variable management.  
🚀 **Airflow** - Automating and orchestrating the workflow.

## ⚙️ Local Setup and Execution

Follow these steps to run the project on your local machine:

### 1. Clone and Setup the Environment

- **Clone the Repository:**
  ```bash
  git clone https://github.com/Ahak99/Data-Data-Challenge.git
  cd Data-Data-Challenge
  ```
- **Create a Virtual Environment:**
  - **On Windows:**
  ```bash
  python -m venv venv
  venv\Scripts\activate
  ```
  - **On Windows:**
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

- **Install Dependencies:**
  ```bash
    pip install -r requirements.txt
  ```

### 2. Running the Jupyter Notebooks
The notebooks in the notebook/ folder provide interactive analysis and insights:

- **Launch Jupyter Notebook:**
  ```bash
    jupyter notebook
  ```

- **Open and Run Notebooks:**
Navigate to the notebook/ directory and open the following notebooks:

  - Data&Data - EDA & Business Analysis for Panerai-2021.ipynb
  - Data&Data - EDA & Business Analysis for Panerai-2025.ipynb
  - Data&Data - Comparison between Panerai-2021 & Panerai-2025.ipynb

### 3. Running the Data Pipeline with Airflowctl

- **Navigate to the Orchestrator Folder:**
  ```bash
    cd orchestrator
  ```

- **Start the Data Pipeline:**
  ```bash
    airflowctl start
  ```

### 4. Running Unit Tests

- **Execute Unit Tests:**
  ```bash
    python -m unittest discover -s src/scraper/tests
  ```

## 🔮 Future Enhancements

🔜 Expand dataset to analyze **multiple luxury watch brands**.  
🔜 Deploy **machine learning models** to forecast future price trends.  
🔜 Build an **interactive dashboard** for real-time price monitoring.

## 🤝 Collaboration with Data&Data

Founded in 2012, [Data&Data](http://data-and-data.com) is specialized in the luxury sector—from watches and leather goods to fragrances. We help key decision makers understand the ever-changing dynamics of the global online market, enabling them to make more informed and effective strategies.
