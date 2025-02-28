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

🔗 **Apache Airflow** powers this pipeline, automating data extraction, transformation, and analysis.

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

## 🛠️ Tech Stack

This project is powered by **cutting-edge tools & frameworks**:

🐍 **Python** - The backbone of our data pipeline.  
🕵️‍♂️ **Selenium & Webdriver-Manager** - Dynamic web scraping automation.  
📊 **Pandas** - Data wrangling and manipulation.  
🌐 **Requests** - Fetching additional data when needed.  
🔐 **python-dotenv** - Secure environment variable management.  
🚀 **Airflow** - Automating and orchestrating the workflow.  

## 🔮 Future Enhancements

🔜 Expand dataset to analyze **multiple luxury watch brands**.  
🔜 Deploy **machine learning models** to forecast future price trends.  
🔜 Build an **interactive dashboard** for real-time price monitoring.  

## 🤝 Collaboration with Data&Data

Founded in 2012, [Data&Data](http://data-and-data.com) is specialised in the luxury sector from watches and leather goods to fragrances. We help key decision makers to understand the ever-changing dynamics of the global online market. This enables them to make more informed and effective strategies.
