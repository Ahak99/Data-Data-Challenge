import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def describe_country(data, country=None):
    """
    Describe the country data.

    Parameters:
    data (pd.DataFrame): The data to describe.
    country (str, optional): The country to filter by. Defaults to None.

    Returns:
    tuple: A tuple containing the number of rows, unique collections, top collection, unique references, top reference, and a list of unique collections.
    """
    # Filter the original data if a specific country is provided
    if country:
        data = data[data["country"] == country]
    
    # Create a summary of the object columns if needed
    summary = data.describe(include="object")
    
    # Extract required details directly from the original data
    number_rows = data.shape[0]
    number_unique_collections = data['collection'].nunique()
    top_collection = data['collection'].mode()[0]
    
    number_unique_references = data['reference'].nunique()
    top_reference = data['reference'].mode()[0]
    
    collection_list = data['collection'].unique()
    
    return number_rows, number_unique_collections, top_collection, number_unique_references, top_reference, collection_list

def collection_visualization(data, country):
    """
    Visualize the collection data.

    Parameters:
    data (pd.DataFrame): The data to visualize.
    country (str): The country to filter by.

    Returns:
    None
    """
    # Filter the original data if a specific country is provided
    data = data[data["country"] == country]
    
    # Choose a color palette
    palette = sns.color_palette("pastel")  # You can choose different palettes like "deep", "pastel", etc.

    # Create the count plot
    ax = sns.countplot(x="collection", data=data, palette=palette)

    # Add the count value on top of each bar
    for p in ax.patches:
        ax.annotate(f'{p.get_height()}', 
                    (p.get_x() + p.get_width() / 2., p.get_y() + p.get_height() / 2.), 
                    ha='center', va='center', 
                    fontsize=10, color='black', 
                    xytext=(0, 5), textcoords='offset points')

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)

    # Labels and title
    plt.xlabel("Collection")
    plt.ylabel("Watch Count")
    plt.title(f"Watch Count per Collection in {country}")

    # Show the plot
    plt.show()
    
def analyze_country(data, country=None):
    """
    Analyze the country data.

    Parameters:
    data (pd.DataFrame): The data to analyze.
    country (str, optional): The country to filter by. Defaults to None.

    Returns:
    tuple: A tuple containing the overall statistics and the collection statistics.
    """
    # Filter the original data if a specific country is provided
    if country:
        data = data[data["country"] == country]
    
    # Identify the cheapest and most expensive product in the whole subset
    cheapest_product_data = data.nsmallest(1, 'price')
    most_expensive_product_data = data.nlargest(1, 'price')
    
    overall_stats = {
        "cheapest": {
            "collection": cheapest_product_data['collection'].values[0],
            "reference": cheapest_product_data['reference'].values[0],
            "price": cheapest_product_data['price'].values[0]
        },
        "most_expensive": {
            "collection": most_expensive_product_data['collection'].values[0],
            "reference": most_expensive_product_data['reference'].values[0],
            "price": most_expensive_product_data['price'].values[0]
        }
    }
    
    # Identify the cheapest and most expensive product per collection
    collection_stats = []
    unique_collections = data["collection"].unique()
    
    for collection in unique_collections:
        collection_data = data[data["collection"] == collection]
        
        cheapest_collection_data = collection_data.nsmallest(1, 'price')
        most_expensive_collection_data = collection_data.nlargest(1, 'price')
        
        collection_stats.append({
            "collection": collection,
            "cheapest": {
                "reference": cheapest_collection_data['reference'].values[0],
                "price": cheapest_collection_data['price'].values[0]
            },
            "most_expensive": {
                "reference": most_expensive_collection_data['reference'].values[0],
                "price": most_expensive_collection_data['price'].values[0]
            }
        })
    
    return overall_stats, collection_stats

def calculate_price_difference(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each product (grouped by collection and reference), calculates the minimum price across all countries,
    computes the difference of each record's price from this minimum, and identifies the country that offers
    the minimum price.
    
    Parameters:
        data (pd.DataFrame): Input DataFrame with columns:
            - 'name': Product name.
            - 'country': Country of the product.
            - 'product_url': URL for the product.
            - 'collection'
            - 'reference'
            - 'price_USD'
    
    Returns:
        pd.DataFrame: The input DataFrame with additional columns:
                      - 'min_price': The minimum price for each product.
                      - 'price_diff': The difference between the price and the minimum price.
                      - 'min_price_country': The country corresponding to the minimum price.
                      The resulting DataFrame is re-ordered to include 'name', 'country', 'min_price_country', and 'product_url'.
    """
    df = data.copy()
    group_cols = ['collection', 'reference']
    
    # Calculate the minimum price for each product group
    df['min_price'] = df.groupby(group_cols)['price_USD'].transform('min')
    
    # Calculate the price difference from the minimum price
    df['price_diff'] = df['price_USD'] - df['min_price']
    
    # Determine the country corresponding to the minimum price for each group.
    # This finds the index of the minimum price for each group and extracts the country.
    min_price_country_df = df.loc[df.groupby(group_cols)['price_USD'].idxmin(), group_cols + ['country']]
    min_price_country_df = min_price_country_df.rename(columns={'country': 'min_price_country'})
    
    # Merge the min_price_country back into the main DataFrame using the group columns as key.
    df = df.merge(min_price_country_df, on=group_cols, how='left')
    
    # Reorder columns for clarity in the CSV output.
    desired_columns = ['name', 'country', 'min_price_country', 'product_url', 'collection', 'reference', 'price_USD', 'min_price', 'price_diff']
    available_columns = [col for col in desired_columns if col in df.columns]
    df = df[available_columns]
    
    # Save the result to a CSV file.
    df.to_csv('price_difference.csv', index=False)
    
    return df

def find_cheapest_prices(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each product (grouped by collection and reference), identifies the cheapest price and its corresponding country.
    
    Parameters:
        data (pd.DataFrame): Input DataFrame with columns 'collection', 'reference', 'price_USD', and 'country'.
    
    Returns:
        pd.DataFrame: A DataFrame containing:
                      - 'collection'
                      - 'reference'
                      - 'cheapest_price': The lowest price found for the product.
                      - 'cheapest_country': The country corresponding to the cheapest price.
    """
    group_cols = ['collection', 'reference']
    cheapest = data.loc[data.groupby(group_cols)['price_USD'].idxmin()].copy()
    cheapest = cheapest[['collection', 'reference', 'price_USD', 'country']]
    cheapest.rename(columns={'price_USD': 'cheapest_price', 'country': 'cheapest_country'}, inplace=True)
    cheapest.to_csv('cheapest_prices.csv', index=False)
    
    return cheapest

def find_best_sell_prices(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each product (grouped by collection and reference), identifies the best selling option,
    defined as the country where the product has the highest price. Also calculates potential profit
    by finding the difference between the best selling price and the minimum price.
    
    Parameters:
        data (pd.DataFrame): Input DataFrame with columns:
            - 'collection'
            - 'reference'
            - 'price_USD'
            - 'country'

    Returns:
        pd.DataFrame: A DataFrame containing:
            - 'collection'
            - 'reference'
            - 'best_sell_price': The highest price found for the product.
            - 'best_sell_country': The country corresponding to the highest price.
            - 'min_price': The minimum price for the product.
            - 'min_price_country': The country where the minimum price is found.
            - 'profit': The potential profit from buying at the cheapest price and selling at the highest price.
    """
    group_cols = ['collection', 'reference']

    # Find the best selling price (highest price) and the country
    best_sell = data.loc[data.groupby(group_cols)['price_USD'].idxmax()].copy()
    best_sell = best_sell[['collection', 'reference', 'price_USD', 'country']]
    best_sell.rename(columns={'price_USD': 'best_sell_price', 'country': 'best_sell_country'}, inplace=True)

    # Find the minimum price and its country
    min_price = data.loc[data.groupby(group_cols)['price_USD'].idxmin()].copy()
    min_price = min_price[['collection', 'reference', 'price_USD', 'country']]
    min_price.rename(columns={'price_USD': 'min_price', 'country': 'min_price_country'}, inplace=True)

    # Merge both DataFrames to have best selling and minimum price information in one table
    result = pd.merge(best_sell, min_price, on=['collection', 'reference'], how='left')

    # Calculate potential profit
    result['profit'] = result['best_sell_price'] - result['min_price']

    # Save the results
    result.to_csv('best_sell_prices_with_profit.csv', index=False)
    
    return result

def sum_profit_per_country(data: pd.DataFrame):
    """
    Computes the sum of potential profits per country.

    Parameters:
        data (pd.DataFrame): DataFrame with best sell prices and profit computed.

    Returns:
        Dictionary of DataFrames:
            - 'profit_by_reference': Profit summed per country based on product reference.
            - 'profit_by_collection': Profit summed per country based on collection.
    """
    # Aggregate total profit per country (based on reference)
    profit_by_reference = data.groupby('min_price_country', as_index=False)['profit'].sum()
    profit_by_reference.rename(columns={'profit': 'total_profit_by_reference'}, inplace=True)

    # Aggregate total profit per country (based on collection)
    profit_by_collection = data.groupby('collection', as_index=False)['profit'].sum()
    profit_by_collection.rename(columns={'profit': 'total_profit_by_collection'}, inplace=True)

    return {
        "profit_by_reference": profit_by_reference["total_profit_by_reference"].values[0],
        "profit_by_collection": profit_by_collection
    }