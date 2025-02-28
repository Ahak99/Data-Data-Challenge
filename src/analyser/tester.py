import pandas as pd

def calculate_price_difference(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each product (grouped by collection and reference), calculates the minimum price across all countries,
    and then computes the difference of each record's price from this minimum.
    
    Parameters:
        data (pd.DataFrame): Input DataFrame with columns 'collection', 'reference', 'price_USD', and 'country'.
    
    Returns:
        pd.DataFrame: The input DataFrame with two new columns:
                      - 'min_price': The minimum price for each product.
                      - 'price_diff': The difference between the price and the minimum price.
    """
    df = data.copy()
    group_cols = ['collection', 'reference']
    df['min_price'] = df.groupby(group_cols)['price_USD'].transform('min')
    df['price_diff'] = df['price_USD'] - df['min_price']
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
    return cheapest

def find_best_sell_prices(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each product (grouped by collection and reference), identifies the best selling option,
    defined as the country where the product has the highest price.
    
    Parameters:
        data (pd.DataFrame): Input DataFrame with columns 'collection', 'reference', 'price_USD', and 'country'.
    
    Returns:
        pd.DataFrame: A DataFrame containing:
                      - 'collection'
                      - 'reference'
                      - 'best_sell_price': The highest price found for the product.
                      - 'best_sell_country': The country corresponding to the highest price.
    """
    group_cols = ['collection', 'reference']
    best_sell = data.loc[data.groupby(group_cols)['price_USD'].idxmax()].copy()
    best_sell = best_sell[['collection', 'reference', 'price_USD', 'country']]
    best_sell.rename(columns={'price_USD': 'best_sell_price', 'country': 'best_sell_country'}, inplace=True)
    return best_sell