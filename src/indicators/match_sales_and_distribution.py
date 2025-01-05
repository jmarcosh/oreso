import pandas as pd


def merge_sales_and_distribution(sales_df, distribution_df, grouping_unit, date_resolution='daily'):
    left = sales_df.copy()
    right = distribution_df.copy()
    left[grouping_unit] = left[grouping_unit].str.replace(' ', '')
    right[grouping_unit] = right[grouping_unit].str.replace(' ', '')
    left['date'] = left['sale_date']
    right['date'] = right['delivery_date']
    right_columns = right.columns

    if date_resolution == 'daily':
        counter_max = 30
    elif date_resolution == 'monthly':
        counter_max = 2
    else:
        counter_max = 1

    merge_columns = ['date', grouping_unit]
    counter = 0
    merged_dfs = []
    while len(right) > 0 and counter < counter_max:
        if counter == 0:
            merged = pd.merge(left, right, on=merge_columns, how='outer')
        else:
            merged = pd.merge(left, right, on=merge_columns, how='right')
        merged_dfs.append(merged[~merged['sale_date'].isna()])
        right = merged[merged['sale_date'].isna()]
        right = right[right_columns]
        left['date'] = left['date'] - pd.DateOffset(days=1) if date_resolution == 'daily' else left['date'] - pd.DateOffset(months=1)
        counter += 1
    merged_df = pd.concat(merged_dfs).reset_index(drop=True)  #
    merged_df = merged_df.drop_duplicates(subset=merge_columns, keep='last').sort_values(by='date')
    merged_df = merged_df.drop_duplicates(subset=['sale_date', grouping_unit], keep='first')
    return merged_df

def fill_missing_dates(group, grouping_unit, date_resolution, date_column_name):
    freq = 'D' if date_resolution == 'daily' else 'MS'
    # Define the group's specific date range
    group_date_range = pd.date_range(start=group[date_column_name].min(), end=group[date_column_name].max(), freq=freq)

    # Set date as index and reindex to fill missing dates
    group = group.set_index(date_column_name).reindex(group_date_range)

    group[['year', grouping_unit]] = group[['year', grouping_unit]].ffill()
    return group.reset_index().rename(columns={'index': date_column_name})

def add_inventory_columns(df, grouping_unit, date_resolution='daily'):
    df[['mxn_cum', 'units_cum']] = (
        df.fillna({'mxn': 0, 'units': 0}).groupby(grouping_unit)[['mxn', 'units']].cumsum()
    )
    df['delivered_cum'] = df.groupby(grouping_unit)['delivered'].cumsum()
    df['delivered_cum_fill'] = df.groupby(grouping_unit)['delivered_cum'].ffill()
    df = df.dropna(subset=['delivered_cum_fill']).sort_values(by=[grouping_unit, 'date'])

    df['inventory_eod'] = df['delivered_cum_fill'] - df['units_cum'].fillna(0)
    df['inventory_bod'] = df.groupby(grouping_unit)['inventory_eod'].shift(1).fillna(df['delivered_cum'])
    return df

def add_cost_columns(df, grouping_unit):
    #TODO switch to first in first out. use inventory_bod to track for the left quantity and multiply by previous cost
    df['delivered_cost'] = df['cost'] * df['delivered']
    df['delivered_cost_cum'] = df.groupby(grouping_unit)['delivered_cost'].cumsum()
    df['delivered_cost_cum_fill'] = df.groupby(grouping_unit)['delivered_cost_cum'].ffill()
    df['unit_cost'] = df['delivered_cost_cum_fill'] / df['delivered_cum_fill']
    df['mxn_cost'] = df['units'] * df['unit_cost']
    return df
