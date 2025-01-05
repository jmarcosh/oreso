from datetime import datetime, timedelta
import pandas as pd
import os

from typing import Optional


def process_liverpool_file(df: pd.DataFrame) -> pd.DataFrame:
    if len(df.columns) == 9:
        df.columns = ["sale_date", "sku", "description", "status", "style_lvp", "upc", "bus",
                         "units", "mxn"]
    if 'sku' in df.columns and 'sale_date' in df.columns:
        df = df[(~df['sale_date'].str.contains('Resultado', na=False)) &
                (~df['sku'].str.contains('Resultado', na=False))].reset_index(drop=True)
    elif 'sale_date' in df.columns:
        df = df[(~df['sale_date'].str.contains('Resultado', na=False))].reset_index(drop=True)
    df['sale_date'] = [datetime.strptime(x, "%d.%m.%Y") for x in df['sale_date']]
    # df['week'] = [x + timedelta(days=6 - x.weekday()) for x in df['sale_date']]
    # df['year'] = df['sale_date'].dt.year
    # df['month'] = df['sale_date'].dt.month
    df['sku'] = df['sku'].astype('int')
    df['color'] = [x.rsplit(',', 1)[1][1:] if ',' in x else None for x in df['description']]
    df['bus'] = df['bus'].replace({'BÃ¡sico': 'basics', 'Compra Ãºnica': 'fashion'})
    df[['units', 'mxn']] = df[['units', 'mxn']].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')
    return df


def process_suburbia_sales_file(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['FECHA_DIA', 'MATERIAL', 'MATERIAL_T', 'ESTATUS_ARTICULO', 'MARCA', 'TIENDA', 'EAN', 'VENTA_NETA_ANTES_MSI',
             'VENTA_NETA_LC', 'VENTA_PZAS', 'COSTO_DE_LO_VENDIDO']]
    df.columns = ["sale_date", "sku", "description", "status", 'brand', "store", "upc", "mxn", "net_mxn", "units", "cost_sub"]
    df = df[(~df['mxn'].isna())]
    df['sale_date'] = [parse_date(x) for x in df['sale_date']]
    # df['week'] = [x + timedelta(days=6 - x.weekday()) for x in df['sale_date']]
    # df['year'] = df['sale_date'].dt.year
    # df['month'] = df['sale_date'].dt.month
    df['sku'] = df['sku'].astype('int')
    df['color'] = [x.rsplit(' ', 2)[1:] if ' ' in x else None for x in df['description']]
    return df


def multi_column_merge(df1, df2, keys, how='left', suffixes=('_x', '_y')):
    """
    Merge two DataFrames using multiple fallback keys.

    Parameters:
        df1 (pd.DataFrame): The first DataFrame (left).
        df2 (pd.DataFrame): The second DataFrame (right).
        keys (list): List of columns to use as keys, in order of priority.
        how (str): Type of merge ('left', 'right', 'inner', 'outer'). Default is 'left'.
        suffixes (tuple): Suffixes for overlapping column names. Default is ('_x', '_y').

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    final_lst = []  # To accumulate matched rows
    remaining = df1.copy().reset_index(drop=True)  # Start with all rows in `df1`

    for i in range(len(keys)):
        # Merge the remaining rows with df2 on the current key
        df2_copy = df2.copy().rename({keys[i]: keys[0]}, axis=1).drop_duplicates(subset=keys[0], keep='last').dropna(subset=keys[0]).reset_index(drop=True)
        merged = pd.merge(remaining, df2_copy.drop(keys[i+1:], axis=1), on=keys[0], how=how, suffixes=suffixes)

        # Identify matched rows (rows where merge succeeded)
        non_matched_rows = merged.iloc[:, len(df1.columns):].isnull().all(axis=1)

        # Append matched rows to the final DataFrame
        final_lst.append(merged[~non_matched_rows])

        # Update remaining rows (rows without a match)
        remaining = remaining[non_matched_rows].reset_index(drop=True)
        df2.drop(keys[i], axis=1, inplace=True)
        # Stop if there are no unmatched rows left
        if remaining.empty:
            break

    # Combine final matched rows and any remaining rows
    remaining.columns = [x + suffixes[0] if x in df2.columns else x for x in remaining.columns]
    final_lst.append(remaining)
    final = pd.concat(final_lst, ignore_index=True)

    return final


def add_oreso_info(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    utils_dir = os.path.dirname(__file__)
    catalog_path = os.path.join(utils_dir, '..', 'files/sales/catalog.csv')
    catalog = pd.read_csv(catalog_path)
    catalog.columns = ['rd', 'sku', 'generic_sku', 'style', 'size', 'description', 'upc', 'brand', 'classification',
                       'photo', 'wsp', 'style_color', 'style_liverpool', 'rp', 'rp_no_vat', '1', '2', '3', '4', '5']
    # catalog.drop(['style_liverpool'], axis=1, inplace=True)
    # for col in ['wsp', 'rp', 'rp_no_vat']:
    #     catalog[col] = [float(x[1:]) if isinstance(x, str) else 0 for x in catalog[col]]
    catalog = catalog[['sku', 'generic_sku', 'style', 'brand', 'rd', 'classification']]
    catalog.rename({'classification': 'group'}, axis=1, inplace=True)
    df['sku'] = [float(x) for x in df['sku']]
    catalog['generic_sku'] = pd.to_numeric(catalog['generic_sku'], errors='coerce').astype('float')
    catalog['style_number'] = [x.split('-', 1)[0][2:] for x in catalog['style']]
    catalog['size'] = [x.rsplit('-', 1)[1] if len(x.rsplit('-', 1)) > 1 else None for x in catalog['style']]
    return multi_column_merge(df, catalog, keys=['sku', 'generic_sku'], how='left', suffixes=(suffix, '_oreso'))


def create_df_with_all_combinations(date_min: datetime, date_max: datetime, stores: list = False,
                                    styles: list = False, periodicity: str = 'weekly') -> pd.DataFrame:
    if periodicity == 'weekly':
        all_dates = pd.date_range(start=date_min, end=date_max, freq='W')
        colnames = ['week']
    elif periodicity == 'monthly':
        all_dates = pd.date_range(start=date_min, end=date_max, freq='MS')
        colnames = ['month']
    combinations = [[x] for x in all_dates]
    for lst, name in zip([stores, styles], ['store', 'style']):
        if lst:
            lst.sort()
            colnames.append(name)
            combinations = [sublist + [element] for sublist in combinations for element in lst]
    return pd.DataFrame(combinations, columns=colnames)


def find_closest_non_zero(lst, index):
    # Helper function to find the closest non-zero value to the given index
    left_index = right_index = index
    while lst[left_index] == 0 or lst[right_index] == 0:
        left_index -= 1
        right_index += 1
        left_index = max(0, left_index)
        right_index = min(len(lst) - 1, right_index)
        if lst[left_index] != 0:
            return lst[left_index]
        if lst[right_index] != 0:
            return lst[right_index]


def convert_orders_date_to_datetime(date, delta=0):
    date = date.split(' ')[0]
    return datetime.strptime(date, "%Y-%m-%d") + timedelta(days=delta)  # 24 = 11 in warehouse + 7 CeDis to store


def process_orders_date(df):
    df = df[~(df['date'].isna())]
    df['date'] = [convert_orders_date_to_datetime(x, 15) for x in df['date']]
    df['week'] = [x + timedelta(days=6 - x.weekday()) for x in df['date']]
    return df


def parse_date(x):
    x = str(x)
    if len(x) == 8 and x.isdigit():  # Matches format "%Y%m%d"
        return datetime.strptime(x, "%Y%m%d")
    elif '.' in x and len(x.split('.')) == 3:  # Matches format "%d.%m.%Y"
        return datetime.strptime(x, "%d.%m.%Y")
    else:
        raise ValueError(f"Date format not recognized: {x}")


def process_currency_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify and process columns containing currency values in the DataFrame.
    Currency columns are identified by the presence of a dollar symbol ('$').
    This function removes symbols like '$' and ',' and converts the values to numeric.

    Parameters:
        df (pd.DataFrame): DataFrame with potential currency columns.

    Returns:
        pd.DataFrame: DataFrame with processed currency columns.
    """
    # Identify potential currency columns with a dollar sign ('$')
    currency_columns = [
        col for col in df.columns
        if df[col].dtype == 'object' and df[col].str.contains(r'\$', na=False).any()
    ]
    df_copy = df.copy()
    # Process currency columns: remove '$' and ',' symbols, convert to numeric
    df_copy[currency_columns] = df_copy[currency_columns].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
    return df_copy

def process_distribution_file(df):
    df_columns = ['Cobrado', 'delivery_date', 'F. Factura', 'opid', 'busid', 'Cliente', 'Orden de Compra', 'Factura',
                     'Tienda envío', 'Sal=1, Ent=0', 'Código TS', 'style', 'Descripción', 'UPC', 'SKU',
                     'Ubicación en inv.', 'Ordenado', 'delivered', 'Facturado', 'Dif. Entregado vs. Facturado', 'cost',
                     'Subtotal', 'Descuento', 'Menos descuento', 'IVA Incluido', 'Costo Integrado',
                     'Valor a costo integrado', 'clasification',
                     ]
    extra_columns_len = len(df.columns) - len(df_columns)
    extra_columns = [str(i) for i in range(extra_columns_len)]
    df.columns = df_columns + extra_columns
    df = df[(df['delivery_date'].notna()) & (df['opid'] == 'V')]
    df = process_currency_columns(df)
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='%m/%d/%Y')
    df['style_number'] = [x.split('-', 1)[0][2:] for x in df['style']]
    df['size'] = [x.rsplit('-', 1)[1] if len(x.rsplit('-', 1)) > 1 else None for x in df['style']]
    df['style_color'] = [x.rsplit('-', 1)[0] if len(x.rsplit('-', 1)) > 1 else None for x in df['style']]
    return df
    
