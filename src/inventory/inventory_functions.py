import pandas as pd
from datetime import datetime

DELIVERED = 'Delivered'
ORDERED = 'Ordered'
DELIVERED_TOTAL = 'Delivered_Total'
CANTIDAD_CUMSUM = 'Cantidad_Cumsum'
MISSING = 'Missing'


def read_files_and_backup_inventory():
    inventory_df = pd.read_excel('../../files/inventory/inventory.xlsx', sheet_name='Inventario')
    today_date = datetime.today().strftime('%Y%m%d-%H%M')
    inventory_df.to_csv('../../files/inventory/inventory_' + today_date + '.csv', index=False)
    po_df = pd.read_excel('../../files/inventory/purchase_order.xlsx', sheet_name='Sheet_1')
    return inventory_df, po_df


def split_df_by_sku(df):
    sku0 = df[df['SKU'] == 0]
    df = df.drop(index=sku0.index)
    split_dfs = [sku0]
    while not df.empty:
        unique_df = df.drop_duplicates(subset="SKU", keep="first")
        split_dfs.append(unique_df)
        df = df.drop(index=unique_df.index)
    return split_dfs


def assign_deliveries_by_lot_number(inv_dfs, po_df):
    dfs = [inv_dfs[0]]
    for i, unique_inventory in enumerate(inv_dfs[1:]):
        df = po_df.merge(unique_inventory, on='SKU', how='right')
        df.index = unique_inventory.index
        df['Cantidad'] = df['Cantidad'].fillna(0)
        df[DELIVERED] = df[['Cantidad', 'INVENTORY']].min(axis=1)
        df['INVENTORY'] = df['INVENTORY'] - df[DELIVERED]
        dfs.append(df.copy())
        df['Cantidad'] = df['Cantidad'] - df[DELIVERED]
        po_df = df.loc[(df['Cantidad'] > 0), ['SKU', 'Cantidad']]
        if len(po_df) == 0:
            dfs += inv_dfs[i+2:]
            break
    return dfs


def update_inventory_in_memory(dfs):
    df = pd.concat(dfs)
    df.sort_index(inplace=True)
    df_filtered = df[df['INVENTORY'] > 0]
    df_filtered.drop(['Cantidad', DELIVERED], axis=1).to_csv('../../files/inventory/inventory_u.csv', index=False)
    df_grouped = df_filtered.groupby(['SKU', 'UPC']).agg({'INVENTORY': 'sum'}).reset_index()
    df_grouped.to_csv('../../files/inventory/inventory_grouped.csv', index=False)
    return df


def summarize_po_deliveries_by_lot_number(df):
    summary = df.loc[(df['Cantidad'] > 0),
    ['RD', 'WAREHOUSE_CODE', 'STYLE', 'UPC', 'SKU', 'Cantidad', DELIVERED]]
    summary = summary.reset_index().sort_values(by=['SKU', 'index']).drop('index', axis=1).reset_index(drop=True)
    summary[ORDERED] = summary.groupby('SKU').apply(
        lambda group: group[DELIVERED].where(group.index != group.index[-1], group['Cantidad'])
    ).reset_index(drop=True)
    summary = summary[summary[ORDERED] > 0]
    summary.to_csv('../../files/inventory/po_delivered.csv', index=False)
    return summary


inventory, po_by_store = read_files_and_backup_inventory()
inventory = inventory[inventory['SKU'] == 1145889908]
po_by_store = po_by_store[po_by_store['Sku'] == 1145889908]
split_inventory = split_df_by_sku(inventory)
po = po_by_store.groupby(['Sku']).agg({'Cantidad': 'sum'}).reset_index(names='SKU')
po_copy = po.copy()
new_inventory_dfs = assign_deliveries_by_lot_number(split_inventory, po_copy)
new_inventory = update_inventory_in_memory(new_inventory_dfs)
po_summary = summarize_po_deliveries_by_lot_number(new_inventory)
split_po_summary = split_df_by_sku(po_summary)[1:]

po_by_store_delivered_dfs = []
for po_summary_df in split_po_summary:
    po_by_store_delivered = po_by_store.rename({'Sku': 'SKU'}, axis=1).merge(
        po_summary_df[['SKU', 'WAREHOUSE_CODE', DELIVERED]].rename({DELIVERED: DELIVERED_TOTAL}, axis=1), on='SKU', how='left')
    po_by_store_delivered[CANTIDAD_CUMSUM] = po_by_store_delivered.groupby('SKU')['Cantidad'].cumsum()
    po_by_store_delivered[MISSING] = po_by_store_delivered[DELIVERED_TOTAL] - po_by_store_delivered[CANTIDAD_CUMSUM]
    po_by_store_delivered[DELIVERED] = (po_by_store_delivered['Cantidad'] + po_by_store_delivered[MISSING].where(po_by_store_delivered[MISSING] < 0, 0)).clip(lower=0)
    po_by_store_delivered_dfs.append(po_by_store_delivered.copy())
    po_by_store['Cantidad'] = po_by_store['Cantidad'] - po_by_store_delivered[DELIVERED]
    po_by_store = po_by_store.loc[po_by_store['Cantidad'] > 0]
# merge with the original_po_by_store

# .drop([DELIVERED_TOTAL, CANTIDAD_CUMSUM, MISSING], axis=1)
x=1