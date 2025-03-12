import pandas as pd
from datetime import datetime

DELIVERED = 'Delivered'
ORDERED = 'Ordered'
DELIVERED_TOTAL = 'Delivered_Total'
CANTIDAD_CUMSUM = 'Cantidad_Cumsum'
CANTIDAD_SPLIT = 'Cantidad_Split'
AVAILABLE = 'Available'


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
        lambda group: group[DELIVERED].where(group.index != group.index[-1], group['Cantidad'])).reset_index(drop=True).squeeze()
    summary = summary[summary[ORDERED] > 0]
    summary.to_csv('../../files/inventory/po_delivered.csv', index=False)
    return summary


inventory, po_by_store = read_files_and_backup_inventory()
sku = 1145889908
# inventory = inventory[inventory['SKU'] == sku]
# po_by_store = po_by_store[po_by_store['Sku'] == sku]
# 1018195077 multiple sku no split
# 1035942633 split by store
# 1145889908 missing
split_inventory = split_df_by_sku(inventory)
po = po_by_store.groupby(['Sku']).agg({'Cantidad': 'sum'}).reset_index(names='SKU')
po_copy = po.copy()
new_inventory_dfs = assign_deliveries_by_lot_number(split_inventory, po_copy)
new_inventory = update_inventory_in_memory(new_inventory_dfs)
po_summary = summarize_po_deliveries_by_lot_number(new_inventory)
po_dfs = split_df_by_sku(po_summary)[1:]

dfs = []
po_by_store_temp = po_by_store.copy()
po_by_store_temp[CANTIDAD_SPLIT] = po_by_store_temp['Cantidad']
for unique_po in po_dfs:
    df = po_by_store_temp.rename({'Sku': 'SKU'}, axis=1).merge(
        unique_po[['SKU', 'WAREHOUSE_CODE', DELIVERED]].rename({DELIVERED: DELIVERED_TOTAL}, axis=1), on='SKU', how='left')
    df.index = po_by_store_temp.index
    df[CANTIDAD_CUMSUM] = df.groupby('SKU')[CANTIDAD_SPLIT].cumsum()
    df[AVAILABLE] = df[DELIVERED_TOTAL].fillna(0) - df[CANTIDAD_CUMSUM]
    df[DELIVERED] = (df[CANTIDAD_SPLIT] + df[AVAILABLE].where(df[AVAILABLE] < 0, 0)).clip(lower=0)
    cantidad_remaining = (df[CANTIDAD_SPLIT] - df[DELIVERED]).copy()
    # df = df.loc[df[DELIVERED] > 0]
    dfs.append(df.copy())
    po_by_store_temp[CANTIDAD_SPLIT] = cantidad_remaining
    po_by_store_temp = po_by_store_temp.loc[po_by_store_temp[CANTIDAD_SPLIT] > 0]

po_by_store_delivered = pd.concat(dfs)
po_by_store_delivered = po_by_store_delivered.reset_index().sort_values(by=['index', 'WAREHOUSE_CODE']).drop(columns=['index']).reset_index(drop=True)
po_by_store_delivered[CANTIDAD_SPLIT] = po_by_store_delivered.groupby(['SKU', 'Tienda']).apply(
    lambda group: group[DELIVERED].where(group.index != group.index[-1],
                                         group[CANTIDAD_SPLIT] if len(group) > 1 else group[CANTIDAD_SPLIT])).reset_index(drop=True)
po_by_store_delivered = po_by_store_delivered.loc[po_by_store_delivered[CANTIDAD_SPLIT] > 0]

# .drop([DELIVERED_TOTAL, CANTIDAD_CUMSUM, MISSING], axis=1)

po_by_store_delivered.to_csv('/home/jmarcosh/Downloads/po_delivered_by_store.csv', index=False)
po_by_store.to_csv('/home/jmarcosh/Downloads/po_by_store.csv', index=False)

x=1
