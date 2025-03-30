import pandas as pd
from datetime import datetime

DELIVERED = 'Delivered'
ORDERED = 'Ordered'
DELIVERED_TOTAL = 'Delivered_Total'
QUANTITY_CUMSUM = 'Cantidad_Cumsum'
QUANTITY_SPLIT = 'Cantidad_Split'
AVAILABLE = 'Available'
CAPACITY = 'Capacity'
CAPACITY_CUMSUM = 'Capacity_Cumsum'
BOX_NUMBER = 'Box_Number'
BOX_SEQUENCE = 'Box_Sequence'

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
    df_filtered.drop(['Cantidad', DELIVERED], axis=1).to_csv('../../files/inventory/inventory_updated.csv', index=False)
    df_grouped = df_filtered.groupby(['SKU', 'UPC']).agg({'INVENTORY': 'sum'}).reset_index()
    df_grouped.to_csv('../../files/inventory/inventory_grouped.csv', index=False)
    return df


def summarize_po_deliveries_by_lot_number(df):
    summary = df.loc[(df['Cantidad'] > 0),
    ['RD', 'WAREHOUSE_CODE', 'STYLE', 'DESCRIPTION', 'UPC', 'SKU', 'Cantidad', DELIVERED]]
    summary = summary.reset_index().sort_values(by=['SKU', 'index']).drop('index', axis=1).reset_index(drop=True)
    summary[ORDERED] = summary.groupby('SKU').apply(
        lambda group: group[DELIVERED].where(group.index != group.index[-1], group['Cantidad'])).reset_index(drop=True).squeeze()
    summary = summary[summary[ORDERED] > 0]
    summary.to_csv('../../files/inventory/po_delivered.csv', index=False)
    return summary


def assign_store_deliveries_by_lot_number(po_orig, po_dfs):
    dfs = []
    po_by_store_copy = po_orig.rename({'Sku': 'SKU'}, axis=1).copy()
    po_by_store_copy[QUANTITY_SPLIT] = po_by_store_copy['Cantidad']
    for po_df_i in po_dfs:
        df = po_by_store_copy.merge(po_df_i[['SKU', 'WAREHOUSE_CODE', 'STYLE', 'DESCRIPTION', DELIVERED]]
                                    .rename({DELIVERED: DELIVERED_TOTAL}, axis=1), on='SKU', how='left')
        df.index = po_by_store_copy.index
        df[QUANTITY_CUMSUM] = df.groupby('SKU')[QUANTITY_SPLIT].cumsum()
        df[AVAILABLE] = df[DELIVERED_TOTAL].fillna(0) - df[QUANTITY_CUMSUM]
        df[DELIVERED] = (df[QUANTITY_SPLIT] + df[AVAILABLE].where(df[AVAILABLE] < 0, 0)).clip(lower=0)
        quantity_remaining = (df[QUANTITY_SPLIT] - df[DELIVERED]).copy()
        # df = df.loc[df[DELIVERED] > 0]
        dfs.append(df.copy())
        po_by_store_copy[QUANTITY_SPLIT] = quantity_remaining
        po_by_store_copy = po_by_store_copy.loc[po_by_store_copy[QUANTITY_SPLIT] > 0]
    return dfs

def concat_po_by_store_by_lot_dfs(dfs):
    df = pd.concat(dfs)
    df = (df.reset_index().sort_values(by=['index', 'WAREHOUSE_CODE'])
          .drop(columns=['index']).reset_index(drop=True))
    df[QUANTITY_SPLIT] = df.groupby(['SKU', 'Tienda']).apply(
        lambda group: group[DELIVERED].where(group.index != group.index[-1],
                                             group[QUANTITY_SPLIT])).reset_index(drop=True)
    df = df.loc[df[QUANTITY_SPLIT] > 0]
    df.to_csv('/home/jmarcosh/Downloads/po_delivered_by_store.csv', index=False)
    return df.reset_index(drop=True)


capacity_per_box = 60
inventory, po_by_store = read_files_and_backup_inventory()
# sku = 1145889908
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
po_by_lot_dfs = split_df_by_sku(po_summary)[1:]
po_by_store_by_lot_dfs = assign_store_deliveries_by_lot_number(po_by_store, po_by_lot_dfs)
po_by_store_delivered = concat_po_by_store_by_lot_dfs(po_by_store_by_lot_dfs)

df_sort = po_by_store_delivered.sort_values(['Tienda', 'STYLE']).reset_index(drop=True).copy()
df_sort['STYLE'] = df_sort['STYLE'].str.replace(' ', '')
df_sort['STYLE_COLOR'] = df_sort['STYLE'].str.rsplit('-', n=1).str[0]
df_style = df_sort
df_sort[CAPACITY] = df_sort[DELIVERED] / capacity_per_box
df_sort[CAPACITY_CUMSUM] = df_sort.groupby('Tienda').cumsum()



x=1
