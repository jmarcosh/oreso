import warnings

import numpy as np
import pandas as pd
from datetime import date

DELIVERED = 'Delivered'
ORDERED = 'Ordered'
DELIVERED_TOTAL = 'Delivered_Total'
CANTIDAD_CUMSUM = 'Cantidad_Cumsum'
MISSING = 'Missing'
SKU = 'SKU'
DEMAND = 'Cantidad'
STOCK = 'INVENTORY'
DELIVERED_CS = 'delivered_cs'


def read_files_and_backup_inventory():
    inventory_df = pd.read_excel('../../files/inventory/inventory.xlsx', sheet_name='Inventario')
    today_date = date.today().strftime('%Y%m%d-%H%M')
    # inventory_df.to_csv('../../files/inventory/inventory_' + today_date + '.csv', index=False)
    po_df = pd.read_excel('../../files/inventory/purchase_order.xlsx', sheet_name='Sheet_1')

    return inventory_df, po_df.rename({'Sku': SKU}, axis=1).sort_values(by=SKU)


def split_df_by_sku(df):
    sku0 = df[df['SKU'] == 0]
    df = df.drop(index=sku0.index)
    split_dfs = [sku0]
    while not df.empty:
        unique_df = df.drop_duplicates(subset=['SKU'], keep='first')
        split_dfs.append(unique_df)
        df = df.drop(index=unique_df.index)
    return split_dfs


def assign_deliveries_by_lot_number(inv_dfs, po_by_store):
    po_df = po_by_store.groupby([SKU]).agg({DELIVERED: 'sum'}).reset_index()
    dfs = [inv_dfs[0]]
    for i, unique_inventory in enumerate(inv_dfs[1:]):
        #TODO add function that checks whether catalog price and po price match
        df = unique_inventory.merge(po_df, on=SKU, how='left')
        df.index = unique_inventory.index
        df['INVENTORY'] = df['INVENTORY'] - df[DELIVERED]
        dfs.append(df.copy())
        df['Cantidad'] = df['Cantidad'] - df[DELIVERED]
        po_df = df.loc[(df['Cantidad'] > 0), [SKU, 'Cantidad']]
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

def allocate_stock(inventory, po_by_store):
    sku_lst = po_by_store['SKU'].unique()
    delivered = []
    for sku in sku_lst:
        po_sku = po_by_store[po_by_store['SKU'] == sku]
        inventory_sku = inventory[inventory[SKU] == sku]
        demand = po_sku[DEMAND].sum()
        stock = inventory_sku[STOCK].sum()
        if stock >= demand:
            delivered_sku = po_sku[DEMAND]
        else:
            demand_store = po_sku[DEMAND].values
            delivered_sku = np.zeros_like(demand_store)
            while stock > 0:
                allocate_i = (demand_store == demand_store.max()).astype(int)
                if allocate_i.sum() >= stock:
                    indices = np.flatnonzero(allocate_i)  # [1, 3, 4]
                    keep_indices = indices[:stock]  # [1, 3]
                    allocate_i = np.zeros_like(allocate_i)
                    allocate_i[keep_indices] = 1
                delivered_sku += allocate_i
                demand_store -= allocate_i
                stock -= allocate_i.sum()
        delivered.append(delivered_sku)
    return np.concatenate(delivered)

def assign_warehouse_codes_from_sku(inventory, po):
    split_inventory = split_df_by_sku(inventory)
    po_wh = []
    po_original_cols = po.columns
    for i, inventory_wh in enumerate(split_inventory[1:]):
        po = po.merge(
            inventory_wh[[SKU, 'WAREHOUSE_CODE', 'STYLE', 'DESCRIPTION', STOCK, 'WHOLESALE_PRICE', 'PCS_BOX']],
            on=['SKU'], how='left')
        delivered_cs = po.groupby(SKU)[DELIVERED].cumsum()
        missing = (po[STOCK] - delivered_cs).clip(upper=0).fillna(0)
        delivered_i = (po[DELIVERED] + missing).clip(lower=0)
        to_deliver = po[DELIVERED] - delivered_i
        po[DELIVERED] = delivered_i
        po_wh.append(po.loc[po[DELIVERED] > 0].copy())
        po[DELIVERED] = to_deliver
        po = po.loc[po[DELIVERED] > 0, po_original_cols]
        if len(po) == 0:
            break
    po = pd.concat(po_wh)
    return po.sort_values(['Tienda', 'SKU']).reset_index(drop=True)

def replace_quantity_in_last_index(group):
    group = group.copy()
    if len(group) > 1:
        group.iloc[:-1, group.columns.get_loc('Cantidad')] = group[DELIVERED].iloc[:-1]
        group.iloc[-1, group.columns.get_loc('Cantidad')] = group['aux'].iloc[-1]
    return group

def split_ordered_quantity_into_warehouse_codes(po):
    po['aux'] = po['Cantidad'] - po.groupby(['Tienda', 'SKU'])[DELIVERED].transform(lambda x: x.cumsum().shift(1))
    po = po.groupby(['Tienda', 'SKU'], group_keys=False).apply(replace_quantity_in_last_index)
    return po.drop(columns=['aux'])


ts_rename = {
    'Orden Compra' : '# OC',
    'WAREHOUSE_CODE': 'Código Tecs',
    'STYLE': 'Grupo',
    'DESCRIPTION': 'Descripción',
    SKU: 'Sku',
    'Tienda': '# Sucursal',
    'Nombre tienda': 'Nombre sucursal',
    'Cantidad': 'Cantidad OC',
    'Delivered': 'Cantidad',
    'box_in_store': 'Caja inicial',
    'box_id': 'Contenedor'
}

columns = ["Tipo", "FECHA", "Cliente final", "# factura", "# OC", "Código Tecs", "Grupo", "Descripción", "Sku",
           "# Sucursal", "Nombre sucursal", "Contenedor", "Cantidad OC", "Cantidad", "Faltante", "Factor",
           "Caja inicial", "Caja final", "FECHA DE CITA", "# Cita", "OBSERVACIÓN"]

def run_process_purchase_orders(customer, rfid_series):
    inventory, po = read_files_and_backup_inventory()
    # inventory = inventory[inventory[SKU] == 1145889908]
    # po = po[po[SKU] == 1035942579]
    # po = po[po[SKU] == 1035942641] # split
    po[DELIVERED] = allocate_stock(inventory, po)
    po = assign_warehouse_codes_from_sku(inventory, po)
    po = split_ordered_quantity_into_warehouse_codes(po)
    styles_pd = po.loc[(po['Costo'] != po['WHOLESALE_PRICE']), 'Modelo'].unique()
    if len(styles_pd) > 0:
        warnings.warn(f"""The following styles have price conflicts: {", ".join(styles_pd)}""", UserWarning)

    box_space = (1 / po['PCS_BOX']) * po[DELIVERED]


    box_assignment = []
    i = 0
    box, end_box = [int(r[1:]) for r in rfid_series[i]]
    cum_space = []
    for space in box_space:
        cum_space.append(space)
        if sum(cum_space) > 1:
            box += 1
            cum_space = []
        box_assignment.append(box)
        if box > end_box:
            i += 1
            box, end_box = [int(r[1:]) for r in rfid_series[i]]

    po['box_id'] = ["C" + str(i) for i in box_assignment]

    po['box_change'] = (po['box_id'] != po['box_id'].shift()).astype(int)
    box_space_cum = po.groupby(['Tienda'])['box_change'].cumsum()

    po.to_csv('/home/jmarcosh/Downloads/inv_deb.csv')

    ts = po.copy()
    ts = ts.rename(columns=ts_rename)

    ts['Tipo'] = 'Salida'
    ts['FECHA'] = date.today().strftime('%Y%m%d')
    ts['Cliente final'] = customer
    ts['# factura'] = np.nan
    ts['Faltante'] = ts['Cantidad OC'] - ts['Cantidad']
    ts['Factor'] = np.nan
    ts['Caja final'] = ts['Caja inicial']
    ts['FECHA DE CITA'] = np.nan
    ts['# Cita'] = np.nan
    ts['OBSERVACIÓN'] = np.nan
   
    # po[(po['Tienda'] == 688) & (po['SKU'] == 1035942641)]
    ts = ts[columns]
    ts.to_csv('/home/jmarcosh/Downloads/inv_deb.csv')
    x=1

if __name__ == '__main__':
    CUSTOMER = 'Liverpool'
    RFID_SERIES = [['C52762712', 'C52762891'],
                   ['C52763895', 'C52764000']]
    run_process_purchase_orders(CUSTOMER, RFID_SERIES)
# new_inventory = update_inventory_in_memory(new_inventory_dfs)
# po_summary = summarize_po_deliveries_by_lot_number(new_inventory)

    x=1