import pandas as pd
import requests

from inventory.update_inventory_utils import create_and_save_techsmart_txt_file, add_dash_before_size
from inventory.varnames import ColNames as C




def receive_goods(sp, po, inventory, config, delivery_date, update_from_sharepoint, log_id):
    if update_from_sharepoint:
        # inventory["_row_order"] = range(len(inventory))
        updated_inv = inventory.copy()
        original_column_order = inventory.columns.copy()
        updated_inv.set_index([C.RD, C.MOVEX_PO, C.UPC], inplace=True)
        po.set_index([C.RD, C.MOVEX_PO, C.UPC], inplace=True)
        cols = updated_inv.columns.intersection(po.columns).difference([C.LOG_ID])
        common_index = updated_inv.index.intersection(po.index)
        mask = (updated_inv.loc[common_index, cols] != po.loc[common_index, cols]).any(axis=1)
        changes_index = common_index[mask]
        updated_inv.update(po[cols])
        updated_inv.loc[changes_index, C.LOG_ID] = log_id
        updated_inv = updated_inv.reset_index()[original_column_order]
        # updated_inv = reset_rows_and_columns_order(updated_inv, original_column_order)
        files_save_path = None
    else:
        po[C.LOG_ID] = log_id
        po = po.loc[~po[C.RD].isna()].reset_index(drop=True)
        po[C.WAREHOUSE_CODE] = (po[C.MOVEX_PO]
                                .fillna(0)
                                .astype(str)
                                .str.replace(r"\D", "", regex=True)
                                .str.pad(6, side='right', fillchar='0').str[:6]
                                + po[C.UPC].str.zfill(6).str[-6:]).astype(int)
        po[C.RECEIVED_DATE] = pd.to_datetime(delivery_date)
        po[C.STYLE] = add_dash_before_size(po[C.STYLE])
        rd = po.loc[0, C.RD]
        update_master_entry_file(sp, po, rd[:3])
        files_save_path = f"OC/Recibos/{delivery_date.split('/')[2]}/{delivery_date.split('/')[0]}/{log_id}_{str(rd)}"
        sp.create_folder_path(files_save_path)
        po = add_inventory_cols(po)
        customer_mapping = config.get("bus_key_to_customer")
        customer = [customer_mapping.get(x.split("_", 1)[0], x) for x, y in zip(po[C.BUS_KEY], po[C.RECEIVED]) if y > 0]
        techsmart = create_and_save_techsmart_txt_file(sp, po, customer, config, rd, files_save_path)
        updated_inv = pd.concat([inventory, po[inventory.columns]], ignore_index=True)
    return updated_inv, files_save_path


def add_inventory_cols(po):
    po[C.INVENTORY] = po[C.RECEIVED]
    po[C.SIZE] = [x.rsplit('-', 1)[-1] for x in po[C.STYLE]]
    po['BOX_STORE_NUM'] = 1
    po[C.DELIVERED] = - po[C.RECEIVED]
    po[C.PO_NUM] = po[C.RD]
    return po


def reset_rows_and_columns_order(inventory, original_column_order):
    updated_inv = inventory.reset_index()[original_column_order]
    # Restore original row order if needed
    updated_inv.sort_values("_row_order", inplace=True)
    updated_inv.drop(columns="_row_order", inplace=True)
    return updated_inv


def update_master_entry_file(sp, po, rd):
    """
    Updates a master Excel file in SharePoint by appending new purchase order data.

    Parameters:
    - files_save_path: str, the SharePoint path to the Excel file.
    - po: pd.DataFrame, the new purchase order data to append.
    """

    try:
        # Try to read the existing master file from SharePoint
        season_po = sp.read_excel(f"CATALOGO/{rd}.xlsx")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # File does not exist yet — create a new DataFrame
            season_po = pd.DataFrame()
        else:
            # Other HTTP error occurred — log and exit
            print(f"Failed to update master entry file: HTTP {e.response.status_code}")
            return

    # Append new data and save back to SharePoint
    season_po = pd.concat([season_po, po], ignore_index=True).drop_duplicates(subset=[C.RD, C.MOVEX_PO, C.UPC],
                                                                              keep="last")
    season_po[C.RECEIVED_DATE] = season_po[C.RECEIVED_DATE].dt.date
    sp.save_excel(season_po, f"CATALOGO/{rd}.xlsx")
