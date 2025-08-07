import pandas as pd
import requests

from src.api_integrations.sharepoint_client import SharePointClient
from src.inventory.varnames import ColNames as C

invoc = SharePointClient()



def receive_goods(po, inventory, delivery_date, update_from_sharepoint, log_id):
    po[C.LOG_ID] = log_id
    if update_from_sharepoint:
        inventory["_row_order"] = range(len(inventory))
        original_column_order = inventory.columns.copy()
        inventory.set_index([C.RD, C.MOVEX_PO, C.UPC], inplace=True)
        po.set_index([C.RD, C.MOVEX_PO, C.UPC], inplace=True)
        for col in [C.LOG_ID, C.SKU, C.COST, C.WHOLESALE_PRICE, C.RETAIL_PRICE]:
            inventory.loc[po.index, col] = po[col]
        updated_inv = reset_rows_and_columns_order(inventory, original_column_order)
        files_save_path = update_from_sharepoint
    else:
        po = po.loc[~po[C.RD].isna()].reset_index(drop=True)
        po[C.WAREHOUSE_CODE] = (po[C.MOVEX_PO]
                                .fillna(0)
                                .astype(str)
                                .str.replace(r"\D", "", regex=True)
                                .str.pad(6, side='right', fillchar='0').str[:6]
                                + po[C.UPC].fillna(0).astype(int).astype(str).str.zfill(6).str[-6:]).astype(int)
        po[C.RECEIVED_DATE] = pd.to_datetime(delivery_date)
        po[C.INVENTORY] = po[C.RECEIVED]
        updated_inv = pd.concat([inventory, po], ignore_index=True)
        season = po.loc[0, C.RD][:3]
        files_save_path = f"RECIBOS/{season}.xlsx"
        update_master_entry_file(files_save_path, po)
    # po[C.DELIVERED] = - po[C.RECEIVED]
    txn_key = 'C'
    return po, updated_inv, files_save_path, txn_key


def reset_rows_and_columns_order(inventory, original_column_order):
    updated_inv = inventory.reset_index()[original_column_order]
    # Restore original row order if needed
    updated_inv.sort_values("_row_order", inplace=True)
    updated_inv.drop(columns="_row_order", inplace=True)
    return updated_inv


def update_master_entry_file(files_save_path, po):
    """
    Updates a master Excel file in SharePoint by appending new purchase order data.

    Parameters:
    - files_save_path: str, the SharePoint path to the Excel file.
    - po: pd.DataFrame, the new purchase order data to append.
    """

    try:
        # Try to read the existing master file from SharePoint
        season_po = invoc.read_excel(files_save_path)
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
    invoc.save_excel(season_po, files_save_path)
