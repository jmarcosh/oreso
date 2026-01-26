
import pandas as pd
import numpy as np
import requests
import streamlit as st

from inventory.common_app import extract_size_from_style
from inventory.process_orders_utils import add_dash_before_size
from inventory.varnames import ColNames as C


def validate_file(df, po_type, config):
    valid_brand_product = set(df[C.BRAND] + "_" + df[C.PRODUCT]) <=set(config['brand_product_categories'].keys())
    if not valid_brand_product:
        st.write(f"Enable the following brand product combinations: {set(df[C.BRAND] + "_" + df[C.PRODUCT])                                                       - set(config['brand_product_categories'].keys())}")
    valid_factories = set(df[C.FACTORY]) <= set(config['factories'])
    if not valid_factories:
        st.write(f"Enable the missing factories: {set(df[C.FACTORY]) - set(config['factories'])}")
    valid_business_keys = set(df[C.BUS_KEY]).issubset(set(config['business_keys'])) if po_type == "supplier" else True
    if not valid_business_keys:
        st.write(f"Enable the missing business keys: {set(df[C.BUS_KEY]) - set(config['business_keys'])}")
    if not valid_brand_product * valid_factories * valid_business_keys:
        st.stop()


def process_supplier_orders(sp, po, inventory, po_type, config, delivery_date, log_id):
    po = po.loc[~po[C.RD].isna()].reset_index(drop=True)
    validate_file(po, po_type, config)
    po[C.LOG_ID] = log_id
    po[C.WAREHOUSE_CODE] = (po[C.MOVEX_PO]
                            .fillna(0)
                            .astype(str)
                            .str.replace(r"\D", "", regex=True)
                            .str.pad(6, side='right', fillchar='0').str[:6]
                            + po[C.UPC].str.zfill(6).str[-6:]).astype(int)
    po[C.RECEIVED_DATE] = pd.to_datetime(delivery_date)
    po[C.STYLE] = add_dash_before_size(po[C.STYLE], config)
    po[C.WAREHOUSE] = "on_order"
    po[C.INVOICE_NUM] = np.nan
    rd = po.loc[0, C.RD]
    files_save_path = update_master_entry_file(sp, po, rd[:3])
    po = add_inventory_cols(po, inventory)
    updated_inv = pd.concat([inventory, po[inventory.columns]], ignore_index=True)
    return updated_inv, files_save_path


def add_inventory_cols(po, inventory):
    po[C.INVENTORY] = 0
    po[C.SIZE] = extract_size_from_style(po)
    missing_cols = inventory.columns.difference(po.columns)
    po[missing_cols] = np.nan
    return po



def update_master_entry_file(sp, po, rd):
    """
    Updates a master Excel file in SharePoint by appending new purchase order data.

    Parameters:
    - files_save_path: str, the SharePoint path to the Excel file.
    - po: pd.DataFrame, the new purchase order data to append.
    """
    purchases_file_path = f"COMPRAS/{rd}.xlsx"
    try:
        # Try to read the existing master file from SharePoint
        season_po = sp.read_excel(purchases_file_path)
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
    season_po[C.X_FTY] = season_po[C.X_FTY].dt.date
    sp.save_excel(season_po, purchases_file_path)
    return purchases_file_path
