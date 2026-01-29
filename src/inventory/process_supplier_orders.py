
import pandas as pd
import numpy as np
import requests
import streamlit as st

from inventory.common_app import extract_size_from_style, convert_numeric_id_cols_to_text, \
    validate_unique_ids_and_status_in_updatable_table, read_or_create_file, save_purchases_file_and_logs
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
    po[C.COST] = np.nan # this column will be used when updating the receipt of the goods
    po[C.RECEIVED_DATE] = pd.to_datetime(delivery_date)
    po[C.STYLE] = add_dash_before_size(po[C.STYLE], config)
    po[C.WAREHOUSE] = "on_order"
    po[C.INVOICE_NUM] = np.nan
    rd = po.loc[0, C.RD]
    files_path = update_purchases_table(sp, po, rd[:3], config, log_id)
    po = add_inventory_cols(po, inventory)
    updated_inv = pd.concat([inventory, po[inventory.columns]], ignore_index=True)
    return updated_inv, files_path


def add_inventory_cols(po, inventory):
    po[C.INVENTORY] = 0
    po[C.SIZE] = extract_size_from_style(po)
    missing_cols = inventory.columns.difference(po.columns)
    po[missing_cols] = np.nan
    return po




def update_purchases_table(sp, po, rd, config, log_id):
    """
    Updates an Excel file in SharePoint by appending new purchase order data.

    Parameters:
    - files_save_path: str, the SharePoint path to the Excel file.
    - po: pd.DataFrame, the new purchase order data to append.
    """
    purchases_file_path = f"COMPRAS/{rd}.xlsx"
    purchases = read_or_create_file(sp, purchases_file_path)
    if purchases is None:
        return

    # Append new data and save back to SharePoint
    purchases = pd.concat([purchases, po], ignore_index=True)
    validate_unique_ids_and_status_in_updatable_table(purchases, config)
    save_purchases_file_and_logs(sp, purchases, rd, log_id)
    return purchases_file_path


