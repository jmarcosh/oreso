import sys
import os
import re
import numpy as np
import pandas as pd
import streamlit as st
from datetime import date, datetime

from inventory.common_app import create_and_save_br_summary_table, filter_active_logs
from inventory.varnames import ColNames as C

def get_all_csv_files_in_directory(directory_path):
    if not os.path.isdir(directory_path):
        raise FileNotFoundError(f"{directory_path} is not a valid directory.")

    return [
        os.path.join(directory_path, f)
        for f in os.listdir(directory_path)
        if os.path.isfile(os.path.join(directory_path, f)) and (f.lower().endswith('.csv') | f.lower().endswith('.xlsx'))
    ]

def read_temp_files(temp_files):
    po_dfs = []
    for temp_file in temp_files:
        filename = temp_file.lower()
        if filename.endswith(".xlsx"):
            po_dfs.append(pd.read_excel(temp_file))
        elif filename.endswith(".csv"):
            po_dfs.append(pd.read_csv(temp_file, encoding="latin1"))
        else:
            st.error(f"Unsupported file type: {filename}")
            st.stop()  # Stop the script immediately
    po_df = pd.concat(po_dfs)
    return po_df


def read_files(sp, temp_paths, update_from_sharepoint):
    config = sp.read_json("config/config.json")
    inventory_df = sp.read_csv('INVENTARIO/INVENTARIO.csv')
    if update_from_sharepoint:
        po_df = sp.read_excel(f'CATALOGO/{update_from_sharepoint}.xlsx')
        action = po_type = 'update'
        matching_column = 'index'
        cols = po_df.columns.tolist()
    else: # uploaded files
        if sp.is_local:
            po_read_path = '../../files/inventory/drag_and_drop'  ##for local debugging
            temp_paths = get_all_csv_files_in_directory(po_read_path)
        po_df = read_temp_files(temp_paths)
        po_type = auto_assign_po_type(po_df)
        cols_rename = config[f'{po_type.lower()}_rename']
        po_df = po_df.rename(columns=cols_rename)
        if po_type in config.get("customers"):
            matching_cols = [C.WAREHOUSE_CODE, C.SKU, C.UPC, C.STYLE]
            matching_column = auto_assign_matching_column(po_df, matching_cols)
            cols = [matching_column] + [x for x in cols_rename.values() if x not in matching_cols]
            action = 'withdrawal'
        else: # po_type == 'receipt':
            matching_column = 'index'
            cols = list(cols_rename.values())
            action = 'receipt'
    for df in [po_df, inventory_df]:
        convert_numeric_id_cols_to_text(df, [C.WAREHOUSE_CODE, C.UPC, C.SKU, C.MOVEX_PO])
    return ((po_df.reset_index()
             .sort_values(by=matching_column)[cols]
             .reset_index(drop=True)),
            inventory_df, config, po_type, matching_column, action)

def convert_numeric_id_cols_to_text(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = (pd.to_numeric(df[col], errors='coerce').fillna(df[col].fillna(0))
                       .astype(str).replace('\..*$', '', regex=True))

def auto_assign_po_type(df):
    if '# Prov' in df.columns:
        return 'liverpool'
    elif 'Num. Prov' in df.columns:
        return 'suburbia'
    elif C.MOVEX_PO in df.columns:
        return 'receipt'
    return 'interno'

def auto_assign_matching_column(df, lst):
    for col in lst:
        if col in df.columns:
            return col
    sys.exit("Error: File must contain a matching column.")



def allocate_stock(po, inventory, column):
    code_lst = po[column].unique()
    delivered = []
    for code in code_lst:
        po_sku = po[po[column] == code]
        inventory_sku = inventory[inventory[column] == code]
        demand = po_sku[C.ORDERED].sum()
        stock = inventory_sku[C.INVENTORY].sum()
        if stock >= demand:
            delivered_sku = po_sku[C.ORDERED]
        else:
            demand_store = po_sku[C.ORDERED].values
            delivered_sku = np.zeros_like(demand_store)
            while stock > 0:
                allocate_i = (demand_store == demand_store.max()).astype(int)
                if allocate_i.sum() >= stock:
                    indices = np.flatnonzero(allocate_i)  # [1, 3, 4]
                    keep_indices = indices[:int(stock)]  # [1, 3]
                    allocate_i = np.zeros_like(allocate_i)
                    allocate_i[keep_indices] = 1
                delivered_sku += allocate_i
                demand_store -= allocate_i
                stock -= allocate_i.sum()
        delivered.append(delivered_sku)
    return np.concatenate(delivered)

def create_and_save_techsmart_txt_file(sp, po, customer, config, po_nums, files_save_path):
    ts_rename = config["ts_rename"]
    ts_columns_txt = config["ts_columns_txt"]
    ts_columns_csv = config["ts_columns_csv"]
    ts = po.copy()
    ts = ts[ts[C.DELIVERED] != 0].reset_index(drop=True)
    ts = ts.rename(columns=ts_rename)
    ts['Tipo'] = np.where(ts['Cantidad'] > 0, 'Salida', 'Entrada')
    ts['Cantidad'] = ts['Cantidad'].abs()
    ts['FECHA'] = date.today().strftime('%d/%m/%Y')
    ts['Cliente final'] = customer.title() if isinstance(customer, str) else [x.title() for x in customer]
    ts['Unidad'] = 'pzas'
    ts['Caja final'] = ts['Caja inicial']
    add_nan_cols(ts, list(set(ts_columns_txt + ts_columns_csv)))
    # po[(po[STORE_ID] == 688) & (po[SKU] == 1035942641)]
    sp.save_csv(ts[ts_columns_txt], f"{files_save_path}/techsmart_{str(po_nums)}.txt", sep='\t')
    sp.save_csv(ts[ts_columns_txt], f"{files_save_path}/techsmart_{str(po_nums)}.csv")
    return ts[ts_columns_csv]

def add_nan_cols(df, cols):
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

def save_checklist(sp, po_style, po_store, techsmart, config, po_nums, files_save_path):
    cols = config['checklist_columns']
    checklist = po_style[cols]
    dfs = [checklist, po_store, techsmart]
    sheet_names = ["CHECKLIST", "TIENDA", "TECHSMART"]
    checklist_path = f"{files_save_path}/Checklist_{po_nums}.xlsx"
    sp.save_multiple_dfs_to_excel(dfs, sheet_names, checklist_path, auto_adjust_columns=True)


def update_billing_record(sp, po_style, customer, delivery_date, config, txn_key, log_id):
    br_columns = config["br_columns"]
    br = sp.read_csv('FACTURACION/FACTURACION.csv')
    po_br = po_style.copy()
    po_br[C.DELIVERY_DATE] = datetime.strptime(delivery_date, "%m/%d/%Y")
    po_br[C.KEY] = txn_key[0] if txn_key else np.nan
    po_br[C.CUSTOMER] = customer.title()
    po_br[C.SUBTOTAL] = po_br[C.DELIVERED] * po_br[C.WHOLESALE_PRICE] if txn_key == "V" else 0
    po_br[C.DISCOUNT] = 0
    if customer.lower() == 'liverpool':
        po_br[C.DISCOUNT] = po_br[C.SUBTOTAL] * .035
    po_br[C.SUBTOTAL_NET] = po_br[C.SUBTOTAL] - po_br[C.DISCOUNT]
    po_br[C.VAT] = po_br[C.SUBTOTAL_NET] * 1.16
    po_br[C.SUBTOTAL_COST]= po_br[C.DELIVERED] * po_br[C.COST]
    po_br[C.LOG_ID] = log_id
    bru = pd.concat([br, po_br], ignore_index=True)[br_columns]
    bru[C.DELIVERY_DATE] = pd.to_datetime(bru[C.DELIVERY_DATE]).dt.date
    sp.save_csv(bru, 'FACTURACION/FACTURACION.csv')
    create_and_save_br_summary_table(sp, bru, config)



def append_log_id(lst, log_id):
        if not isinstance(lst, list):
            lst = lst.strip("[]").split(",")
        return lst.append(log_id)

def save_raw_po_and_create_file_paths(sp, customer, delivery_date, po, po_nums, log_id):
    po_save_path = f"OC/RAW/{customer.title()}"
    sp.create_folder_path(po_save_path)
    for po_num in po_nums:
        sp.save_csv(po[po[C.PO_NUM].astype(str) == po_num], f"{po_save_path}/{po_num}.csv")
    po_num = "_".join(po_nums)
    files_save_path = f"OC/{customer.title()}/{delivery_date.split('/')[2]}/{delivery_date.split('/')[0]}/{log_id}_{str(po_num)}"
    sp.create_folder_path(files_save_path)
    return files_save_path

def warn_processed_orders(sp, logs, po, update_from_sharepoint):
    po_column = po[C.RD] if update_from_sharepoint else po[C.PO_NUM]
    po_nums = [str(x) for x in po_column.unique()]
    active_logs = filter_active_logs(logs)
    prev_po_nums = []
    for item in active_logs["po"].dropna():
        # split on _ if it is followed by a digit
        parts = re.split(r'_(?=\d)', item)
        parts = [re.sub(r"\.0$", "", x) for x in parts]
        prev_po_nums.extend(parts)
    intersection = list(set(po_nums) & set(prev_po_nums))
    if not update_from_sharepoint and (len(intersection) > 0):
        pause_for_reprocess_decision(intersection)
        st.success("Continuing processing...")
        br = sp.read_csv('FACTURACION/FACTURACION.csv')
        br.loc[br[C.PO_NUM].astype(str).isin(intersection), [C.SUBTOTAL, C.DISCOUNT, C.SUBTOTAL_NET, C.VAT]] = 0
        sp.save_csv(br, 'FACTURACION/FACTURACION.csv')
    return po_nums


def pause_for_reprocess_decision(intersection):
    st.warning(f"PO {intersection} was already processed.")
    proceed = st.radio(
        "Do you want to continue processing this order anyway?",
        options=["No", "Yes"],
        index=None,
        horizontal=True
    )
    # Pause until user selects
    if proceed is None:
        st.info("Please select an option to continue.")
        st.stop()
    # Stop script if user chooses "No"
    if proceed == "No":
        st.info("Processing stopped for this order.")
        st.stop()







