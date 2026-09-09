from typing import Any
import re
from datetime import date
import numpy as np
import requests
import streamlit as st
import pandas as pd
from pandas import DataFrame

from inventory.varnames import ColNames as C

INVENTORY_LOG_FOLDER = 'INVENTARIO/LOGS'
INVENTORY_LOG_PATH = f'{INVENTORY_LOG_FOLDER}/logs_inventory.csv'
INVENTORY_KEYS = [C.MOVEX_PO, C.UPC]


def record_active_logs(sp, logs_u):
    active_logs = filter_active_logs(logs_u)
    sp.save_csv(active_logs, "logs/logs_active.csv")


def record_log(sp, logs, log_id, po_type, action, status, po_number=None, files_save_path=None):
    new_row = {"log_id": [log_id], "po_type": [po_type], "action": [action], "status": [status], "po": [po_number],
               "files_path": [files_save_path]}
    logs_u = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    sp.save_csv(logs_u,"logs/logs.csv")
    if status == "success":
        record_active_logs(sp, logs_u)


def stop_if_locked_files(sp, additional_files: list | None=None):
    files = ["INVENTARIO/SUMMARY.xlsx"]
    if additional_files:
        files += additional_files
    for file in files:
        df = sp.read_excel(file)
        sp.save_excel(df, file)


def create_and_save_br_summary_table(sp, po_br, config):
    br_summ_indexes = config['br_summ_indexes']
    br_summ_values = config['br_summ_values']
    summ = po_br.groupby(br_summ_indexes).agg(br_summ_values).sort_values(by=C.DELIVERY_DATE).reset_index()
    sp.save_excel(summ, 'FACTURACION/SUMMARY.xlsx')


def create_and_save_inventory_summary_table(sp, updated_inv, config):
    inv_summ_indexes = config.get('inventory_summ_indexes')
    warehouses = [item for lst in config.get("item_status").values() for item in lst]
    warehouses = [w for w in warehouses if w in updated_inv[C.WAREHOUSE].unique()]
    inv_pivot = updated_inv.pivot_table(
        index=inv_summ_indexes,
        columns=C.WAREHOUSE,
        values=C.INVENTORY,
        aggfunc='sum',
        fill_value=0)
    on_order = updated_inv[updated_inv[C.WAREHOUSE] == 'on_order']
    on_order_summ = on_order.groupby(inv_summ_indexes)[C.RECEIVED].sum()
    inv_pivot['on_order'] = inv_pivot.index.map(on_order_summ)
    inv_pivot = inv_pivot[warehouses].reset_index()

# inv_summ = updated_inv.groupby(inv_summ_indexes)[C.INVENTORY].sum().reset_index()
    # on_order = updated_inv[updated_inv[C.WAREHOUSE] == 'on_order']
    # on_order_pivot = on_order.pivot(index=inv_summ_indexes, columns=C.RECEIVED_DATE, values=C.RECEIVED).reset_index()
    # inv_summ = inv_summ.merge(on_order_pivot, on=inv_summ_indexes, how='left')
    sp.save_excel(inv_pivot, 'INVENTARIO/SUMMARY.xlsx')

def filter_active_logs(logs):
    undo_logs = logs.loc[logs['action'] == 'undo_inventory_update', 'po'].dropna().astype(int).values
    active_logs = logs.loc[(logs['status'] == 'success') & (logs['action'] != 'undo_inventory_update') &
                           ~(logs['log_id'].isin(undo_logs))].copy()
    return active_logs

class InventoryLog:
    """Collects inventory rows as they are modified, so an action appends only the rows it touched
    instead of dropping a full snapshot. Rows are logged where the change happens, which also lets a
    caller keep context columns the inventory table itself does not have, e.g. C.DELIVERED."""

    def __init__(self, log_id: int):
        self.log_id = log_id
        self.entries = []

    def add(self, rows: DataFrame, action: str):
        """`action` says what happened to the row: 'added', 'modified' or 'removed'. Added and
        modified rows are logged in their post change state, removed ones as they were last seen."""
        if rows.empty:
            return
        entry = rows.reset_index() if set(INVENTORY_KEYS).issubset(rows.index.names) else rows.copy()
        entry[C.ACTION] = action
        entry[C.LOG_ID] = self.log_id
        self.entries.append(entry)

    def save(self, sp):
        if not self.entries:
            return
        log = read_or_create_file(sp, INVENTORY_LOG_PATH)
        if log.empty:
            sp.create_folder_path(INVENTORY_LOG_FOLDER)
        sp.save_csv(pd.concat([log, *self.entries], ignore_index=True), INVENTORY_LOG_PATH)


def update_inventory_in_memory(sp, updated_inv, inv_log: 'InventoryLog', config):
    sp.save_csv(updated_inv, 'INVENTARIO/INVENTARIO.csv')
    sp.save_csv(updated_inv, 'INVENTARIO/SNAPSHOTS/INVENTARIO.csv')
    inv_log.save(sp)
    create_and_save_inventory_summary_table(sp, updated_inv, config)


def extract_size_from_style(df) -> list[Any | None]:
    return [x.rsplit('-', 1)[-1] if '-' in x else None for x in df[C.STYLE]]

def warn_processed_orders(logs, po, po_type):
    if po_type == "supplier":
        po_num_column = po[C.MOVEX_PO]
    elif po_type == "receipt":
        po_num_column = po[C.RD]
    else:
        po_num_column = po[C.PO_NUM]
    intersection, po_nums = find_processed_orders(logs, po_num_column)
    if (len(intersection) > 0) and not st.session_state.get("ignore_processed", False):
        st.error(f"PO {intersection} was already processed.")
        st.stop()
    return po_nums


def find_processed_orders(logs, po_num_column) -> tuple[list[str], list[str]]:
    po_nums = [str(x) for x in po_num_column.unique()]
    active_logs = filter_active_logs(logs)
    prev_po_nums = []
    for item in active_logs["po"].dropna():
        # split on _ if it is followed by a digit
        parts = re.split(r'_(?=\d)', item)
        parts = [re.sub(r"\.0$", "", x) for x in parts]
        prev_po_nums.extend(parts)
    intersection = list(set(po_nums) & set(prev_po_nums))
    return intersection, po_nums

def convert_numeric_id_cols_to_text(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = (pd.to_numeric(df[col], errors='coerce').fillna(df[col].fillna(0))
                       .astype(str).replace(r'\..*$', '', regex=True))

def create_and_save_techsmart_txt_file(sp, po, customer, config, files_save_path):
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
    # one file per ('Tipo', '# OC') pair, techsmart does not accept mixed files
    for (tipo, po_num), ts_group in ts.groupby(['Tipo', '# OC'], dropna=False, sort=False):
        file_name = f"techsmart_{str(po_num)}_{tipo}"
        sp.save_csv(ts_group[ts_columns_txt], f"{files_save_path}/{file_name}.txt", sep='\t')
        sp.save_csv(ts_group.reset_index()[ts_columns_txt], f"{files_save_path}/{file_name}.csv")
    return ts[ts_columns_csv]


def add_nan_cols(df, cols):
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

def validate_unique_ids_and_status_in_updatable_table(purchases: DataFrame, config: dict):
    duplicated = purchases.duplicated(subset=[C.MOVEX_PO, C.UPC], keep=False)
    has_duplicates = duplicated.any()
    if has_duplicates:
        st.write("Fix the MOVEX PO for duplicated products.")
        st.dataframe(
            purchases.loc[duplicated, [C.STYLE, C.MOVEX_PO, C.UPC]],
            use_container_width=True,
            hide_index=True,
            column_config={C.MOVEX_PO: st.column_config.NumberColumn(format="%.0f"),
                           C.UPC: st.column_config.NumberColumn(format="%.0f"), }
        )
        st.stop()
    valid_status = set({item for lst in config.get("item_status").values() for item in lst})
    is_valid_status = set(purchases[C.WAREHOUSE].unique()) <= valid_status
    if not is_valid_status:
        st.write(f"{C.WAREHOUSE} values must be one of {valid_status}")
        st.stop()

def validate_rfid_series(rfid_series_str: str) -> bool:
    if not rfid_series_str.strip():
        return True  # empty allowed

    ranges = [r.strip() for r in rfid_series_str.split(',')]

    prefix = None
    prev_end_num = None

    for r in ranges:
        # Validate format: prefix + digits - prefix + digits
        m = re.fullmatch(r'(C\d{8}|SB\d{7})-(C\d{8}|SB\d{7})', r)
        if not m:
            return False

        start, end = r.split('-')

        # On first range, save prefix
        if prefix is None:
            if start.startswith('C'):
                prefix = 'C'
            elif start.startswith('SB'):
                prefix = 'SB'
            else:
                return False
        # All following ranges must have the same prefix
        if not start.startswith(prefix) or not end.startswith(prefix):
            return False

        # Remove prefix to get numeric parts
        start_num = int(start[len(prefix):])
        end_num = int(end[len(prefix):])

        # Check start <= end for each range
        if start_num > end_num:
            return False

        # Check strictly increasing ranges: start > previous range's end
        if prev_end_num is not None and start_num <= prev_end_num:
            return False

        prev_end_num = end_num

    return True

def normalize_date_cols(df: DataFrame):
    """`hard_memory` comes from a CSV and `purchases` from an Excel file, so dates arrive as strings on
    one side and as timestamps on the other. Parse both so they compare on value, not on dtype."""
    for col in [C.RECEIVED_DATE, C.X_FTY, C.DELIVERY_DATE]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed').dt.date

def save_purchases_file_and_logs(sp, table, purchases: DataFrame, purchases_logs=None):
    normalize_date_cols(purchases)
    if purchases_logs is not None:
        normalize_date_cols(purchases_logs)
        sp.save_csv(purchases_logs, f"COMPRAS/LOGS/logs_{table}.csv")
    sp.save_excel(purchases, f"COMPRAS/{table}.xlsx")
    sp.save_csv(purchases, f"COMPRAS/LOGS/{table}.csv")

def read_or_create_file(sp, file_path):
    """
    Reads an existing file from SharePoint or creates a new empty DataFrame.

    Parameters:
    - sp: SharePointClient instance
    - file_path: str, the SharePoint path to the file.

    Returns:
    - pd.DataFrame: existing file data or empty DataFrame
    """
    try:
        # Try to read the existing master file from SharePoint
        if file_path.endswith(".xlsx"):
            purchases = sp.read_excel(file_path)
        elif file_path.endswith(".csv"):
            purchases = sp.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_path}")
        convert_numeric_id_cols_to_text(purchases, [C.UPC, C.SKU, C.MOVEX_PO, C.WAREHOUSE_CODE])
        return purchases
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # File does not exist yet — create a new DataFrame
            return pd.DataFrame()
        else:
            # Other HTTP error occurred — log and exit
            print(f"Failed to read file: HTTP {e.response.status_code}")
            return None