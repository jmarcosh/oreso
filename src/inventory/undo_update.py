import re
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
from pandas import DataFrame

from inventory.common_app import record_log, filter_active_logs, \
    create_and_save_br_summary_table, update_inventory_in_memory, stop_if_locked_files, read_or_create_file, \
    save_purchases_file_and_logs, convert_numeric_id_cols_to_text, add_nan_cols, normalize_date_cols
from inventory.update_items import find_common_rows_with_inventory, get_active_inactive_changes, \
    update_inventory_from_purchases, insert_and_delete_status_rows, restore_inventory_row_and_columns_order
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient



def undo_rfid(sp, recovery_id, undo_log):
    customer = undo_log['po_type']
    rfid_df = sp.read_excel(f"config/rfid_{customer}.xlsx")
    log_id_num = pd.to_numeric(rfid_df[C.LOG_ID], errors='coerce')
    undo = (log_id_num == float(recovery_id))
    rfid_df[C.LOG_ID] = rfid_df[C.LOG_ID].where(~undo, np.nan)
    sp.save_excel(rfid_df, f"config/rfid_{customer}.xlsx")


def undo_withdrawal_in_inventory(sp, recovery_id, config):
    records = sp.read_csv("FACTURACION/FACTURACION.csv")
    logid_condition = records[C.LOG_ID] == recovery_id
    undo = records.loc[logid_condition].copy()
    records = records.loc[~logid_condition]
    records[C.DELIVERY_DATE] = pd.to_datetime(records[C.DELIVERY_DATE]).dt.date
    inventory = sp.read_csv(f"INVENTARIO/SNAPSHOTS/INVENTARIO.csv")

    merge_cols = [C.MOVEX_PO, C.UPC]
    # for df in [undo, inventory]:
    #     convert_numeric_id_cols_to_text(df, merge_cols)
    inventory_index = inventory.index
    updated_inv = inventory.merge(undo[merge_cols + [C.DELIVERED]], on=merge_cols, how="left")
    updated_inv.index = inventory_index
    updated_inv[C.DELIVERED] = updated_inv[C.DELIVERED].fillna(0)
    updated_inv[C.INVENTORY] = updated_inv[C.INVENTORY] + updated_inv[C.DELIVERED]
    updated_inv = updated_inv.drop(columns=[C.DELIVERED])

    update_inventory_in_memory(sp, updated_inv, updated_inv, recovery_id, config)
    sp.save_csv(records, "FACTURACION/FACTURACION.csv")
    create_and_save_br_summary_table(sp, records, config)

#TODO add season to log to know which receipt file to update
def undo_catalog(recovery_id, config):
    return


def undo_inventory_update(undo_id=None):
    log_id = int(datetime.today().strftime('%Y%m%d%H%M%S'))
    sp = SharePointClient()
    stop_if_locked_files(sp)
    logs = sp.read_csv("logs/logs.csv")
    record_log(sp, logs, log_id, 'undo', 'undo_inventory_update', "started")
    if not undo_id:
        undo_id = logs.loc[(logs['status'] == 'success') &
                           (logs['action'] != 'undo_inventory_update'), 'log_id'].values[-1]
    active_logs = filter_active_logs(logs)
    if undo_id not in active_logs['log_id'].values:
        st.write("The log id should be active")
        st.stop()
    config = sp.read_json("config/config.json")

    undo_log = active_logs.loc[active_logs['log_id'] == undo_id].squeeze()
    action = undo_log['action']
    if action == 'withdrawal':
        undo_rfid(sp, undo_id, undo_log)
        undo_withdrawal_in_inventory(sp, undo_id, config)
        folder_path = undo_log['files_path']
        if pd.notna(folder_path):
            new_name = f"{folder_path.split('/')[-1]}_UNDO_{log_id}"
            sp.rename_folder(folder_path, new_name)
    elif action in ['purchase', 'update']:
        undo_purchases_table(sp, undo_id, undo_log, action, log_id, config)
    elif action == 'receipt':
        st.write('Undo receipts not supported yet')
        st.stop()
    record_log(sp, logs, log_id, 'undo', 'undo_inventory_update', "success", undo_id)
    return undo_log[['log_id', 'po_type', 'action', 'po']]


def undo_purchases_table(sp: SharePointClient, undo_id: int, undo_log: DataFrame, action:str, log_id: int, config: dict):
    files_path = undo_log['files_path']
    if np.isnan(files_path):
        undo_table = undo_log['po']
    else:
        match = re.search(r'/([^/]+)\.xlsx', undo_log['files_path'])
        undo_table = match.group(1) if match else None
    purchases_columns = config['purchases_columns']
    purchases_logs = read_or_create_file(sp, f"COMPRAS/LOGS/logs_{undo_table}.csv")
    purchases_logs.set_index([C.MOVEX_PO, C.UPC], inplace=True)
    if C.ACTION not in purchases_logs.columns:
        purchases_logs[C.ACTION] = np.nan

    undone = purchases_logs.loc[purchases_logs[C.LOG_ID] == undo_id]
    prior = purchases_logs.loc[(purchases_logs[C.LOG_ID] < undo_id) & purchases_logs.index.isin(undone.index)]
    # Rows the undone run had modified go back to their last previous state. Rows it created have no
    # previous state, so they are appended as 'undo' tombstones and dropped from the purchases table.
    restored = prior.loc[~prior.index.duplicated(keep="last")]
    removed = undone.loc[~undone.index.isin(restored.index)].copy()
    removed[C.ACTION] = 'removed'
    reverted = pd.concat([restored, removed])
    reverted[C.LOG_ID] = log_id

    purchases_logs = pd.concat([purchases_logs, reverted])
    purchases_last = purchases_logs.loc[~purchases_logs.index.duplicated(keep="last")]
    purchases_last = purchases_last.loc[purchases_last[C.ACTION] != 'removed']

    purchases = sp.read_csv(f"COMPRAS/LOGS/{undo_table}.csv")
    convert_numeric_id_cols_to_text(purchases, [C.MOVEX_PO, C.UPC])
    purchases.set_index([C.MOVEX_PO, C.UPC], inplace=True)
    purchases_index = purchases.index.intersection(purchases_last.index)
    purchases_new = purchases_last.loc[purchases_index].reset_index()
    purchases_logs = purchases_logs.reset_index()
    for df in [purchases_new, purchases_logs]:
        add_nan_cols(df, purchases_columns)
    purchases_new = purchases_new[purchases_columns]
    purchases_logs = purchases_logs[purchases_columns + [C.ACTION]]
    save_purchases_file_and_logs(sp, undo_table, purchases_new, purchases_logs)
    inventory = sp.read_csv(f"INVENTARIO/SNAPSHOTS/INVENTARIO.csv")
    convert_numeric_id_cols_to_text(inventory, [C.MOVEX_PO, C.UPC, C.SKU, C.WAREHOUSE_CODE])
    inventory = normalize_date_cols(inventory)

    if action == 'purchase':
        # The undone run created these rows, so they leave the inventory altogether.
        updated_inv = inventory.set_index([C.MOVEX_PO, C.UPC])
        active_to_inactive = removed.index
        updated_inv = updated_inv.loc[~updated_inv.index.isin(active_to_inactive)]
    else: # action == 'update':
        updated_inv, common_index = find_common_rows_with_inventory(inventory, purchases_new)

        active_to_inactive, inactive_to_warehouse, inactive_to_on_order = get_active_inactive_changes(common_index,
                                                                                                      purchases_new,
                                                                                                      updated_inv)
        purchases_new, updated_inv = update_inventory_from_purchases(common_index, log_id, purchases_new, updated_inv)
        purchases_new, updated_inv = insert_and_delete_status_rows(active_to_inactive, inactive_to_on_order,
                                                                   inactive_to_warehouse, log_id, purchases_new, updated_inv)

    updated_inv = restore_inventory_row_and_columns_order(inventory, updated_inv, active_to_inactive)

    update_inventory_in_memory(sp, updated_inv, inventory, log_id, config)



if __name__ == '__main__':
    undo_inventory_update(20260827180041)


# TODO add updated files to log

# def undo_entry_file(recovery_id):
#     entry_file = invoc.read_csv(f"RECIBOS/{season}.xlsx")
#     entry_file = entry_file.loc[(entry_file[C.LOG_ID] != recovery_id)]
#     invoc.save_excel(entry_file, f"RECIBOS/{season}.xlsx")

