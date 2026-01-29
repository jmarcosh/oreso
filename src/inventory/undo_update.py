from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

from inventory.common_app import record_log, filter_active_logs, \
    create_and_save_br_summary_table, update_inventory_in_memory, stop_if_locked_files, read_or_create_file, \
    save_purchases_file_and_logs, convert_numeric_id_cols_to_text
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient



def undo_inventory(sp, recovery_id, log_id, config):
    updated_inv = sp.read_csv(f"INVENTARIO/SNAPSHOTS/inventory_{recovery_id}.csv")
    update_inventory_in_memory(sp, updated_inv, updated_inv, log_id, config)

def undo_rfid(sp, recovery_id, config):
    customers = config.get("customers_rfid")
    for customer in customers:
        rfid_df = sp.read_excel(f"config/rfid_{customer}.xlsx")
        rfid_df[C.LOG_ID] = rfid_df[C.LOG_ID].where(rfid_df[C.LOG_ID] < recovery_id, np.nan)
        sp.save_excel(rfid_df, f"config/rfid_{customer}.xlsx")


def undo_records(sp, recovery_id, config):
    records = sp.read_csv("FACTURACION/FACTURACION.csv")
    records = records.loc[(records[C.LOG_ID] < recovery_id)]
    records[C.DELIVERY_DATE] = pd.to_datetime(records[C.DELIVERY_DATE]).dt.date
    sp.save_csv(records, "FACTURACION/FACTURACION.csv")
    create_and_save_br_summary_table(sp, records, config)

#TODO add season to log to know which receipt file to update
def undo_catalog(recovery_id, config):
    return


def undo_inventory_update(recovery_id=None):
    log_id = int(datetime.today().strftime('%Y%m%d%H%M%S'))
    sp = SharePointClient()
    stop_if_locked_files(sp)
    logs = sp.read_csv("logs/logs.csv")
    record_log(sp, logs, log_id, 'undo', 'undo_inventory_update', "started")
    if not recovery_id:
        recovery_id = logs.loc[(logs['status'] =='success') &
                               (logs['action'] != 'undo_inventory_update'), 'log_id'].values[-1]
    active_logs = filter_active_logs(logs)
    if recovery_id not in active_logs['log_id'].values:
        st.write("The log id should be active")
        st.stop()
    config = sp.read_json("config/config.json")
    undo_rfid(sp, recovery_id, config)
    undo_inventory(sp, recovery_id, log_id, config)
    undo_records(sp, recovery_id, config)
    undone_logs = active_logs.loc[active_logs['log_id'] >= recovery_id]
    undo_purchases_table(recovery_id, sp, undone_logs)
    record_log(sp, logs, log_id, 'undo', 'undo_inventory_update', "success", recovery_id)
    for folder_path in undone_logs.loc[undone_logs['action'] == 'withdrawal','files_path']:
        if pd.notna(folder_path):
            new_name = f"{folder_path.split('/')[-1]}_UNDO_{recovery_id}"
            sp.rename_folder(folder_path, new_name)
    return undone_logs[['log_id', 'customer', 'action', 'po']]


def undo_purchases_table(recovery_id, sp: SharePointClient, undone_logs):
    undone_receipts = \
    undone_logs.loc[undone_logs['action'].isin(['receipt', 'on_order']), 'files_path'].str.extract(r'/([^/]+)\.xlsx').iloc[:, 0]
    undone_updates = undone_logs.loc[(undone_logs['action'] == 'update'), 'po']
    undone_tables = (pd.concat([undone_receipts, undone_updates], ignore_index=True)
                     .dropna().unique().tolist())
    
    for table in undone_tables:
        purchases_logs = read_or_create_file(sp, f"COMPRAS/LOGS/logs_{table}.csv")
        purchases_logs.set_index([C.MOVEX_PO, C.UPC], inplace=True)
        pre_recovery = purchases_logs.loc[purchases_logs[C.LOG_ID] < recovery_id]
        purchases_last = pre_recovery.loc[~pre_recovery.index.duplicated(keep="last")]
        purchases = sp.read_csv(f"COMPRAS/LOGS/{table}.csv")
        convert_numeric_id_cols_to_text(purchases, [C.MOVEX_PO, C.UPC])
        purchases_columns = purchases.columns
        purchases.set_index([C.MOVEX_PO, C.UPC], inplace=True)
        purchases_index = purchases.index.intersection(purchases_last.index)
        purchases = purchases_last.loc[purchases_index].reset_index()[purchases_columns]
        save_purchases_file_and_logs(sp, purchases, table)



if __name__ == '__main__':
    undo_inventory_update()


# TODO add updated files to log

# def undo_entry_file(recovery_id):
#     entry_file = invoc.read_csv(f"RECIBOS/{season}.xlsx")
#     entry_file = entry_file.loc[(entry_file[C.LOG_ID] != recovery_id)]
#     invoc.save_excel(entry_file, f"RECIBOS/{season}.xlsx")

