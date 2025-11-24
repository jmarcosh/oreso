import streamlit as st

import pandas as pd
from inventory.varnames import ColNames as C


def record_active_logs(sp, logs_u):
    active_logs = filter_active_logs(logs_u)
    sp.save_csv(active_logs, "logs/logs_active.csv")


def record_log(sp, logs, log_id, customer, action, status, po_number=None, files_save_path=None):
    new_row = {"log_id": [log_id], "customer": [customer], "action": [action], "status": [status], "po": [po_number],
               "files_save_path": [files_save_path]}
    logs_u = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    sp.save_csv(logs_u,"logs/logs.csv")
    if status == "success":
        record_active_logs(sp, logs_u)


def stop_if_locked_files(sp):
    for file in ["INVENTARIO/SUMMARY.xlsx"]:
        df = sp.read_excel(file)
        try:
            sp.save_excel(df, file)
        except Exception as e:
            st.write(f"{e}. Run again")
            st.stop()




def create_and_save_br_summary_table(sp, po_br, config):
    br_summ_indexes = config['br_summ_indexes']
    br_summ_values = config['br_summ_values']
    summ = po_br.groupby(br_summ_indexes).agg(br_summ_values).sort_values(by=C.DELIVERY_DATE).reset_index()
    sp.save_excel(summ, 'FACTURACION/SUMMARY.xlsx')


def create_and_save_inventory_summary_table(sp, updated_inv, config):
    inv_summ_indexes = config.get('inventory_summ_indexes')
    inv_summ = updated_inv.groupby(inv_summ_indexes)[C.INVENTORY].sum().reset_index()
    sp.save_excel(inv_summ, 'INVENTARIO/SUMMARY.xlsx')

def filter_active_logs(logs):
    active_logs = logs.loc[logs['status'] == 'success'].copy()
    undo_pairs = logs.loc[logs['action'] == 'undo_inventory_update', ['po', 'log_id']]
    undo_pairs["po"] = undo_pairs["po"].fillna(undo_pairs["log_id"]).astype(float)
    for pair in undo_pairs.values:
        active_logs = active_logs.loc[(active_logs['log_id'] < pair[0]) | (active_logs['log_id'] > pair[1])]
    return active_logs

def update_inventory_in_memory(sp, updated_inv, inventory, log_id, config):
    updated_inv[C.RECEIVED_DATE] = pd.to_datetime(updated_inv[C.RECEIVED_DATE]).dt.date
    # for col in [C.WAREHOUSE_CODE, C.UPC, C.SKU]:
    #     updated_inv[col] = updated_inv[col].astype(int)
    sp.save_csv(updated_inv, 'INVENTARIO/INVENTARIO.csv')
    sp.save_csv(inventory, f'INVENTARIO/SNAPSHOTS/inventory_{log_id}.csv')
    create_and_save_inventory_summary_table(sp, updated_inv, config)
