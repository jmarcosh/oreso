import sys

import pandas as pd
from inventory.varnames import ColNames as C



def record_log(sp, logs, log_id, customer, action, status, po_number=None, files_save_path=None):
    new_row = {"log_id": [log_id], "customer": [customer], "action": [action], "status": [status], "po": [po_number],
               "files_save_path": [files_save_path]}
    logs_u = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    sp.save_csv(logs_u,"logs/logs.csv")


def stop_if_locked_files(sp):
    for file in ["INVENTARIO/INVENTARIO.xlsx", "FACTURACION/FACTURACION.xlsx"]:
        if sp.is_excel_file_locked(file):
            sys.exit(f"Close {file.split('/', 1)[-1]} and start again!")




def create_and_save_br_summary_table(sp, po_br, config):
    br_summ_indexes = config['br_summ_indexes']
    br_summ_values = config['br_summ_values']
    summ = po_br.groupby(br_summ_indexes).agg(br_summ_values).sort_values(by=C.DELIVERY_DATE)
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
