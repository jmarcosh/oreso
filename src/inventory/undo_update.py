from datetime import datetime

import numpy as np
import pandas as pd

from inventory.common_app import record_log, filter_active_logs, \
    create_and_save_br_summary_table, create_and_save_inventory_summary_table
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient



def undo_inventory(sp, recovery_id, log_id, config):
    updated_inv = sp.read_csv(f"INVENTARIO/SNAPSHOTS/inventory_{recovery_id}.csv", encoding="latin1")
    updated_inv[C.RECEIVED_DATE] = pd.to_datetime(updated_inv[C.RECEIVED_DATE]).dt.date
    sp.save_csv(updated_inv, f'INVENTARIO/SNAPSHOTS/inventory_{log_id}.csv') #save with the new date to keep track of changes
    sp.save_excel(updated_inv, 'INVENTARIO/INVENTARIO.xlsx')
    create_and_save_inventory_summary_table(sp, updated_inv, config)

def undo_rfid(sp, recovery_id, config):
    customers = config.get("customers_rfid")
    for customer in customers:
        rfid_df = sp.read_csv(f"config/rfid_{customer}.csv")
        rfid_df[C.LOG_ID] = rfid_df[C.LOG_ID].where(rfid_df[C.LOG_ID] < recovery_id, np.nan)
        sp.save_csv(rfid_df, f"config/rfid_{customer}.csv")


def undo_records(sp, recovery_id, config):
    records = sp.read_excel("FACTURACION/FACTURACION.xlsx")
    records = records.loc[(records[C.LOG_ID] < recovery_id)]
    records[C.DELIVERY_DATE] = pd.to_datetime(records[C.DELIVERY_DATE]).dt.date
    sp.save_excel(records, "FACTURACION/FACTURACION.xlsx")
    create_and_save_br_summary_table(sp, records, config)

#TODO add season to log to know which receipt file to update
def undo_catalog(recovery_id, config):
    return


def undo_inventory_update(recovery_id=None):
    # stop_if_locked_files()
    log_id = datetime.today().strftime('%Y%m%d%H%M%S')
    sp = SharePointClient()
    logs = sp.read_csv("logs/logs.csv")
    record_log(sp, logs, log_id, 'undo', 'undo update', "started")
    if not recovery_id:
        recovery_id = logs.loc[(logs['status'] =='success') &
                               (logs['action'] != 'undo_inventory_update'), 'log_id'].values[-1]
    config = sp.read_json("config/config.json")

    undo_rfid(sp, recovery_id, config)
    undo_inventory(sp, recovery_id, log_id, config)
    undo_records(sp, recovery_id, config)
    record_log(sp, logs, log_id, 'undo', 'undo_inventory_update', "success", recovery_id)
    active_logs = filter_active_logs(logs)
    undone_logs = active_logs.loc[active_logs['log_id'] >= recovery_id]
    for folder_path in undone_logs['files_save_path']:
        if pd.notna(folder_path):
            new_name = f"{folder_path.split('/')[-1]}_UNDO_{recovery_id}.csv"
            sp.rename_folder(folder_path, new_name)
    return undone_logs[['log_id', 'customer', 'action', 'po']]


if __name__ == '__main__':
    undo_inventory_update(20250825191916)


# TODO add updated files to log
# TODO add recovery_id to log

# def undo_entry_file(recovery_id):
#     entry_file = invoc.read_csv(f"RECIBOS/{season}.xlsx")
#     entry_file = entry_file.loc[(entry_file[C.LOG_ID] != recovery_id)]
#     invoc.save_excel(entry_file, f"RECIBOS/{season}.xlsx")

