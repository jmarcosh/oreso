from datetime import datetime

import numpy as np
import pandas as pd

from inventory.common_app import record_log, stop_if_locked_files, \
    create_and_save_br_summary_table, create_and_save_inventory_summary_table
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import invoc



def undo_inventory(recovery_id, config):
    updated_inv = invoc.read_csv(f"INVENTARIO/SNAPSHOTS/inventory_{recovery_id}.csv", encoding="latin1")
    updated_inv[C.RECEIVED_DATE] = pd.to_datetime(updated_inv[C.RECEIVED_DATE]).dt.date
    invoc.save_csv(f"INVENTARIO/SNAPSHOTS/inventory_{recovery_id}.csv", encoding="latin1") #save with the new date to keep track of changes
    invoc.save_excel(updated_inv, 'INVENTARIO/INVENTARIO.xlsx')
    create_and_save_inventory_summary_table(updated_inv, config)

def undo_rfid(recovery_id, config):
    customers = config.get("rfid_customers")
    for customer in customers:
        rfid_df = invoc.read_csv(f"config/rfid_{customer}.csv")
        rfid_df[C.LOG_ID] = rfid_df[C.LOG_ID].where(rfid_df[C.LOG_ID] < recovery_id, np.nan)
        invoc.save_csv(rfid_df, f"config/rfid_{customer}.csv")


def undo_records(recovery_id, config):
    records = invoc.read_excel("FACTURACION/FACTURACION.xlsx")
    records = records.loc[(records[C.LOG_ID] < recovery_id)]
    records[C.DELIVERY_DATE] = pd.to_datetime(records[C.DELIVERY_DATE]).dt.date
    invoc.save_excel(records, "FACTURACION/FACTURACION.xlsx")
    create_and_save_br_summary_table(records, config)

#TODO add season to log to know which receipt file to update
def undo_receipt(recovery_id, config):
    return


def undo_inventory_update(recovery_id=None):
    # stop_if_locked_files()
    log_id = datetime.today().strftime('%Y%m%d%H%M%S')
    logs = invoc.read_csv("logs/logs.csv")
    record_log(logs, log_id, 'undo', 'undo update', "started")
    if not recovery_id:
        recovery_id = logs.loc[(logs['status'] =='success') &
                               (logs['action'] != 'undo_inventory_update'), 'log_id'].values[-1]
    config = invoc.read_json("config/config.json")

    undo_rfid(recovery_id, config)
    undo_inventory(recovery_id, config)
    undo_records(recovery_id, config)
    record_log(logs, log_id, 'undo', 'undo_inventory_update', "success")
    return logs.loc[logs['log_id'] >= recovery_id, ['log_id', 'action']]


if __name__ == '__main__':
    undo_inventory_update()


# TODO add updated files to log
# TODO add recovery_id to log

# def undo_entry_file(recovery_id):
#     entry_file = invoc.read_csv(f"RECIBOS/{season}.xlsx")
#     entry_file = entry_file.loc[(entry_file[C.LOG_ID] != recovery_id)]
#     invoc.save_excel(entry_file, f"RECIBOS/{season}.xlsx")

