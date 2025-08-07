from datetime import datetime

import pandas as pd

from inventory.common_app import record_log, stop_if_locked_files
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient
invoc = SharePointClient()

def undo_update(recovery_id=None):
    stop_if_locked_files()
    log_id = datetime.today().strftime('%Y%m%d%H%M%S')
    logs = invoc.read_csv("logs/logs.csv")
    record_log(logs, log_id, 'undo', 'undo update')
    if not recovery_id:
        recovery_id = logs.loc[(logs['status'] =='success') &
                               (logs['action'] != 'undo_update'), 'log_id'].values[-1]
    undo_inventory(recovery_id)
    undo_records(recovery_id)
    record_log(logs, log_id, 'undo', 'undo update', "success")


def undo_records(recovery_id):
    records = invoc.read_excel("FACTURACION/FACTURACION.xlsx")
    records = records.loc[(records[C.LOG_ID] != recovery_id)]
    records[C.DELIVERY_DATE] = pd.to_datetime(records[C.DELIVERY_DATE]).dt.date
    invoc.save_excel(records, "FACTURACION/FACTURACION.xlsx")


def undo_inventory(recovery_id):
    inventory = invoc.read_csv(f"INVENTARIO/SNAPSHOTS/inventory_{recovery_id}.csv", encoding="latin1")
    invoc.save_excel(inventory, f"INVENTARIO/INVENTARIO.xlsx")

# TODO add updated files to log
# def undo_entry_file(recovery_id):
#     entry_file = invoc.read_csv(f"RECIBOS/{season}.xlsx")
#     entry_file = entry_file.loc[(entry_file[C.LOG_ID] != recovery_id)]
#     invoc.save_excel(entry_file, f"RECIBOS/{season}.xlsx")


if __name__ == '__main__':
    undo_update()