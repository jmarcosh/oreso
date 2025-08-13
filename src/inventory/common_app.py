import sys

import pandas as pd
from inventory.varnames import ColNames as C

from api_integrations.sharepoint_client import invoc


def record_log(logs, log_id, customer, action, status):
    new_row = {"log_id": [log_id], "customer": [customer], "action": [action], "status": [status]}
    logs_u = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    invoc.save_csv(logs_u,"logs/logs.csv")


def stop_if_locked_files():
    for file in ["INVENTARIO/INVENTARIO.xlsx", "FACTURACION/FACTURACION.xlsx"]:
        if invoc.is_excel_file_locked(file):
            sys.exit(f"Close {file.split('/', 1)[-1]} and start again!")




def create_and_save_br_summary_table(po_br, config):
    br_summ_indexes = config["br_summ_indexes"]
    br_summ_values = config["br_summ_values"]
    summ = po_br.groupby(br_summ_indexes).agg(br_summ_values).sort_values(by=C.DELIVERY_DATE)
    invoc.save_excel(summ, 'FACTURACION/SUMMARY.xlsx')


def create_and_save_inventory_summary_table(updated_inv, config):
    inv_summ_indexes = config.get('inventory_summ_indexes')
    inv_summ = updated_inv.groupby(inv_summ_indexes)[C.INVENTORY].sum().reset_index()
    invoc.save_excel(inv_summ, 'INVENTARIO/SUMMARY.xlsx')