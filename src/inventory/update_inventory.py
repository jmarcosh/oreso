import argparse
from datetime import datetime
import streamlit as st

from inventory.assign_warehouse_codes import assign_warehouse_codes_from_column_and_update_inventory
from inventory.common_app import record_log, update_inventory_in_memory
from inventory.update_inventory_utils import read_files, allocate_stock, update_billing_record, warn_processed_orders, \
    save_raw_po_and_create_file_paths
from inventory.process_internal_orders import run_internal_orders
from inventory.process_purchase_orders import run_process_purchase_orders
from inventory.receive_goods import receive_goods

from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient

def parse_rfid_series_simple(rfid_str):
    """
    Parse RFID series from format: C52767864-C52768000,C56916036-C56917000
    → Output: [['C52767864', 'C52768000'], ['C56916036', 'C56917000']]
    """
    try:
        series = []
        for pair in rfid_str.split(","):
            start, end = pair.strip().split("-")
            series.append([start, end])
        return series
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid RFID_SERIES format: {e}")


def run_update_inventory(delivery_date:str, temp_paths:list =[], update_from_sharepoint:str=False):
    # stop_if_locked_files()
    log_id = int(datetime.today().strftime('%Y%m%d%H%M%S'))
    sp = SharePointClient()
    logs = sp.read_csv("logs/logs.csv")
    po, inventory, config, po_type, matching_columns, action = read_files(sp, temp_paths, update_from_sharepoint)
    record_log(sp, logs, log_id, po_type, action, "started")
    po_nums = warn_processed_orders(logs, po, po_type, update_from_sharepoint)
    if po_type in config.get("customers"):
        customer = po_type
        po[C.DELIVERED] = allocate_stock(po, inventory, matching_columns)
        po[C.DELIVERY_DATE] = delivery_date
        po, updated_inv = assign_warehouse_codes_from_column_and_update_inventory(po, inventory, matching_columns, log_id)
        files_save_path = save_raw_po_and_create_file_paths(sp, customer, delivery_date, po, po_nums, log_id)
        if customer in config.get("customers_rfid"):
            po = run_process_purchase_orders(sp, po, config, customer, delivery_date, files_save_path, log_id)
            txn_key = "V"
        else: # customer == 'interno':
            run_internal_orders(sp, po, config, customer, files_save_path)
            txn_key = po.loc[0, C.PO_NUM].rsplit("_", 1)[-1][0]
            if not txn_key.isalpha():
                st.write("Internal orders should end with _KEY")
                st.stop()
        update_billing_record(sp, po, po_type, delivery_date, config, txn_key, log_id)
    else: # po_type == receipt
        updated_inv, files_save_path = receive_goods(sp, po, inventory, config, delivery_date,
                                                                      update_from_sharepoint, log_id)
    update_inventory_in_memory(sp, updated_inv, inventory, log_id, config)
    po_num = files_save_path.rsplit('/', 1)[-1].split('_', 1)[-1] if files_save_path \
        else update_from_sharepoint
    record_log(sp, logs, log_id, po_type, action, "success", po_num, files_save_path)
    return files_save_path



if __name__ == '__main__':
    DELIVERY_DATE = "06/10/2025"
    files_path = run_update_inventory(DELIVERY_DATE)  #, update_from_sharepoint="B25"
    # parser = argparse.ArgumentParser(description="Run PO Parser with delivery date and RFID series.")
    #
    # parser.add_argument("--date", type=str, required=True, help="Delivery date m/d/Y (e.g., '8/16/2025')")
    # parser.add_argument("--rfid", type=parse_rfid_series_simple, required=True,
    #                     help='RFID series, e.g., C52767864-C52768000,C56916036-C56917000')
    #
    # args = parser.parse_args()
    #
    #
    # run_po_parser(args.date, args.rfid)