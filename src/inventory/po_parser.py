import argparse
from datetime import datetime
import streamlit as st

from inventory.common_app import record_log, stop_if_locked_files
from inventory.common_parser import read_files, allocate_stock, \
    assign_warehouse_codes_from_column_and_update_inventory, update_billing_record, update_inventory_in_memory
from inventory.internal_orders import run_internal_orders
from inventory.process_purchase_orders import run_process_purchase_orders
from inventory.receive_goods import receive_goods

from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import invoc

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


def save_raw_po_and_create_file_paths(customer, delivery_date, po, log_id):
    po_nums = po[C.PO_NUM].unique()
    po_save_path = f"OC/RAW/{customer.title()}"
    invoc.create_folder_path(po_save_path)
    for po_num in po_nums:
        invoc.save_csv(po[po[C.PO_NUM] == po_num], f"{po_save_path}/{po_num}.csv")
    po_num = "_".join(map(str, po_nums))
    files_save_path = f"OC/{customer.title()}/{delivery_date.split('/')[2]}/{delivery_date.split('/')[0]}/{log_id}_{str(po_num)}"
    invoc.create_folder_path(files_save_path)
    return files_save_path

def run_po_parser(delivery_date:str, temp_paths:list =[], update_from_sharepoint:str=None):
    # stop_if_locked_files()
    log_id = datetime.today().strftime('%Y%m%d%H%M%S')
    logs = invoc.read_csv("logs/logs.csv")
    po, inventory, config, po_type, matching_column, action = read_files(temp_paths, update_from_sharepoint)
    record_log(logs, log_id, po_type, action, "started")
    if po_type in config.get("customers"):
        customer = po_type
        po[C.DELIVERED] = allocate_stock(po, inventory, matching_column)
        po[C.DELIVERY_DATE] = delivery_date
        po, updated_inv = assign_warehouse_codes_from_column_and_update_inventory(po, inventory, matching_column, log_id)
        files_save_path = save_raw_po_and_create_file_paths(customer, delivery_date, po, log_id)
        if customer in config.get("customers_rfid"):
            po = run_process_purchase_orders(po, config, customer, delivery_date, files_save_path, log_id)
            txn_key = "V"
        else: # customer == 'interno':
            run_internal_orders(po, config, customer, files_save_path)
            txn_key = po.loc[0, C.PO_NUM].rsplit("_", 1)[-1][0]
            if not txn_key.isalpha():
                st.write("Internal orders should end with _[KEY]")
                st.stop()
        update_billing_record(po, po_type, delivery_date, config, txn_key, log_id)
    else: # po_type == receipt
        po, updated_inv, files_save_path, txn_key = receive_goods(po, inventory, config, delivery_date,
                                                                      update_from_sharepoint, log_id)
    update_inventory_in_memory(updated_inv, inventory, log_id, config)
    record_log(logs, log_id, po_type, action, "success")
    return files_save_path



if __name__ == '__main__':
    DELIVERY_DATE = "09/08/2025"
    files_path = run_po_parser(DELIVERY_DATE) #, update_from_sharepoint="B25"
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