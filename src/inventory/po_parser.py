import argparse
from datetime import datetime

import pandas as pd

from src.inventory.common import read_files_and_backup_inventory, allocate_stock, \
    assign_warehouse_codes_from_column_and_update_inventory
from src.inventory.manual_adjustments import run_manual_adjustments
from src.inventory.process_purchase_orders import run_process_purchase_orders
from src.inventory.varnames import ColNames as C
from src.api_integrations.sharepoint_client import SharePointClient
invoc = SharePointClient()

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

def record_log(log_id, status='started'):
    logs = invoc.read_csv("logs/logs.csv")
    if status == 'success':
        logs = logs.iloc[:-1]
    new_row = {"log_id": [log_id], "status": [status]}
    logs = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    invoc.save_csv(logs,"logs/logs.csv")




def run_po_parser(delivery_date, rfid_series=None):
    log_id = datetime.today().strftime('%Y%m%d%H%M%S')
    record_log(log_id)
    po, inventory, config, customer, matching_column = read_files_and_backup_inventory(log_id)
    po[C.DELIVERED] = allocate_stock(po, inventory, matching_column)
    po[C.DELIVERY_DATE] = delivery_date
    po = assign_warehouse_codes_from_column_and_update_inventory(po, inventory, matching_column)
    files_save_path = save_raw_po_and_create_file_paths(customer, delivery_date, po)
    if customer in ['liverpool', 'suburbia']:
        run_process_purchase_orders(po, config, customer, delivery_date, files_save_path, rfid_series)
    else:
        po[C.STORE_ID] = 0
        run_manual_adjustments(po, config, customer, delivery_date, files_save_path)
    record_log(log_id, "success")


def save_raw_po_and_create_file_paths(customer, delivery_date, po):
    po_nums = po[C.PO_NUM].unique()
    po_save_path = f"OC/RAW/{customer}"
    invoc.create_folder_path(po_save_path)
    for po_num in po_nums:
        invoc.save_csv(po[po[C.PO_NUM] == po_num], f"{po_save_path}/{po_num}.csv")
    po_num = "_".join(map(str, po_nums))
    files_save_path = f"OC/{customer}/{delivery_date.split('/')[2]}/{delivery_date.split('/')[0]}/{str(po_num)}"
    invoc.create_folder_path(files_save_path)
    return files_save_path


if __name__ == '__main__':
    RFID_SERIES = [['C52767864', 'C52768000'],
                   ['C56916036', 'C56917000']]
    DELIVERY_DATE = "8/16/2025"
    run_po_parser(DELIVERY_DATE, RFID_SERIES)
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