import sys
import re

import pandas as pd

from src.api_integrations.sharepoint_client import SharePointClient
invoc = SharePointClient()


def record_log(logs, log_id, customer, action, status='started'):
    new_row = {"log_id": [log_id], "customer": [customer], "action": [action], "status": [status]}
    logs = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    invoc.save_csv(logs,"logs/logs.csv")


def stop_if_locked_files():
    for file in ["INVENTARIO/INVENTARIO.xlsx", "FACTURACION/FACTURACION.xlsx"]:
        if invoc.is_excel_file_locked(file):
            sys.exit(f"Close {file.split('/', 1)[-1]} and start again!")


def validate_rfid_series(rfid_series_str: str) -> bool:
    if not rfid_series_str.strip():
        return True  # empty allowed

    ranges = [r.strip() for r in rfid_series_str.split(',')]

    prefix = None
    prev_end_num = None

    for r in ranges:
        # Validate format: prefix + digits - prefix + digits
        m = re.fullmatch(r'(C\d{8}|SB\d{7})-(C\d{8}|SB\d{7})', r)
        if not m:
            return False

        start, end = r.split('-')

        # On first range, save prefix
        if prefix is None:
            if start.startswith('C'):
                prefix = 'C'
            elif start.startswith('SB'):
                prefix = 'SB'
            else:
                return False
        # All subsequent ranges must have the same prefix
        if not start.startswith(prefix) or not end.startswith(prefix):
            return False

        # Remove prefix to get numeric parts
        start_num = int(start[len(prefix):])
        end_num = int(end[len(prefix):])

        # Check start <= end for each range
        if start_num > end_num:
            return False

        # Check strictly increasing ranges: start > previous range's end
        if prev_end_num is not None and start_num <= prev_end_num:
            return False

        prev_end_num = end_num

    return True
