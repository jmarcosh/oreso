import pandas as pd

from src.api_integrations.sharepoint_client import SharePointClient
invoc = SharePointClient()


def record_log(logs, log_id, customer, action, status='started'):
    new_row = {"log_id": [log_id], "customer": [customer], "action": [action], "status": [status]}
    logs = pd.concat([logs, pd.DataFrame(new_row)], ignore_index=True)
    invoc.save_csv(logs,"logs/logs.csv")