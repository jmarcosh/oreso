from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st
from pandas import DataFrame, Index, Series

from api_integrations.sharepoint_client import SharePointClient
from inventory.common_app import (stop_if_locked_files, record_log, update_inventory_in_memory,
                                  extract_size_from_style, warn_processed_orders, create_and_save_techsmart_txt_file,
                                  convert_numeric_id_cols_to_text, validate_unique_ids_and_status_in_updatable_table)
from inventory.varnames import ColNames as C

def _unify_similar_costs(lst):
    result = [lst[0]]
    accepted = [lst[0]]

    for i in range(1, len(lst)):
        for val in accepted:
            if abs(lst[i] - val) <= 1:
                result.append(val)
                break
        else:
            accepted.append(lst[i])
            result.append(lst[i])
    return pd.Series(result)


def get_po_nums(files_save_path) -> str | None:
    if files_save_path:
        po_nums = [s.split('/')[-1].split('_')[-1] for s in files_save_path]
        po_nums_str = "_".join(po_nums)
        return po_nums_str
    return None


def insert_and_delete_status_rows(active_to_inactive: Index,
                                  inactive_to_on_order: Series,
                                  inactive_to_warehouse: Series, log_id: int, purchases: DataFrame,
                                  updated_inv: DataFrame) -> tuple[DataFrame, DataFrame]:
    updated_inv = delete_inactive_rows(active_to_inactive, updated_inv)

    updated_inv = insert_active_rows(inactive_to_on_order, inactive_to_warehouse, log_id, purchases, updated_inv)
    purchases.loc[(inactive_to_warehouse | inactive_to_on_order), C.LOG_ID] = log_id
    return purchases, updated_inv


def insert_active_rows(inactive_to_on_order: Series,
                       inactive_to_warehouse: Series, log_id: int, purchases: DataFrame,
                       updated_inv: DataFrame) -> DataFrame:
    purchases_warehouse = filter_rows_in_warehouse(inactive_to_warehouse, log_id, purchases)

    purchases_on_order = filter_rows_on_order(inactive_to_on_order, log_id, purchases)
    updated_inv_cols = updated_inv.columns
    updated_inv = pd.concat([updated_inv, purchases_warehouse[updated_inv_cols], purchases_on_order[updated_inv_cols]])
    return updated_inv


def delete_inactive_rows(active_to_inactive: Index, updated_inv: DataFrame) -> DataFrame:
    updated_inv = updated_inv[~updated_inv.index.isin(active_to_inactive)]
    return updated_inv


def filter_rows_on_order(inactive_to_on_order: Series, log_id: int, purchases: DataFrame) -> DataFrame:
    purchases_on_order = purchases.loc[inactive_to_on_order].copy()
    purchases_on_order = add_inventory_cols_to_purchases(log_id, purchases_on_order, purchases_on_order[C.RECEIVED])
    return purchases_on_order


def filter_rows_in_warehouse(inactive_to_warehouse: Series, log_id: int,
                             purchases: DataFrame) -> DataFrame:
    purchases_warehouse = purchases.loc[inactive_to_warehouse].copy()
    purchases_warehouse = add_inventory_cols_to_purchases(log_id, purchases_warehouse)
    return purchases_warehouse


def add_inventory_cols_to_purchases(log_id: int, purchases: DataFrame, inventory_qty: int | Series = 0):
    purchases[C.LOG_ID] = log_id
    purchases[C.SIZE] = extract_size_from_style(purchases)
    purchases[C.INVENTORY] = inventory_qty
    return purchases


def update_inventory_from_purchases(common_index: Index, log_id: int, purchases: DataFrame,
                                    updated_inv: DataFrame):
    cols = updated_inv.columns.intersection(purchases.columns).difference([C.LOG_ID])
    update_mask = ~(
        updated_inv.loc[common_index, cols].fillna(-99)
        .eq(purchases.loc[common_index, cols].fillna(-99))
        .fillna(True)  # NaNs considered equal
        .all(axis=1)
    )
    update_index = common_index[update_mask]
    purchases_with_data =  purchases.drop("0", level="MOVEX_PO", errors="ignore")
    updated_inv.update(purchases_with_data[cols])
    updated_inv.loc[update_index, C.LOG_ID] = log_id
    purchases.loc[update_index, C.LOG_ID] = log_id
    return purchases, updated_inv


def get_active_inactive_changes(common_index: Index, purchases: DataFrame, updated_inv: DataFrame) -> (
        tuple[Index, Series, Series]):
    active_to_inactive = get_from_active_to_inactive_index(common_index, purchases, updated_inv)
    inactive_to_warehouse = get_from_inactive_to_warehouse_index(common_index, purchases)
    inactive_to_on_order = get_from_inactive_to_on_order_index(common_index, purchases)
    return active_to_inactive, inactive_to_warehouse, inactive_to_on_order


def get_from_inactive_to_on_order_index(common_index: Index, purchases: DataFrame) -> Series:
    inactive_on_order_to_warehouse = ((~purchases.index.isin(common_index)) & (purchases[C.COST].isna()) &
                                      (purchases[C.WAREHOUSE].isin(["techsmart"])))  # raise_error
    if inactive_on_order_to_warehouse.any():
        st.write("Update these items to on_order")
        st.table(purchases.loc[inactive_on_order_to_warehouse])
        st.stop()
    inactive_on_order_to_on_order = ((~purchases.index.isin(common_index)) & (purchases[C.COST].isna()) &
                                     (purchases[C.WAREHOUSE] == 'on_order'))

    return inactive_on_order_to_on_order


def get_from_inactive_to_warehouse_index(common_index: Index, purchases: DataFrame) -> Series:
    inactive_warehouse_to_warehouse = ((~purchases.index.isin(common_index)) & (~purchases[C.COST].isna()) &
                                       (purchases[C.WAREHOUSE].isin(["techsmart"])))
    inactive_warehouse_to_on_order = ((~purchases.index.isin(common_index)) & (~purchases[C.COST].isna()) &
                                      (purchases[C.WAREHOUSE] == 'on_order'))  # _raise_error
    if inactive_warehouse_to_on_order.any():
        st.write("Update these items to a warehouse")
        st.table(purchases.loc[inactive_warehouse_to_on_order])
        st.stop()
    return inactive_warehouse_to_warehouse


def get_from_active_to_inactive_index(common_index: Index, purchases: DataFrame, updated_inv: DataFrame):
    on_order_to_inactive_index = ((updated_inv.loc[common_index, C.WAREHOUSE] == 'on_order') &
                                  (purchases.loc[common_index, C.WAREHOUSE] == 'inactive'))
    warehouse_to_inactive_index = ((updated_inv.loc[common_index, C.WAREHOUSE].isin(["techsmart"])) &
                                   (updated_inv.loc[common_index, C.INVENTORY] == 0) &
                                   (purchases.loc[common_index, C.WAREHOUSE] == 'inactive'))
    return common_index[on_order_to_inactive_index + warehouse_to_inactive_index]



def update_items_from_on_order_to_warehouse(common_index: Index, config: dict, delivery_date: str, log_id: int,
                                            purchases: DataFrame, sp: SharePointClient, action: str,
                                            updated_inv: DataFrame, logs: DataFrame) -> tuple[DataFrame, str,  list | None]:
    on_order_to_warehouse_index = ((updated_inv.loc[common_index, C.WAREHOUSE] == 'on_order') &
                                   (purchases.loc[common_index, C.WAREHOUSE].isin(['techsmart'])))
    if not on_order_to_warehouse_index.any():
        return purchases, action, None
    receipt = create_goods_receipt_table_and_validate(on_order_to_warehouse_index, purchases, logs)
    receipt, proforma = calculate_costs_and_create_proforma(config, delivery_date, receipt)
    files_save_path = save_goods_receipt_and_techsmart_files(config, delivery_date, log_id, proforma, receipt, sp)
    purchases = add_cost_and_delivery_date_to_purchases(purchases, receipt)
    purchases[C.INVENTORY] = np.nan
    purchases.loc[on_order_to_warehouse_index, C.INVENTORY] = purchases.loc[on_order_to_warehouse_index, C.RECEIVED]
    action = 'receipt'
    return purchases, action, files_save_path


def add_cost_and_delivery_date_to_purchases(purchases: DataFrame, receipt: DataFrame):
    receipt.set_index([C.MOVEX_PO, C.UPC], inplace=True)
    purchases.update(receipt[[C.COST, C.RECEIVED_DATE]])
    return purchases


def save_goods_receipt_and_techsmart_files(config: dict, delivery_date: str, log_id: int, proforma: DataFrame,
                                           receipt: DataFrame, sp: SharePointClient) -> list:
    receipts = {k: v for k, v in receipt.groupby(C.RD)}
    proformas = {k: v for k, v in proforma.groupby(C.RD)}
    files_save_path = []
    for rd in receipt[C.RD].unique():
        file_save_path = f"OC/Recibos/{delivery_date.split('/')[2]}/{delivery_date.split('/')[0]}/{log_id}_{str(rd)}"
        sp.save_excel(receipts[rd], file_save_path + f"/{rd}.xlsx")
        sp.save_excel(proformas[rd], file_save_path + f"/proforma_{rd}.xlsx")
        pre_techsmart = preprocess_receipts_for_techsmart_conversion(receipts[rd])
        create_and_save_techsmart_txt_file(sp, pre_techsmart, 'Oreso', config, rd, file_save_path)
        files_save_path.append(file_save_path)
    return files_save_path


def calculate_costs_and_create_proforma(config: dict, delivery_date: str, receipt: DataFrame) -> tuple[DataFrame, DataFrame]:
    unit_cost = compute_unit_costs(config, receipt)
    receipt[C.COST] = unit_cost
    receipt[C.RECEIVED_DATE] = delivery_date
    proforma = create_proforma_table(config, receipt)
    return receipt, proforma


def create_proforma_table(config: dict, receipt: DataFrame) -> DataFrame:
    proforma = receipt.copy()
    proforma_rename = config.get("proforma_rename")
    proforma = proforma.rename(columns=proforma_rename)[list(proforma_rename.values())]
    proforma['SUBTOTAL'] = proforma['CANTIDAD'] * proforma['PRECIO']
    return proforma


def compute_unit_costs(config: dict, receipt: DataFrame) -> Series:
    brand_net_payments, cost_factor = get_costing_parameters(config)
    margin_from_wholesale_price = receipt[C.BRAND].map(lambda x: brand_net_payments[x] - 1 + cost_factor)
    unit_cost_mx_raw = (receipt[C.WHOLESALE_PRICE] * margin_from_wholesale_price).round(2)
    unit_cost_mx_unif = _unify_similar_costs(unit_cost_mx_raw.tolist())
    random_cost_dct = create_random_shocks_dict(unit_cost_mx_unif)
    unit_cost = (unit_cost_mx_unif * unit_cost_mx_unif.map(random_cost_dct)).round(2)
    return unit_cost


def create_random_shocks_dict(unit_cost_mx_unif: Series) -> dict[Any, Any]:
    cost_keys = list(unit_cost_mx_unif.unique())
    np.random.seed(42)
    random_shocks = np.clip(np.random.normal(loc=1, scale=0.04, size=len(cost_keys)), 0.95, 1.05)
    random_cost_dct = dict(zip(cost_keys, random_shocks))
    return random_cost_dct


def get_costing_parameters(config: dict):
    brand_net_payments = config.get("brand_net_payments")
    cost_factor = config.get("cost_factor")
    return brand_net_payments, cost_factor


def create_goods_receipt_table_and_validate(on_order_to_warehouse_index: Series, purchases: DataFrame, logs: DataFrame) -> DataFrame:
    receipt = purchases.loc[on_order_to_warehouse_index].reset_index()
    warn_processed_orders(logs, receipt, "receipt")
    validate_goods_receipt_table(receipt)
    return receipt


def validate_goods_receipt_table(receipt: DataFrame):
    valid_rd = all(len(rd) >= 4 for rd in receipt[C.RD].unique())
    if not valid_rd:
        st.write("The RD you're trying to update is not valid")
    valid_origin_invoice = receipt[C.INVOICE_NUM].notna().all()
    if not valid_origin_invoice:
        st.write("Fill invoices for the RD you're trying to update")
    if not valid_rd * valid_origin_invoice:
        st.stop()


def find_common_rows_with_inventory(inventory: DataFrame, purchases: DataFrame) -> tuple[DataFrame, Index]:
    updated_inv = inventory.copy().sort_index()
    updated_inv.set_index([C.MOVEX_PO, C.UPC], inplace=True)
    purchases.set_index([C.MOVEX_PO, C.UPC], inplace=True)
    common_index = updated_inv.index.intersection(purchases.index)
    return updated_inv, common_index


def read_files_and_validate_updatable_table(sp: SharePointClient, table: str) -> tuple[DataFrame, DataFrame, dict, str, str]:
    purchases = sp.read_excel(f'COMPRAS/{table}.xlsx')
    config = sp.read_json("config/config.json")
    validate_no_changes_in_id_cols(purchases, sp, table)
    validate_unique_ids_and_status_in_updatable_table(purchases, config)
    inventory = sp.read_csv('INVENTARIO/INVENTARIO.csv')
    for df in [purchases, inventory]:
        convert_numeric_id_cols_to_text(df, [C.WAREHOUSE_CODE, C.UPC, C.SKU, C.MOVEX_PO])
    po_type = action = 'update'
    return purchases, inventory, config, po_type, action


def validate_no_changes_in_id_cols(purchases: DataFrame, sp: SharePointClient, table: str):
    hard_memory = sp.read_csv(f"COMPRAS/BACKUPS/{table}.csv")

    # Validate that protected columns haven't been edited
    protected_cols = [C.MOVEX_PO, C.UPC]

    # Vectorized comparison to find differences
    mask = (purchases[protected_cols] != hard_memory[protected_cols]).any(axis=1)

    if mask.any():
        diff_rows = purchases.index[mask]
        differences = []

        for col in protected_cols:
            col_diff_mask = purchases.loc[diff_rows, col] != hard_memory.loc[diff_rows, col]
            col_diff_idx = diff_rows[col_diff_mask]

            for idx in col_diff_idx:
                differences.append({
                    'STYLE': purchases.loc[idx, C.STYLE],
                    'Column': col,
                    'Correct Value': hard_memory.loc[idx, col],
                    'Edited Value': purchases.loc[idx, col]
                })

        st.error(
            f"Protected columns ({', '.join(protected_cols)}) cannot be edited. Please revert the following changes:")
        st.dataframe(pd.DataFrame(differences), use_container_width=True, hide_index=True,
                     column_config={"Correct Value": st.column_config.NumberColumn(format="%.0f"),
                                    "Edited Value": st.column_config.NumberColumn(format="%.0f"),})
        st.stop()



def preprocess_receipts_for_techsmart_conversion(po):
        po['BOX_STORE_NUM'] = 1
        po[C.DELIVERED] = - po[C.RECEIVED]
        po[C.PO_NUM] = po[C.RD]
        return po

def reset_rows_and_columns_order(inventory, original_column_order):
    updated_inv = inventory.reset_index()[original_column_order]
    # Restore original row order if needed
    updated_inv.sort_values("_row_order", inplace=True)
    updated_inv.drop(columns="_row_order", inplace=True)
    return updated_inv

def update_items_from_purchases_table(table, delivery_date):
    log_id = int(datetime.today().strftime('%Y%m%d%H%M%S'))
    sp = SharePointClient()
    stop_if_locked_files(sp, [f'COMPRAS/{table}.xlsx'])
    logs = sp.read_csv("logs/logs.csv")
    purchases, inventory, config, po_type, action = read_files_and_validate_updatable_table(sp, table)
    purchases_original_column_order = purchases.columns
    record_log(sp, logs, log_id, po_type, action, "started")
    updated_inv, common_index = find_common_rows_with_inventory(inventory, purchases)
    purchases, action, files_save_path = update_items_from_on_order_to_warehouse(common_index, config, delivery_date, log_id,
                                                                         purchases, sp, action, updated_inv, logs)
    active_to_inactive, inactive_to_warehouse, inactive_to_on_order = get_active_inactive_changes(common_index, purchases, updated_inv)
    purchases, updated_inv = update_inventory_from_purchases(common_index, log_id, purchases, updated_inv)
    # updated_inv = reset_rows_and_columns_order(updated_inv, original_column_order)
    purchases, updated_inv = insert_and_delete_status_rows(active_to_inactive, inactive_to_on_order,
                                                           inactive_to_warehouse, log_id, purchases, updated_inv)
    updated_inv = restore_inventory_row_and_columns_order(inventory, updated_inv, active_to_inactive)
    update_inventory_in_memory(sp, updated_inv, inventory, log_id, config)
    save_updated_purchases_table(purchases, purchases_original_column_order, sp, table)
    po_nums_str = get_po_nums(files_save_path)
    if not po_nums_str:
        po_nums_str = table
    record_log(sp, logs, log_id, po_type, action, "success", po_nums_str, files_save_path)
    if not files_save_path:
        files_save_path = "Inventory updated. No new files generated."
    return files_save_path
    # inventory["_row_order"] = range(len(inventory))


def save_updated_purchases_table(purchases: DataFrame, purchases_original_column_order: Index,
                                 sp: SharePointClient, table):
    purchases = purchases.reset_index()[purchases_original_column_order]
    sp.save_excel(purchases, f"COMPRAS/{table}.xlsx")
    sp.save_csv(purchases, f"COMPRAS/BACKUPS/{table}.xlsx")


def restore_inventory_row_and_columns_order(inventory: DataFrame, updated_inv: DataFrame, active_to_inactive: Index) -> DataFrame:
    inv_original_column_order = inventory.columns
    inv_original_index_order = pd.MultiIndex.from_frame(inventory[[C.MOVEX_PO, C.UPC]])
    inv_original_index_order = inv_original_index_order[~inv_original_index_order.isin(active_to_inactive)]
    new_items = updated_inv.index.difference(inv_original_index_order)
    inv_original_index_order = inv_original_index_order.append(new_items)
    updated_inv = updated_inv.loc[inv_original_index_order].reset_index()[inv_original_column_order]
    return updated_inv


if __name__ == '__main__':
    DELIVERY_DATE = "01/21/2026"

    update_items_from_purchases_table('S26', DELIVERY_DATE)