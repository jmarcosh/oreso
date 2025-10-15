import pandas as pd
import streamlit as st
from inventory.varnames import ColNames as C



def assign_warehouse_codes_from_column_and_update_inventory(po, inventory, column, log_id):
    po_has_rd = C.RD in po.columns
    column = [C.RD, column] if po_has_rd else [column]
    split_inventory = [pd.DataFrame(), inventory.copy()] if po_has_rd else split_df_by_column(inventory.copy(), column)
    update_inv_col = [C.RD, C.WAREHOUSE_CODE] if po_has_rd else [C.WAREHOUSE_CODE]
    po_missing = po.loc[(po[C.DELIVERED] == 0)].merge(
            split_inventory[1],
            on=column, how='left')
    validate_all_po_codes_in_inventory(po_missing, column)
    po_original_cols = po.columns
    it = 0
    po_wh = [po_missing]
    updated_inv = [split_inventory[0]]
    for inventory_wh in split_inventory[1:]:
        it += 1
        po, to_deliver = assign_warehouse_codes(po, inventory_wh, column)
        po_wh.append(po.loc[po[C.DELIVERED] != 0].copy())
        update_inventory(inventory_wh, po, update_inv_col, updated_inv, log_id)
        po[C.DELIVERED] = to_deliver
        po = po.loc[po[C.DELIVERED] > 0, po_original_cols]
        if len(po) == 0:
            break
    po = pd.concat(po_wh)
    po = split_ordered_quantity_by_warehouse_codes(po, column)
    updated_inv_lst = updated_inv + split_inventory[it+1:]
    updated_inv = concat_inv_lst(updated_inv_lst)
    return po.sort_values([C.STORE_ID, *column]).reset_index(drop=True), updated_inv


def validate_all_po_codes_in_inventory(po_missing, column):
    code_not_found = po_missing.loc[(po_missing[C.STYLE].isna()), column].drop_duplicates().reset_index(drop=True)
    if not code_not_found.empty:
        st.write(f"""The following codes were not found in inventory:""")
        st.table(code_not_found)
        st.stop()


def concat_inv_lst(dfs):
    df = pd.concat(dfs)
    # df = df.sort_values(
    #     by=['RD', 'WAREHOUSE_CODE'],
    #     key=lambda col: col.map(sort_rd) if col.name == 'code' else col,
    #     ignore_index=True
    # )
    df.sort_index(inplace=True)
    return df


def assign_warehouse_codes(po, inventory_wh, column):
    po = po.merge(
        inventory_wh,
        on=column, how="left")
    delivered_cs = po.groupby(column)[C.DELIVERED].cumsum()
    missing = (po[C.INVENTORY] - delivered_cs).clip(upper=0).fillna(0)
    delivered_cs_p_missing = po[C.DELIVERED] + missing
    delivered_i = delivered_cs_p_missing.clip(lower=0).where(missing < 0, delivered_cs_p_missing)
    to_deliver = po[C.DELIVERED] - delivered_i
    po[C.DELIVERED] = delivered_i
    return po, to_deliver


def split_df_by_column(df, column):
    column0 = df[df[column[0]] == "0"]
    df = df.drop(index=column0.index)
    split_dfs = [column0]
    while not df.empty:
        unique_df = df.drop_duplicates(subset=column, keep='first')
        split_dfs.append(unique_df)
        df = df.drop(index=unique_df.index)
    return split_dfs

def update_inventory(inventory_wh, po, update_inv_col, updated_inv, log_id):
    inventory_wh = inventory_wh.merge(po.groupby(update_inv_col)[C.DELIVERED].sum(), on=update_inv_col, how="left")
    inventory_wh[C.DELIVERED] = inventory_wh[C.DELIVERED].fillna(0)
    inventory_wh.loc[inventory_wh[C.DELIVERED] > 0, C.LOG_ID] = log_id
    inventory_wh[C.INVENTORY] = inventory_wh[C.INVENTORY] - inventory_wh[C.DELIVERED]
    updated_inv.append(inventory_wh.drop(columns=[C.DELIVERED]))

def split_ordered_quantity_by_warehouse_codes(po, column):
    if C.STORE_ID not in po.columns:
        po[C.STORE_ID] = 0
    group_cols = [C.STORE_ID, *column]
    po[C.MISSING] = po[C.ORDERED] - po.groupby(group_cols)[C.DELIVERED].transform("sum")

    group_indices = po.groupby(group_cols).cumcount()
    group_sizes = po.groupby(group_cols).transform('size')

    # Identify the last row in each group by comparing group index to size - 1
    po['_is_last'] = group_indices == (group_sizes - 1)

    # Adjust ORDERED based on position
    po[~po['_is_last']][C.ORDERED] = po.loc[~po['_is_last'], C.DELIVERED].values
    po[po['_is_last']][C.ORDERED] = (
        po.loc[po['_is_last'], C.DELIVERED] + po.loc[po['_is_last'], C.MISSING]
    ).values

    # Clean up
    po = po.drop(columns=[C.MISSING, '_is_last'])
    return po