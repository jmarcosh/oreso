from itertools import product

import numpy as np
import pandas as pd
import re
import streamlit as st
from inventory.update_inventory_utils import (create_and_save_techsmart_txt_file, save_checklist,
                                              add_nan_cols)
from inventory.varnames import ColNames as C

def find_best_carton_combo(total_volume, max_cartons, capacities, costs):
    best_combo = ()
    best_score = float('inf')

    for combo in product(range(max_cartons + 1), repeat=len(capacities)):
        total_cartons = sum(combo)
        if total_cartons > max_cartons:
            continue  # Skip this combo

        combo_volume = sum(capacities[i] * combo[i] for i in range(len(combo)))

        if (combo_volume >= total_volume) & ((total_cartons > 0) & (total_volume > 0)):
            score = sum(costs[i] * combo[i] for i in range(len(combo)))

            if score < best_score:
                best_score = score
                best_combo = tuple(capacities[i] for i in range(len(combo)) for _ in range(combo[i]))
    return best_combo

def assign_box_number(sp, po, customer, config, log_id):
    cartons = config['cartons']
    names, capacities, costs, dimensions = get_cartons_info(cartons)
    po = assign_box_combos_per_store(po, capacities, costs)

    # Assignments
    stores = po[C.STORE_ID].values
    row_volume = po['ROW_VOLUME'].values
    combo = po['COMBO'].values
    box_assignment = []
    cum_space = []
    store_prev = stores[0]
    c = 0
    # box, end_box = [int(i[len(rfid_prefix):]) for i in rfid_series[rs]]
    rfid_series_df = sp.read_excel(f"config/rfid_{customer.lower()}.xlsx")
    first_col = rfid_series_df.columns[0]
    rfid_series = rfid_series_df[first_col].tolist()
    start_box = rfid_series_df[rfid_series_df[C.LOG_ID].isna()].index[0]
    box = start_box
    for store_s, space_s, combo_s in zip(stores, row_volume, combo):
        cum_space.append(space_s)
        max_vol = combo_s[c] if c < (len(combo_s) - 1) else capacities[0]
        if (sum(cum_space) > max_vol) | ((store_s != store_prev) & (len(combo_s) > 0)):
            box += 1
            c += 1
            cum_space = [space_s]
        if store_s != store_prev:
            c = 0
            store_prev = store_s
        box_assignment.append(rfid_series[box]) if (len(combo_s) > 0) else box_assignment.append(None)
    rfid_series_df.loc[start_box: box, C.LOG_ID] = log_id
    sp.save_excel(rfid_series_df, f"config/rfid_{customer.lower()}.xlsx")
    po = add_box_related_columns(po, box_assignment, names, capacities, dimensions)
    return po


def get_cartons_info(cartons):
    names = [c["name"] for c in cartons]
    capacities = [c["capacity"] for c in cartons]
    costs = [c["cost"] for c in cartons]
    dimensions = [tuple(c["dimensions"]) for c in cartons]
    return names, capacities, costs, dimensions


def add_box_related_columns(po, box_assignment, names, capacities, dimensions):
    po[C.BOX_ID] = box_assignment
    po['BOX_CHANGE'] = (po[C.BOX_ID] != po[C.BOX_ID].shift()).astype(int)
    po['BOX_STORE_NUM'] = po.groupby([C.STORE_ID])['BOX_CHANGE'].cumsum()
    po['BOX_VOLUME'] = po.groupby(C.BOX_ID)['ROW_VOLUME'].transform('sum')
    bins = [x * 1.04 for x in [0] + capacities[::-1]] # account that there's more space in boxes than reported
    bins[-1] *= 2 # avoid errors because last box is too loaded doue to split
    po[C.BOX_TYPE] = pd.cut(po['BOX_VOLUME'], bins=bins, labels=names[::-1], right=True).astype(str)
    name_to_dimensions = {n: d for n, d in zip(names, dimensions)}
    po['BOX_DIMENSION'] = [name_to_dimensions.get(x, (0, 0, 0)) for x in po[C.BOX_TYPE]]
    po[['LENGTH', 'WIDTH', 'HEIGHT']] = pd.DataFrame(po['BOX_DIMENSION'].tolist(), index=po.index)
    return po


def assign_box_combos_per_store(po, capacities, costs):
    po['ROW_VOLUME'] = (capacities[0] / po[C.PCS_BOX]) * po[C.DELIVERED]
    store_volumes = po.groupby([C.STORE_ID])['ROW_VOLUME'].sum().reset_index(name='STORE_VOLUME')
    store_volumes['STORE_VOLUME'] = store_volumes[
                                        "STORE_VOLUME"] * 1.04  # to account that a greedy assignment by row leaves empty space
    store_volumes['MAX_CARTON'] = np.ceil(store_volumes['STORE_VOLUME'] / capacities[2]).astype(int)
    store_volumes['COMBO'] = [find_best_carton_combo(vol, max_crtn, capacities, costs) for
                              vol, max_crtn in zip(store_volumes['STORE_VOLUME'], store_volumes['MAX_CARTON'])]
    po = po.merge(store_volumes[[C.STORE_ID, 'COMBO']], on=[C.STORE_ID], how='left')
    return po

def create_po_summary_by_store(po, config):
    po_gb = po.groupby(config['store_indexes'])[C.DELIVERED].sum().reset_index()
    return po_gb

def upload_po_files_to_sharepoint(sp, po, customer, delivery_date, config, files_save_path):
    po_nums = files_save_path.rsplit('/', 1)[-1].split('_', 1)[-1]
    section = po.loc[0, C.SECTION]
    po_style = create_po_summary_by_style(po, config)
    po_store = create_po_summary_by_store(po, config)
    techsmart = create_and_save_techsmart_txt_file(sp, po, customer, config, po_nums, files_save_path)
    save_checklist(sp, po_style, po_store, techsmart, config, po_nums, files_save_path)
    create_and_save_delivery_note(sp, po_style, customer, delivery_date, config, po_nums, section, files_save_path)
    create_and_save_asn_file(sp, po, config, po_nums, files_save_path)
    return po_style



def create_and_save_delivery_note(sp, po_style, customer, delivery_date, config, po_nums, section, files_save_path):
    po_nums_lst = po_nums.rsplit('_')
    dn_structure = config["dn_structure"]
    customer_map = config["dn_customers"].get(customer, {})
    dn_discounts =config["dn_discounts"].get(customer, {})
    dn_structure.update(customer_map)
    for po_num in po_nums_lst:
        delivery_num = int(dn_structure["NOTA DE REMISION"]) + 1
        for key, value in zip(["NOTA DE REMISION", "Orden de compra:", "Departamento", "Fecha orden de compra:"],
                              [delivery_num, po_num, section, delivery_date]):
            dn_structure[key] = str(value)
        dn_structure_df = [[k, v] for k, v in dn_structure.items()]
        blank_row = pd.DataFrame([[]])
        dn_columns = config["dn_columns"]
        dn = po_style.loc[(po_style[C.PO_NUM] == int(po_num))].groupby(dn_columns[1:5]).agg({
            C.DELIVERED: 'sum', C.CUSTOMER_COST: 'mean'
        }).reset_index()[dn_columns]
        dn['SUBTOTAL'] = dn[C.DELIVERED] * dn[C.CUSTOMER_COST]
        subtotal = dn['SUBTOTAL'].sum()
        discount = subtotal * dn_discounts
        subtotal_2 = subtotal - discount
        vat = subtotal_2 * .16
        total = subtotal_2 + vat
        dn_totals = pd.DataFrame({5: ["Subtotal", f"Descuento {dn_discounts:.2%}", "SubTotal Menos", "IVA", "Total"],
                                  6: [subtotal, discount, subtotal_2, vat, total], })
        delivery_note = pd.concat([pd.DataFrame(dn_structure_df), blank_row, dn.T.reset_index().T, dn_totals],
                                  ignore_index=True)
        dn_file_path = f"{files_save_path}/Nota_Remision_{po_num}_{delivery_num}.xlsx"
        sp.save_delivery_note_excel(delivery_note, dn_file_path)

def create_and_save_asn_file(sp, po, config, po_nums, files_save_path):
    asn_rename = config["asn_rename"]
    asn_columns = config["asn_columns"]
    asn = po.copy()
    asn = asn[asn[C.DELIVERED] > 0].reset_index(drop=True)
    asn = asn.rename(columns=asn_rename)
    asn['CENTRO/ALMACEN DESTINO'] = [f"{c:04}" for c in asn['CENTRO/ALMACEN DESTINO']]
    asn['TRANSPORTE'] = 1
    asn[C.CUSTOMER_UPC] = np.nan
    asn[C.SKU] = asn[C.SKU].astype(int)
    add_nan_cols(asn, asn_columns)
    sp.save_excel(asn[asn_columns], f"{files_save_path}/asn_{po_nums}.xlsx")

def sort_rd(rd):
    match = re.match(r'([a-zA-Z])(\d+)([a-zA-Z]?)', rd)
    prefix = match.group(1)
    number = int(match.group(2))
    suffix = match.group(3) or ''
    return number, suffix, prefix

def create_po_summary_by_style(po, config):
    po_gb = po.groupby(config['po_style_indexes'], dropna=False).agg(config['po_style_values']).reset_index()
    po_gb = po_gb.sort_values(
        by=[C.RD, C.WAREHOUSE_CODE],
        key=lambda col: col.map(sort_rd) if col.name == 'code' else col,
        ignore_index=True
    )
    return po_gb

def assign_store_name(sp, po_df, customer):
    if customer in ['liverpool', 'suburbia']:
        store_mapping = sp.read_excel(f"config/tiendas_{customer.lower()}.xlsx")
        po_df = po_df.merge(store_mapping, on=C.STORE_ID, how='left')
        return po_df[C.STORE_NAME].fillna("NotFound")
    return np.zeros(len(po_df))

def run_process_purchase_orders(sp, po, config, customer, delivery_date, files_save_path, log_id):
    po = po[(~po[C.STYLE].isna())].reset_index(drop=True)
    conflicts = po.loc[(po[C.CUSTOMER_COST] != po[C.WHOLESALE_PRICE]),
        [C.STYLE, C.WHOLESALE_PRICE, C.CUSTOMER_COST]].drop_duplicates() #pc = price_conflict
    if not conflicts.empty:
        st.write(f"""The following styles have price conflicts:""")
        st.table(conflicts)
        st.stop()
    po = assign_box_number(sp, po, customer, config, log_id)
    po[C.STORE_NAME] = assign_store_name(sp, po, customer)
    po_style = upload_po_files_to_sharepoint(sp, po, customer, delivery_date, config, files_save_path)
    sp.save_json(config, "config/config.json")
    return po_style


# new_inventory = update_inventory_in_memory(new_inventory_dfs)
# po_summary = summarize_po_deliveries_by_lot_number(new_inventory)
    # inventory = inventory[inventory[SKU] == 1145889908]
    # po = po[po[SKU] == 1035942579]
    # po = po[po[C.SKU] == 1035942641] # split
