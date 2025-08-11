from itertools import product

import numpy as np
import pandas as pd

from inventory.common_parser import (create_and_save_techsmart_txt_file, save_checklist,
                                         add_nan_cols)
from inventory.varnames import ColNames as C
from api_integrations.sharepoint_client import SharePointClient
invoc = SharePointClient()

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

def assign_box_number(po, customer, config, log_id):
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
    rfid_series_df = invoc.read_csv(f"config/rfid_{customer.lower()}.csv")
    first_col = rfid_series_df.columns[0]
    rfid_series = rfid_series_df[rfid_series_df[C.LOG_ID].isna()][first_col].tolist()
    box = 0
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
    rfid_series_df.loc[: box + 1, C.LOG_ID] = log_id
    invoc.save_csv(rfid_series_df, f"config/rfid_{customer.lower()}.csv")
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
    po[C.BOX_TYPE] = pd.cut(po['BOX_VOLUME'], bins=[0] + capacities[::-1], labels=names[::-1], right=True).astype(str)
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

def upload_po_files_to_sharepoint(po, customer, delivery_date, config, files_save_path):
    po_nums = files_save_path.rsplit('/', 1)[-1].split('_', 1)[-1]
    section = po.loc[0, C.SECTION]
    po_style = create_po_summary_by_style(po, config)
    po_store = create_po_summary_by_store(po, config)
    techsmart = create_and_save_techsmart_txt_file(po, customer, config, po_nums, files_save_path)
    save_checklist(po_style, po_store, techsmart, config, po_nums, files_save_path)
    create_and_save_delivery_note(po_style, delivery_date, config, po_nums, section, files_save_path)
    create_and_save_asn_file(po, config, po_nums, files_save_path)
    return po_style



def create_and_save_delivery_note(po_style, delivery_date, config, po_nums, section, files_save_path):
    po_nums_lst = po_nums.rsplit('_')
    for po_num in po_nums_lst:
        dn_structure = config["dn_structure"]
        delivery_num = int(dn_structure[0][1]) + 1
        dn_structure[0][1] = delivery_num
        for line, value in zip([0, 8, 9, 10], [delivery_num, po_num, section, delivery_date]):
            dn_structure[line][1] = str(value)
        blank_row = pd.DataFrame([[]])
        dn_columns = config["dn_columns"]
        dn = po_style.groupby(dn_columns[1:5]).agg({
            C.DELIVERED: 'sum', C.CUSTOMER_COST: 'mean'
        }).reset_index()[dn_columns]
        dn['SUBTOTAL'] = dn[C.DELIVERED] * dn[C.CUSTOMER_COST]
        subtotal = dn['SUBTOTAL'].sum()
        discount = subtotal * .045
        subtotal_2 = subtotal - discount
        vat = subtotal_2 * .016
        total = subtotal_2 + vat
        dn_totals = pd.DataFrame({5: ["Subtotal", "Descuento 4.5%", "SubTotal Menos", "IVA", "Total"],
                                  6: [subtotal, discount, subtotal_2, vat, total], })
        delivery_note = pd.concat([pd.DataFrame(dn_structure), blank_row, dn.T.reset_index().T, dn_totals],
                                  ignore_index=True)
        dn_file_path = f"{files_save_path}/Nota_Remision_{po_num}_{delivery_num}.xlsx"
        invoc.save_delivery_note_excel(delivery_note, dn_file_path)

def create_and_save_asn_file(po, config, po_nums, files_save_path):
    asn_rename = config["asn_rename"]
    asn_columns = config["asn_columns"]
    asn = po.copy()
    asn = asn[asn[C.DELIVERED] > 0].reset_index(drop=True)
    asn = asn.rename(columns=asn_rename)
    asn['CENTRO/ALMACEN DESTINO'] = [f"{c:04}" for c in asn['CENTRO/ALMACEN DESTINO']]
    asn['TRANSPORTE'] = 1
    asn[C.CUSTOMER_UPC] = np.nan
    add_nan_cols(asn, asn_columns)
    invoc.save_excel(asn[asn_columns], f"{files_save_path}/asn_{po_nums}.xlsx")

def create_po_summary_by_style(po, config):
    po_gb = po.groupby(config['po_style_indexes']).agg(config['po_style_values']).reset_index()
    return po_gb

def run_process_purchase_orders(po, config, customer, delivery_date, files_save_path, log_id):
    po = po[(~po[C.STYLE].isna())].reset_index(drop=True)
    conflicts = po.loc[(po[C.CUSTOMER_COST] != po[C.WHOLESALE_PRICE]),
        [C.STYLE, C.WHOLESALE_PRICE, C.CUSTOMER_COST]].drop_duplicates() #pc = price_conflict
    if not conflicts.empty:
        print(f"""The following styles have price conflicts:\n{conflicts}""")
        raise SystemExit("Process stopped due to price conflicts.")
    po = assign_box_number(po, customer, config, log_id)
    po_style = upload_po_files_to_sharepoint(po, customer, delivery_date, config, files_save_path)
    invoc.save_json(config, "config/config.json")
    return po_style


# new_inventory = update_inventory_in_memory(new_inventory_dfs)
# po_summary = summarize_po_deliveries_by_lot_number(new_inventory)
    # inventory = inventory[inventory[SKU] == 1145889908]
    # po = po[po[SKU] == 1035942579]
    # po = po[po[C.SKU] == 1035942641] # split
