import sys

import pandas as pd
import numpy as np
import re

from api_integrations.read_excel_files import SharePointContext

# INPUTS


costing_structure = {'maiden_top': 1.23,
                     'maiden_bottom': 1.19,
                     'private_label': 1.15,
                     'tahari': 1.22,
                     'no_commission': 1,
                     'commission_15': 1.15}

customer_discounts = {'lvp': 0.88, 'sub': 0.8}

retention_rate = 0.1015


def style_to_style_number(x):
    x = x.split('-')[0]
    d = re.sub(r'^\D*(\d)', r'\1', x)
    if len(d) > 3:
        return str(d)
    return x


def _parse_charges_and_invoice_from_other_charges(charges):
    charges = 0 if charges != charges else charges
    lst = str(charges).split(',')
    for i in range(len(lst)):
        lst[i] = lst[i].split('/') if '/' in lst[i] else ['0', lst[i]]
        lst[i][0] = lst[i][0].split('&')
    return lst


def _compute_unit_other_charges(df, general_weight):
    unit_charges = pd.Series([0] * len(df))
    for lst in OTHER_CHARGES_MX:
        invoice_data_temp = []
        if lst[1][0] != '0':
            for num in lst[0]:
                invoice_data_temp.append(df[df['INVOICE'] == int(num)])
            invoice_data = pd.concat(invoice_data_temp)
            total_invoice = invoice_data['QUANTITY'] @ invoice_data['FOB']
            invoice_weight = invoice_data['FOB'] / total_invoice
            unit_charges_i = pd.concat([df['INVOICE'], invoice_weight * float(lst[1])], axis=1)
            unit_charges += unit_charges_i.iloc[:, 1].fillna(0)
        else:
            unit_charges += general_weight * float(lst[1])
    return unit_charges


s = SharePointContext()
product_data = s.read_excel_file("E8D5FCCC-72C3-4EA3-97EC-C0E5D61D6387", 'Delta')

customs_data = s.read_excel_file("FC11D2D0-5248-4A1A-9EE2-2225C82A6144", dtype={'STYLE': str})
pars = s.read_excel_file("1A9D13A1-4B4B-4618-BA00-205D8A3B4572").set_index('input')

RD = pars.loc['rd'][0]
if RD != product_data.loc[0, 'RD']:
    print('RD in parameters and invoice do not match')
    sys.exit()

output_folder = f'/Imports/Files/{RD[:-1]}/{RD}'

s.write_df_to_excel(product_data, output_folder, f"FOB_{RD}.xlsx")
s.write_df_to_excel(customs_data, output_folder, f"customs_data_{RD}.xlsx")
s.write_df_to_excel(pars.reset_index(), output_folder, f"parameters_{RD}.xlsx")

GOODS_TOTAL_COST_MX = float(pars.loc['goods_total_cost_mx'])
CUSTOMS_RETENTION_MX = float(pars.loc['customs_retention_mx'])
TAXES_MX = float(pars.loc['taxes_mx'])
SEA_FREIGHT_US = float(pars.loc['sea_freight_us'])
LAND_FREIGHT_MX = float(pars.loc['land_freight_mx'])
BROKER_FEE_US = float(pars.loc['broker_fee_us'])
BROKER_XE = float(pars.loc['broker_xe'])
COST_FACTOR = float(pars.loc['cost_factor'])
TOTAL_PAYMENTS_MX = float(pars.loc['total_payments_mx']) - CUSTOMS_RETENTION_MX
OTHER_CHARGES_MX = _parse_charges_and_invoice_from_other_charges(pars.loc['other_charges_mx', 'value'])

delta_factor = [costing_structure[x] for x in product_data['TYPE']]
unit_origin_cost_us = product_data['FOB'] * delta_factor
total_origin_invoice_us = product_data['QUANTITY'] @ unit_origin_cost_us
total_quantity = product_data['QUANTITY'].sum()
goods_xe = GOODS_TOTAL_COST_MX / total_origin_invoice_us
unit_origin_cost_mx = unit_origin_cost_us * goods_xe

unit_weight = unit_origin_cost_us / total_origin_invoice_us
unit_freight_mx = (SEA_FREIGHT_US * BROKER_XE + LAND_FREIGHT_MX) * unit_weight

unit_cost_mx = product_data['WHOLESALE_PRICE'] * COST_FACTOR
transfer_commission_mx = (product_data['QUANTITY'] @ unit_cost_mx) * 0.04
unit_commission_mx = (BROKER_FEE_US * BROKER_XE + transfer_commission_mx) * unit_weight
product_data['STYLE_NUMBER'] = [style_to_style_number(x) for x in product_data['STYLE']]
customs_data['TOTAL_TAX_RATE'] = (customs_data['TAX_RATE'] + 1.008) * 1.16 - 1
customs_table = product_data.merge(customs_data.rename({'FOB': 'FOB_CUSTOMS', 'STYLE': 'STYLE_NUMBER'},
                                                       axis=1), on='STYLE_NUMBER', how='left')
estimated_taxes_mx = ((customs_data['PCS'] * customs_data['FOB']) @ customs_data['TOTAL_TAX_RATE']) * BROKER_XE
tax_correction_factor = TAXES_MX / estimated_taxes_mx
print('tax_correction_factor:', tax_correction_factor)
unit_tax_mx = (customs_table['FOB_CUSTOMS'] * customs_table['PCS_PER_PACK'] *
               customs_table['TOTAL_TAX_RATE'] * tax_correction_factor * BROKER_XE)
estimated_retention_mx = (((customs_data['PCS'] * (customs_data['REFERENCE'] - customs_data['FOB']).clip(lower=0)) @
                           customs_data['TOTAL_TAX_RATE']) * BROKER_XE)
retention_correction_factor = CUSTOMS_RETENTION_MX / estimated_retention_mx if estimated_retention_mx > 0 else 0
unit_retention_mx = ((customs_table['REFERENCE'] - customs_table['FOB_CUSTOMS']).clip(lower=0) * customs_table['PCS_PER_PACK'] *
                     customs_table['TOTAL_TAX_RATE'] * retention_correction_factor * BROKER_XE)

unit_other_charges_mx = _compute_unit_other_charges(product_data, unit_weight)

unit_basic_cost_mx = (unit_origin_cost_mx + unit_other_charges_mx + unit_freight_mx + unit_commission_mx + unit_tax_mx +
                      unit_retention_mx * retention_rate)
net_wholesale_price = product_data['WHOLESALE_PRICE'] * [customer_discounts[x] for x in product_data['CUSTOMER']]
unit_margin = 1 - (unit_basic_cost_mx / 1.16) / net_wholesale_price

basic_cost = pd.DataFrame({'STYLE': product_data['STYLE'], 'QUANTITY': product_data['QUANTITY'],
                           'FOB': product_data['FOB'], 'COST_US': unit_basic_cost_mx / goods_xe,
                           'GOODS_MX': unit_origin_cost_mx, 'FREIGHT_MX': unit_freight_mx,
                           'COMMISSION_MX': unit_commission_mx, 'TAX_MX': unit_tax_mx,
                           'RETENTION_MX': unit_retention_mx * retention_rate, 'OTHER_CHARGES_MX': unit_other_charges_mx,
                           'COST_MX': unit_basic_cost_mx, 'NET_WHOLESALE_PRICE': net_wholesale_price,
                           'MARGIN': unit_margin, 'MARGIN_MX': net_wholesale_price - unit_basic_cost_mx / 1.16})

comparison = TOTAL_PAYMENTS_MX + transfer_commission_mx + CUSTOMS_RETENTION_MX * retention_rate - \
               product_data['QUANTITY'] @ unit_basic_cost_mx


print('Difference between payments and basic cost', comparison)
if abs(comparison) > 3 or np.isnan(comparison):
    comp_goods = GOODS_TOTAL_COST_MX - product_data['QUANTITY'] @ unit_origin_cost_mx
    comp_freight = (SEA_FREIGHT_US * BROKER_XE + LAND_FREIGHT_MX) - product_data['QUANTITY'] @ unit_freight_mx
    comp_comm = (BROKER_FEE_US * BROKER_XE + transfer_commission_mx) - product_data['QUANTITY'] @ unit_commission_mx
    comp_tax = TAXES_MX - product_data['QUANTITY'] @ unit_tax_mx
    comp_ret = CUSTOMS_RETENTION_MX * retention_rate - product_data['QUANTITY'] @ unit_retention_mx * retention_rate
    comp_oc = sum(float(charge[1]) for charge in OTHER_CHARGES_MX) - product_data['QUANTITY'] @ unit_other_charges_mx

    comparisons = {
        "goods": comp_goods,
        "freight": comp_freight,
        "commission": comp_comm,
        "tax": comp_tax,
        "retention": comp_ret,
        "other charges": comp_oc
    }

    for comp_name, comp_value in comparisons.items():
        if abs(comp_value) > 1:
            print(f"Check the {comp_name} values: {comp_value:.2f}")

    sys.exit()

basic_cost_save = basic_cost.copy()

basic_cost['STYLE_NUMBER'] = product_data['STYLE_NUMBER']
basic_cost['IMPORT_FACTOR'] = basic_cost['COST_US'] / unit_origin_cost_us

wm = lambda x: np.average(x, weights=basic_cost.loc[x.index, 'QUANTITY'])

indicator = basic_cost.groupby(['STYLE_NUMBER']).agg(QUANTITY=('QUANTITY', 'sum'),
                                                     GOODS_MX=('GOODS_MX', wm),
                                                     FREIGHT_MX=('FREIGHT_MX', wm),
                                                     COMMISSION_MX=('COMMISSION_MX', wm),
                                                     TAX_MX=('TAX_MX', wm),
                                                     RETENTION_MX=('RETENTION_MX', wm),
                                                     OTHER_CHARGES_MX=('OTHER_CHARGES_MX', wm),
                                                     COST_MX=('COST_MX', wm),
                                                     NET_WHOLESALE_PRICE_MX=('NET_WHOLESALE_PRICE', wm),
                                                     MARGIN=('MARGIN', wm),
                                                     FOB=('FOB', wm),
                                                     IMPORT_FACTOR=('IMPORT_FACTOR', wm)).reset_index()
indicator_weights = indicator['QUANTITY'] / indicator['QUANTITY'].sum()
total_row = ['TOTAL', indicator['QUANTITY'].sum()]
for col in ['GOODS_MX', 'FREIGHT_MX', 'COMMISSION_MX', 'TAX_MX', 'RETENTION_MX', 'OTHER_CHARGES_MX', 'COST_MX',
            'NET_WHOLESALE_PRICE_MX', 'MARGIN', 'FOB', 'IMPORT_FACTOR']:
    total_row.append(indicator[col].dot(indicator_weights))

indicator.loc[len(indicator)] = total_row

if unit_other_charges_mx.sum() == 0:
    basic_cost_save.drop(columns='OTHER_CHARGES_MX', axis=1, inplace=True)
    indicator.drop(columns='OTHER_CHARGES_MX', axis=1, inplace=True)

s.write_df_to_excel(basic_cost_save, output_folder, f"basic_cost_{RD}.xlsx")
s.write_df_to_excel(indicator, output_folder, f"indicator_{RD}.xlsx")

proforma = product_data.iloc[:, :8]
proforma['PRICE'] = unit_cost_mx
proforma['SUBTOTAL'] = proforma['QUANTITY'] * proforma['PRICE']
s.write_df_to_excel(proforma, output_folder, f"proforma_{RD}.xlsx")
