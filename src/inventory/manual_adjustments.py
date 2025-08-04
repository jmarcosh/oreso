
from src.inventory.common_parser import create_and_save_techsmart_txt_file, save_checklist, update_billing_record

from src.inventory.varnames import ColNames as C



def run_manual_adjustments(po, config, customer, delivery_date, po_save_path):
    po['Caja inicial'] = 1
    po_num = po.loc[0, C.PO_NUM]
    techsmart = create_and_save_techsmart_txt_file(po, customer, config, po_num, po_save_path)
    save_checklist(po, None, techsmart, config, po_num, po_save_path)


