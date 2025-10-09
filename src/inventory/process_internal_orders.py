
from inventory.update_inventory_utils import create_and_save_techsmart_txt_file, save_checklist



def run_internal_orders(sp, po, config, customer, files_save_path):
    po['Caja inicial'] = 1
    po_num = files_save_path.rsplit('/', 1)[-1]
    techsmart = create_and_save_techsmart_txt_file(sp, po, customer, config, po_num, files_save_path)
    save_checklist(sp, po, None, techsmart, config, po_num, files_save_path)


